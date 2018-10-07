from __future__ import absolute_import
import subprocess, os, logging, io, sys, json, tempfile, gzip, copy, time
from urlparse import urlparse
from collections import defaultdict
from datetime import datetime, timedelta
from PIL import Image
from django.conf import settings
from dva.celery import app
from . import models
from .operations.retrieval import Retrievers
from .operations.decoding import VideoDecoder
from .operations.dataset import DatasetCreator
from .operations.training import train_lopq, train_faiss
from .operations.livestreaming import LivestreamCapture
from .processing import process_next
from . import global_model_retriever
from . import task_handlers
from dva.in_memory import redis_client
from django.utils import timezone
from celery.signals import task_prerun, celeryd_init
from . import fs
from . import task_shared
from .waiter import Waiter
from django_celery_results.models import TaskResult

try:
    import numpy as np
except ImportError:
    pass

W = None
TASK_ID_TO_OBJECT = {}
DELETED_COUNT = None


@celeryd_init.connect
def configure_workers(sender, conf, **kwargs):
    global W
    W = models.Worker()
    W.pid = os.getpid()
    W.host = sender.split('.')[-1]
    W.last_ping = timezone.now()
    W.queue_name = sender.split('@')[1].split('.')[0]
    W.save()


@task_prerun.connect
def start_task(task_id, task, args, **kwargs):
    global TASK_ID_TO_OBJECT
    global W
    if task.name.startswith('perform'):
        dt = models.TEvent.objects.get(pk=args[0])
        TASK_ID_TO_OBJECT[args[0]] = dt
        dt.task_id = task_id
        if dt.start_ts is None:
            dt.start_ts = timezone.now()
        if W and dt.worker is None:
            dt.worker_id = W.pk
        dt.save()


def get_and_check_task(task_id, skip_started_check=False):
    global TASK_ID_TO_OBJECT
    if task_id in TASK_ID_TO_OBJECT:
        dt = TASK_ID_TO_OBJECT[task_id]
    else:
        logging.warning("Task {} not found in cache querying DB ".format(task_id))
        TASK_ID_TO_OBJECT[task_id] = models.TEvent.objects.get(pk=task_id)
        dt = TASK_ID_TO_OBJECT[task_id]
    if dt.started:
        if skip_started_check:
            return dt
        else:
            return None
    elif dt.queue.startswith(settings.GLOBAL_MODEL) and global_model_retriever.defer(dt):
        logging.info("rerouting...")
        return None
    elif dt.queue.startswith(settings.GLOBAL_RETRIEVER) and global_model_retriever.defer(dt):
        logging.info("rerouting...")
        return None
    else:
        dt.started = True
        dt.save()
        return dt


@app.task(track_started=True, name="perform_reduce")
def perform_reduce(task_id):
    dt = get_and_check_task(task_id, skip_started_check=True)
    if dt is None:
        raise ValueError("task is None")
    timeout_seconds = dt.arguments.get('timeout', settings.DEFAULT_REDUCER_TIMEOUT_SECONDS)
    reduce_waiter = Waiter(dt)
    if reduce_waiter.is_complete():
        next_ids = process_next(dt)
        dt.mark_as_completed()
        return next_ids
    else:
        eta = datetime.utcnow() + timedelta(seconds=timeout_seconds)
        app.send_task(dt.operation, args=[dt.pk, ], queue=dt.queue, eta=eta)


@app.task(track_started=True, name="perform_process_monitoring")
def perform_process_monitoring(task_id):
    dt = get_and_check_task(task_id, skip_started_check=True)
    if dt is None:
        raise ValueError("task is None")
    timeout_seconds = dt.arguments.get('timeout', settings.DEFAULT_REDUCER_TIMEOUT_SECONDS)
    for oldt in models.TEvent.objects.filter(parent_process=dt.parent_process, started=True, completed=False):
        # Check if celery task has failed
        exception_traceback = ""
        try:
            tr = TaskResult.objects.get(task_id=oldt.task_id)
        except TaskResult.DoesNotExist:
            pass
        else:
            if tr.status == 'FAILURE':
                exception_traceback = tr.traceback
                oldt.errored = True
                oldt.save()
        # Check if worker processing the task has failed
        if oldt.worker and oldt.worker.alive == False and oldt.errored == False:
            oldt.error_message = "Worker {} processing task is no longer alive.".format(oldt.worker_id)
            exception_traceback = "Worker {} processing task is no longer alive.".format(oldt.worker_id)
            oldt.errored = True
            oldt.save()
        # If failed attempt to restart it.
        if oldt.errored:
            task_shared.restart_task(oldt, exception_traceback)
    # Following is "1" instead of "0" since the current task is marked as pending.
    if models.TEvent.objects.filter(parent_process=dt.parent_process, completed=False).count() == 1:
        dt.parent_process.completed = True
        dt.parent_process.save()
        dt.mark_as_completed()
    else:
        eta = datetime.utcnow() + timedelta(seconds=timeout_seconds)
        app.send_task(dt.operation, args=[dt.pk, ], queue=dt.queue, eta=eta)


@app.task(track_started=True, name="perform_indexing")
def perform_indexing(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    sync = task_handlers.handle_perform_indexing(dt)
    next_ids = process_next(dt, sync=sync)
    dt.mark_as_completed()
    return next_ids


@app.task(track_started=True, name="perform_index_approximation")
def perform_index_approximation(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    sync = task_handlers.handle_perform_index_approximation(dt)
    next_ids = process_next(dt, sync=sync)
    dt.mark_as_completed()
    return next_ids


@app.task(track_started=True, name="perform_transformation")
def perform_transformation(task_id):
    """
    Crop detected or annotated regions
    :param task_id:
    :return:
    """
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    resize = args.get('resize', None)
    kwargs = args.get('filters', {})
    paths_to_regions = defaultdict(list)
    kwargs['video_id'] = dt.video_id
    logging.info("executing crop with kwargs {}".format(kwargs))
    queryset = models.Region.objects.all().filter(**kwargs)
    for dr in queryset:
        paths_to_regions[dr.frame_path()].append(dr)
    for path, regions in paths_to_regions.iteritems():
        img = Image.open(path)
        for dr in regions:
            cropped = img.crop((dr.x, dr.y, dr.x + dr.w, dr.y + dr.h))
            if resize:
                resized = cropped.resize(tuple(resize), Image.BICUBIC)
                resized.save(dr.path())
            else:
                cropped.save(dr.path())
    process_next(dt)
    dt.mark_as_completed()


@app.task(track_started=True, name="perform_retrieval")
def perform_retrieval(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    target = args.get('target', 'query')  # by default target is query
    index_retriever, dr = Retrievers.get_retriever(args)
    if target == 'query':
        vector = np.load(io.BytesIO(redis_client.get("query_vector_{}".format(dt.parent_id))))
        Retrievers.retrieve(dt, index_retriever, dr, vector, args.get('count', 20))
    elif target == 'query_region_index_vectors':
        qr_pk_vector = redis_client.hgetall("query_region_vectors_{}".format(dt.parent_id))
        for query_region_pk, vector in qr_pk_vector.items():
            vector = np.load(io.BytesIO(vector))
            Retrievers.retrieve(dt, index_retriever, dr, vector, args.get('count', 20), region_pk=query_region_pk)
    else:
        raise NotImplementedError(target)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="perform_matching")
def perform_matching(task_id):
    """
    Generates relations (within selected video/dataset) or hyper-relations (matching Indexed entries in
    selected video/dataset with external video/dataset) by performing K-NN matching using a selected
    indexer or approximator.
    :param task_id:
    :return:
    """
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    task_handlers.handle_perform_matching(dt)
    process_next(dt)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="perform_dataset_extraction")
def perform_dataset_extraction(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    if args == {}:
        args['rescale'] = 0
        args['rate'] = 30
        dt.arguments = args
    dt.save()
    video_id = dt.video_id
    dv = models.Video.objects.get(id=video_id)
    task_shared.ensure('/{}/video/{}.zip'.format(video_id, video_id))
    dv.create_directory(create_subdirs=True)
    v = DatasetCreator(dvideo=dv, media_dir=settings.MEDIA_ROOT)
    v.extract(dt)
    process_next(dt)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="perform_video_segmentation")
def perform_video_segmentation(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    if 'rescale' not in args:
        args['rescale'] = 0
    if 'rate' not in args:
        args['rate'] = 30
    dt.arguments = args
    video_id = dt.video_id
    dv = models.Video.objects.get(id=video_id)
    task_shared.ensure(dv.path(media_root=''))
    dv.create_directory(create_subdirs=True)
    v = VideoDecoder(dvideo=dv, media_dir=settings.MEDIA_ROOT)
    v.get_metadata()
    segments_batch = v.segment_video(task_id)
    dt.finalize({"Segment":segments_batch})
    if args.get('sync', False):
        next_args = {'rescale': args['rescale'], 'rate': args['rate']}
        next_task = models.TEvent.objects.create(video=dv, operation='perform_video_decode', arguments=next_args,
                                                 parent=dt)
        perform_video_decode(next_task.pk)  # decode it synchronously for testing in Travis
        process_next(dt, sync=True, launch_next=False)
    else:
        process_next(dt)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="perform_video_decode", ignore_result=False)
def perform_video_decode(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    video_id = dt.video_id
    dv = models.Video.objects.get(id=video_id)
    dv.create_directory()
    kwargs = args.get('filters', {})
    kwargs['video_id'] = video_id
    v = VideoDecoder(dvideo=dv, media_dir=settings.MEDIA_ROOT)
    if 'target' not in args:
        args['target'] = 'segments'
    queryset, target = task_shared.build_queryset(args, video_id, dt.parent_process_id)
    if target != 'segments':
        raise NotImplementedError("Cannot decode target:{}".format(target))
    task_shared.ensure_files(queryset, target)
    frame_batch = []
    for ds in queryset:
        frame_batch += v.decode_segment(ds,dt.pk,denominator=args.get('rate', 30))
    dt.finalize({"Frame":frame_batch})
    process_next(dt)
    dt.mark_as_completed()
    return task_id


@app.task(track_started=True, name="perform_detection")
def perform_detection(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    query_flow = ('target' in dt.arguments and dt.arguments['target'] == 'query')
    if dt.queue.startswith(settings.GLOBAL_MODEL):
        logging.info("Running in new process")
        global_model_retriever.run_task_in_model_specific_flask_server(dt)
    else:
        task_handlers.handle_perform_detection(dt)
    launched = process_next(dt)
    dt.mark_as_completed()
    if query_flow:
        return launched
    else:
        return 0


@app.task(track_started=True, name="perform_analysis")
def perform_analysis(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    task_handlers.handle_perform_analysis(dt)
    process_next(dt)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="perform_export")
def perform_export(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    exports = []
    try:
        if 'video_selector' in args:
            dv = models.Video.objects.get(**args['video_selector'])
            exports.append(task_shared.export_video_to_file(dv,dt))
        elif 'trainedmodel_selector' in args:
            dm = models.TrainedModel.objects.get(**args['trainedmodel_selector'])
            exports.append(task_shared.export_model_to_file(dm, dt))
        elif 'trainingset_selector' in args:
            raise NotImplementedError
        else:
            raise ValueError("one of ('video_selector','trainedmodel_selector','trainingset_selector') not "
                             "found in {}".format(args))
    except:
        dt.errored = True
        dt.error_message = "Could not export"
        dt.save()
        exc_info = sys.exc_info()
        raise exc_info[0], exc_info[1], exc_info[2]
    dt.finalize({"Export":exports})
    dt.mark_as_completed()


@app.task(track_started=True, name="perform_model_import")
def perform_model_import(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    dm = models.TrainedModel.objects.get(pk=args['pk'])
    dm.download()
    process_next(dt)
    dt.mark_as_completed()


@app.task(track_started=True, name="perform_import")
def perform_import(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    dv = dt.video
    path = dt.video.url
    youtube_dl_download = False
    if path.startswith('http'):
        u = urlparse(path)
        if u.hostname == 'www.youtube.com' or dt.arguments.get('force_youtube_dl', False):
            youtube_dl_download = True
    export_file = path.split('?')[0].endswith('.dva_export')
    framelist_file = path.split('?')[0].endswith('.json') or path.split('?')[0].endswith('.gz')
    dv.uploaded = True
    # Donwload videos via youtube-dl
    if youtube_dl_download:
        fs.retrieve_video_via_url(dv, path)
    # Download list frames in JSON format
    elif framelist_file:
        task_shared.import_path(dv, path, framelist=True)
        dv.metadata = path
        dv.frames = task_shared.count_framelist(dv)
        dv.uploaded = False
    # Download and import previously exported file from DVA
    elif export_file:
        logging.info("importing exported file {}".format(path))
        task_shared.import_path(dv, path, export=True)
        logging.info("loading exported file {}".format(path))
        task_shared.load_dva_export_file(dv,dt)
    # Download and import .mp4 and .zip files which contain raw video / images.
    elif path.startswith('/') and settings.ENABLE_CLOUDFS and not (export_file or framelist_file):
        # TODO handle case when going from s3 ---> gs and gs ---> s3
        fs.copy_remote(dv, path)
    else:
        task_shared.import_path(dv, path)
    dv.save()
    process_next(dt)
    dt.mark_as_completed()


@app.task(track_started=True, name="perform_region_import")
def perform_region_import(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    path = dt.arguments.get('path', None)
    dv = dt.video
    tempdirname = tempfile.mkdtemp()
    temp_filename = ""
    try:
        if path.endswith('.json'):
            temp_filename = "{}/temp.json".format(tempdirname)
            fs.get_path_to_file(path, temp_filename)
            j = json.load(file(temp_filename))
        else:
            temp_filename = "{}/temp.gz".format(tempdirname)
            fs.get_path_to_file(path, temp_filename)
            j = json.load(gzip.GzipFile(temp_filename))
    except:
        raise ValueError("{}".format(temp_filename))
    task_shared.import_frame_regions_json(j, dv, dt)
    dv.save()
    process_next(dt)
    os.remove(temp_filename)
    dt.mark_as_completed()


@app.task(track_started=True, name="perform_frame_download")
def perform_frame_download(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    dv = dt.video
    if dv.metadata.endswith('.gz'):
        fs.ensure('/{}/framelist.gz'.format(dv.pk), safe=True, event_id=task_id)
    else:
        fs.ensure('/{}/framelist.json'.format(dv.pk), safe=True, event_id=task_id)
    filters = dt.arguments['filters']
    dv.create_directory(create_subdirs=True)
    task_shared.load_frame_list(dv, dt, frame_index__gte=filters['frame_index__gte'],
                                frame_index__lt=filters.get('frame_index__lt', -1))
    process_next(dt)
    dt.mark_as_completed()


@app.task(track_started=True, name="perform_sync")
def perform_sync(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    dt.parent.upload()
    dt.mark_as_completed()


@app.task(track_started=True, name="perform_deletion")
def perform_deletion(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    video_pk = int(args['video_pk'])
    deleter_pk = args.get('deleter_pk', None)
    video = models.Video.objects.get(pk=video_pk)
    deleted = models.DeletedVideo()
    deleted.name = video.name
    deleted.deleter_id = deleter_pk
    deleted.uploader = video.uploader
    deleted.url = video.url
    deleted.description = video.description
    deleted.original_pk = video_pk
    deleted.save()
    video.delete()
    src = '{}/{}/'.format(settings.MEDIA_ROOT, int(video_pk))
    args = ['rm', '-rf', src]
    command = " ".join(args)
    deleter = subprocess.Popen(args)
    deleter.wait()
    if deleter.returncode != 0:
        dt.errored = True
        dt.error_message = "Error while executing : {}".format(command)
        dt.save()
        return
    if settings.MEDIA_BUCKET:
        dest = 's3://{}/{}/'.format(settings.MEDIA_BUCKET, int(video_pk))
        args = ['aws', 's3', 'rm', '--quiet', '--recursive', dest]
        command = " ".join(args)
        syncer = subprocess.Popen(args)
        syncer.wait()
        if syncer.returncode != 0:
            dt.errored = True
            dt.error_message = "Error while executing : {}".format(command)
            dt.save()
            return
    else:
        logging.info("Media bucket name not specified, nothing was synced.")
        dt.error_message = "Media bucket name is empty".format(settings.MEDIA_BUCKET)
    dt.mark_as_completed()
    return


@app.task(track_started=True, name="perform_stream_capture")
def perform_stream_capture(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    l = LivestreamCapture(dt.video, dt)
    l.start_process()
    l.poll()
    l.finalize()
    dt.mark_as_completed()
    return


@app.task(track_started=True, name="perform_training_set_creation")
def perform_training_set_creation(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    if 'training_set_pk' in args:
        train_set = models.TrainingSet.objects.get(pk=args['training_set_pk'])
    elif 'trainingset_selector' in args:
        train_set = models.TrainingSet.objects.get(**args['trainingset_selector'])
    else:
        raise ValueError("Could not find training set {}".format(args))
    if train_set.built:
        raise ValueError("Training set has been already built or failed to build, please clone instead of rebuilding.")
    if train_set.training_task_type == models.TrainingSet.TRAINAPPROX:
        file_list = []
        filters = copy.deepcopy(train_set.source_filters)
        filters['approximate'] = False
        queryset, target = task_shared.build_queryset(args=args, target="index_entries", filters=filters)
        total_count = 0
        for di in queryset:
            file_list.append({
                "path": di.npy_path(""),
                "count": di.count,
                "pk": di.pk
            })
            total_count += di.count
        train_set.built = True
        train_set.count = total_count
        train_set.files = file_list
        train_set.save()
    else:
        raise NotImplementedError
    process_next(dt)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="perform_training")
def perform_training(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    trainer = args['trainer']
    if trainer == 'LOPQ':
        train_lopq(dt, args)
    elif trainer == 'FAISS':
        train_faiss(dt, args)
    else:
        raise ValueError("Unknown trainer {}".format(trainer))
    process_next(dt)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="perform_test")
def perform_test(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    try:
        current_attempt = models.TaskRestart.objects.get(launched_event_pk=task_id).attempts
    except:
        current_attempt = 0
    if 'sleep_seconds' in args:
        time.sleep(args['sleep_seconds'])
    if 'kill' in args:
        os.kill(os.getpid(), 9)
    if 'throw_error_until' in args:
        throw_error_until = int(args['throw_error_until'])
        if current_attempt < throw_error_until:
            raise ValueError("Throwing error until attempt {}, current attempt {} ".format(throw_error_until,
                                                                                           current_attempt))
    process_next(dt)
    dt.mark_as_completed()
    return 0


@app.task(track_started=True, name="manage_host", bind=True)
def manage_host(self, op, ping_index=None, worker_name=None):
    """
    - Manage host by deleting folders associated with deleted videos.
    - Marking dead workers as failed.
    - For Kubernetes shutting down / exiting and in Compose mode by restarting the dead worker.
    """
    global DELETED_COUNT
    host_name = self.request.hostname
    DELETED_COUNT = task_shared.collect_garbage(DELETED_COUNT)
    models.ManagementAction.objects.create(op=op, parent_task=self.request.id, message="", host=host_name,
                                           ping_index=ping_index)
    for w in models.Worker.objects.filter(host=host_name.split('.')[-1], alive=True, shutdown=False):
        if not task_shared.pid_exists(w.pid):
            w.alive = False
            w.save()
            # launch all queues EXCEPT worker processing manager queue
            if w.queue_name != 'manager':
                if settings.KUBE_MODE:
                    models.ManagementAction.objects.create(op=op, parent_task=self.request.id,
                                                           message="Worker died manager exiting.",
                                                           host=host_name)
                    wm = models.Worker.objects.filter(host=host_name.split('.')[-1], alive=True,
                                                      queue_name="manager")[0]
                    wm.alive = False
                    wm.save()
                    sys.exit()
                else:
                    task_shared.launch_worker(w.queue_name, worker_name)
                    message = "worker processing {} is dead, restarting".format(w.queue_name)
                    models.ManagementAction.objects.create(op='worker_restart', parent_task=self.request.id,
                                                           message=message, host=host_name)
        else:
            w.last_ping = timezone.now()
            w.save()


@app.task(track_started=True, name="monitor_system")
def monitor_system():
    """
    This task used by scheduler to monitor state of the system.
    :return:
    """
    last_action = models.ManagementAction.objects.filter(ping_index__isnull=False).last()
    if last_action:
        ping_index = last_action.ping_index + 1
    else:
        ping_index = 0
    # TODO: Handle the case where host manager has not responded to last and itself has died
    _ = app.send_task('manage_host', args=['list', ping_index], exchange='qmanager')
    worker_stats = {'alive':0,
                    'transition':0,
                    'dead': models.Worker.objects.filter(alive=False).count()
                    }
    for w in models.Worker.objects.filter(alive=True):
        # if worker is not heard from via manager for more than 10 minutes
        # mark it as dead, so that processes_monitor can mark tasks are errored and restart if possible.
        if (timezone.now() - w.last_ping).total_seconds() > 600:
            w.alive = False
            w.save()
            worker_stats['transition'] += 1
        else:
            worker_stats['alive'] += 1
    process_stats = {'processes': models.DVAPQL.objects.count(),
                     'completed_processes': models.DVAPQL.objects.filter(completed=True).count(),
                     'tasks': models.TEvent.objects.count(),
                     'pending_tasks': models.TEvent.objects.filter(started=False).count(),
                     'completed_tasks': models.TEvent.objects.filter(started=True, completed=True).count()}
    retriever_state = redis_client.hgetall("retriever_state")
    if retriever_state:
        retriever_stats = {k: json.loads(v) for k, v in retriever_state.items()}
    else:
        retriever_stats = {}
    _ = models.SystemState.objects.create(redis_stats=redis_client.info(),
                                          process_stats=process_stats,
                                          retriever_stats=retriever_stats,
                                          worker_stats=worker_stats)


@app.task(track_started=True, name="monitor_retrievers")
def monitor_retrievers():
    """
    This task used by scheduler to refresh retrievers every minute. This can modified to optionally not launch
    refresh tasks. e.g. if no new IndexEntry has been created.
    :return:
    """
    _ = app.send_task('refresh_retriever', args=[], exchange=settings.Q_REFRESHER)


@app.task(track_started=True, name="refresh_retriever")
def refresh_retriever():
    global W
    if W.queue_name == settings.GLOBAL_RETRIEVER:
        for dr in Retrievers._selector_to_dr.values():
            logging.info("Starting index refresh on queue {} for retriever {}".format(W.queue_name,dr.pk))
            start_ts = time.time()
            index_entries_count,vectors_count = Retrievers.refresh_index(dr)
            entry = {
                'index_entries_count':index_entries_count,
                'vectors_count':vectors_count,
                "delta":time.time() - start_ts,
                'worker_id':W.pk,
                'retriever_id':dr.pk,
                'queue_name':W.queue_name,
                'ts':time.time()
            }
            redis_client.hset("retriever_state", "{},{},{}".format(W.pk, W.queue_name, dr.pk),json.dumps(entry))
            logging.info("Finished index refresh on queue {} for retriever {}".format(W.queue_name, dr.pk))
    elif 'retriever_' in W.queue_name:
        pk = int(W.queue_name.split('_')[-1])
        logging.info("Starting index refresh on queue {} for retriever {}".format(W.queue_name, pk))
        start_ts = time.time()
        _, dr = Retrievers.get_retriever(args={'retriever_selector': {'pk': pk}})
        index_entries_count, vectors_count = Retrievers.refresh_index(dr)
        entry = {
            'index_entries_count': index_entries_count,
            'vectors_count': vectors_count,
            "delta": time.time() - start_ts,
            'worker_id': W.pk,
            'retriever_id': dr.pk,
            'queue_name': W.queue_name,
            'ts': time.time()
        }
        redis_client.hset("retriever_state", "{},{},{}".format(W.pk, W.queue_name, dr.pk), json.dumps(entry))
        logging.info("Finished index refresh on queue {} for retriever {}".format(W.queue_name, pk))
    else:
        raise ValueError("{} is not valid for retriever".format(W.queue_name))