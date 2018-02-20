from __future__ import absolute_import
import subprocess, os, logging, io, sys, json, tempfile, gzip, copy
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
from .operations.training import train_lopq
from .operations.livestreaming import LivestreamCapture
from .processing import process_next, mark_as_completed
from . import global_model_retriever
from . import task_handlers
from dva.in_memory import redis_client
from django.utils import timezone
from celery.signals import task_prerun, celeryd_init
from . import fs
from . import task_shared
from .waiter import Waiter

try:
    import numpy as np
except ImportError:
    pass

W = None
TASK_ID_TO_OBJECT = {}


@celeryd_init.connect
def configure_workers(sender, conf, **kwargs):
    global W
    W = models.Worker()
    W.pid = os.getpid()
    W.host = sender.split('.')[-1]
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


def get_and_check_task(task_id,skip_started_check=False):
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
    dt = get_and_check_task(task_id,skip_started_check=True)
    if dt is None:
        raise ValueError("task is None")
    timeout_seconds = dt.arguments.get('timeout',settings.DEFAULT_REDUCER_TIMEOUT_SECONDS)
    reduce_waiter = Waiter(dt)
    if reduce_waiter.is_complete():
        next_ids = process_next(dt)
        mark_as_completed(dt)
        return next_ids
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
    mark_as_completed(dt)
    return next_ids


@app.task(track_started=True, name="perform_index_approximation")
def perform_index_approximation(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    sync = task_handlers.handle_perform_index_approximation(dt)
    next_ids = process_next(dt, sync=sync)
    mark_as_completed(dt)
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
    kwargs['materialized'] = False
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
    queryset.update(materialized=True)
    process_next(dt)
    mark_as_completed(dt)


@app.task(track_started=True, name="perform_retrieval")
def perform_retrieval(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    target = args.get('target', 'query')  # by default target is query
    if target == 'query':
        vector = np.load(io.BytesIO(redis_client.get(dt.parent_id)))
        Retrievers.retrieve(dt, args.get('retriever_pk', 20), vector, args.get('count', 20))
    elif target == 'query_region_index_vectors':
        queryset, target = task_shared.build_queryset(args=args)
        for dr in queryset:
            vector = np.load(io.BytesIO(dr.vector))
            Retrievers.retrieve(dt, args.get('retriever_pk', 20), vector, args.get('count', 20),
                                       region=dr.query_region)
    else:
        raise NotImplementedError(target)
    mark_as_completed(dt)
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
    mark_as_completed(dt)
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
    v.segment_video(task_id)
    if args.get('sync', False):
        next_args = {'rescale': args['rescale'], 'rate': args['rate']}
        next_task = models.TEvent.objects.create(video=dv, operation='perform_video_decode', arguments=next_args, parent=dt)
        perform_video_decode(next_task.pk)  # decode it synchronously for testing in Travis
        process_next(dt, sync=True, launch_next=False)
    else:
        process_next(dt)
    mark_as_completed(dt)
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
    for ds in queryset:
        v.decode_segment(ds=ds, denominator=args.get('rate', 30), event_id=task_id)
    process_next(dt)
    mark_as_completed(dt)
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
    mark_as_completed(dt)
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
    mark_as_completed(dt)
    return 0


@app.task(track_started=True, name="perform_export")
def perform_export(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    video_id = dt.video_id
    dv = models.Video.objects.get(pk=video_id)
    if settings.DISABLE_NFS:
        fs.download_video_from_remote_to_local(dv)
    destination = dt.arguments['destination']
    try:
        if destination == "FILE":
            file_name = task_shared.export_file(dv, export_event_pk=dt.pk)
            dt.arguments['file_name'] = file_name
        elif destination == "S3":
            path = dt.arguments['path']
            returncode = task_shared.perform_s3_export(dv, path, export_event_pk=dt.pk)
            if returncode != 0:
                raise ValueError("return code != 0")
    except:
        dt.errored = True
        dt.error_message = "Could not export"
        dt.duration = (timezone.now() - dt.start_ts).total_seconds()
        dt.save()
        exc_info = sys.exc_info()
        raise exc_info[0], exc_info[1], exc_info[2]
    mark_as_completed(dt)


@app.task(track_started=True, name="perform_model_import")
def perform_model_import(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    dm = models.TrainedModel.objects.get(pk=args['pk'])
    dm.download()
    process_next(dt)
    mark_as_completed(dt)


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
    export_file = path.split('?')[0].endswith('.dva_export.zip')
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
        task_shared.import_path(dv, path, export=True)
        task_shared.load_dva_export_file(dv)
    # Download and import .mp4 and .zip files which contain raw video / images.
    elif path.startswith('/') and settings.DISABLE_NFS and not (export_file or framelist_file):
        # TODO handle case when going from s3 ---> gs and gs ---> s3
        fs.copy_remote(dv, path)
    else:
        task_shared.import_path(dv, path)
    dv.save()
    process_next(dt)
    mark_as_completed(dt)


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
    task_shared.import_frame_regions_json(j, dv, task_id)
    dv.save()
    process_next(dt)
    os.remove(temp_filename)
    mark_as_completed(dt)


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
    task_shared.load_frame_list(dv, dt.pk, frame_index__gte=filters['frame_index__gte'],
                                frame_index__lt=filters.get('frame_index__lt', -1))
    process_next(dt)
    mark_as_completed(dt)


@app.task(track_started=True, name="perform_sync")
def perform_sync(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    if settings.MEDIA_BUCKET:
        dirname = args.get('dirname', None)
        task_shared.upload(dirname, dt.parent_id, dt.video_id)
    else:
        logging.info("Media bucket name not specified, nothing was synced.")
        dt.error_message = "Media bucket name is empty".format(settings.MEDIA_BUCKET)
    mark_as_completed(dt)
    return


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
    mark_as_completed(dt)
    return


@app.task(track_started=True, name="perform_stream_capture")
def perform_stream_capture(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    l = LivestreamCapture(dt.video,dt)
    l.start_process()
    l.poll()
    l.finalize()
    mark_as_completed(dt)
    return


@app.task(track_started=True, name="perform_training_set_creation")
def perform_training_set_creation(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    if 'training_set_pk'in args:
        train_set = models.TrainingSet.objects.get(pk=args['training_set_pk'])
    elif 'training_set_selector'in args:
        train_set = models.TrainingSet.objects.get(**args['training_set_selector'])
    else:
        raise ValueError("Could not find training set {}".format(args))
    if train_set.event:
        raise ValueError("Training set has been already built or failed to build, please clone instead of rebuilding.")
    if train_set.training_task_type == models.TrainingSet.LOPQINDEX:
        file_list = []
        filters = copy.deepcopy(train_set.source_filters)
        filters['approximate'] = False
        queryset, target = task_shared.build_queryset(args=args,target="index_entries",filters=filters)
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
        train_set.event = dt
        train_set.save()
    else:
        raise NotImplementedError
    process_next(dt)
    mark_as_completed(dt)
    return 0


@app.task(track_started=True, name="perform_training")
def perform_training(task_id):
    dt = get_and_check_task(task_id)
    if dt is None:
        return 0
    args = dt.arguments
    trainer = args['trainer']
    if trainer == 'LOPQ':
        train_lopq(dt,args)
    elif trainer == 'YOLO':
        train_detector = subprocess.Popen(['fab', 'train_yolo:{}'.format(dt.pk)],
                                          cwd=os.path.join(os.path.abspath(__file__).split('tasks.py')[0], '../'))
        train_detector.wait()
        if train_detector.returncode != 0:
            dt.errored = True
            dt.error_message = "fab train_yolo:{} failed with return code {}".format(dt.pk, train_detector.returncode)
            dt.duration = (timezone.now() - dt.start_ts).total_seconds()
            dt.save()
            raise ValueError(dt.error_message)
    process_next(dt)
    mark_as_completed(dt)
    return 0


@app.task(track_started=True, name="perform_compression")
def perform_compression(task_id):
    """
    TODO Compress a video or a dataset by removing all materialized regions
    and frames/segments (for videos). While retaining metadata and indexes.
    :param task_id:
    :return:
    """
    raise NotImplementedError


@app.task(track_started=True, name="perform_decompression")
def perform_decompression(task_id):
    """
    TODO De-compress a compressed video or a dataset by re-creating all materialized regions
    and frames/segments (for videos). Implementing this tasks correctly requires, exact
    FFmpeg version otherwise the segements might be split at different frames.
    :param task_id:
    :return:
    """
    raise NotImplementedError


@app.task(track_started=True, name="manage_host", bind=True)
def manage_host(self, op, ping_index=None, worker_name=None, queue_name=None):
    """
    Manage host
    This task is handled by workers consuming from a broadcast management queue.
    It  allows quick inspection of GPU memory utilization launch of additional queues.
    Since TensorFlow workers need to be in SOLO concurrency mode, having additional set of workers
    enables easy management without a long timeout.
    Example use
    1. Launch worker to consume from a specific queue
    2. Gather GPU memory utilization info
    """
    host_name = self.request.hostname
    if op == "list":
        models.ManagementAction.objects.create(op=op, parent_task=self.request.id, message="", host=host_name,
                                               ping_index=ping_index)
        for w in models.Worker.objects.filter(host=host_name.split('.')[-1], alive=True):
            # launch all queues EXCEPT worker processing manager queue
            if not task_shared.pid_exists(w.pid):
                w.alive = False
                w.save()
                for t in models.TEvent.objects.filter(started=True, completed=False, errored=False, worker=w):
                    t.errored = True
                    t.save()
                if w.queue_name != 'manager':
                    task_shared.launch_worker(w.queue_name, worker_name)
                    message = "worker processing {} is dead, restarting".format(w.queue_name)
                    models.ManagementAction.objects.create(op='worker_restart', parent_task=self.request.id,
                                                           message=message, host=host_name)
    elif op == "launch":
        if worker_name == host_name:
            message = task_shared.launch_worker(queue_name, worker_name)
            models.ManagementAction.objects.create(op='worker_launch', parent_task=self.request.id,
                                                   message=message, host=host_name)
    elif op == "gpuinfo":
        try:
            message = subprocess.check_output(
                ['nvidia-smi', '--query-gpu=memory.free,memory.total', '--format=csv']).splitlines()[1]
        except:
            message = "No GPU available"
        models.ManagementAction.objects.create(op=op, parent_task=self.request.id, message=message, host=host_name)


@app.task(track_started=True, name="monitor_system")
def monitor_system():
    """
    This task used by scheduler to monitor state of the system.
    :return:
    """
    for p in models.DVAPQL.objects.filter(completed=False):
        if models.TEvent.objects.filter(parent_process=p, completed=False).count() == 0:
            p.completed = True
            p.save()
    last_action = models.ManagementAction.objects.filter(ping_index__isnull=False).last()
    if last_action:
        ping_index = last_action.ping_index + 1
    else:
        ping_index = 0
    # TODO: Handle the case where host manager has not responded to last and itself has died
    _ = app.send_task('manage_host', args=['list', ping_index], exchange='qmanager')
    s = models.SystemState()
    s.processes = models.DVAPQL.objects.count()
    s.completed_processes = models.DVAPQL.objects.filter(completed=True).count()
    s.tasks = models.TEvent.objects.count()
    s.pending_tasks = models.TEvent.objects.filter(started=False).count()
    s.completed_tasks = models.TEvent.objects.filter(started=True, completed=True).count()
    s.save()
