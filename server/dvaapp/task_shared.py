import os, json, copy, time, subprocess, logging, shutil, zipfile, uuid
from models import QueryRegion, DVAPQL, Region, Frame, Segment, IndexEntries, TEvent, DeletedVideo, TaskRestart, Export

from django.conf import settings
from PIL import Image
from . import serializers
from dva.in_memory import redis_client
from .fs import ensure, upload_file_to_remote, upload_video_to_remote, get_path_to_file, \
    download_video_from_remote_to_local, upload_file_to_path
from dva.celery import app
from django.apps import apps


def pid_exists(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def restart_task(dt, exception_traceback):
    if dt.operation in settings.RESTARTABLE_TASKS:
        try:
            previous_attempt = TaskRestart.objects.get(launched_event_pk=dt.pk)
        except:
            previous_attempt = None
        if previous_attempt and previous_attempt.attempts > settings.MAX_TASK_ATTEMPTS:
            logging.info("TaskRestart ID : {} Exceeded Max attempts not"
                         " launching new task.".format(previous_attempt.pk))
            return None
        else:
            logging.info("Restarting {}".format(dt.pk))
            new_dt = TEvent.objects.create(parent_process=dt.parent_process,
                                           task_group_id=dt.task_group_id,
                                           parent=dt.parent,
                                           video=dt.video,
                                           arguments=dt.arguments,
                                           queue=dt.queue,
                                           operation=dt.operation)
            new_dt.save()
            if previous_attempt:
                TaskRestart.objects.create(original_event_pk=previous_attempt.original_event_pk,
                                           launched_event_pk=new_dt.pk,
                                           process=dt.parent_process,
                                           exception=exception_traceback,
                                           attempts=previous_attempt.attempts + 1)
            else:
                TaskRestart.objects.create(original_event_pk=dt.pk,
                                           launched_event_pk=new_dt.pk,
                                           exception=exception_traceback,
                                           process=dt.parent_process,
                                           attempts=1)
            app.send_task(name=new_dt.operation, args=[new_dt.pk, ], queue=new_dt.queue)
            dt.delete()
            return new_dt.pk
    else:
        logging.info("Task {} operation {} not restartable".format(dt.pk, dt.operation))
        return None


def collect_garbage(deleted_count):
    if deleted_count is None:
        deleted_count = 0
    new_deleted_count = DeletedVideo.objects.count()
    if new_deleted_count > deleted_count:
        for k in DeletedVideo.objects.all().order_by('-created')[:((new_deleted_count + 1) - deleted_count)]:
            video_dir = '{}/{}'.format(settings.MEDIA_ROOT, k.video_uuid)
            if os.path.isdir(video_dir):
                shutil.rmtree(video_dir)
                logging.info("Deleteing directory {}".format(video_dir))
            else:
                logging.info("Video directory {} was never synced on this host".format(video_dir))
    return new_deleted_count


def launch_worker(queue_name, worker_name):
    p = subprocess.Popen(['./startq.py', '{}'.format(queue_name)], close_fds=True)
    message = "launched {} with pid {} on {}".format(queue_name, p.pid, worker_name)
    return message


def import_path(dv, path, export=False, framelist=False):
    if export:
        dv.create_directory(create_subdirs=False)
        output_filename = "{}/{}/{}.zip".format(settings.MEDIA_ROOT, dv.pk, dv.pk)
    else:
        dv.create_directory(create_subdirs=True)
        extension = path.split('?')[0].split('.')[-1]
        if framelist:
            output_filename = "{}/{}/framelist.{}".format(settings.MEDIA_ROOT, dv.pk, extension)
        else:
            output_filename = "{}/{}/video/{}.{}".format(settings.MEDIA_ROOT, dv.pk, dv.pk, extension)
    get_path_to_file(path, output_filename)


def count_framelist(dv):
    frame_list = dv.get_frame_list()
    return len(frame_list['frames'])


def load_dva_export_file(dv, dt):
    video_id = dv.pk
    if settings.ENABLE_CLOUDFS:
        fname = "/{}/{}.zip".format(video_id, video_id)
        logging.info("Downloading {}".format(fname))
        ensure(fname)
    zipf = zipfile.ZipFile("{}/{}/{}.zip".format(settings.MEDIA_ROOT, video_id, video_id), 'r')
    zipf.extractall("{}/{}/".format(settings.MEDIA_ROOT, video_id))
    zipf.close()
    video_root_dir = "{}/{}/".format(settings.MEDIA_ROOT, video_id)
    for k in os.listdir(video_root_dir):
        unzipped_dir = "{}{}".format(video_root_dir, k)
        if os.path.isdir(unzipped_dir):
            for subdir in os.listdir(unzipped_dir):
                shutil.move("{}/{}".format(unzipped_dir, subdir), "{}".format(video_root_dir))
            shutil.rmtree(unzipped_dir)
            break
    with open("{}/{}/table_data.json".format(settings.MEDIA_ROOT, video_id)) as input_json:
        video_json = json.load(input_json)
    importer = serializers.VideoImporter(video=dv, video_json=video_json, root_dir=video_root_dir, import_event=dt)
    importer.import_video()
    source_zip = "{}/{}.zip".format(video_root_dir, video_id)
    os.remove(source_zip)


def export_video_to_file(video_obj, task_obj):
    export = Export()
    export.event = task_obj
    export.export_type = export.VIDEO_EXPORT
    if settings.ENABLE_CLOUDFS:
        download_video_from_remote_to_local(video_obj)
    video_id = video_obj.pk
    export_uuid = str(uuid.uuid4())
    file_name = '{}.dva_export'.format(export_uuid)
    try:
        os.mkdir("{}/{}".format(settings.MEDIA_ROOT, 'exports'))
    except:
        pass
    shutil.copytree('{}/{}'.format(settings.MEDIA_ROOT, video_id),
                    "{}/exports/{}".format(settings.MEDIA_ROOT, export_uuid))
    a = serializers.VideoExportSerializer(instance=video_obj)
    data = copy.deepcopy(a.data)
    data['version'] = settings.SERIALIZER_VERSION
    with file("{}/exports/{}/table_data.json".format(settings.MEDIA_ROOT, export_uuid), 'w') as output:
        json.dump(data, output)
    zipper = subprocess.Popen(['zip', file_name, '-r', '{}'.format(export_uuid)],
                              cwd='{}/exports/'.format(settings.MEDIA_ROOT))
    zipper.wait()
    shutil.rmtree("{}/exports/{}".format(settings.MEDIA_ROOT, export_uuid))
    local_path = "{}/exports/{}".format(settings.MEDIA_ROOT, file_name)
    path = task_obj.arguments.get('path', None)
    if path:
        if not path.endswith('.dva_export'):
            if path.endswith('.zip'):
                path = path.replace('.zip', '.dva_export')
            else:
                path = '{}.dva_export'.format(path)
        upload_file_to_path(local_path, path, task_obj.arguments.get("public",False))
        os.remove(local_path)
        export.url = path
    else:
        if settings.ENABLE_CLOUDFS:
            upload_file_to_remote("/exports/{}".format(file_name))
        export.url = "{}/exports/{}".format(settings.MEDIA_URL, file_name).replace('//exports', '/exports')
    return export


def export_model_to_file(model_obj, task_obj):
    export = Export()
    export.event = task_obj
    export.export_type = export.MODEL_EXPORT
    if settings.ENABLE_CLOUDFS:
        model_obj.ensure()
    model_id = model_obj.uuid
    export_uuid = str(uuid.uuid4())
    file_name = '{}.dva_model_export'.format(export_uuid)
    try:
        os.mkdir("{}/{}".format(settings.MEDIA_ROOT, 'exports'))
    except:
        pass
    shutil.copytree('{}/models/{}'.format(settings.MEDIA_ROOT, model_id),
                    "{}/exports/{}".format(settings.MEDIA_ROOT, export_uuid))
    a = serializers.TrainedModelExportSerializer(instance=model_obj)
    data = copy.deepcopy(a.data)
    data['version'] = settings.SERIALIZER_VERSION
    with file("{}/exports/{}/model_spec.json".format(settings.MEDIA_ROOT, export_uuid), 'w') as output:
        json.dump(data, output)
    zipper = subprocess.Popen(['zip', file_name, '-r', '{}'.format(export_uuid)],
                              cwd='{}/exports/'.format(settings.MEDIA_ROOT))
    zipper.wait()
    shutil.rmtree("{}/exports/{}".format(settings.MEDIA_ROOT, export_uuid))
    local_path = "{}/exports/{}".format(settings.MEDIA_ROOT, file_name)
    path = task_obj.arguments.get('path', None)
    if path:
        if not path.endswith('dva_model_export'):
            if path.endswith('.zip'):
                path = path.replace('.zip', '.dva_model_export')
            else:
                path = '{}.dva_model_export'.format(path)
        upload_file_to_path(local_path, path, task_obj.arguments.get("public",False))
        os.remove(local_path)
        export.url = path
    else:
        if settings.ENABLE_CLOUDFS:
            upload_file_to_remote("/exports/{}".format(file_name))
        export.url = "{}/exports/{}".format(settings.MEDIA_URL, file_name).replace('//exports', '/exports')
    return export


def build_queryset(args, video_id=None, query_id=None, target=None, filters=None):
    if target is None:
        target = args['target']
    if filters is None:
        kwargs = args.get('filters', {})
    else:
        kwargs = filters
    if video_id:
        kwargs['video_id'] = video_id
    if target == 'frames':
        queryset = Frame.objects.all().filter(**kwargs)
    elif target == 'regions':
        queryset = Region.objects.all().filter(**kwargs)
    elif target == 'query':
        kwargs['pk'] = query_id
        queryset = DVAPQL.objects.all().filter(**kwargs)
    elif target == 'index_entries':
        queryset = IndexEntries.objects.all().filter(**kwargs)
    elif target == 'query_regions':
        queryset = QueryRegion.objects.all().filter(**kwargs)
    elif target == 'segments':
        queryset = Segment.objects.filter(**kwargs)
    else:
        raise ValueError("target {} not found".format(target))
    return queryset, target


def load_frame_list(dv, event, frame_index__gte=0, frame_index__lt=-1):
    """
    Add ability load frames & regions specified in a JSON file and then automatically
    retrieve them in a distributed manner them through CPU workers.
    """
    frame_list = dv.get_frame_list()
    temp_path = "{}.jpg".format(uuid.uuid1()).replace('-', '_')
    video_id = dv.pk
    frames = []
    regions = []
    for i, f in enumerate(frame_list['frames']):
        if i == frame_index__lt:
            break
        elif i >= frame_index__gte:
            try:
                get_path_to_file(f['path'], temp_path)
                im = Image.open(temp_path)
                w, h = im.size
                im.close()
            except:
                logging.exception("Failed to get {}".format(f['path']))
                pass
            else:
                df, drs = serializers.import_frame_json(f, i, event.pk, video_id, w, h)
                regions.extend(drs)
                frames.append(df)
                shutil.move(temp_path, df.path())
    event.finalize({'Region':regions,'Frame':frames})


def download_and_get_query_path(start):
    local_path = "{}/queries/{}_{}.png".format(settings.MEDIA_ROOT, start.pk, start.parent_process.uuid)
    if not os.path.isfile(local_path):
        source_path = "/queries/{}.png".format(start.parent_process.uuid)
        image_data = redis_client.get(source_path)
        if image_data:
            with open(local_path, 'w') as fh:
                fh.write(str(image_data))
        else:
            ensure(source_path, safe=True)
            shutil.copy("{}{}".format(settings.MEDIA_ROOT, source_path), local_path)
    return local_path


def download_and_get_query_region_path(start, regions):
    query_local_path = download_and_get_query_path(start)
    imdata = Image.open(query_local_path)
    rpaths = []
    for r in regions:
        region_path = "{}/queries/region_{}_{}.png".format(settings.MEDIA_ROOT, r.pk, start.parent_process.uuid)
        img2 = imdata.crop((r.x, r.y, r.x + r.w, r.y + r.h))
        img2.save(region_path)
        rpaths.append(region_path)
    return rpaths


def get_query_dimensions(start):
    query_local_path = download_and_get_query_path(start)
    imdata = Image.open(query_local_path)
    width, height = imdata.size
    return width, height


def ensure_files(queryset, target):
    dirnames = {}
    if target == 'frames':
        for k in queryset:
            ensure(k.path(media_root=''), dirnames)
    elif target == 'regions':
        for k in queryset:
            ensure(k.frame_path(media_root=''), dirnames)
    elif target == 'segments':
        for k in queryset:
            ensure(k.path(media_root=''), dirnames)
    elif target == 'indexes':
        for k in queryset:
            ensure(k.npy_path(media_root=''), dirnames)
    else:
        raise NotImplementedError


def import_frame_regions_json(regions_json, video, event):
    """
    Import regions from a JSON with frames identified by immutable identifiers such as filename/path
    :param regions_json:
    :param video:
    :param event_id:
    :return:
    """
    video_id = video.pk
    filename_to_pk = {}
    event_id = event.pk
    if video.dataset:
        # For dataset frames are identified by original_path
        filename_to_pk = {df.original_path(): (df.pk, df.frame_index) for df in Frame.objects.filter(video_id=video_id)}
    regions = []
    not_found = 0
    for k in regions_json:
        if k['target'] == 'filename':
            fname = k['filename']
            if not fname.startswith('/'):
                fname = '/{}'.format(fname)
            if fname in filename_to_pk:
                pk, findx = filename_to_pk[fname]
                regions.append(
                    serializers.import_region_json(k, frame_index=findx, video_id=video_id, event_id=event_id,
                                                   ))
            else:
                not_found += 1
        elif k['target'] == 'index':
            findx = k['frame_index']
            regions.append(serializers.import_region_json(k, frame_index=findx, video_id=video_id, event_id=event_id))
        else:
            raise ValueError('invalid target: {}'.format(k['target']))
    logging.info("{} filenames not found in the dataset".format(not_found))
    event.finalize({"Region":regions})


def generate_tpu_training_set(event):
    """
    Generate training set on GCS for training using Cloud TPUs
    :param event:
    :return:
    """
    pass

