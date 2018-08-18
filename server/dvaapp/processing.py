import base64, copy, os, json, logging, time
from django.utils import timezone
from django.conf import settings
from dva.celery import app
from dva.in_memory import redis_client

try:
    from dvalib import indexer, clustering, retriever
    import numpy as np
except ImportError:
    np = None
    logging.warning("Could not import indexer / clustering assuming running in front-end mode")
from django.apps import apps
from models import Video, DVAPQL, TEvent, TrainedModel, Retriever, Worker, DeletedVideo, TrainingSet
from celery.result import AsyncResult
import fs

CURRENT_QUEUES = set()
LAST_UPDATED = None


def refresh_queue_names():
    return {w.queue_name for w in Worker.objects.all().filter(alive=True)}


def get_queues():
    global CURRENT_QUEUES
    global LAST_UPDATED
    if LAST_UPDATED is None or (time.time() - LAST_UPDATED) > 120:
        CURRENT_QUEUES = refresh_queue_names()
        LAST_UPDATED = time.time()
    return CURRENT_QUEUES


def get_model_specific_queue_name(operation, args):
    """
    :param operation:
    :param args:
    :return:
    """
    if 'trainedmodel_selector' in args:
        return 'q_model_{}'.format(TrainedModel.objects.filter(**args['trainedmodel_selector']).first().pk)
    elif 'retriever_selector' in args:
        return 'q_retriever_{}'.format(Retriever.objects.filter(**args['retriever_selector']).first().pk)
    else:
        raise NotImplementedError("{}, {}".format(operation, args))


def get_queue_name_and_operation(operation, args):
    global CURRENT_QUEUES
    if 'queue' in args:
        return args['queue'], operation
    elif operation in settings.TASK_NAMES_TO_QUEUE:
        # Here we return directly since queue name is not per model
        return settings.TASK_NAMES_TO_QUEUE[operation], operation
    else:
        queue_name = get_model_specific_queue_name(operation, args)
        if queue_name not in CURRENT_QUEUES:
            CURRENT_QUEUES = refresh_queue_names()
        if queue_name not in CURRENT_QUEUES:
            if queue_name.startswith('q_retriever'):
                # Global retriever queue process all retrieval operations
                # If a worker processing the retriever queue does not exists send it to global
                if settings.GLOBAL_RETRIEVER_QUEUE_ENABLED:
                    return settings.GLOBAL_RETRIEVER, operation
                else:
                    return queue_name, operation
            else:
                # Check if global queue is enabled
                if settings.GLOBAL_MODEL_QUEUE_ENABLED:
                    # send it to a  global queue which loads model at every execution
                    return settings.GLOBAL_MODEL, operation
        return queue_name, operation


def perform_substitution(args, parent_task, inject_filters, map_filters):
    """
    Its important to do a deep copy of args before executing any mutations.
    :param args:
    :param parent_task:
    :return:
    """
    args = copy.deepcopy(args)  # IMPORTANT otherwise the first task to execute on the worker will fill the filters
    inject_filters = copy.deepcopy(
        inject_filters)  # IMPORTANT otherwise the first task to execute on the worker will fill the filters
    map_filters = copy.deepcopy(
        map_filters)  # IMPORTANT otherwise the first task to execute on the worker will fill the filters
    filters = args.get('filters', {})
    parent_args = parent_task.arguments
    if filters == '__parent__':
        parent_filters = parent_args.get('filters', {})
        logging.info('using filters from parent arguments: {}'.format(parent_args))
        args['filters'] = parent_filters
    elif filters:
        for k, v in args.get('filters', {}).items():
            if v == '__parent_event__':
                args['filters'][k] = parent_task.pk
            elif v == '__grand_parent_event__':
                args['filters'][k] = parent_task.parent.pk
    if inject_filters:
        if 'filters' not in args:
            args['filters'] = inject_filters
        else:
            args['filters'].update(inject_filters)
    if map_filters:
        if 'filters' not in args:
            args['filters'] = map_filters
        else:
            args['filters'].update(map_filters)
    return args


def get_map_filters(k, v):
    """
    TO DO add vstart=0,vstop=None
    """
    vstart = 0
    map_filters = []
    if 'segments_batch_size' in k['arguments']:
        step = k['arguments']["segments_batch_size"]
        vstop = v.segments
        for gte, lt in [(start, start + step) for start in range(vstart, vstop, step)]:
            if lt < v.segments:
                map_filters.append({'segment_index__gte': gte, 'segment_index__lt': lt})
            else:  # ensures off by one error does not happens [gte->
                map_filters.append({'segment_index__gte': gte})
    elif 'frames_batch_size' in k['arguments']:
        step = k['arguments']["frames_batch_size"]
        vstop = v.frames
        for gte, lt in [(start, start + step) for start in range(vstart, vstop, step)]:
            if lt < v.frames:  # to avoid off by one error
                map_filters.append({'frame_index__gte': gte, 'frame_index__lt': lt})
            else:
                map_filters.append({'frame_index__gte': gte})
    else:
        map_filters.append({})  # append an empty filter
    # logging.info("Running with map filters {}".format(map_filters))
    return map_filters


def launch_tasks(k, dt, inject_filters, map_filters=None, launch_type=""):
    v = dt.video
    op = k['operation']
    p = dt.parent_process
    if map_filters is None:
        map_filters = [{}, ]
    tids = []
    for f in map_filters:
        args = perform_substitution(k['arguments'], dt, inject_filters, f)
        logging.info("launching {} -> {} with args {} as specified in {}".format(dt.operation, op, args, launch_type))
        q, op = get_queue_name_and_operation(k['operation'], args)
        if op in settings.NON_PROCESSING_TASKS:
            video_per_task = None
        else:
            if "video_selector" in k['arguments']:
                video_per_task = Video.objects.get(**k['arguments']['video_selector'])
            else:
                video_per_task = v
        if op in settings.TRAINING_TASKS:
            if "training_set_id" in k:
                training_set = TrainingSet.objects.get(pk=k['training_set_id'])
            elif "trainingset_selector" in k['arguments']:
                training_set = TrainingSet.objects.get(**k['arguments']['trainingset_selector'])
            else:
                training_set = dt.training_set
        else:
            training_set = None
        if op == 'perform_sync':
            task_group_id = k.get('task_group_id', -1)
        else:
            task_group_id = k['task_group_id']
        next_task = TEvent.objects.create(video=video_per_task, operation=op, arguments=args, parent=dt,
                                          task_group_id=task_group_id, parent_process=p, queue=q,
                                          training_set=training_set)
        tids.append(app.send_task(k['operation'], args=[next_task.pk, ], queue=q).id)
    return tids


def process_next(dt, inject_filters=None, custom_next_tasks=None, sync=True, launch_next=True, map_filters=None):
    if custom_next_tasks is None:
        custom_next_tasks = []
    launched = []
    args = copy.deepcopy(dt.arguments)
    logging.info("next tasks for {}".format(dt.operation))
    next_tasks = args.get('map', []) if args and launch_next else []
    if sync and settings.MEDIA_BUCKET:
        if settings.ENABLE_CLOUDFS:
            dt.upload()
        else:
            launched += launch_tasks(dt, inject_filters, None, 'sync')
    for k in next_tasks + custom_next_tasks:
        if map_filters is None:
            map_filters = get_map_filters(k, dt.video)
        launched += launch_tasks(k, dt, inject_filters, map_filters, 'map')
    for reduce_task in args.get('reduce', []):
        next_task = TEvent.objects.create(video=dt.video, operation="perform_reduce",
                                          arguments=reduce_task['arguments'], parent=dt,
                                          task_group_id=reduce_task['task_group_id'],
                                          parent_process_id=dt.parent_process_id, queue=settings.Q_REDUCER)
        launched.append(app.send_task(next_task.operation, args=[next_task.pk, ], queue=settings.Q_REDUCER).id)
    return launched


class DVAPQLProcess(object):

    def __init__(self, process=None, media_dir=None):
        self.process = process
        self.media_dir = media_dir
        self.task_results = {}
        self.created_objects = []
        self.task_group_index = 0
        self.task_group_name_to_index = {}
        self.parent_task_group_index = {}
        self.root_task = None

    def launch_root_task(self):
        pass

    def create_from_json(self, j, user=None):
        if self.process is None:
            self.process = DVAPQL()
        if not (user is None):
            self.process.user = user
        if j['process_type'] == DVAPQL.QUERY:
            image_data = None
            if j['image_data_b64'].strip():
                image_data = base64.decodestring(j['image_data_b64'])
                j['image_data_b64'] = None
            self.process.process_type = DVAPQL.QUERY
            self.process.script = j
            self.process.save()
            if image_data:
                query_path = "{}/queries/{}.png".format(settings.MEDIA_ROOT, self.process.uuid)
                redis_client.set("/queries/{}.png".format(self.process.uuid), image_data, ex=1200)
                with open(query_path, 'w') as fh:
                    fh.write(image_data)
                if settings.ENABLE_CLOUDFS:
                    query_key = "/queries/{}.png".format(self.process.uuid)
                    fs.upload_file_to_remote(query_key)
                    os.remove(query_path)
        elif j['process_type'] == DVAPQL.PROCESS:
            self.process.process_type = DVAPQL.PROCESS
            self.process.script = j
            self.process.save()
        elif j['process_type'] == DVAPQL.SCHEDULE:
            raise NotImplementedError
        else:
            raise ValueError
        return self.process

    def validate(self):
        pass

    def assign_task_group_id(self, tasks, parent_group_index=None):
        for t in tasks:
            t['task_group_id'] = self.task_group_index
            self.task_group_index += 1
            if parent_group_index:
                self.parent_task_group_index[t['task_group_id']] = parent_group_index
            task_group_name = t['arguments'].get('task_group_name', None)
            if task_group_name:
                if task_group_name in self.task_group_name_to_index:
                    self.process.failed = True
                    self.process.error_message = "Repeated task group name."
                else:
                    self.task_group_name_to_index[task_group_name] = t['task_group_id']
            if 'map' in t.get('arguments', {}):
                self.assign_task_group_id(t['arguments']['map'], t['task_group_id'])
            if 'reduce' in t.get('arguments', {}):
                self.assign_task_group_id(t['arguments']['reduce'], t['task_group_id'])

    def launch(self):
        if self.process.script['process_type'] == DVAPQL.PROCESS:
            self.delete_instances()
            self.create_root_task()
            self.create_instances()
            self.launch_processing_tasks()
            self.launch_process_monitor()
        elif self.process.script['process_type'] == DVAPQL.QUERY:
            self.launch_query_tasks()
        else:
            raise NotImplementedError
        self.process.script['task_group_name_to_index'] = self.task_group_name_to_index
        self.process.script['parent_task_group_index'] = self.parent_task_group_index
        self.process.save()

    def delete_instances(self):
        for d in self.process.script.get('delete', []):
            if d['MODEL'] == 'Video':
                d_copy = copy.deepcopy(d)
                m = apps.get_model(app_label='dvaapp', model_name=d['MODEL'])
                instance = m.objects.get(**d_copy['selector'])
                DeletedVideo.objects.create(deleter=self.process.user, video_uuid=instance.pk)
                instance.delete()
            else:
                self.process.failed = True
                self.process.error_message = "Cannot delete {}; Only video deletion implemented.".format(d['MODEL'])

    def create_instances(self):
        video_id_to_event = {}
        for c in self.process.script.get('create', []):
            c_copy = copy.deepcopy(c)
            m = apps.get_model(app_label='dvaapp', model_name=c['MODEL'])
            for k, v in c['spec'].iteritems():
                if v == '__timezone.now__':
                    c_copy['spec'][k] = timezone.now()
            if c['MODEL'] != 'Video' and c['MODEL'] != 'TrainingSet':
                if 'video_id' in c_copy['spec']:
                    vid = c_copy['spec']['video_id']
                    if vid not in video_id_to_event:
                        # if spec includes video_id then the same video_id must be associated with the event
                        video_id_to_event[vid] = TEvent.objects.create(operation="perform_create",
                                                                       task_group_id=self.task_group_index,
                                                                       completed=False, started=True,
                                                                       parent=self.root_task, video_id=vid,
                                                                       start_ts=timezone.now(),
                                                                       parent_process_id=self.process.pk, queue="sync")
                        self.task_group_index += 1
                    c_copy['spec']['event_id'] = video_id_to_event[vid].pk
                else:
                    c_copy['spec']['event_id'] = self.root_task.pk
            instance = m.objects.create(**c_copy['spec'])
            self.created_objects.append(instance)
        for dt in video_id_to_event.values():
            dt.mark_as_completed()

    def create_root_task(self):
        self.root_task = TEvent.objects.create(operation="perform_launch", task_group_id=self.task_group_index,
                                               completed=True, started=True, start_ts=timezone.now(), duration=0,
                                               parent_process_id=self.process.pk, queue="sync")
        self.task_group_index += 1

    def launch_processing_tasks(self):
        self.assign_task_group_id(self.process.script.get('map', []), 0)
        for t in self.process.script.get('map', []):
            self.launch_task(t)
        self.assign_task_group_id(self.process.script.get('reduce', []), 0)
        for t in self.process.script.get('reduce', []):
            if 'operation' not in t:
                t['operation'] = 'perform_reduce'
                self.launch_task(t)
            else:
                raise ValueError('{} is not a valid reduce operation, reduce tasks should not have an operation'.format(
                    t['operation']))

    def launch_query_tasks(self):
        self.assign_task_group_id(self.process.script.get('map', []))
        for t in self.process.script['map']:
            operation = t['operation']
            arguments = t.get('arguments', {})
            queue_name, operation = get_queue_name_and_operation(operation, arguments)
            next_task = TEvent.objects.create(parent_process=self.process, operation=operation, arguments=arguments,
                                              queue=queue_name, task_group_id=t['task_group_id'])
            self.task_results[next_task.pk] = app.send_task(name=operation, args=[next_task.pk, ], queue=queue_name,
                                                            priority=5)

    def launch_process_monitor(self):
        monitoring_task = TEvent.objects.create(operation="perform_process_monitoring", arguments={}, parent=None,
                                                task_group_id=-1, parent_process=self.process,
                                                queue=settings.Q_REDUCER)
        app.send_task(name=monitoring_task.operation, args=[monitoring_task.pk, ],
                      queue=monitoring_task.queue)

    def wait_query(self, timeout=60):
        if self.process.process_type != DVAPQL.QUERY:
            raise ValueError("wait query is only supported by Query processes")
        for _, result in self.task_results.iteritems():
            try:
                next_task_ids = result.get(timeout=timeout)
                while next_task_ids:
                    if type(next_task_ids) is list:
                        for next_task_id in next_task_ids:
                            next_result = AsyncResult(id=next_task_id)
                            next_task_ids = next_result.get(timeout=timeout)
            except Exception, e:
                raise ValueError(e)

    def get_created_object_pk(self, arg):
        return self.created_objects[int(arg.split('__created__')[-1])].pk

    def launch_task(self, t):
        for k, v in t.get('arguments', {}).iteritems():
            if (type(v) is str or type(v) is unicode) and v.startswith('__created__'):
                t['arguments'][k] = self.get_created_object_pk(v)
        dv = None
        if t['operation'] in settings.NON_PROCESSING_TASKS:
            dv = None
        elif 'video_id' in t:
            if t['video_id'].startswith('__created__'):
                t['video_id'] = self.get_created_object_pk(t['video_id'])
            dv = Video.objects.get(pk=t['video_id'])
        elif 'video_selector' in t['arguments']:
            dv = Video.objects.get(**t['arguments']['video_selector'])
            t['video_id'] = dv.pk
        if dv:
            map_filters = get_map_filters(t, dv)
        else:
            map_filters = [{}]
        # This is useful in case of perform_stream_capture where batch size is used but number of segments is unknown
        if map_filters == []:
            map_filters = [{}]
        for f in map_filters:
            args = copy.deepcopy(t.get('arguments', {}))  # make copy so that spec isnt mutated.
            if f:
                if 'filters' not in args:
                    args['filters'] = f
                else:
                    args['filters'].update(f)
            dt = TEvent()
            dt.parent_process = self.process
            dt.task_group_id = t['task_group_id']
            dt.parent = self.root_task
            if 'video_id' in t:
                dt.video_id = t['video_id']
            if 'training_set_id' in t:
                dt.training_set_id = t['training_set_id']
            elif 'trainingset_selector' in t['arguments']:
                dt.training_set_id = TrainingSet.objects.get(**t['arguments']['trainingset_selector'])
            dt.arguments = args
            dt.queue, op = get_queue_name_and_operation(t['operation'], t.get('arguments', {}))
            dt.operation = op
            dt.save()
            self.task_results[dt.pk] = app.send_task(name=dt.operation, args=[dt.pk, ], queue=dt.queue)
