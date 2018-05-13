import os, json, requests, cStringIO, base64, uuid, logging
from copy import deepcopy
import dvaapp.models
from django.conf import settings
from django_celery_results.models import TaskResult
from collections import defaultdict
from dvaapp import processing
from dvaapp import fs
from PIL import Image
from dvaapp.processing import DVAPQLProcess


def create_retriever(name, algorithm, filters, indexer_shasum, approximator_shasum, user=None):
    p = DVAPQLProcess()
    spec = {
        'process_type': dvaapp.models.DVAPQL.PROCESS,
        'create': [
            {
                "MODEL": "Retriever",
                "spec": {
                    "name": name,
                    "algorithm": algorithm,
                    "indexer_shasum": indexer_shasum,
                    "approximator_shasum": approximator_shasum,
                    "source_filters": filters
                }
            }
        ]
    }
    p.create_from_json(spec, user)
    p.launch()
    return p.process.pk


def model_apply(model_pk, video_pks, filters, target, segments_batch_size, frames_batch_size, user=None):
    trained_model = dvaapp.models.TrainedModel.objects.get(pk=model_pk)
    if trained_model.model_type == dvaapp.models.TrainedModel.INDEXER:
        operation = 'perform_indexing'
        args = {"indexer_pk": model_pk, 'filters': filters, 'target': target}
    elif trained_model.model_type == dvaapp.models.TrainedModel.DETECTOR:
        operation = 'perform_detection'
        args = {"detector_pk": model_pk, 'filters': filters, 'target': target}
    elif trained_model.model_type == dvaapp.models.TrainedModel.ANALYZER:
        operation = 'perform_analysis'
        args = {"analyzer_pk": model_pk, 'filters': filters, 'target': target}
    else:
        operation = ""
        args = {}
    p = DVAPQLProcess()
    spec = {
        'process_type': dvaapp.models.DVAPQL.PROCESS,
        'map': []
    }
    for vpk in video_pks:
        dv = dvaapp.models.Video.objects.get(pk=vpk)
        video_specific_args = deepcopy(args)
        if dv.dataset:
            video_specific_args['frames_batch_size'] = frames_batch_size
        else:
            video_specific_args['segments_batch_size'] = segments_batch_size
        spec['map'].append(
            {
                'operation': operation,
                'arguments': video_specific_args,
                'video_id': vpk

            })
    p.create_from_json(spec, user)
    p.launch()
    return p.process.pk


def refresh_task_status():
    for t in dvaapp.models.TEvent.objects.all().filter(started=True, completed=False, errored=False):
        try:
            tr = TaskResult.objects.get(task_id=t.task_id)
        except TaskResult.DoesNotExist:
            pass
        else:
            if tr.status == 'FAILURE':
                t.errored = True
                t.save()


def delete_video_object(video_pk, deleter):
    p = processing.DVAPQLProcess()
    query = {
        'process_type': dvaapp.models.DVAPQL.PROCESS,
        'delete': [
            {
                'MODEL': 'Video',
                'selector': {'pk': video_pk},
            }
        ]
    }
    p.create_from_json(j=query, user=deleter)
    p.launch()


def handle_uploaded_file(f, name, user=None, rate=None):
    if rate is None:
        rate = settings.DEFAULT_RATE
    filename = f.name
    filename = filename.lower()
    vuid = str(uuid.uuid1()).replace('-', '_')
    extension = filename.split('.')[-1]
    if filename.endswith('.dva_export.zip'):
        local_fname = '{}/ingest/{}.dva_export.zip'.format(settings.MEDIA_ROOT, vuid)
        fpath = '/ingest/{}.dva_export.zip'.format(vuid)
        with open(local_fname, 'wb+') as destination:
            for chunk in f.chunks():
                destination.write(chunk)
        if settings.ENABLE_CLOUDFS:
            fs.upload_file_to_remote(fpath)
            os.remove(local_fname)
        p = processing.DVAPQLProcess()
        query = {
            'process_type': dvaapp.models.DVAPQL.PROCESS,
            'create': [
                {'spec': {
                    'name': name,
                    'uploader_id': user.pk if user else None,
                    'created': '__timezone.now__',
                    'url': fpath
                },
                    'MODEL': 'Video',
                },
            ],
            'map': [
                {'arguments': {}, 'video_id': '__created__0', 'operation': 'perform_import', }
            ]
        }
        p.create_from_json(j=query, user=user)
        p.launch()
    elif extension in ['zip', 'gz', 'json', 'mp4']:
        local_fname = '{}/ingest/{}.{}'.format(settings.MEDIA_ROOT, vuid, filename.split('.')[-1])
        fpath = '/ingest/{}.{}'.format(vuid, filename.split('.')[-1])
        with open(local_fname, 'wb+') as destination:
            for chunk in f.chunks():
                destination.write(chunk)
        if settings.ENABLE_CLOUDFS:
            fs.upload_file_to_remote(fpath)
            os.remove(local_fname)
        p = processing.DVAPQLProcess()
        if extension == 'zip':
            query = {
                'process_type': dvaapp.models.DVAPQL.PROCESS,
                'create': [
                    {
                        'spec': {
                            'name': name,
                            'dataset': True,
                            'uploader_id': user.pk if user else None,
                            'url': fpath,
                            'created': '__timezone.now__'},
                        'MODEL': 'Video',
                    },
                ],
                'map': [
                    {'arguments': {
                        'map': [
                            {
                                'arguments': {'map': json.load(
                                    file("../configs/custom_defaults/dataset_processing.json"))},
                                'operation': 'perform_dataset_extraction',
                            }
                        ]
                    },
                        'video_id': '__created__0',
                        'operation': 'perform_import'
                    }
                ]
            }
        elif extension == 'json' or extension == 'gz':
            query = {
                'process_type': dvaapp.models.DVAPQL.PROCESS,
                'create': [
                    {
                        'spec': {
                            'name': name,
                            'dataset': True,
                            'url': fpath,
                            'uploader_id': user.pk if user else None,
                            'created': '__timezone.now__'},
                        'MODEL': 'Video',
                    },
                ],
                'map': [
                    {'arguments': {
                        'map': [
                            {
                                'operation': 'perform_frame_download',
                                'arguments': {
                                    'frames_batch_size': settings.DEFAULT_FRAMES_BATCH_SIZE,
                                    'map': json.load(
                                        file("../configs/custom_defaults/framelist_processing.json"))
                                },
                            }
                        ]
                    },
                        'video_id': '__created__0',
                        'operation': 'perform_import'
                    }
                ]
            }
        else:
            query = {
                'process_type': dvaapp.models.DVAPQL.PROCESS,
                'create': [
                    {'spec': {
                        'name': name,
                        'uploader_id': user.pk if user else None,
                        'url': fpath,
                        'created': '__timezone.now__'
                    },
                        'MODEL': 'Video',
                    },
                ],
                'map': [
                    {'arguments': {
                        'map': [
                            {
                                'arguments': {
                                    'map': [
                                        {'operation': 'perform_video_decode',
                                         'arguments': {
                                             'segments_batch_size': settings.DEFAULT_SEGMENTS_BATCH_SIZE,
                                             'rate': rate,
                                             'map': json.load(
                                                 file("../configs/custom_defaults/video_processing.json"))
                                         }
                                         }
                                    ]},
                                'operation': 'perform_video_segmentation',
                            }
                        ]
                    }, 'video_id': '__created__0',
                        'operation': 'perform_import',
                    }
                ]
            }
        p.create_from_json(j=query, user=user)
        p.launch()
    else:
        raise ValueError("Extension {} not allowed".format(filename.split('.')[-1]))
    return p.created_objects[0]


def create_annotation(form, object_name, labels, frame):
    annotation = dvaapp.models.Region()
    annotation.object_name = object_name
    if form.cleaned_data['high_level']:
        annotation.full_frame = True
        annotation.x = 0
        annotation.y = 0
        annotation.h = 0
        annotation.w = 0
    else:
        annotation.full_frame = False
        annotation.x = form.cleaned_data['x']
        annotation.y = form.cleaned_data['y']
        annotation.h = form.cleaned_data['h']
        annotation.w = form.cleaned_data['w']
    annotation.text = form.cleaned_data['text']
    annotation.metadata = form.cleaned_data['metadata']
    annotation.frame = frame
    annotation.video = frame.video
    annotation.region_type = dvaapp.models.Region.ANNOTATION
    annotation.save()
    for lname in labels:
        if lname.strip():
            dl, _ = dvaapp.models.Label.objects.get_or_create(name=lname, set="UI")
            rl = dvaapp.models.RegionLabel()
            rl.video = annotation.video
            rl.frame = annotation.frame
            rl.region = annotation
            rl.label = dl
            rl.save()


def create_query_from_request(p, request):
    """
    Create JSON object representing the query from request received from Dashboard.
    :param p: Process
    :param request:
    :return:
    """
    query_json = {'process_type': dvaapp.models.DVAPQL.QUERY}
    count = request.POST.get('count')
    generate_tags = request.POST.get('generate_tags')
    selected_indexers = json.loads(request.POST.get('selected_indexers', "[]"))
    selected_detectors = json.loads(request.POST.get('selected_detectors', "[]"))
    query_json['image_data_b64'] = request.POST.get('image_url')[22:]
    query_json['map'] = []
    indexer_tasks = defaultdict(list)
    if generate_tags and generate_tags != 'false':
        query_json['map'].append({'operation': 'perform_analysis',
                                    'arguments': {'analyzer': 'tagger', 'target': 'query', }
                                    })

    if selected_indexers:
        for k in selected_indexers:
            indexer_pk, retriever_pk = k.split('_')
            indexer_tasks[int(indexer_pk)].append(int(retriever_pk))
    for i in indexer_tasks:
        di = dvaapp.models.TrainedModel.objects.get(pk=i, model_type=dvaapp.models.TrainedModel.INDEXER)
        rtasks = []
        for r in indexer_tasks[i]:
            rtasks.append({'operation': 'perform_retrieval', 'arguments': {'count': int(count), 'retriever_pk': r}})
        query_json['map'].append(
            {
                'operation': 'perform_indexing',
                'arguments': {
                    'index': di.name,
                    'target': 'query',
                    'map': rtasks
                }

            }
        )
    if selected_detectors:
        for d in selected_detectors:
            dd = dvaapp.models.TrainedModel.objects.get(pk=int(d), model_type=dvaapp.models.TrainedModel.DETECTOR)
            if dd.name == 'textbox':
                query_json['map'].append({'operation': 'perform_detection',
                                            'arguments': {'detector_pk': int(d),
                                                          'target': 'query',
                                                          'map': [{
                                                              'operation': 'perform_analysis',
                                                              'arguments': {'target': 'query_regions',
                                                                            'analyzer': 'crnn',
                                                                            'filters': {'event_id': '__parent_event__'}
                                                                            }
                                                          }]
                                                          }
                                            })
            elif dd.name == 'face':
                dr = dvaapp.models.Retriever.objects.get(name='facenet', algorithm=dvaapp.models.Retriever.EXACT)
                query_json['map'].append({'operation': 'perform_detection',
                                            'arguments': {'detector_pk': int(d),
                                                          'target': 'query',
                                                          'map': [{
                                                              'operation': 'perform_indexing',
                                                              'arguments': {'target': 'query_regions',
                                                                            'index': 'facenet',
                                                                            'filters': {'event_id': '__parent_event__'},
                                                                            'map': [{
                                                                                'operation': 'perform_retrieval',
                                                                                'arguments': {'retriever_pk': dr.pk,
                                                                                              'filters': {
                                                                                                  'event_id': '__parent_event__'},
                                                                                              'target': 'query_region_index_vectors',
                                                                                              'count': 10}
                                                                            }]}
                                                          }]
                                                          }
                                            })
            else:
                query_json['map'].append({'operation': 'perform_detection',
                                            'arguments': {'detector_pk': int(d), 'target': 'query', }})
    user = request.user if request.user.is_authenticated else None
    p.create_from_json(query_json, user)
    return p.process


def collect(p):
    context = {'results': defaultdict(list), 'regions': []}
    rids_to_names = {}
    for rd in dvaapp.models.QueryRegion.objects.all().filter(query=p.process):
        rd_json = get_query_region_json(rd)
        for r in dvaapp.models.QueryRegionResults.objects.filter(query=p.process, query_region=rd):
            gather_results(r, rids_to_names, rd_json['results'])
        context['regions'].append(rd_json)
    for r in dvaapp.models.QueryResults.objects.all().filter(query=p.process):
        gather_results(r, rids_to_names, context['results'])
    for k, v in context['results'].iteritems():
        if v:
            context['results'][k].sort()
            context['results'][k] = zip(*v)[1]
    for rd in context['regions']:
        for k, v in rd['results'].iteritems():
            if v:
                rd['results'][k].sort()
                rd['results'][k] = zip(*v)[1]
    return context


def gather_results(r, rids_to_names, results):
    name = get_retrieval_event_name(r, rids_to_names)
    results[name].append((r.rank, get_result_json(r)))


def get_url(r):
    if r.detection_id:
        dd = r.detection
        frame_index = r.frame.frame_index
        if dd.materialized:
            return '{}{}/regions/{}.jpg'.format(settings.MEDIA_URL, r.video_id, r.detection_id)
        else:
            if settings.ENABLE_CLOUDFS:
                cached_frame = fs.get_from_cache('/{}/frames/{}.jpg'.format(r.video_id,frame_index))
                if cached_frame:
                    if settings.DEBUG:
                        logging.info("Cache used!")
                    content = cStringIO.StringIO(cached_frame)
                else:
                    if settings.DEBUG:
                        logging.info("Cache NOT used!")
                    frame_url = '{}{}/frames/{}.jpg'.format(settings.MEDIA_URL, r.video_id, frame_index)
                    response = requests.get(frame_url)
                    content = cStringIO.StringIO(response.content)
                img = Image.open(content)
            else:
                img = Image.open('{}/{}/frames/{}.jpg'.format(settings.MEDIA_ROOT, r.video_id, frame_index))
            cropped = img.crop((dd.x, dd.y, dd.x + dd.w, dd.y + dd.h))
            ibuffer = cStringIO.StringIO()
            cropped.save(ibuffer, format="JPEG")
            return "data:image/jpeg;base64, {}".format(base64.b64encode(ibuffer.getvalue()))
    else:
        return '{}{}/frames/{}.jpg'.format(settings.MEDIA_URL, r.video_id, r.frame.frame_index)


def get_sequence_name(i, r):
    return "Indexer {} -> {} {} retriever".format(i.name, r.get_algorithm_display(), r.name)


def get_result_json(r):
    return dict(url=get_url(r), result_type="Region" if r.detection_id else "Frame", rank=r.rank, frame_id=r.frame_id,
                frame_index=r.frame.frame_index, distance=r.distance, video_id=r.video_id, video_name=r.video.name)


def get_query_region_json(rd):
    return dict(object_name=rd.object_name, event_id=rd.event_id, pk=rd.pk, x=rd.x, y=rd.y, w=rd.w,
                confidence=round(rd.confidence, 2), text=rd.text, metadata=rd.metadata,
                region_type=rd.get_region_type_display(), h=rd.h, results=defaultdict(list))


def get_retrieval_event_name(r, rids_to_names):
    if r.retrieval_event_id not in rids_to_names:
        retriever = dvaapp.models.Retriever.objects.get(pk=r.retrieval_event.arguments['retriever_pk'])
        if 'index' in r.retrieval_event.parent.arguments:
            indexer = dvaapp.models.TrainedModel.objects.get(name=r.retrieval_event.parent.arguments['index'],
                                                             model_type=dvaapp.models.TrainedModel.INDEXER)
        else:
            indexer = dvaapp.models.TrainedModel.objects.get(pk=r.retrieval_event.parent.arguments['indexer_pk'])
        rids_to_names[r.retrieval_event_id] = get_sequence_name(indexer, retriever)
    return rids_to_names[r.retrieval_event_id]


def create_approximator_training_set(name, indexer_shasum, video_pks, user=None):
    spec = {
        'process_type': dvaapp.models.DVAPQL.PROCESS,
        'create': [
            {
                "MODEL": "TrainingSet",
                "spec": {
                    "name": name,
                    "training_task_type": dvaapp.models.TrainingSet.LOPQINDEX,
                    "instance_type": dvaapp.models.TrainingSet.INDEX,
                    "source_filters": {
                        "indexer_shasum": indexer_shasum,
                        "video_id__in": video_pks,
                    }
                },
            }
        ],
        "map": [
            {
                "operation": "perform_training_set_creation",
                "arguments": {"training_set_pk": '__created__0'}
            }
        ]

    }
    p = DVAPQLProcess()
    p.create_from_json(spec, user)
    p.launch()


def perform_training(training_set_pk, args, user=None):
    args['selector'] = {"pk": training_set_pk}
    spec = {
        'process_type': dvaapp.models.DVAPQL.PROCESS,
        'map': [

            {
                "operation": "perform_training",
                "arguments": args
            }
        ]
    }
    p = DVAPQLProcess()
    p.create_from_json(spec, user)
    p.launch()
    return p.process.pk
