from django.conf import settings
from .operations import indexing, detection, analysis, approximation, retrieval
import io
import logging
import tempfile
from . import task_shared
from . import models
from dva.in_memory import redis_client

try:
    import numpy as np
except ImportError:
    pass


def handle_perform_indexing(start):
    json_args = start.arguments
    target = json_args.get('target', 'frames')
    visual_index, di = indexing.Indexers.get_trained_model(json_args)
    sync = True
    if target == 'query':
        local_path = task_shared.download_and_get_query_path(start)
        vector = visual_index.apply(local_path)
        # TODO: figure out a better way to store numpy arrays.
        s = io.BytesIO()
        np.save(s, vector)
        redis_client.set("query_vector_{}".format(start.pk), s.getvalue())
        sync = False
    elif target == 'query_regions':
        queryset, target = task_shared.build_queryset(args=start.arguments)
        region_paths = task_shared.download_and_get_query_region_path(start, queryset)
        for i, dr in enumerate(queryset):
            local_path = region_paths[i]
            vector = visual_index.apply(local_path)
            s = io.BytesIO()
            np.save(s, vector)
            redis_client.hset("query_region_vectors_{}".format(start.pk), dr.pk, s.getvalue())
        sync = False
    elif target == 'regions':
        # For regions simply download/ensure files exists.
        queryset, target = task_shared.build_queryset(args=start.arguments, video_id=start.video_id)
        task_shared.ensure_files(queryset, target)
        indexing.Indexers.index_queryset(di, visual_index, start, target, queryset)
    elif target == 'frames':
        queryset, target = task_shared.build_queryset(args=start.arguments, video_id=start.video_id)
        if visual_index.cloud_fs_support and settings.ENABLE_CLOUDFS:
            # if NFS is disabled and index supports cloud file systems natively (e.g. like Tensorflow)
            indexing.Indexers.index_queryset(di, visual_index, start, target, queryset, cloud_paths=True)
        else:
            # Otherwise download and ensure that the files exist
            task_shared.ensure_files(queryset, target)
            indexing.Indexers.index_queryset(di, visual_index, start, target, queryset)
    return sync


def handle_perform_index_approximation(start):
    args = start.arguments
    approx, da = approximation.Approximators.get_trained_model(args)
    if args['target'] == 'index_entries':
        queryset, target = task_shared.build_queryset(args, start.video_id, start.parent_process_id)
        approximation.Approximators.approximate_queryset(approx, da, queryset, start)
    else:
        raise ValueError("Target {} not allowed, only index_entries are allowed".format(args['target']))
    return True


def handle_perform_detection(start):
    video_id = start.video_id
    args = start.arguments
    frame_detections_list = []
    dv = None
    dd_list = []
    query_flow = ('target' in args and args['target'] == 'query')
    cd = models.TrainedModel.objects.get(**args['trainedmodel_selector'])
    detector_name = cd.name
    detection.Detectors.load_detector(cd)
    detector = detection.Detectors._detectors[cd.pk]
    if detector.session is None:
        logging.info("loading detection model")
        detector.load()
    if query_flow:
        local_path = task_shared.download_and_get_query_path(start)
        frame_detections_list.append((None, detector.detect(local_path)))
    else:
        if 'target' not in args:
            args['target'] = 'frames'
        dv = models.Video.objects.get(id=video_id)
        queryset, target = task_shared.build_queryset(args, video_id, start.parent_process_id)
        task_shared.ensure_files(queryset, target)
        for k in queryset:
            if target == 'frames':
                local_path = k.path()
            elif target == 'regions':
                local_path = k.frame_path()
            else:
                raise NotImplementedError("Invalid target:{}".format(target))
            frame_detections_list.append((k, detector.detect(local_path)))
    per_event_counter = 0
    for df, detections in frame_detections_list:
        for d in detections:
            if query_flow:
                dd = models.QueryRegion()
            else:
                dd = models.Region()
                dd.per_event_index = per_event_counter
                per_event_counter += 1
            dd.region_type = models.Region.DETECTION
            if query_flow:
                dd.query_id = start.parent_process_id
            else:
                dd.video_id = dv.pk
                dd.frame_index = df.frame_index
                dd.segment_index = df.segment_index
            if detector_name == 'textbox':
                dd.object_name = 'TEXTBOX'
                dd.confidence = 100.0 * d['score']
            elif detector_name == 'face':
                dd.object_name = 'MTCNN_face'
                dd.confidence = 100.0
            else:
                dd.object_name = d['object_name']
                dd.confidence = 100.0 * d['score']
            dd.x = d['x']
            dd.y = d['y']
            dd.w = d['w']
            dd.h = d['h']
            dd.event_id = start.pk
            dd_list.append(dd)
    if query_flow:
        _ = models.QueryRegion.objects.bulk_create(dd_list, 1000)
    else:
        start.finalize({"Region": dd_list})
    return query_flow


def handle_perform_analysis(start):
    task_id = start.pk
    video_id = start.video_id
    args = start.arguments
    da = models.TrainedModel.objects.get(**args['trainedmodel_selector'])
    analysis.Analyzers.load_analyzer(da)
    analyzer = analysis.Analyzers._analyzers[da.name]
    regions_batch = []
    relations_batch = []
    queryset, target = task_shared.build_queryset(args, video_id, start.parent_process_id)
    query_path = None
    query_regions_paths = None
    if target == 'query':
        query_path = task_shared.download_and_get_query_path(start)
    elif target == 'query_regions':
        query_regions_paths = task_shared.download_and_get_query_region_path(start, queryset)
    else:
        task_shared.ensure_files(queryset, target)
    image_data = {}
    source_regions = []
    temp_root = tempfile.mkdtemp()
    for i, f in enumerate(queryset):
        if query_regions_paths:
            path = query_regions_paths[i]
            a = models.QueryRegion()
            a.query_id = start.parent_process_id
            a.x = f.x
            a.y = f.y
            a.w = f.w
            a.h = f.h
        elif query_path:
            path = query_path
            w, h = task_shared.get_query_dimensions(start)
            a = models.QueryRegion()
            a.query_id = start.parent_process_id
            a.x = 0
            a.y = 0
            a.w = w
            a.h = h
            a.full_frame = True
        else:
            a = models.Region()
            a.video_id = f.video_id
            if target == 'regions':
                a.x = f.x
                a.y = f.y
                a.w = f.w
                a.h = f.h
                a.frame_index = f.frame_index
                a.segment_index = f.segment_index
                source_regions.append(f)
                path = f.crop_and_get_region_path(image_data, temp_root)
            elif target == 'frames':
                a.full_frame = True
                a.frame_index = f.frame_index
                a.segment_index = f.segment_index
                path = f.path()
            else:
                raise NotImplementedError
        object_name, text, metadata, _ = analyzer.apply(path)
        a.region_type = models.Region.ANNOTATION
        a.object_name = object_name
        a.text = text
        a.metadata = metadata
        a.event_id = task_id
        regions_batch.append(a)
    if query_regions_paths or query_path:
        models.QueryRegion.objects.bulk_create(regions_batch, 1000)
    else:
        if target == 'regions':
            for i, k in enumerate(regions_batch):
                dr = models.RegionRelation(source_region_id=source_regions[i].id, name='analysis', event_id=start.pk,
                                           video_id=start.video_id)
                relations_batch.append((dr, {'target_region_id': i}))
        start.finalize({"Region": regions_batch, "RegionRelation": relations_batch})


def handle_perform_matching(dt):
    args = dt.arguments
    video_id = dt.video_id
    k = args.get('k', 5)
    indexer_shasum = args['indexer_shasum']
    approximator_shasum = args.get('approximator_shasum', None)
    match_self = args.get('match_self', False)
    source_filters = args.get('source_filters', {'event__completed': True})
    target_filters = args.get('target_filters', {'event__completed': True})
    source_filters.update({'video_id': dt.video_id})
    source_filters.update({'indexer_shasum': indexer_shasum})
    target_filters.update({'indexer_shasum': indexer_shasum})
    if approximator_shasum:
        source_filters.update({'approximator_shasum': approximator_shasum})
        target_filters.update({'approximator_shasum': approximator_shasum})
    else:
        source_filters.update({'approximator_shasum': None})
        target_filters.update({'approximator_shasum': None})
    retriever = None
    relations = []
    regions = []
    region_count = 0
    if match_self:
        query_set = models.IndexEntries.objects.filter(**target_filters)
    else:
        query_set = models.IndexEntries.objects.filter(**target_filters).exclude(video_id=dt.video_id)
    index_entries = {}
    for di in query_set:
        mat = di.get_vectors()
        print mat.shape
        if di.count:
            mat = np.atleast_2d(mat.squeeze())
            print mat.shape
            if retriever is None:
                if approximator_shasum:
                    approximator, da = approximation.Approximators.get_trained_model({'shasum':approximator_shasum})
                    da.ensure()
                    approximator.load()
                    retriever = retrieval.retriever.FaissApproximateRetriever(name="approx_matcher",
                                                                              approximator=approximator)
                else:
                    components = mat.shape[1]
                    retriever = retrieval.retriever.FaissFlatRetriever("matcher", components=components)
            retriever.add_vectors(mat, di.count, di.pk)
            if di.pk not in index_entries:
                index_entries[di.pk] = di
    frame_to_region_index = {}
    for di in models.IndexEntries.objects.filter(**source_filters):
        mat = di.get_vectors()
        if di.count:
            mat = np.atleast_2d(mat.squeeze())
            print mat.shape
            results_batch = retriever.nearest_batch(mat, k)
            for i, entry in enumerate(di.iter_entries()):
                results = results_batch[i]
                if match_self:
                    pass
                else:
                    if di.target == 'frames':
                        if entry is list:
                            entry = entry[0]
                        if entry not in frame_to_region_index:
                            if di.video.dataset:
                                df = models.Frame.objects.get(frame_index=entry,video_id=di.video_id)
                                regions.append(models.Region(video_id=di.video_id, x=0, y=0, event=dt, w=df.w, h=df.h,
                                                             frame_index=entry, full_frame=True))
                            else:
                                regions.append(models.Region(video_id=di.video_id, x=0, y=0, event=dt, w=di.video.width,
                                                             h=di.video.height, frame_index=entry, full_frame=True))
                            frame_to_region_index[entry] = region_count
                            region_count += 1
                        region_id = None
                        value_map = {'region_id': frame_to_region_index[entry]}
                    else:
                        region_id = entry
                        value_map = {}
                    for result in results:
                        if 'indexentries_pk' in result:
                            di = index_entries[result['indexentries_pk']]
                            result['type'] = di.target
                            result['video'] = di.video_id
                            result['id'] = di.get_entry(result['offset'])
                        dr = models.HyperRegionRelation()
                        dr.video_id = video_id
                        dr.metadata = result
                        dr.region_id = region_id
                        if result['type'] == 'regions':
                            tdr = models.Region.objects.get(pk=result['id'])
                            dr.x = tdr.x
                            dr.y = tdr.y
                            dr.w = tdr.w
                            dr.h = tdr.h
                            dr.full_frame = tdr.full_frame
                            dr.path = tdr.global_frame_path()
                            dr.metadata = tdr.metadata
                        else:
                            target_video = models.Video.objects.get(pk=result['video'])
                            dr.x = 0
                            dr.y = 0
                            dr.full_frame = True
                            if target_video.dataset:
                                tdf = models.Frame.objects.get(video=target_video,frame_index=result['id'])
                                dr.w = tdf.w
                                dr.h = tdf.h
                                dr.path = tdf.global_path()
                            else:
                                dr.w = target_video.width
                                dr.h = target_video.height
                                dr.path = "{}::{}".format(target_video.url, result['id'])
                        dr.weight = result['dist']
                        dr.event_id = dt.pk
                        relations.append((dr, value_map))
    if match_self:
        pass
    else:
        dt.finalize({'Region': regions, 'HyperRegionRelation': relations})
