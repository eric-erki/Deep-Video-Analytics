from rest_framework import serializers
from django.contrib.auth.models import User
from models import Video, Frame, Region, DVAPQL, QueryResult, TEvent, IndexEntries, Tube, Segment, TrainedModel, \
    Retriever, SystemState, QueryRegion, Worker, TrainingSet, RegionRelation, TubeRegionRelation, TubeRelation, \
    Export, HyperRegionRelation, HyperTubeRegionRelation
import os, json, glob
from collections import defaultdict
from django.conf import settings


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = ('url', 'username', 'email', 'password')
        extra_kwargs = {
            'password': {'write_only': True},
        }


class VideoSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = Video
        fields = '__all__'


class ExportSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = Export
        fields = '__all__'


class RetrieverSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = Retriever
        fields = '__all__'


class TrainedModelSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = TrainedModel
        fields = '__all__'


class TrainingSetSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = TrainingSet
        fields = '__all__'


class WorkerSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Worker
        fields = ('queue_name', 'id')


class FrameSerializer(serializers.HyperlinkedModelSerializer):
    media_url = serializers.SerializerMethodField()

    def get_media_url(self, obj):
        return "{}{}/frames/{}.jpg".format(settings.MEDIA_URL, obj.video_id, obj.frame_index)

    class Meta:
        model = Frame
        fields = ('url', 'media_url', 'video', 'frame_index', 'keyframe', 'w', 'h', 't',
                  'name', 'id', 'segment_index')


class SegmentSerializer(serializers.HyperlinkedModelSerializer):
    media_url = serializers.SerializerMethodField()

    def get_media_url(self, obj):
        return "{}{}/segments/{}.mp4".format(settings.MEDIA_URL, obj.video_id, obj.segment_index)

    class Meta:
        model = Segment
        fields = ('video', 'segment_index', 'start_time', 'end_time', 'metadata',
                  'frame_count', 'start_index', 'url', 'media_url', 'id')


class RegionSerializer(serializers.HyperlinkedModelSerializer):
    frame_media_url = serializers.SerializerMethodField()

    def get_frame_media_url(self, obj):
        return "{}{}/frames/{}.jpg".format(settings.MEDIA_URL, obj.video_id, obj.frame_index)

    class Meta:
        model = Region
        fields = ('url', 'frame_media_url', 'region_type', 'video', 'user', 'event', 'frame_index',
                  'segment_index', 'text', 'metadata', 'full_frame', 'x', 'y', 'h', 'w',
                  'polygon_points', 'created', 'object_name', 'confidence', 'png', 'id')


class RegionRelationSerializer(serializers.HyperlinkedModelSerializer):
    source_frame_media_url = serializers.SerializerMethodField()
    target_frame_media_url = serializers.SerializerMethodField()

    def get_source_frame_media_url(self, obj):
        return "{}{}/frames/{}.jpg".format(settings.MEDIA_URL, obj.video_id, obj.source_region.frame_index)

    def get_target_frame_media_url(self, obj):
        return "{}{}/frames/{}.jpg".format(settings.MEDIA_URL, obj.video_id, obj.target_region.frame_index)

    class Meta:
        model = RegionRelation
        fields = ('url', 'source_frame_media_url', 'source_frame_media_url', 'target_frame_media_url', 'video',
                  'source_region', 'target_region', 'name', 'weight', 'event', 'metadata', 'id')


class HyperRegionRelationSerializer(serializers.HyperlinkedModelSerializer):
    frame_media_url = serializers.SerializerMethodField()

    def get_frame_media_url(self, obj):
        return "{}{}/frames/{}.jpg".format(settings.MEDIA_URL, obj.video_id, obj.region.frame_index)

    class Meta:
        model = HyperRegionRelation
        fields = ('url', 'frame_media_url', 'video', 'path', 'region', 'name', 'weight', 'event', 'metadata', 'id',
                  'x', 'y', 'w', 'h', 'full_frame')


class HyperTubeRegionRelationSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = HyperTubeRegionRelation
        fields = ('url', 'source_tube', 'target_tube', 'name', 'weight', 'video', 'event', 'metadata', 'id',
                  'x', 'y', 'w', 'h', 'full_frame')


class TubeRelationSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = TubeRelation
        fields = ('url', 'source_tube', 'target_tube', 'name', 'weight', 'video', 'event', 'metadata', 'id')


class TubeRegionRelationSerializer(serializers.HyperlinkedModelSerializer):
    region_frame_media_url = serializers.SerializerMethodField()

    def get_region_frame_media_url(self, obj):
        return "{}{}/frames/{}.jpg".format(settings.MEDIA_URL, obj.video_id, obj.region.frame_index)

    class Meta:
        model = TubeRegionRelation
        fields = (
            'url', 'region_frame_media_url', 'region', 'tube', 'video', 'name', 'weight', 'event', 'metadata', 'id')


class TubeSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = Tube
        fields = '__all__'


class QueryRegionSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = QueryRegion
        fields = '__all__'


class SystemStateSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = SystemState
        fields = '__all__'


class QueryResultsSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = QueryResult
        fields = '__all__'


class QueryResultsExportSerializer(serializers.ModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = QueryResult
        fields = '__all__'


class QueryRegionExportSerializer(serializers.ModelSerializer):
    query_results = QueryResultsExportSerializer(source='queryresults_set', read_only=True, many=True)

    class Meta:
        model = QueryRegion
        fields = (
            'id', 'region_type', 'query', 'event', 'text', 'metadata', 'full_frame', 'x', 'y', 'h', 'w',
            'polygon_points', 'created', 'object_name', 'confidence', 'png', 'query_results')


class TaskExportSerializer(serializers.ModelSerializer):
    query_results = QueryResultsExportSerializer(source='queryresults_set', read_only=True, many=True)
    query_regions = QueryRegionExportSerializer(source='queryregion_set', read_only=True, many=True)

    class Meta:
        model = TEvent
        fields = ('started', 'completed', 'errored', 'worker', 'error_message', 'video', 'operation', 'queue',
                  'created', 'start_ts', 'duration', 'arguments', 'task_id', 'parent', 'parent_process',
                  'training_set', 'imported', 'query_results', 'query_regions', 'id')


class IndexEntriesSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()

    class Meta:
        model = IndexEntries
        fields = '__all__'


class RegionExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Region
        fields = '__all__'


class RegionRelationExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = RegionRelation
        fields = '__all__'


class HyperRegionRelationExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = HyperRegionRelation
        fields = '__all__'


class HyperTubeRegionRelationRelationExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = HyperTubeRegionRelation
        fields = '__all__'


class TubeRelationExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = TubeRelation
        fields = '__all__'


class TubeRegionRelationExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = TubeRegionRelation
        fields = '__all__'


class FrameExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Frame
        fields = ('frame_index', 'keyframe', 'w', 'h', 't', 'event', 'name', 'id', 'segment_index')


class IndexEntryExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = IndexEntries
        fields = '__all__'


class TEventExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = TEvent
        fields = '__all__'


class TubeExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Tube
        fields = '__all__'


class SegmentExportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Segment
        fields = '__all__'


class DVAPQLSerializer(serializers.HyperlinkedModelSerializer):
    tasks = TaskExportSerializer(source='tevent_set', read_only=True, many=True)
    query_image_url = serializers.SerializerMethodField()

    def get_query_image_url(self, obj):
        if obj.process_type == DVAPQL.QUERY:
            return "{}queries/{}.png".format(settings.MEDIA_URL, obj.uuid)
        else:
            return None

    class Meta:
        model = DVAPQL
        fields = ('process_type', 'query_image_url', 'created', 'user', 'uuid', 'script', 'tasks',
                  'results_metadata', 'results_available', 'completed', 'id')


class TEventSerializer(serializers.HyperlinkedModelSerializer):
    id = serializers.ReadOnlyField()
    region_list = RegionExportSerializer(source='region_set', read_only=True, many=True)
    hyper_region_relation_list = HyperRegionRelationExportSerializer(source='hyperregionrelation_set',
                                                                     read_only=True, many=True)
    index_entries_list = IndexEntryExportSerializer(source='indexentries_set', read_only=True, many=True)
    tube_list = TubeExportSerializer(source='tube_set', read_only=True, many=True)
    region_relation_list = RegionRelationExportSerializer(source='regionrelation_set', read_only=True, many=True)

    class Meta:
        model = TEvent
        fields = ('id', 'started', 'completed', 'errored', 'error_message', 'operation', 'queue', 'created',
                  'start_ts', 'duration', 'arguments', 'task_id', 'parent', 'parent_process', 'task_group_id',
                  'results', 'region_list', 'hyper_region_relation_list', 'index_entries_list',
                  'tube_list', 'region_relation_list')


class VideoExportSerializer(serializers.ModelSerializer):
    frame_list = FrameExportSerializer(source='frame_set', read_only=True, many=True)
    segment_list = SegmentExportSerializer(source='segment_set', read_only=True, many=True)
    event_list = TEventExportSerializer(source='tevent_set', read_only=True, many=True)

    class Meta:
        model = Video
        fields = ('name', 'length_in_seconds', 'height', 'width', 'metadata', 'frames', 'created', 'description',
                  'uploaded', 'dataset', 'uploader', 'segments', 'url', 'frame_list', 'segment_list',
                  'event_list', "stream")


def import_frame_json(f, frame_index, event_id, video_id, w, h):
    regions = []
    df = Frame()
    df.video_id = video_id
    df.event_id = event_id
    df.w = w
    df.h = h
    df.frame_index = frame_index
    df.name = f['path']
    for r in f.get('regions', []):
        regions.append(import_region_json(r, frame_index, video_id, event_id))
    return df, regions


def import_region_json(r, frame_index, video_id, event_id, segment_index=None):
    dr = Region()
    dr.frame_index = frame_index
    dr.video_id = video_id
    dr.event_id = event_id
    dr.object_name = r['object_name']
    dr.region_type = r.get('region_type', Region.ANNOTATION)
    dr.full_frame = r.get('full_frame', False)
    if segment_index:
        dr.segment_index = segment_index
    dr.x = r.get('x', 0)
    dr.y = r.get('y', 0)
    dr.w = r.get('w', 0)
    dr.h = r.get('h', 0)
    dr.confidence = r.get('confidence', 0.0)
    if r.get('text', None):
        dr.text = r['text']
    else:
        dr.text = ""
    dr.metadata = r.get('metadata', None)
    return dr


def create_event(e, v):
    de = TEvent()
    de.imported = True
    e.id = e['id']  # id is a uuid
    de.results = e.get('results', None)
    de.started = e.get('started', False)
    de.start_ts = e.get('start_ts', None)
    de.completed = e.get('completed', False)
    de.errored = e.get('errored', False)
    de.error_message = e.get('error_message', "")
    de.video_id = v.pk
    de.operation = e.get('operation', "")
    de.created = e['created']
    if 'seconds' in e:
        de.duration = e.get('seconds', -1)
    else:
        de.duration = e.get('duration', -1)
    de.arguments = e.get('arguments', {})
    de.task_id = e.get('task_id', "")
    return de


class VideoImporter(object):
    def __init__(self, video, video_json, root_dir):
        self.video = video
        self.json = video_json
        self.root = root_dir
        self.region_to_pk = {}
        self.region_relation_to_pk = {}
        self.frame_to_pk = {}
        self.segment_to_pk = {}
        self.label_to_pk = {}
        self.tube_to_pk = {}
        self.name_to_shasum = {'inception': '48b026cf77dfbd5d9841cca3ee550ef0ee5a0751',
                               'facenet': '9f99caccbc75dcee8cb0a55a0551d7c5cb8a6836',
                               'vgg': '52723231e796dd06fafd190957c8a3b5a69e009c'}

    def import_video(self):
        if self.video.name is None or not self.video.name:
            self.video.name = self.json['name']
        self.video.frames = self.json['frames']
        self.video.height = self.json['height']
        self.video.width = self.json['width']
        self.video.segments = self.json.get('segments', 0)
        self.video.stream = self.json.get('stream', False)
        self.video.dataset = self.json['dataset']
        self.video.description = self.json['description']
        self.video.metadata = self.json['metadata']
        self.video.length_in_seconds = self.json['length_in_seconds']
        self.video.save()
        if not self.video.dataset:
            old_video_path = [fname for fname in glob.glob("{}/video/*.mp4".format(self.root))][0]
            new_video_path = "{}/video/{}.mp4".format(self.root, self.video.pk)
            os.rename(old_video_path, new_video_path)
        self.import_events(self.json.get('event_list', []))
        self.import_segments()
        self.bulk_import_frames()
        self.convert_regions_files()

    def import_segments(self):
        old_ids = []
        segments = []
        for s in self.json.get('segment_list', []):
            old_ids.append(s['id'])
            segments.append(self.create_segment(s))
        segment_ids = Segment.objects.bulk_create(segments, 1000)
        for i, k in enumerate(segment_ids):
            self.segment_to_pk[old_ids[i]] = k.id

    def create_segment(self, s):
        ds = Segment()
        ds.video_id = self.video.pk
        ds.segment_index = s.get('segment_index', '-1')
        ds.start_time = s.get('start_time', 0)
        ds.framelist = s.get('framelist', {})
        ds.end_time = s.get('end_time', 0)
        ds.metadata = s.get('metadata', "")
        ds.event_id = s['event']
        ds.frame_count = s.get('frame_count', 0)
        ds.start_index = s.get('start_index', 0)
        return ds

    def import_events(self, event_list_json):
        old_ids = []
        children_ids = defaultdict(list)
        events = []
        for e in event_list_json:
            old_ids.append(e['id'])
            if 'parent' in e:
                children_ids[e['parent']].append(e['id'])
            events.append(create_event(e, self.video))
        event_ids = TEvent.objects.bulk_create(events, 1000)
        for ej in event_list_json:
            self.bulk_import_regions(ej.get('region_list', []))
        for ej in event_list_json:
            self.bulk_import_region_relations(ej.get('region_relation_list', []))
        for ej in event_list_json:
            self.bulk_import_index_entries(ej.get('index_entries_list', []))
        for old_id in old_ids:
            parent_id = old_id
            for child_old_id in children_ids[old_id]:
                ce = TEvent.objects.get(pk=child_old_id)
                ce.parent_id = parent_id
                ce.save()

    def convert_regions_files(self):
        convert_list = []
        for k, v in self.region_to_pk.iteritems():
            dd = Region.objects.get(pk=v)
            original = '{}/{}/{}.jpg'.format(self.root, 'regions', k)
            temp_file = "{}/regions/d_{}.jpg".format(self.root, v)
            converted = "{}/regions/{}.jpg".format(self.root, v)
            if os.path.isfile(original):
                try:
                    os.rename(original, temp_file)
                    convert_list.append((temp_file, converted))
                except:
                    raise ValueError("could not copy {} to {}".format(original, temp_file))
        for temp_file, converted in convert_list:
            os.rename(temp_file, converted)

    def bulk_import_index_entries(self, index_entries_list_json):
        for i in index_entries_list_json:
            di = IndexEntries()
            di.video = self.video
            di.id = i['id']
            di.per_event_index = i['per_event_index']
            di.algorithm = i['algorithm']
            di.indexer_shasum = i['indexer_shasum']
            di.approximator_shasum = i['approximator_shasum']
            di.count = i['count']
            di.approximate = i['approximate']
            di.created = i['created']
            di.event_id = i['event']
            di.features_file_name = i['features_file_name']
            if 'entries_file_name' in i:
                entries = json.load(file('{}/indexes/{}'.format(self.root, i['entries_file_name'])))
            else:
                entries = i['entries']
            di.target = i['target']
            di.metadata = i.get('metadata', {})
            transformed = []
            for entry in entries:
                if 'detection_primary_key' in entry:
                    entry['detection_primary_key'] = self.region_to_pk[entry['detection_primary_key']]
                if 'frame_primary_key' in entry:
                    entry['frame_primary_key'] = self.frame_to_pk[entry['frame_primary_key']]
                transformed.append(entry)
            di.entries = transformed
            di.save()

    def bulk_import_frames(self):
        frames = []
        frame_index_to_fid = {}
        for i, f in enumerate(self.json['frame_list']):
            frames.append(self.create_frame(f))
            frame_index_to_fid[i] = f['id']
        bulk_frames = Frame.objects.bulk_create(frames)
        for i, k in enumerate(bulk_frames):
            self.frame_to_pk[frame_index_to_fid[i]] = k.id

    def bulk_import_regions(self, region_list_json):
        regions = []
        region_index_to_fid = {}
        for i, a in enumerate(region_list_json):
            ra = self.create_region(a)
            regions.append(ra)
            region_index_to_fid[i] = a['id']
        bulk_regions = Region.objects.bulk_create(regions)
        for i, k in enumerate(bulk_regions):
            self.region_to_pk[region_index_to_fid[i]] = k.id

    def bulk_import_region_relations(self, region_relation_list_json):
        region_relations = []
        region_relations_index_to_fid = {}
        for i, f in enumerate(region_relation_list_json):
            region_relations.append(self.create_region_relation(f))
            region_relations_index_to_fid[i] = f['id']
        bulk_rr = RegionRelation.objects.bulk_create(region_relations)
        for i, k in enumerate(bulk_rr):
            self.region_relation_to_pk[region_relations_index_to_fid[i]] = k.id

    def create_region(self, a):
        da = Region()
        da.video_id = self.video.pk
        da.x = a['x']
        da.y = a['y']
        da.h = a['h']
        da.w = a['w']
        da.text = a['text']
        da.metadata = a['metadata']
        da.png = a.get('png', False)
        da.per_event_index = a['per_event_index']
        da.region_type = a['region_type']
        da.confidence = a['confidence']
        da.object_name = a['object_name']
        da.full_frame = a['full_frame']
        da.event_id = a['event']
        da.id = '{}_{}'.format(da.event_id, da.per_event_index)
        da.frame_index = a['frame_index']
        da.segment_index = a.get('segment_index', -1)
        return da

    def create_region_relation(self, a):
        da = RegionRelation()
        da.video_id = self.video.pk
        if 'metadata' in a:
            da.metadata = a['metadata']
        if 'weight' in a:
            da.weight = a['weight']
        if 'name' in a:
            da.name = a['name']
        if a.get('event', None):
            da.event_id = a['event']
        da.source_region_id = self.region_to_pk[a['source_region']]
        da.target_region_id = self.region_to_pk[a['target_region']]
        return da

    def create_frame(self, f):
        df = Frame()
        df.video_id = self.video.pk
        df.name = f['name']
        df.frame_index = f['frame_index']
        df.h = f.get('h', 0)
        df.w = f.get('w', 0)
        df.t = f.get('t', 0)
        df.event_id = f['event']
        df.segment_index = f.get('segment_index', 0)
        df.keyframe = f.get('keyframe', False)
        return df
