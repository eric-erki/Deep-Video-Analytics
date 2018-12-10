from __future__ import unicode_literals
import os, json, gzip, sys, shutil, zipfile, uuid, hashlib, logging

sys.path.append(os.path.join(os.path.dirname(__file__),
                             "../../client/"))  # This ensures that the constants are same between client and server
from django.db import models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField
from django.conf import settings
from django.utils import timezone
from dvaclient import constants
from . import fs
from PIL import Image
import time
import lmdb

try:
    import numpy as np
except ImportError:
    pass
from uuid import UUID
from json import JSONEncoder

JSONEncoder_old = JSONEncoder.default
OPENED_DBS = {}

def JSONEncoder_new(self, o):
    if isinstance(o, UUID): return str(o)
    return JSONEncoder_old(self, o)


JSONEncoder.default = JSONEncoder_new


class Worker(models.Model):
    queue_name = models.CharField(max_length=500, default="")
    host = models.CharField(max_length=500, default="")
    pid = models.IntegerField()
    alive = models.BooleanField(default=True)
    shutdown = models.BooleanField(default=False)
    last_ping = models.DateTimeField('date last ping', null=True)
    created = models.DateTimeField('date created', auto_now_add=True)


class DVAPQL(models.Model):
    SCHEDULE = constants.SCHEDULE
    PROCESS = constants.PROCESS
    QUERY = constants.QUERY
    TYPE_CHOICES = ((SCHEDULE, 'Schedule'), (PROCESS, 'Process'), (QUERY, 'Query'))
    process_type = models.CharField(max_length=1, choices=TYPE_CHOICES, default=QUERY, )
    created = models.DateTimeField('date created', auto_now_add=True)
    user = models.ForeignKey(User, null=True, related_name="submitter")
    script = JSONField(blank=True, null=True)
    results_metadata = models.TextField(default="")
    results_available = models.BooleanField(default=False)
    completed = models.BooleanField(default=False)
    failed = models.BooleanField(default=False)
    error_message = models.TextField(default="", blank=True, null=True)
    uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)


class TrainingSet(models.Model):
    DETECTION = constants.DETECTION
    INDEXING = constants.INDEXING
    TRAINAPPROX = constants.TRAINAPPROX
    CLASSIFICATION = constants.CLASSIFICATION
    IMAGES = constants.IMAGES
    VIDEOS = constants.VIDEOS
    INDEX = constants.INDEX
    INSTANCE_TYPES = (
        (IMAGES, 'images'),
        (INDEX, 'index'),
        (VIDEOS, 'videos'),
    )
    TRAIN_TASK_TYPES = (
        (DETECTION, 'Detection'),
        (INDEXING, 'Indexing'),
        (TRAINAPPROX, 'Approximation'),
        (CLASSIFICATION, 'Classification')
    )
    uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    source_filters = JSONField(blank=True, null=True)
    training_task_type = models.CharField(max_length=1, choices=TRAIN_TASK_TYPES, db_index=True, default=DETECTION)
    instance_type = models.CharField(max_length=1, choices=INSTANCE_TYPES, db_index=True, default=IMAGES)
    count = models.IntegerField(null=True)
    name = models.CharField(max_length=500, default="")
    files = JSONField(blank=True, null=True)
    built = models.BooleanField(default=False)
    created = models.DateTimeField('date created', auto_now_add=True)


class Video(models.Model):
    id = models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True)
    name = models.CharField(max_length=500, default="")
    length_in_seconds = models.IntegerField(default=0)
    height = models.IntegerField(default=0)
    width = models.IntegerField(default=0)
    metadata = models.TextField(default="")
    frames = models.IntegerField(default=0)
    created = models.DateTimeField('date created', auto_now_add=True)
    description = models.TextField(default="")
    uploaded = models.BooleanField(default=False)
    dataset = models.BooleanField(default=False)
    uploader = models.ForeignKey(User, null=True)
    segments = models.IntegerField(default=0)
    stream = models.BooleanField(default=False)
    url = models.TextField(default="")
    parent_process = models.ForeignKey(DVAPQL, null=True)

    def __unicode__(self):
        return u'{}'.format(self.name)

    def path(self, media_root=None):
        if not (media_root is None):
            return "{}/{}/video/{}.mp4".format(media_root, self.pk, self.pk)
        else:
            return "{}/{}/video/{}.mp4".format(settings.MEDIA_ROOT, self.pk, self.pk)

    def segments_dir(self, media_root=None):
        if not (media_root is None):
            return "{}/{}/segments/".format(media_root, self.pk, self.pk)
        else:
            return "{}/{}/segments/".format(settings.MEDIA_ROOT, self.pk, self.pk)

    def get_frame_list(self, media_root=None):
        if media_root is None:
            media_root = settings.MEDIA_ROOT
        framelist_path = "{}/{}/framelist".format(media_root, self.pk)
        if os.path.isfile('{}.json'.format(framelist_path)):
            return json.load(file('{}.json'.format(framelist_path)))
        elif os.path.isfile('{}.gz'.format(framelist_path)):
            return json.load(gzip.GzipFile('{}.gz'.format(framelist_path)))
        else:
            raise ValueError("Frame list could not be found at {}".format(framelist_path))

    def create_directory(self, create_subdirs=True):
        d = '{}/{}'.format(settings.MEDIA_ROOT, self.pk)
        if not os.path.exists(d):
            try:
                os.mkdir(d)
            except OSError:
                pass
        if create_subdirs:
            for s in ['video', 'frames', 'segments', 'events', 'audio']:
                d = '{}/{}/{}/'.format(settings.MEDIA_ROOT, self.pk, s)
                if not os.path.exists(d):
                    try:
                        os.mkdir(d)
                    except OSError:
                        pass


class TEvent(models.Model):
    id = models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True)
    started = models.BooleanField(default=False)
    completed = models.BooleanField(default=False)
    errored = models.BooleanField(default=False)
    worker = models.ForeignKey(Worker, null=True)
    error_message = models.TextField(default="")
    video = models.ForeignKey(Video, null=True)
    training_set = models.ForeignKey(TrainingSet, null=True)
    operation = models.CharField(max_length=100, default="")
    queue = models.CharField(max_length=100, default="")
    created = models.DateTimeField('date created', auto_now_add=True)
    start_ts = models.DateTimeField('date started', null=True)
    duration = models.FloatField(default=-1)
    arguments = JSONField(blank=True, null=True)
    task_id = models.TextField(null=True)
    parent = models.ForeignKey('self', null=True, related_name="parent_task")
    imported = models.ForeignKey('self', null=True, related_name="importer_task")
    parent_process = models.ForeignKey(DVAPQL, null=True)
    task_group_id = models.IntegerField(default=-1)
    results = JSONField(blank=True, null=True)
    min_frame_index = models.IntegerField(null=True)
    max_frame_index = models.IntegerField(null=True)

    def create_dir(self,media_root=None):
        if self.video_id:
            dirnames = ['{}/{}/'.format(settings.MEDIA_ROOT, self.video_id),
                        '{}/{}/events/'.format(settings.MEDIA_ROOT, self.video_id), self.get_dir(media_root)]
        elif self.training_set_id:
            dirnames = ['{}/{}/'.format(settings.MEDIA_ROOT, self.training_set_id),
                        '{}/{}/events/'.format(settings.MEDIA_ROOT, self.training_set_id), self.get_dir(media_root)]
        else:
            dirnames = [self.get_dir(media_root),]
        for dirname in dirnames:
            if not os.path.isdir(dirname):
                try:
                    os.mkdir(dirname)
                except:
                    error_message = "error creating {}".format(dirname)
                    self.error_message += error_message
                    logging.exception(error_message)
                    pass

    def get_dir(self,media_root=None):
        if media_root is None:
            media_root = settings.MEDIA_ROOT
        if self.video_id:
            return "{}/{}/events/{}/".format(media_root,self.video_id,self.pk)
        elif self.training_set_id:
            return "{}/{}/events/{}/".format(media_root,self.training_set_id,self.pk)
        else:
            return None

    def finalize(self, bulk_create, results=None):
        created_regions = []
        created_tubes = []
        ancestor_events = set()
        frame_indexes = set()
        if self.results:
            raise ValueError("Finalize should be only called once")
        else:
            self.results = {'created_objects': {}}
        if 'Export' in bulk_create:
            temp = []
            for i, d in enumerate(bulk_create['Export']):
                temp.append(d)
            created_exports = Export.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['Export'] = len(created_exports)
        if 'Frame' in bulk_create:
            temp = []
            for i, d in enumerate(bulk_create['Frame']):
                temp.append(d)
                frame_indexes.add(d.frame_index)
            created_frames = Frame.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['Frame'] = len(created_frames)
        if 'Segment' in bulk_create:
            temp = []
            for i, d in enumerate(bulk_create['Segment']):
                temp.append(d)
                frame_indexes.add(d.start_index)
                frame_indexes.add(d.start_index + d.frame_count - 1)
            created_segments = Segment.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['Segment'] = len(created_segments)
        if 'IndexEntries' in bulk_create:
            temp = []
            for i, d in enumerate(bulk_create['IndexEntries']):
                d.per_event_index = i
                if d.max_frame_index:
                    frame_indexes.add(d.max_frame_index)
                if d.min_frame_index:
                    frame_indexes.add(d.min_frame_index)
                d.id = '{}_{}'.format(self.id, i)
                temp.append(d)
            created_index_entries = IndexEntries.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['IndexEntries'] = len(created_index_entries)
        if 'Region' in bulk_create:
            temp = []
            for i, d in enumerate(bulk_create['Region']):
                d.per_event_index = i
                frame_indexes.add(d.frame_index)
                d.id = '{}_{}'.format(self.id, i)
                temp.append(d)
            created_regions = Region.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['Region'] = len(created_regions)
        if 'Tube' in bulk_create:
            temp = []
            for i, d in enumerate(bulk_create['Tube']):
                d.per_event_index = i
                frame_indexes.add(d.start_frame_index)
                frame_indexes.add(d.end_frame_index)
                d.id = '{}_{}'.format(self.id, i)
                temp.append(d)
            created_tubes = Tube.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['Tube'] = len(created_tubes)
        if 'RegionRelation' in bulk_create:
            temp = []
            for i, d_value_map in enumerate(bulk_create['RegionRelation']):
                d, value_map = d_value_map
                if 'source_region_id' in value_map:
                    d.source_region_id = created_regions[value_map['source_region_id']].id
                if 'target_region_id' in value_map:
                    d.target_region_id = created_regions[value_map['target_region_id']].id
                ancestor_events.add(d.source_region_id.split('_')[0])
                ancestor_events.add(d.target_region_id.split('_')[0])
                d.id = '{}_{}'.format(self.id, i)
                d.per_event_index = i
                temp.append(d)
            RegionRelation.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['RegionRelation'] = len(temp)
        if 'TubeRelation' in bulk_create:
            temp = []
            for i, d_value_map in enumerate(bulk_create['TubeRelation']):
                d, value_map = d_value_map
                d.per_event_index = i
                temp.append(d)
                ancestor_events.add(d.source_tube_id.split('_')[0])
                ancestor_events.add(d.target_tube_id.split('_')[0])
            TubeRelation.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['TubeRelation'] = len(temp)
        if 'TubeRegionRelation' in bulk_create:
            temp = []
            for i, d_value_map in enumerate(bulk_create['TubeRegionRelation']):
                d, value_map = d_value_map
                d.per_event_index = i
                d.id = '{}_{}'.format(self.id, i)
                temp.append(d)
                ancestor_events.add(d.tube_id.split('_')[0])
                ancestor_events.add(d.region_id.split('_')[0])
            TubeRegionRelation.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['TubeRegionRelation'] = len(temp)
        if 'HyperRegionRelation' in bulk_create:
            temp = []
            for i, d_value_map in enumerate(bulk_create['HyperRegionRelation']):
                d, value_map = d_value_map
                if 'region_id' in value_map:
                    d.region_id = created_regions[value_map['region_id']].id
                else:
                    if d.region_id is None:
                        raise ValueError(d_value_map)
                d.per_event_index = i
                d.id = '{}_{}'.format(self.id, i)
                temp.append(d)
                ancestor_events.add(d.region_id.split('_')[0])
            HyperRegionRelation.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['HyperRegionRelation'] = len(temp)
        if 'HyperTubeRegionRelation' in bulk_create:
            temp = []
            for i, d_value_map in enumerate(bulk_create['HyperTubeRegionRelation']):
                d, value_map = d_value_map
                d.per_event_index = i
                if 'tube_id' in value_map:
                    d.tube_id = created_tubes[value_map['tube_id']].id
                else:
                    if d.tube_id is None:
                        raise ValueError(d_value_map)
                d.id = '{}_{}'.format(self.id, i)
                temp.append(d)
                ancestor_events.add(d.tube_id.split('_')[0])
                ancestor_events.add(d.region_id.split('_')[0])
            HyperTubeRegionRelation.objects.bulk_create(temp, batch_size=1000)
            self.results['created_objects']['HyperTubeRegionRelation'] = len(temp)
        ancestor_events.discard(self.pk)  # Remove self from ancestors.
        self.results['ancestors'] = list(ancestor_events)
        if frame_indexes:
            self.max_frame_index = max(frame_indexes)
            self.min_frame_index = min(frame_indexes)
        if results:
            self.results.update(results)

    def finalize_query(self, bulk_create, results=None):
        if self.results is None:
            self.results = {'created_objects': {'QueryResult': 0}}
        if 'QueryResult' in bulk_create:
            created_query_results = QueryResult.objects.bulk_create(bulk_create['QueryResult'], batch_size=1000)
            self.results['created_objects']['QueryResult'] += len(created_query_results)
        if results:
            self.results.update(results)

    def upload(self):
        if self.operation == 'perform_import' and self.video_id:
            fs.upload_video_to_remote(self.video_id)
        else:
            fnames = []
            created_type_count = 0
            if self.results and 'created_objects' in self.results:
                if 'Frame' in self.results['created_objects']:
                    fnames += [k.path(media_root="") for k in Frame.objects.filter(event_id=self.pk)]
                    created_type_count += 1
                if 'Segment' in self.results['created_objects']:
                    fnames += [k.path(media_root="") for k in Segment.objects.filter(event_id=self.pk)]
                    created_type_count += 1
                # If anything else has been created then sync the directory
                if len(self.results['created_objects']) > created_type_count:
                    event_dir = self.get_dir()
                    if event_dir and os.path.isdir(event_dir):
                        for fname in os.listdir(event_dir):
                            path = "{}{}".format(event_dir,fname)
                            if os.path.isfile(path):
                                fnames.append("{}{}".format(self.get_dir(media_root=""),fname))
                            else:
                                raise ValueError("{} is directory, event specific directory can only contain files")
                for fp in fnames:
                    fs.upload_file_to_remote(fp)
                # TODO(akshay): Remove this
                if fnames:
                    time.sleep(2)

    def mark_as_completed(self):
        if self.operation == 'perform_import' and self.video_id:
            # This ensures that all files are uploaded to remote fs.
            # Otherwise a retriever may attempt to load an imported index before its available.
            for dt in TEvent.objects.filter(imported=self):
                dt.completed = True
                dt.save()
        self.completed = True
        if self.start_ts:
            self.duration = (timezone.now() - self.start_ts).total_seconds()
        self.save()


class TrainedModel(models.Model):
    """
    A model Model
    """
    TENSORFLOW = constants.TENSORFLOW
    CAFFE = constants.CAFFE
    PYTORCH = constants.PYTORCH
    OPENCV = constants.OPENCV
    MXNET = constants.MXNET
    INDEXER = constants.INDEXER
    APPROXIMATOR = constants.APPROXIMATOR
    DETECTOR = constants.DETECTOR
    ANALYZER = constants.ANALYZER
    SEGMENTER = constants.SEGMENTER
    YOLO = constants.YOLO
    TFD = constants.TFD
    DETECTOR_TYPES = (
        (TFD, 'Tensorflow'),
        (YOLO, 'YOLO V2'),
    )
    MODES = (
        (TENSORFLOW, 'Tensorflow'),
        (CAFFE, 'Caffe'),
        (PYTORCH, 'Pytorch'),
        (OPENCV, 'OpenCV'),
        (MXNET, 'MXNet'),
    )
    MTYPE = (
        (APPROXIMATOR, 'Approximator'),
        (INDEXER, 'Indexer'),
        (DETECTOR, 'Detector'),
        (ANALYZER, 'Analyzer'),
        (SEGMENTER, 'Segmenter'),
    )
    detector_type = models.CharField(max_length=1, choices=DETECTOR_TYPES, db_index=True, null=True)
    mode = models.CharField(max_length=1, choices=MODES, db_index=True, default=TENSORFLOW)
    model_type = models.CharField(max_length=1, choices=MTYPE, db_index=True, default=INDEXER)
    name = models.CharField(max_length=100, unique=True)
    algorithm = models.CharField(max_length=100, default="")
    shasum = models.CharField(max_length=40, null=True)
    model_filename = models.CharField(max_length=200, default="", null=True)
    created = models.DateTimeField('date created', auto_now_add=True)
    arguments = JSONField(null=True, blank=True)
    event = models.ForeignKey(TEvent, null=True)
    trained = models.BooleanField(default=False)
    training_set = models.ForeignKey(TrainingSet, null=True)
    url = models.CharField(max_length=200, default="")
    files = JSONField(null=True, blank=True)
    # Following allows us to have a hierarchy of models (E.g. inception pretrained -> inception fine tuned)
    parent = models.ForeignKey('self', null=True)
    uuid = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)

    def create_directory(self):
        if not os.path.isdir('{}/models/'.format(settings.MEDIA_ROOT)):
            try:
                os.mkdir('{}/models/'.format(settings.MEDIA_ROOT))
            except:
                pass
        try:
            os.mkdir('{}/models/{}'.format(settings.MEDIA_ROOT, self.uuid))
        except:
            pass

    def get_model_path(self, root_dir=None):
        if root_dir is None:
            root_dir = settings.MEDIA_ROOT
        if self.model_filename:
            return "{}/models/{}/{}".format(root_dir, self.uuid, self.model_filename)
        elif self.files:
            return "{}/models/{}/{}".format(root_dir, self.uuid, self.files[0]['filename'])
        else:
            return None

    def upload(self):
        for m in self.files:
            if settings.ENABLE_CLOUDFS and sys.platform != 'darwin':
                fs.upload_file_to_remote("/models/{}/{}".format(self.uuid, m['filename']))

    def download(self):
        root_dir = settings.MEDIA_ROOT
        model_type_dir = "{}/models/".format(root_dir)
        if not os.path.isdir(model_type_dir):
            os.mkdir(model_type_dir)
        model_dir = "{}/models/{}".format(root_dir, self.uuid)
        if not os.path.isdir(model_dir):
            try:
                os.mkdir(model_dir)
            except:
                pass
        shasums = []
        if 'path' in self.arguments and self.arguments['path'].endswith('.dva_model_export'):
            dlpath = "{}/model.zip".format(model_dir)
            if self.arguments['path'].startswith('/'):
                shutil.copy(self.arguments['path'], dlpath)
            else:
                fs.get_path_to_file(self.arguments['path'], dlpath)
            source_zip = "{}/model.zip".format(model_dir)
            zipf = zipfile.ZipFile(source_zip, 'r')
            zipf.extractall(model_dir)
            zipf.close()
            os.remove(source_zip)
            files = []
            import_dirname = os.listdir("{}/".format(model_dir))[-1]
            for fname in os.listdir("{}/{}/".format(model_dir,import_dirname)):
                shutil.move("{}/{}/{}".format(model_dir,import_dirname,fname),
                            "{}/{}".format(model_dir,fname))
            shutil.rmtree("{}/{}".format(model_dir,import_dirname))
            for fname in os.listdir(model_dir):
                if fname != 'model_spec.json':
                    files.append({"url":"","filename":fname})
                    shasums.append(str(hashlib.sha1(file("{}/{}".format(model_dir,fname)).read()).hexdigest()))
                else:
                    with open('{}/model_spec.json'.format(model_dir),'r') as import_spec:
                        self.arguments['imported_spec'] = json.load(import_spec)
            self.files = files
            self.save()
        else:
            for m in self.files:
                dlpath = "{}/{}".format(model_dir, m['filename'])
                if m['url'].startswith('/'):
                    shutil.copy(m['url'], dlpath)
                else:
                    fs.get_path_to_file(m['url'], dlpath)
                shasums.append(str(hashlib.sha1(file(dlpath).read()).hexdigest()))
        if self.shasum is None:
            if len(shasums) == 1:
                self.shasum = shasums[0]
            else:
                self.shasum = str(hashlib.sha1(''.join(sorted(shasums))).hexdigest())
            self.save()
        self.upload()
        if self.model_type == self.INDEXER:
            if settings.ENABLE_FAISS:
                dr, dcreated = Retriever.objects.get_or_create(name=self.name, source_filters={},
                                                               algorithm=Retriever.FAISS,
                                                               indexer_shasum=self.shasum)
            else:
                dr, dcreated = Retriever.objects.get_or_create(name=self.name, source_filters={},
                                                               algorithm=Retriever.EXACT,
                                                               indexer_shasum=self.shasum)
            if dcreated:
                dr.last_built = timezone.now()
                dr.save()
        elif self.model_type == self.APPROXIMATOR:
            if self.algorithm == 'LOPQ':
                algo = Retriever.LOPQ
            elif self.algorithm == 'FAISS':
                algo = Retriever.FAISS
            else:
                algo = Retriever.EXACT
            dr, dcreated = Retriever.objects.get_or_create(name=self.name,
                                                           source_filters={},
                                                           algorithm=algo,
                                                           approximator_shasum=self.shasum,
                                                           indexer_shasum=self.arguments['indexer_shasum'])
            if dcreated:
                dr.last_built = timezone.now()
                dr.save()

    def ensure(self):
        for m in self.files:
            dlpath = "{}/models/{}/{}".format(settings.MEDIA_ROOT, self.uuid, m['filename'])
            if not os.path.isfile(dlpath):
                fs.ensure("/models/{}/{}".format(self.uuid, m['filename']))


class Retriever(models.Model):
    """
    Here Exact is an L2 Flat retriever
    """
    EXACT = 'E'
    LOPQ = 'L'
    FAISS = 'F'
    MODES = (
        (LOPQ, 'LOPQ'),
        (EXACT, 'Exact'),
        (FAISS, 'FAISS'),
    )
    algorithm = models.CharField(max_length=1, choices=MODES, db_index=True, default=EXACT)
    name = models.CharField(max_length=200, default="")
    indexer_shasum = models.CharField(max_length=40, null=True)
    approximator_shasum = models.CharField(max_length=40, null=True)
    source_filters = JSONField()
    created = models.DateTimeField('date created', auto_now_add=True)


class Frame(models.Model):
    video = models.ForeignKey(Video)
    event = models.ForeignKey(TEvent)
    frame_index = models.IntegerField()
    name = models.CharField(max_length=200, null=True)
    h = models.IntegerField(default=0)
    w = models.IntegerField(default=0)
    t = models.FloatField(null=True)  # time in seconds for keyframes
    keyframe = models.BooleanField(default=False)  # is this a key frame for a video?
    segment_index = models.IntegerField(null=True)

    class Meta:
        unique_together = (("video", "frame_index"),)

    def __unicode__(self):
        return u'{}:{}'.format(self.video_id, self.frame_index)

    def path(self, media_root=None):
        if not (media_root is None):
            return "{}/{}/frames/{}.jpg".format(media_root, self.video_id, self.frame_index)
        else:
            return "{}/{}/frames/{}.jpg".format(settings.MEDIA_ROOT, self.video_id, self.frame_index)

    def original_path(self):
        return self.name

    def global_path(self):
        if self.video.dataset:
            if self.name and not self.name.startswith('/'):
                return self.name
            else:
                return "{}/{}".format(self.video.url, self.name)
        else:
            return "{}::{}".format(self.video.url, self.frame_index)


class Segment(models.Model):
    """
    A video segment useful for parallel dense decoding+processing as well as streaming
    """
    video = models.ForeignKey(Video)
    segment_index = models.IntegerField()
    start_time = models.FloatField(default=0.0)
    end_time = models.FloatField(default=0.0)
    event = models.ForeignKey(TEvent)
    metadata = models.TextField(default="{}")
    frame_count = models.IntegerField(default=0)
    start_index = models.IntegerField(default=0)
    framelist = JSONField(blank=True, null=True)

    class Meta:
        unique_together = (("video", "segment_index"),)

    def __unicode__(self):
        return u'{}:{}'.format(self.video_id, self.segment_index)

    def path(self, media_root=None):
        if not (media_root is None):
            return "{}/{}/segments/{}.mp4".format(media_root, self.video_id, self.segment_index)
        else:
            return "{}/{}/segments/{}.mp4".format(settings.MEDIA_ROOT, self.video_id, self.segment_index)


class Region(models.Model):
    """
    Any 2D region over an image.
    Detections & Transforms have an associated image data.
    """
    id = models.CharField(max_length=100, primary_key=True)
    ANNOTATION = constants.ANNOTATION
    DETECTION = constants.DETECTION
    SEGMENTATION = constants.SEGMENTATION
    TRANSFORM = constants.TRANSFORM
    POLYGON = constants.POLYGON
    REGION_TYPES = (
        (ANNOTATION, 'Annotation'),
        (DETECTION, 'Detection'),
        (POLYGON, 'Polygon'),
        (SEGMENTATION, 'Segmentation'),
        (TRANSFORM, 'Transform'),
    )
    region_type = models.CharField(max_length=1, choices=REGION_TYPES, db_index=True)
    video = models.ForeignKey(Video)
    user = models.ForeignKey(User, null=True)
    # frame = models.ForeignKey(Frame, null=True, on_delete=models.SET_NULL)
    # After significant deliberation I decided that having frame_index was sufficient and ensuring that this relation
    # is updated when frames are decoded breaks the immutability. Instead frame_index allows "lazy" relation enabling
    # cases such as user annotating a video frame which has not been decoded and stored explicitly as a Frame.
    event = models.ForeignKey(TEvent)  # TEvent that created this region
    frame_index = models.IntegerField(default=-1)
    segment_index = models.IntegerField(default=-1, null=True)
    # This ensures that for a specific event Regions are always ordered. (event_uuid, per_event_index) serves as
    # a global unique identifier.
    per_event_index = models.IntegerField()
    text = models.TextField(default="")
    metadata = JSONField(blank=True, null=True)
    full_frame = models.BooleanField(default=False)
    x = models.IntegerField(default=0)
    y = models.IntegerField(default=0)
    h = models.IntegerField(default=0)
    w = models.IntegerField(default=0)
    polygon_points = JSONField(blank=True, null=True)
    created = models.DateTimeField('date created', auto_now_add=True)
    object_name = models.CharField(max_length=100)
    confidence = models.FloatField(default=0.0)
    png = models.BooleanField(default=False)

    def path(self, media_root=None, temp_root=None):
        if temp_root:
            return "{}/{}_{}.jpg".format(temp_root, self.video_id, self.pk)
        elif not (media_root is None):
            return "{}/{}/regions/{}.jpg".format(media_root, self.video_id, self.pk)
        else:
            return "{}/{}/regions/{}.jpg".format(settings.MEDIA_ROOT, self.video_id, self.pk)

    def frame_path(self, media_root=None):
        if not (media_root is None):
            return "{}/{}/frames/{}.jpg".format(media_root, self.video_id, self.frame_index)
        else:
            return "{}/{}/frames/{}.jpg".format(settings.MEDIA_ROOT, self.video_id, self.frame_index)

    def crop_and_get_region_path(self, images, temp_root):
        bare_path = self.path(media_root="")
        cached_data = fs.get_from_cache(bare_path)
        region_path = self.path(temp_root=temp_root)
        if cached_data:
            with open(region_path, 'wb') as out:
                out.write(cached_data)
        else:
            fs.ensure(self.frame_path(""))
            frame_path = self.frame_path()
            if frame_path not in images:
                images[frame_path] = Image.open(frame_path)
            img2 = images[frame_path].crop((self.x, self.y, self.x + self.w, self.y + self.h))
            img2.save(region_path)
            with open(region_path, 'rb') as fr:
                fs.cache_path(bare_path, payload=fr.read())
        return region_path

    def global_frame_path(self):
        if self.video.dataset:
            df = Frame.objects.get(video=self.video, frame_index=self.frame_index)
            if df.name and not df.name.startswith('/'):
                return df.name
            else:
                return "{}/{}".format(self.video.url, df.name)
        else:
            return "{}::{}".format(self.video.url, self.frame_index)

    class Meta:
        unique_together = (("event", "per_event_index"),)


class QueryRegion(models.Model):
    """
    Any 2D region over a query image.
    """
    ANNOTATION = constants.ANNOTATION
    DETECTION = constants.DETECTION
    SEGMENTATION = constants.SEGMENTATION
    TRANSFORM = constants.TRANSFORM
    POLYGON = constants.POLYGON
    REGION_TYPES = (
        (ANNOTATION, 'Annotation'),
        (DETECTION, 'Detection'),
        (POLYGON, 'Polygon'),
        (SEGMENTATION, 'Segmentation'),
        (TRANSFORM, 'Transform'),
    )
    region_type = models.CharField(max_length=1, choices=REGION_TYPES, db_index=True)
    query = models.ForeignKey(DVAPQL)
    event = models.ForeignKey(TEvent)  # TEvent that created this region
    text = models.TextField(default="")
    metadata = JSONField(blank=True, null=True)
    full_frame = models.BooleanField(default=False)
    x = models.IntegerField(default=0)
    y = models.IntegerField(default=0)
    h = models.IntegerField(default=0)
    w = models.IntegerField(default=0)
    polygon_points = JSONField(blank=True, null=True)
    created = models.DateTimeField('date created', auto_now_add=True)
    object_name = models.CharField(max_length=100)
    confidence = models.FloatField(default=0.0)
    png = models.BooleanField(default=False)


class IndexEntries(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    video = models.ForeignKey(Video)
    uuid = models.UUIDField(default=uuid.uuid4, null=True)
    LMDB = constants.LMDB
    RAW = constants.RAW
    STORAGE_TYPES = (
        (LMDB, 'LMDB database'),
        (RAW, 'Entries'),
    )
    storage_type = models.CharField(max_length=1, choices=STORAGE_TYPES, db_index=True, default=RAW)
    entries = JSONField(blank=True, null=True)
    metadata = JSONField(blank=True, null=True)
    algorithm = models.CharField(max_length=100)
    features = models.CharField(max_length=40,null=True)
    indexer_shasum = models.CharField(max_length=40)
    approximator_shasum = models.CharField(max_length=40, null=True)
    target = models.CharField(max_length=100)
    count = models.IntegerField()
    approximate = models.BooleanField(default=False)
    created = models.DateTimeField('date created', auto_now_add=True)
    per_event_index = models.IntegerField()
    event = models.ForeignKey(TEvent)
    min_frame_index = models.IntegerField(null=True)
    max_frame_index = models.IntegerField(null=True)

    def __unicode__(self):
        return "{} in {} index by {}".format(self.target, self.algorithm, self.video.name)

    def npy_path(self, media_root=None):
        if media_root is None:
            media_root = settings.MEDIA_ROOT
        return "{}/{}/events/{}/{}.{}".format(media_root, self.video_id, self.event_id, str(self.uuid).replace('-','_'),
                                              self.features)

    def lmdb_path(self,media_root):
        if media_root is None:
            media_root = settings.MEDIA_ROOT
        dirname = self.event.get_dir()
        fs.ensure("{}{}".format(self.event.get_dir(media_root=""), str(self.uuid).replace('-', '_')),
                  {}, media_root)
        return "{}{}".format(dirname, str(self.uuid).replace('-', '_'))

    def get_vectors(self, media_root=None):
        if media_root is None:
            media_root = settings.MEDIA_ROOT
        video_dir = "{}/{}".format(media_root, self.video_id)
        if not os.path.isdir(video_dir):
            self.video.create_directory()
        event_dir = "{}/{}".format(media_root, self.video_id, self.event_id)
        if not os.path.isdir(event_dir):
            self.event.create_dir()
        dirnames = {}
        if self.features:
            fs.ensure(self.npy_path(media_root=''), dirnames, media_root)
            if self.features.endswith('npy'):
                vectors = np.load(self.npy_path(media_root))
            else:
                vectors = self.npy_path(media_root)
        else:
            return self.entries
        return vectors

    def get_entry(self, offset, media_root=None):
        if self.storage_type == self.LMDB:
            if self.pk not in OPENED_DBS:
                entries_fname = self.lmdb_path(media_root)
                OPENED_DBS[self.pk] = lmdb.open(entries_fname, max_dbs=0, subdir=False, readonly=True).begin(buffers=True)
            return json.loads(str(OPENED_DBS[self.pk].get(str(offset))))
        else:
            return self.entries[offset]

    def copy_entries(self, other_index_entries, event, media_root=None):
        other_index_entries.storage_type = self.storage_type
        if self.storage_type == self.LMDB:
            event.create_dir()
            this_entries_fname = self.lmdb_path(media_root)
            other_entries_fname = "{}{}".format(event.get_dir(), str(other_index_entries.uuid).replace('-', '_'))
            shutil.copy(this_entries_fname,other_entries_fname)
        else:
            other_index_entries.entries = self.entries

    def iter_entries(self,media_root=None):
        if self.storage_type == self.LMDB:
            entries_fname = self.lmdb_path(media_root)
            env = lmdb.open(entries_fname, max_dbs=0, subdir=False, readonly=True)
            entries = []
            with env.begin() as txn:
                with txn.cursor() as curs:
                    for k,v in curs:
                        entries.append((int(k),json.loads(str(v))))
            return [e for i,e in sorted(entries)]
        else:
            return self.entries

    def store_numpy_features(self, features, event):
        event.create_dir()
        self.features = 'npy'
        dirname = event.get_dir()
        feat_fname = "{}/{}.npy".format(dirname, str(self.uuid).replace('-', '_'))
        if type(features) is list:
            if features:
                self.metadata = {'shape':[len(features),]+list(features[0].shape)}
        else:
            self.metadata = {'shape': list(features.shape)}
        with open(feat_fname, 'w') as feats:
            np.save(feats, np.array(features))

    def store_entries(self, entries, event, use_lmdb=True):
        event.create_dir()
        dirname = event.get_dir()
        entries_fname = "{}/{}".format(dirname, str(self.uuid).replace('-', '_'))
        if use_lmdb and entries:
            self.storage_type = self.LMDB
            env = lmdb.open(entries_fname, max_dbs=0, subdir=False)
            with env.begin(write=True) as txn:
                for k, v in enumerate(entries):
                    txn.put(str(k), json.dumps(v))
            env.close()
        else:
            self.entries = entries

    def store_faiss_features(self, event):
        event.create_dir()
        feat_fname = "{}/{}.index".format(event.get_dir(), str(self.uuid).replace('-', '_'))
        self.features = 'index'
        return feat_fname


class Tube(models.Model):
    """
    A tube is a collection of sequential frames / regions that track a certain object
    or describe a specific scene
    """
    id = models.CharField(max_length=100, primary_key=True)
    video = models.ForeignKey(Video, null=True)
    frame_level = models.BooleanField(default=False)
    full_video = models.BooleanField(default=False)
    full_segment = models.BooleanField(default=False)
    start_frame_index = models.IntegerField()
    end_frame_index = models.IntegerField()
    start_region = models.ForeignKey(Region, null=True, related_name="start_region")
    end_region = models.ForeignKey(Region, null=True, related_name="end_region")
    text = models.TextField(default="")
    metadata = JSONField(blank=True, null=True)
    event = models.ForeignKey(TEvent)
    per_event_index = models.IntegerField()

    class Meta:
        unique_together = (("event", "per_event_index"),)


class RegionRelation(models.Model):
    """
    Captures relations between Regions within a video/dataset.
    """
    id = models.CharField(max_length=100, primary_key=True)
    video = models.ForeignKey(Video)
    source_region = models.ForeignKey(Region, related_name='source_region')
    target_region = models.ForeignKey(Region, related_name='target_region')
    event = models.ForeignKey(TEvent)
    name = models.CharField(max_length=400)
    weight = models.FloatField(null=True)
    metadata = JSONField(blank=True, null=True)
    per_event_index = models.IntegerField()

    class Meta:
        unique_together = (("event", "per_event_index"),)


class HyperRegionRelation(models.Model):
    """
    Captures relations between a Region in a video/dataset and an external globally addressed path / URL.
    HyperRegionRelation is an equivalent of anchor tags / hyperlinks.
    e.g. Region -> http://http://akshaybhat.com/static/img/akshay.jpg
    """
    id = models.CharField(max_length=100, primary_key=True)
    video = models.ForeignKey(Video)
    region = models.ForeignKey(Region)
    event = models.ForeignKey(TEvent)
    name = models.CharField(max_length=400)
    weight = models.FloatField(null=True)
    metadata = JSONField(blank=True, null=True)
    path = models.TextField()
    full_frame = models.BooleanField(default=False)
    x = models.IntegerField(default=0)
    y = models.IntegerField(default=0)
    h = models.IntegerField(default=0)
    w = models.IntegerField(default=0)
    # Unlike region frame_index is only required if the path points to a video or a .gif
    frame_index = models.IntegerField(null=True)
    segment_index = models.IntegerField(null=True)
    per_event_index = models.IntegerField()

    class Meta:
        unique_together = (("event", "per_event_index"),)


class HyperTubeRegionRelation(models.Model):
    """
    Captures relations between a Tube in a video/dataset and an external globally addressed path / URL.
    HyperTubeRegionRelation is an equivalent of anchor tags / hyperlinks.
    e.g. Tube -> http://http://akshaybhat.com/static/img/akshay.jpg
    """
    id = models.CharField(max_length=100, primary_key=True)
    video = models.ForeignKey(Video)
    tube = models.ForeignKey(Tube)
    event = models.ForeignKey(TEvent)
    name = models.CharField(max_length=400)
    weight = models.FloatField(null=True)
    metadata = JSONField(blank=True, null=True)
    path = models.TextField()
    full_frame = models.BooleanField(default=False)
    x = models.IntegerField(default=0)
    y = models.IntegerField(default=0)
    h = models.IntegerField(default=0)
    w = models.IntegerField(default=0)
    # Unlike region frame_index is only required if the path points to a video or a .gif
    frame_index = models.IntegerField(null=True)
    segment_index = models.IntegerField(null=True)
    per_event_index = models.IntegerField()

    class Meta:
        unique_together = (("event", "per_event_index"),)


class TubeRelation(models.Model):
    """
    Captures relations between Tubes within a video/dataset.
    """
    id = models.CharField(max_length=100, primary_key=True)
    video = models.ForeignKey(Video)
    source_tube = models.ForeignKey(Tube, related_name='source_tube')
    target_tube = models.ForeignKey(Tube, related_name='target_tube')
    event = models.ForeignKey(TEvent)
    name = models.CharField(max_length=400)
    weight = models.FloatField(null=True)
    metadata = JSONField(blank=True, null=True)
    per_event_index = models.IntegerField()

    class Meta:
        unique_together = (("event", "per_event_index"),)


class TubeRegionRelation(models.Model):
    """
    Captures relations between Tube and Region within a video/dataset.
    """
    id = models.CharField(max_length=100, primary_key=True)
    video = models.ForeignKey(Video)
    tube = models.ForeignKey(Tube)
    region = models.ForeignKey(Region)
    event = models.ForeignKey(TEvent)
    name = models.CharField(max_length=400)
    weight = models.FloatField(null=True)
    metadata = JSONField(blank=True, null=True)
    per_event_index = models.IntegerField()

    class Meta:
        unique_together = (("event", "per_event_index"),)


class DeletedVideo(models.Model):
    deleter = models.ForeignKey(User, related_name="user_deleter", null=True)
    video_uuid = models.UUIDField(default=uuid.uuid4, null=True)
    created = models.DateTimeField('date created', auto_now_add=True)

    def __unicode__(self):
        return u'Deleted {} by {}'.format(self.video_uuid, self.deleter)


class ManagementAction(models.Model):
    parent_task = models.CharField(max_length=500, default="")
    op = models.CharField(max_length=500, default="")
    host = models.CharField(max_length=500, default="")
    message = models.TextField()
    created = models.DateTimeField('date created', auto_now_add=True)
    ping_index = models.IntegerField(null=True)


class SystemState(models.Model):
    created = models.DateTimeField('date created', auto_now_add=True)
    retriever_stats = JSONField(blank=True, null=True)
    process_stats = JSONField(blank=True, null=True)
    worker_stats = JSONField(blank=True, null=True)
    redis_stats = JSONField(blank=True, null=True)
    queues = JSONField(blank=True, null=True)
    hosts = JSONField(blank=True, null=True)


class QueryResult(models.Model):
    query = models.ForeignKey(DVAPQL)
    retrieval_event = models.ForeignKey(TEvent)
    query_region = models.ForeignKey(QueryRegion, null=True)
    video = models.ForeignKey(Video)
    frame_index = models.IntegerField()
    region = models.ForeignKey(Region, null=True)
    tube = models.ForeignKey(Tube, null=True)
    rank = models.IntegerField()
    algorithm = models.CharField(max_length=100)
    distance = models.FloatField(default=0.0)


class Export(models.Model):
    MODEL_EXPORT = constants.MODEL_EXPORT
    VIDEO_EXPORT = constants.VIDEO_EXPORT
    EXPORT_TYPES = (
        (MODEL_EXPORT, 'Model export'),
        (VIDEO_EXPORT, 'Video export'),
    )
    export_type = models.CharField(max_length=1, choices=EXPORT_TYPES, db_index=True)
    event = models.ForeignKey(TEvent)
    url = models.TextField(default="")
    created = models.DateTimeField('date created', auto_now_add=True)


class TaskRestart(models.Model):
    original_event_pk = models.UUIDField(default=uuid.uuid4, null=False)
    launched_event_pk = models.UUIDField(default=uuid.uuid4, null=False)
    attempts = models.IntegerField(default=0)
    arguments = JSONField(blank=True, null=True)
    operation = models.CharField(max_length=100, default="")
    queue = models.CharField(max_length=100, default="")
    exception = models.TextField(default="")
    # We don't want to to associate it with the video as result there is no relation but instead we store UUID
    video_uuid = models.UUIDField(default=uuid.uuid4, null=True)
    process = models.ForeignKey(DVAPQL)
    created = models.DateTimeField('date created', auto_now_add=True)
