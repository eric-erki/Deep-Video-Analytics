import logging, uuid, tempfile
from django.conf import settings
import lmdb

try:
    from dvalib import indexer, retriever
    import numpy as np
except ImportError:
    np = None
    logging.warning("Could not import indexer / clustering assuming running in front-end mode")

from ..models import IndexEntries, TrainedModel


class Indexers(object):
    _visual_indexer = {}
    _shasum_to_index = {}
    _name_to_index = {}
    _session = None

    @classmethod
    def get_index_by_name(cls,name):
        if name not in Indexers._name_to_index:
            di = TrainedModel.objects.get(name=name,model_type=TrainedModel.INDEXER)
            Indexers._name_to_index[name] = di
        else:
            di = Indexers._name_to_index[name]
        return cls.get_index(di),di
    
    @classmethod
    def get_index_by_pk(cls,pk):
        di = TrainedModel.objects.get(pk=pk)
        if di.model_type != TrainedModel.INDEXER:
            raise ValueError("Model {} id: {} is not an Indexer".format(di.name,di.pk))
        return cls.get_index(di),di
    
    @classmethod
    def get_indexer_by_shasum(cls,shasum):
        if shasum not in Indexers._shasum_to_index:
            di = TrainedModel.objects.get(shasum=shasum,model_type=TrainedModel.INDEXER)
            Indexers._shasum_to_index[shasum] = di
        else:
            di = Indexers._shasum_to_index[shasum]
        return di

    @classmethod
    def get_index(cls,di):
        di.ensure()
        if di.pk not in Indexers._visual_indexer:
            iroot = "{}/models/".format(settings.MEDIA_ROOT)
            if di.name == 'inception':
                Indexers._visual_indexer[di.pk] = indexer.InceptionIndexer(iroot + "{}/network.pb".format(di.uuid))
            elif di.name == 'facenet':
                Indexers._visual_indexer[di.pk] = indexer.FacenetIndexer(iroot + "{}/facenet.pb".format(di.uuid))
            elif di.algorithm == 'vgg':
                Indexers._visual_indexer[di.pk] = indexer.VGGIndexer(iroot + "{}/{}".format(di.uuid,di.files[0]['filename']))
            else:
                raise ValueError,"unregistered indexer with id {}".format(di.pk)
        return Indexers._visual_indexer[di.pk]

    @classmethod
    def index_queryset(cls,di,visual_index,event,target, queryset, cloud_paths=False):
        visual_index.load()
        index_entries = []
        temp_root = tempfile.mkdtemp()
        entries, paths, images = [], [], {}
        for i, df in enumerate(queryset):
            if target == 'frames':
                entry = df.frame_index
                if cloud_paths:
                    paths.append(df.path('{}://{}'.format(settings.CLOUD_FS_PREFIX,settings.MEDIA_BUCKET)))
                else:
                    paths.append(df.path())
            elif target == 'segments':
                entry = df.segment_index
                if cloud_paths:
                    paths.append(df.path('{}://{}'.format(settings.CLOUD_FS_PREFIX,settings.MEDIA_BUCKET)))
                else:
                    paths.append(df.path())
            else:
                entry = df.pk
                if df.full_frame:
                    paths.append(df.frame_path())
                else:
                    paths.append(df.crop_and_get_region_path(images,temp_root))
            entries.append(entry)
        if entries:
            logging.info(paths)  # adding temporary logging to check whether s3:// paths are being correctly used.
            # TODO Ensure that "full frame"/"regions" are not repeatedly indexed.
            features = visual_index.index_paths(paths)
            uid = str(uuid.uuid1()).replace('-','_')
            event.create_dir()
            feat_fname = "{}/{}.npy".format(event.get_dir(),uid)
            entries_fname = "{}/{}".format(event.get_dir(),uid)
            env = lmdb.open(entries_fname, max_dbs=0, subdir=False)
            with env.begin(write=True) as txn:
                for k, v in enumerate(entries):
                    txn.put(str(k),str(v))
            env.close()
            with open(feat_fname, 'w') as feats:
                np.save(feats, np.array(features))
            i = IndexEntries()
            i.video_id = event.video_id
            i.count = len(entries)
            i.target = target
            i.algorithm = di.name
            i.indexer_shasum = di.shasum
            i.entries = entries
            i.features_file_name = feat_fname.split('/')[-1]
            i.event_id = event.pk
            i.source_filter_json = event.arguments
            index_entries.append(i)
        event.finalize({'IndexEntries':index_entries})
