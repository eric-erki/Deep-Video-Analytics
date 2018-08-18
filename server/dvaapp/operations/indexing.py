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
    _selector_to_model = {}
    _session = None

    @classmethod
    def get_trained_model(cls,args):
        selector = args['trainedmodel_selector']
        if not str(selector) in cls._selector_to_model:
            di = TrainedModel.objects.get(**selector)
            cls._selector_to_model[str(selector)] = (cls.get_index(di), di)
        return cls._selector_to_model[str(selector)]

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
        frame_indexes = set()
        temp_root = tempfile.mkdtemp()
        entries, paths, images = [], [], {}
        for i, df in enumerate(queryset):
            if target == 'frames':
                entry = df.frame_index
                frame_indexes.add(df.frame_index)
                if cloud_paths:
                    paths.append(df.path('{}://{}'.format(settings.CLOUD_FS_PREFIX,settings.MEDIA_BUCKET)))
                else:
                    paths.append(df.path())
            elif target == 'segments':
                entry = df.segment_index
                frame_indexes.add(df.start_frame_index)
                frame_indexes.add(df.start_frame_index + df.frame_count - 1)
                if cloud_paths:
                    paths.append(df.path('{}://{}'.format(settings.CLOUD_FS_PREFIX,settings.MEDIA_BUCKET)))
                else:
                    paths.append(df.path())
            else:
                entry = df.pk
                frame_indexes.add(df.frame_index)
                if df.full_frame:
                    paths.append(df.frame_path())
                else:
                    paths.append(df.crop_and_get_region_path(images,temp_root))
            entries.append(entry)
        if entries:
            logging.info(paths)  # adding temporary logging to check whether s3:// paths are being correctly used.
            # TODO Ensure that "full frame"/"regions" are not repeatedly indexed.
            features = visual_index.index_paths(paths)
            i = IndexEntries()
            i.store_numpy_features(features,event)
            i.store_entries(entries,event)
            i.video_id = event.video_id
            i.count = len(entries)
            i.min_frame_index = min(frame_indexes)
            i.max_frame_index = max(frame_indexes)
            i.target = target
            i.algorithm = di.name
            i.indexer_shasum = di.shasum
            i.event_id = event.pk
            i.source_filter_json = event.arguments
            index_entries.append(i)
        event.finalize({'IndexEntries':index_entries})
