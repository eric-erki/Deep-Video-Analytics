import logging, uuid, json
from django.conf import settings

try:
    from dvalib import approximator
    import numpy as np
except ImportError:
    np = None
    logging.warning("Could not import indexer / clustering assuming running in front-end mode")

from ..models import TrainedModel, IndexEntries


class Approximators(object):
    _index_approximator = {}
    _selector_to_model = {}
    _session = None

    @classmethod
    def get_trained_model(cls,args):
        selector = args['trainedmodel_selector']
        if not str(selector) in cls._selector_to_model:
            di = TrainedModel.objects.get(**selector)
            cls._selector_to_model[str(selector)] = (cls.get_approximator(di), di)
        return cls._selector_to_model[str(selector)]
    
    @classmethod
    def get_approximator(cls,di):
        di.ensure()
        if di.pk not in Approximators._index_approximator:
            model_dirname = "{}/models/{}".format(settings.MEDIA_ROOT, di.uuid)
            if di.algorithm == 'LOPQ':
                Approximators._index_approximator[di.pk] = approximator.LOPQApproximator(di.name, model_dirname)
            elif di.algorithm == 'PCA':
                Approximators._index_approximator[di.pk] = approximator.PCAApproximator(di.name, model_dirname)
            elif di.algorithm == 'FAISS':
                Approximators._index_approximator[di.pk] = approximator.FAISSApproximator(di.name, model_dirname)
            else:
                raise ValueError,"unknown approximator algorithm {} for pk : {}".format(di.algorithm,di.pk)
        return Approximators._index_approximator[di.pk]

    @classmethod
    def approximate_queryset(cls,approx,da,queryset,event):
        new_approx_indexes = []
        for index_entry in queryset:
            approx_ind = IndexEntries()
            vectors = index_entry.get_vectors()
            if da.algorithm == 'LOPQ':
                new_entries = []
                for i, e in enumerate(index_entry.iter_entries()):
                    new_entries.append((e,approx.approximate(vectors[i, :])))
                approx_ind.entries = new_entries
            elif da.algorithm == 'PCA':
                # TODO optimize this by doing matmul rather than calling for each entry
                event.create_dir()
                approx_vectors = np.array([approx.approximate(vectors[i, :]) for i in range(index_entry.count)])
                approx_ind.store_numpy_features(approx_vectors,event)
                index_entry.copy_entries(approx_ind, event)
            elif da.algorithm == "FAISS":
                feat_fname = approx_ind.store_faiss_features(event)
                approx.approximate_batch(np.atleast_2d(vectors.squeeze()),feat_fname)
                index_entry.copy_entries(approx_ind, event)
            else:
                raise NotImplementedError("unknown approximation algorithm {}".format(da.algorithm))
            approx_ind.indexer_shasum = index_entry.indexer_shasum
            approx_ind.approximator_shasum = da.shasum
            approx_ind.count = index_entry.count
            approx_ind.approximate = True
            approx_ind.min_frame_index = index_entry.min_frame_index
            approx_ind.max_frame_index = index_entry.max_frame_index
            approx_ind.target = index_entry.target
            approx_ind.video_id = index_entry.video_id
            approx_ind.algorithm = da.name
            approx_ind.event_id = event.pk
            new_approx_indexes.append(approx_ind)
        event.finalize({'IndexEntries':new_approx_indexes})
