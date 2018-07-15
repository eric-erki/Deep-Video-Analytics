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
    _session = None

    @classmethod
    def get_trained_model(cls,args):
        di = TrainedModel.objects.get(**args['trainedmodel_selector'])
        if di.model_type != TrainedModel.APPROXIMATOR:
            raise ValueError("Model {} id: {} is not an Approximator".format(di.name,di.pk))
        return cls.get_approximator(di),di
    
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
                raise ValueError,"unknown approximator type {}".format(di.pk)
        return Approximators._index_approximator[di.pk]

    @classmethod
    def approximate_queryset(cls,approx,da,queryset,event):
        new_approx_indexes = []
        for index_entry in queryset:
            uid = str(uuid.uuid1()).replace('-', '_')
            approx_ind = IndexEntries()
            vectors, entries = index_entry.load_index()
            if da.algorithm == 'LOPQ':
                new_entries = []
                for i, e in enumerate(entries):
                    new_entries.append((e,approx.approximate(vectors[i, :])))
                approx_ind.entries = new_entries
                approx_ind.features_file_name = ""
            elif da.algorithm == 'PCA':
                # TODO optimize this by doing matmul rather than calling for each entry
                event.create_dir()
                approx_vectors = np.array([approx.approximate(vectors[i, :]) for i, e in enumerate(entries)])
                feat_fname = "{}/{}.npy".format(event.get_dir(), uid)
                with open(feat_fname, 'w') as featfile:
                    np.save(featfile, approx_vectors)
                approx_ind.features_file_name = "{}.npy".format(uid)
                approx_ind.entries = entries
            elif da.algorithm == "FAISS":
                event.create_dir()
                feat_fname = "{}/{}.index".format(event.get_dir(), uid)
                approx.approximate_batch(np.atleast_2d(vectors.squeeze()),feat_fname)
                approx_ind.features_file_name = "{}.index".format(uid)
                approx_ind.entries = entries
            else:
                raise NotImplementedError("unknown approximation algorithm {}".format(da.algorithm))
            approx_ind.indexer_shasum = index_entry.indexer_shasum
            approx_ind.approximator_shasum = da.shasum
            approx_ind.count = index_entry.count
            approx_ind.approximate = True
            approx_ind.target = index_entry.target
            approx_ind.video_id = index_entry.video_id
            approx_ind.algorithm = da.name
            approx_ind.event_id = event.pk
            new_approx_indexes.append(approx_ind)
        event.finalize({'IndexEntries':new_approx_indexes})
