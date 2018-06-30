import numpy as np
from scipy import spatial
from collections import namedtuple, defaultdict
import uuid
import sys

import logging

try:
    from sklearn.decomposition import PCA
    from lopq import LOPQModel, LOPQSearcher
    from lopq.eval import compute_all_neighbors, get_recall
    from lopq.model import eigenvalue_allocation
    from lopq.utils import compute_codes_parallel
except ImportError:
    logging.warning("Could not import lopq")

try:
    sys.path.append('/root/thirdparty/faiss/python')
    import faiss
except ImportError:
    logging.warning("could not import FAISS")

IndexRange = namedtuple('IndexRange', ['start', 'end'])


class BaseRetriever(object):

    def __init__(self, name, approximator=None, algorithm="EXACT"):
        self.name = name
        self.algorithm = algorithm
        self.approximate = False
        self.approximator = approximator
        self.net = None
        self.loaded_entries = {}
        self.index, self.files, self.findex = None, {}, 0
        self.support_batching = False

    def load_index(self, numpy_matrix, entries):
        temp_index = [numpy_matrix, ]
        for i, e in enumerate(entries):
            self.files[self.findex] = e
            self.findex += 1
        if self.index is None:
            self.index = np.atleast_2d(np.concatenate(temp_index).squeeze())
            logging.info(self.index.shape)
        else:
            self.index = np.concatenate([self.index, np.atleast_2d(np.concatenate(temp_index).squeeze())])
            logging.info(self.index.shape)

    def nearest(self, vector=None, n=12):
        dist = None
        results = []
        if self.approximator:
            vector = np.atleast_2d(self.approximator.approximate(vector))
        if self.index is not None:
            try:
                dist = spatial.distance.cdist(vector, self.index)
            except:
                raise ValueError("Could not compute distance Vector shape {} and index shape {}".format(vector.shape,
                                                                                                        self.index.shape))
        if dist is not None:
            ranked = np.squeeze(dist.argsort())
            for i, k in enumerate(ranked[:n]):
                temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[0, k])}
                temp.update(self.files[k])
                results.append(temp)
        return results  # Next also return computed query_vector


class LOPQRetriever(BaseRetriever):

    def __init__(self, name, approximator):
        super(LOPQRetriever, self).__init__(name=name, approximator=approximator, algorithm="LOPQ")
        self.approximate = True
        self.name = name
        self.loaded_entries = {}
        self.entries = []
        self.support_batching = False
        self.approximator = approximator
        self.approximator.load()
        self.searcher = LOPQSearcher(model=self.approximator.model)

    def load_index(self, numpy_matrix=None, entries=None):
        codes = []
        ids = []
        last_index = len(self.entries)
        for i, e in enumerate(entries):
            codes.append((tuple(e['codes'][0]), tuple(e['codes'][1])))
            ids.append(i + last_index)
            self.entries.append(e)
        self.searcher.add_codes(codes, ids)

    def nearest(self, vector=None, n=12):
        results = []
        pca_vec = self.approximator.get_pca_vector(vector)
        results_indexes, visited = self.searcher.search(pca_vec, quota=n)
        for r in results_indexes:
            results.append(self.entries[r.id])
        return results


class FaissApproximateRetriever(BaseRetriever):

    def __init__(self, name, approximator):
        super(FaissApproximateRetriever, self).__init__(name=name, approximator=approximator, algorithm="FAISS")
        self.index_path = str(approximator.index_path).replace('//', '/')
        self.ivfs = []
        self.ivf_vector = faiss.InvertedListsPtrVector()
        self.uuid = str(uuid.uuid4()).replace('-', '_')
        self.faiss_index = None

    def load_index(self, computed_index_path, entries):
        if len(entries):
            computed_index_path = str(computed_index_path).replace('//', '/')
            logging.info("Adding {}".format(computed_index_path))
            for i, e in enumerate(entries):
                self.files[self.findex] = e
                self.findex += 1
            if self.faiss_index is None:
                self.faiss_index = faiss.read_index(computed_index_path)
            else:
                index = faiss.read_index(computed_index_path)
                self.faiss_index.merge_from(index, self.faiss_index.ntotal)
            logging.info("Index size {}".format(self.faiss_index.ntotal))

    def nearest(self, vector=None, n=12, nprobe=16):
        logging.info("Index size {} with {} loaded entries in {}".format(self.faiss_index.ntotal,
                                                                         len(self.loaded_entries), self.name))
        self.faiss_index.nprobe = nprobe
        vector = np.atleast_2d(vector)
        if vector.shape[-1] != self.faiss_index.d:
            vector = vector.T
        results = []
        dist, ids = self.faiss_index.search(vector, n)
        for i, k in enumerate(ids[0]):
            temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[0, i])}
            temp.update(self.files[k])
            results.append(temp)
        return results

    def nearest_batch(self, vectors=None, n=12, nprobe=16):
        self.faiss_index.nprobe = nprobe
        vectors = np.atleast_2d(vectors)
        if vectors.shape[-1] != self.faiss_index.d:
            vectors = vectors.T
        dist, ids = self.faiss_index.search(vectors, n)
        results = defaultdict(list)
        for vindex in range(ids.shape[0]):
            for i, k in enumerate(ids[vindex]):
                temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[vindex, i])}
                temp.update(self.files[k])
                results[vindex].append(temp)
        return results


class FaissFlatRetriever(BaseRetriever):

    def __init__(self, name, components, metric='Flat'):
        super(FaissFlatRetriever, self).__init__(name=name, algorithm="FAISS_{}".format(metric))
        self.name = name
        self.components = components
        self.algorithm = "FAISS_{}".format(metric)
        self.faiss_index = faiss.index_factory(components, metric)

    def load_index(self, numpy_matrix, entries):
        if len(entries):
            logging.info("Adding {}".format(numpy_matrix.shape))
            numpy_matrix = np.atleast_2d(numpy_matrix.squeeze())
            for i, e in enumerate(entries):
                self.files[self.findex] = e
                self.findex += 1
            self.faiss_index.add(numpy_matrix)
            logging.info("Index size {}".format(self.faiss_index.ntotal))

    def nearest(self, vector=None, n=12):
        vector = np.atleast_2d(vector)
        if vector.shape[-1] != self.components:
            vector = vector.T
        results = []
        dist, ids = self.faiss_index.search(vector, n)
        for i, k in enumerate(ids[0]):
            temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[0, i])}
            if k in self.files:
                temp.update(self.files[k])
            else:
                raise ValueError("Retrieval error {}".format((i,k,list(enumerate(ids[0])),self.files)))
            results.append(temp)
        return results

    def nearest_batch(self, vectors=None, n=12):
        vectors = np.atleast_2d(vectors)
        if vectors.shape[-1] != self.components:
            vectors = vectors.T
        dist, ids = self.faiss_index.search(vectors, n)
        results = defaultdict(list)
        for vindex in range(ids.shape[0]):
            for i, k in enumerate(ids[vindex]):
                temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[vindex, i])}
                temp.update(self.files[k])
                results[vindex].append(temp)
        return results
