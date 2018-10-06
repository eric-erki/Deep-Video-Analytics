import numpy as np
from scipy import spatial
from collections import defaultdict
from .intervaltree import IntervalTree
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
except:
    logging.warning("could not import FAISS")


class SimpleRetriever(object):

    def __init__(self, name, approximator=None, algorithm="EXACT"):
        self.name = name
        self.algorithm = algorithm
        self.approximate = False
        self.approximator = approximator
        self.net = None
        self.loaded_entries = set()
        self.index = None
        self.findex = 0
        self.tree = IntervalTree()
        self.support_batching = False

    def add_vectors(self, numpy_matrix, count, pk):
        self.loaded_entries.add(pk)
        if count:
            self.tree.addi(self.findex, self.findex + count, pk)
            self.findex += count
            temp_index = [numpy_matrix, ]
            if self.index is None:
                self.index = np.atleast_2d(np.concatenate(temp_index).squeeze())
                logging.info(self.index.shape)
            else:
                self.index = np.concatenate([self.index, np.atleast_2d(np.concatenate(temp_index).squeeze())])
                logging.info(self.index.shape)

    def nearest(self, vector=None, n=12, nprobe=None):
        dist = None
        results = []
        if self.approximator:
            vector = np.atleast_2d(self.approximator.approximate(vector))
        if self.index is not None:
            try:
                dist = spatial.distance.cdist(vector, self.index)
            except:
                raise ValueError("Could not compute dist Vector {} and shape {}".format(vector.shape, self.index.shape))
        if dist is not None:
            ranked = np.squeeze(dist.argsort())
            for i, k in enumerate(ranked[:n]):
                index_entry = sorted(self.tree[k])[0]
                temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[0, i]),
                        'indexentries_pk':index_entry.data, 'offset':k - index_entry.begin}
                results.append(temp)
        return results


class LOPQRetriever(object):
    """ Deprecated and soon to be removed """

    def __init__(self, name, approximator):
        self.approximate = True
        self.name = name
        self.algorithm = "LOPQ"
        self.loaded_entries = set()
        self.entries = []
        self.support_batching = False
        self.approximator = approximator
        self.approximator.load()
        self.findex = 0
        self.searcher = LOPQSearcher(model=self.approximator.model)

    def add_entries(self, entries, video_id, entry_type):
        codes = []
        ids = []
        last_index = len(self.entries)
        self.findex = last_index
        for i, e in enumerate(entries):
            codes.append((tuple(e[1][0]), tuple(e[1][1])))
            ids.append(i + last_index)
            self.entries.append({"id":e[0],"type":entry_type,"video":video_id})
        self.searcher.add_codes(codes, ids)

    def nearest(self, vector=None, n=12, nprobe=None):
        results = []
        pca_vec = self.approximator.get_pca_vector(vector)
        results_indexes, visited = self.searcher.search(pca_vec, quota=n)
        for r in results_indexes:
            results.append(self.entries[r.id])
        return results


class FaissApproximateRetriever(object):

    def __init__(self, name, approximator):
        self.name = name
        self.index_path = str(approximator.index_path).replace('//', '/')
        self.ivfs = []
        self.ivf_vector = faiss.InvertedListsPtrVector()
        self.algorithm = "FAISS"
        self.uuid = str(uuid.uuid4()).replace('-', '_')
        self.faiss_index = None
        self.tree = IntervalTree()
        self.loaded_entries = set()
        self.findex = 0

    def add_vectors(self, computed_index_path, count, pk):
        self.loaded_entries.add(pk)
        if count:
            computed_index_path = str(computed_index_path).replace('//', '/')
            logging.info("Adding {}".format(computed_index_path))
            self.tree.addi(self.findex, self.findex + count, pk)
            self.findex += count

            if self.faiss_index is None:
                self.faiss_index = faiss.read_index(computed_index_path)
            else:
                index = faiss.read_index(computed_index_path)
                if type(self.faiss_index) == faiss.swigfaiss.IndexPreTransform:
                    faiss.merge_into(self.faiss_index,index,True)
                else:
                    self.faiss_index.merge_from(index, self.faiss_index.ntotal)
            logging.info("Index size {}".format(self.faiss_index.ntotal))

    def nearest(self, vector=None, n=12, nprobe=16):
        logging.info("Index size {} with {} loaded entries in {}".format(self.faiss_index.ntotal,
                                                                         len(self.loaded_entries), self.name))
        if type(self.faiss_index) == faiss.swigfaiss.IndexPreTransform:
            index_ivf = faiss.downcast_index(self.faiss_index.index)
            index_ivf.nprobe = nprobe
        else:
            self.faiss_index.nprobe = nprobe
        vector = np.atleast_2d(vector)
        if vector.shape[-1] != self.faiss_index.d:
            vector = vector.T
        results = []
        dist, ids = self.faiss_index.search(vector, n)
        for i, k in enumerate(ids[0]):
            if k >= 0:
                index_entry = sorted(self.tree[k])[0]
                temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[0, i]),
                        'indexentries_pk': index_entry.data, 'offset': k - index_entry.begin}
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
                if k >= 0:
                    index_entry = sorted(self.tree[k])[0]
                    temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[vindex, i]),
                            'indexentries_pk':index_entry.data, 'offset':k - index_entry.begin}
                    results[vindex].append(temp)
        return results


class FaissFlatRetriever(object):

    def __init__(self, name, components, metric='Flat'):
        self.findex = 0
        self.name = name
        self.tree = IntervalTree()
        self.components = components
        self.algorithm = "FAISS_{}".format(metric)
        self.loaded_entries = set()
        self.faiss_index = faiss.index_factory(components, metric)

    def add_vectors(self, numpy_matrix, count, pk):
        self.loaded_entries.add(pk)
        if count:
            self.tree.addi(self.findex, self.findex + count, pk)
            self.findex += count
            logging.info("Adding {}".format(numpy_matrix.shape))
            numpy_matrix = np.atleast_2d(numpy_matrix.squeeze())
            self.faiss_index.add(numpy_matrix)
            logging.info("Index size {}".format(self.faiss_index.ntotal))

    def nearest(self, vector=None, n=12, nprobe=None):
        vector = np.atleast_2d(vector)
        if vector.shape[-1] != self.components:
            vector = vector.T
        results = []
        dist, ids = self.faiss_index.search(vector, n)
        for i, k in enumerate(ids[0]):
            if k >= 0:
                index_entry = sorted(self.tree[k])[0]
                temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[0, i]),
                        'indexentries_pk':index_entry.data, 'offset':k - index_entry.begin}
                results.append(temp)
        return results

    def nearest_batch(self, vectors=None, n=12, nprobe=None):
        vectors = np.atleast_2d(vectors)
        if vectors.shape[-1] != self.components:
            vectors = vectors.T
        dist, ids = self.faiss_index.search(vectors, n)
        results = defaultdict(list)
        for vindex in range(ids.shape[0]):
            for i, k in enumerate(ids[vindex]):
                if k >= 0:
                    index_entry = sorted(self.tree[k])[0]
                    temp = {'rank': i + 1, 'algo': self.name, 'dist': float(dist[vindex, i]),
                            'indexentries_pk':index_entry.data, 'offset':k - index_entry.begin}
                    results[vindex].append(temp)
        return results
