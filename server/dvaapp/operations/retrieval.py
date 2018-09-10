import logging
from .approximation import Approximators
from .indexing import Indexers
from collections import defaultdict

try:
    from dvalib import indexer, retriever
    import numpy as np
except ImportError:
    np = None
    logging.warning("Could not import indexer / clustering assuming running in front-end mode")

from ..models import IndexEntries, QueryResult, Region, Retriever


class Retrievers(object):
    _visual_retriever = {}
    _retriever_object = {}
    _selector_to_dr = {}
    _index_entries = {}
    _index_count = defaultdict(int)

    @classmethod
    def get_retriever(cls, args):
        selector = args['retriever_selector']
        if str(selector) in cls._selector_to_dr:
            dr = cls._selector_to_dr[str(selector)]
        else:
            dr = Retriever.objects.get(**selector)
            cls._selector_to_dr[str(selector)] = dr
        retriever_pk = dr.pk
        if retriever_pk not in cls._visual_retriever:
            cls._retriever_object[retriever_pk] = dr
            if dr.algorithm == Retriever.EXACT and dr.approximator_shasum and dr.approximator_shasum.strip():
                approximator, da = Approximators.get_trained_model(
                    {"trainedmodel_selector": {"shasum": dr.approximator_shasum}})
                da.ensure()
                approximator.load()
                cls._visual_retriever[retriever_pk] = retriever.SimpleRetriever(name=dr.name, approximator=approximator)
            elif dr.algorithm == Retriever.EXACT:
                cls._visual_retriever[retriever_pk] = retriever.SimpleRetriever(name=dr.name)
            elif dr.algorithm == Retriever.FAISS and dr.approximator_shasum is None:
                _, di = Indexers.get_trained_model({"trainedmodel_selector": {"shasum": dr.indexer_shasum}})
                cls._visual_retriever[retriever_pk] = retriever.FaissFlatRetriever(name=dr.name,
                                                                                   components=di.arguments[
                                                                                       'components'])
            elif dr.algorithm == Retriever.FAISS:
                approximator, da = Approximators.get_trained_model(
                    {"trainedmodel_selector": {"shasum": dr.approximator_shasum}})
                da.ensure()
                approximator.load()
                cls._visual_retriever[retriever_pk] = retriever.FaissApproximateRetriever(name=dr.name,
                                                                                          approximator=approximator)
            elif dr.algorithm == Retriever.LOPQ:
                approximator, da = Approximators.get_trained_model(
                    {"trainedmodel_selector": {"shasum": dr.approximator_shasum}})
                da.ensure()
                approximator.load()
                cls._visual_retriever[retriever_pk] = retriever.LOPQRetriever(name=dr.name, approximator=approximator)

            else:
                raise ValueError("{} not valid retriever algorithm".format(dr.algorithm))
        return cls._visual_retriever[retriever_pk], cls._retriever_object[retriever_pk]

    @classmethod
    def refresh_index(cls, dr):
        # TODO improve this by either having a separate broadcast queues or using redis
        last_count = cls._index_count[dr.pk]
        current_count = IndexEntries.objects.count()
        visual_index = cls._visual_retriever[dr.pk]
        if last_count == 0 or last_count != current_count or len(visual_index.loaded_entries) == 0:
            cls._index_count[dr.pk] = current_count
            cls.update_index(dr)
        return len(visual_index.loaded_entries), visual_index.findex

    @classmethod
    def update_index(cls, dr):
        source_filters = dr.source_filters.copy()
        # Only select entries with completed events, otherwise indexes might not be synced or complete.
        source_filters['event__completed'] = True
        if dr.indexer_shasum:
            source_filters['indexer_shasum'] = dr.indexer_shasum
        if dr.approximator_shasum:
            source_filters['approximator_shasum'] = dr.approximator_shasum
        else:
            source_filters['approximator_shasum'] = None  # Required otherwise approximate index entries are selected
        index_entries = IndexEntries.objects.filter(**source_filters)
        visual_index = cls._visual_retriever[dr.pk]
        for index_entry in index_entries:
            if index_entry.pk not in visual_index.loaded_entries and index_entry.count > 0:
                cls.add_index_entry(index_entry, visual_index)

    @classmethod
    def add_index_entry(cls, index_entry, visual_index):
        if index_entry.pk not in cls._index_entries:
            cls._index_entries[index_entry.pk] = index_entry
        if visual_index.algorithm == "LOPQ":
            entries = index_entry.get_vectors()
            logging.info("loading approximate index {}".format(index_entry.pk))
            visual_index.add_entries(entries, index_entry.video_id, index_entry.target)
            visual_index.loaded_entries.add(index_entry.pk)
        elif visual_index.algorithm == 'FAISS':
            index_file_path = index_entry.get_vectors()
            logging.info("loading FAISS index {}".format(index_entry.pk))
            visual_index.add_vectors(index_file_path, index_entry.count, index_entry.pk)
        else:
            vectors = index_entry.get_vectors()
            logging.info("Starting {} in {} with shape {}".format(index_entry.video_id, visual_index.name,
                                                                  vectors.shape))
            try:
                visual_index.add_vectors(vectors, index_entry.count, index_entry.pk)
            except:
                logging.info("ERROR Failed to load {} vectors shape {} entries {}".format(
                    index_entry.video_id, vectors.shape, index_entry.count))
            else:
                logging.info("finished {} in {}".format(index_entry.pk, visual_index.name))

    @classmethod
    def retrieve(cls, event, index_retriever, dr, vector, count, region_pk=None):
        cls.refresh_index(dr)
        if 'nprobe' in event.arguments:
            results = index_retriever.nearest(vector=vector, n=count, nprobe=event.arguments['nprobe'])
        else:
            results = index_retriever.nearest(vector=vector, n=count)
        qr_batch = []
        for rank, r in enumerate(results):
            if 'indexentries_pk' in r:
                di = cls._index_entries[r['indexentries_pk']]
                r['type'] = di.target
                r['video'] = di.video_id
                r['id'] = di.get_entry(r['offset'])
            qr = QueryResult()
            if region_pk:
                qr.query_region_id = region_pk
            qr.query = event.parent_process
            qr.retrieval_event_id = event.pk
            if r['type'] == 'regions':
                dd = Region.objects.get(pk=r['id'])
                qr.region = dd
                qr.frame_index = dd.frame_index
                qr.video_id = dd.video_id
            elif r['type'] == 'frames':
                qr.frame_index = int(r['id'])
                qr.video_id = r['video']
            else:
                raise ValueError("No key found {}".format(r))
            qr.algorithm = dr.algorithm
            qr.rank = int(r.get('rank', rank + 1))
            qr.distance = int(r.get('dist', rank + 1))
            qr_batch.append(qr)
        if region_pk:
            event.finalize_query({"QueryResult": qr_batch},
                                 results={region_pk: {"retriever_state": index_retriever.findex}})
        else:
            event.finalize_query({"QueryResult": qr_batch}, results={"retriever_state": index_retriever.findex})
        event.parent_process.results_available = True
        event.parent_process.save()
        return 0
