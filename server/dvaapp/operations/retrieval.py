import logging
from .approximation import Approximators
from .indexing import Indexers

try:
    from dvalib import indexer, retriever
    import numpy as np
except ImportError:
    np = None
    logging.warning("Could not import indexer / clustering assuming running in front-end mode")

from ..models import IndexEntries, QueryResult, Region, Retriever, Frame


class Retrievers(object):
    _visual_retriever = {}
    _retriever_object = {}
    _index_count = 0

    @classmethod
    def get_retriever(cls, retriever_pk):
        if retriever_pk not in cls._visual_retriever:
            dr = Retriever.objects.get(pk=retriever_pk)
            cls._retriever_object[retriever_pk] = dr
            if dr.algorithm == Retriever.EXACT and dr.approximator_shasum and dr.approximator_shasum.strip():
                approximator, da = Approximators.get_approximator_by_shasum(dr.approximator_shasum)
                da.ensure()
                approximator.load()
                cls._visual_retriever[retriever_pk] = retriever.BaseRetriever(name=dr.name, approximator=approximator)
            elif dr.algorithm == Retriever.EXACT:
                cls._visual_retriever[retriever_pk] = retriever.BaseRetriever(name=dr.name)
            elif dr.algorithm == Retriever.FAISS and dr.approximator_shasum is None:
                di = Indexers.get_indexer_by_shasum(dr.indexer_shasum)
                cls._visual_retriever[retriever_pk] = retriever.FaissFlatRetriever(name=dr.name,
                                                                                   components=di.arguments[
                                                                                       'components'])
            elif dr.algorithm == Retriever.FAISS:
                approximator, da = Approximators.get_approximator_by_shasum(dr.approximator_shasum)
                da.ensure()
                approximator.load()
                cls._visual_retriever[retriever_pk] = retriever.FaissApproximateRetriever(name=dr.name,
                                                                                          approximator=approximator)
            elif dr.algorithm == Retriever.LOPQ:
                approximator, da = Approximators.get_approximator_by_shasum(dr.approximator_shasum)
                da.ensure()
                approximator.load()
                cls._visual_retriever[retriever_pk] = retriever.LOPQRetriever(name=dr.name,
                                                                              approximator=approximator)

            else:
                raise ValueError("{} not valid retriever algorithm".format(dr.algorithm))
        return cls._visual_retriever[retriever_pk], cls._retriever_object[retriever_pk]

    @classmethod
    def refresh_index(cls, dr):
        # This has a BUG where total count of index entries remains unchanged
        # TODO: Waiting for https://github.com/celery/celery/issues/3620 to be resolved to enabel ASYNC index updates
        # TODO improve this by either having a seperate broadcast queues or using last update timestampl
        last_count = cls._index_count
        current_count = IndexEntries.objects.count()
        visual_index = cls._visual_retriever[dr.pk]
        if last_count == 0 or last_count != current_count or len(visual_index.loaded_entries) == 0:
            # update the count
            cls._index_count = current_count
            cls.update_index(dr)

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
                if visual_index.algorithm == "LOPQ":
                    vectors, entries = index_entry.load_index()
                    logging.info("loading approximate index {}".format(index_entry.pk))
                    start_index = len(visual_index.entries)
                    visual_index.load_index(None,entries,index_entry.video_id,index_entry.target)
                    visual_index.loaded_entries[index_entry.pk] = indexer.IndexRange(start=start_index,
                                                                                     end=len(visual_index.entries) - 1)
                elif visual_index.algorithm == 'FAISS':
                    index_file_path, entries = index_entry.load_index()
                    logging.info("loading FAISS index {}".format(index_entry.pk))
                    start_index = visual_index.findex
                    visual_index.load_index(index_file_path, entries, index_entry.video_id, index_entry.target)
                    visual_index.loaded_entries[index_entry.pk] = indexer.IndexRange(start=start_index,
                                                                                     end=visual_index.findex - 1)
                else:
                    vectors, entries = index_entry.load_index()
                    logging.info("Starting {} in {} with shape {}".format(index_entry.video_id, visual_index.name,
                                                                          vectors.shape))
                    try:
                        start_index = visual_index.findex
                        visual_index.load_index(vectors, entries, index_entry.video_id, index_entry.target)
                        visual_index.loaded_entries[index_entry.pk] = indexer.IndexRange(start=start_index,
                                                                                         end=visual_index.findex - 1)
                    except:
                        logging.info("ERROR Failed to load {} vectors shape {} entries {}".format(
                            index_entry.video_id, vectors.shape, len(entries)))
                    else:
                        logging.info("finished {} in {}".format(index_entry.pk, visual_index.name))

    @classmethod
    def retrieve(cls, event, retriever_pk, vector, count, region_pk=None):
        index_retriever, dr = cls.get_retriever(retriever_pk)
        cls.refresh_index(dr)
        results = index_retriever.nearest(vector=vector, n=count)
        qr_batch = []
        for rank, r in enumerate(results):
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
                dd = Frame.objects.get(frame_index=r['id'],video_id=r['video_id'])
                qr.frame_index = dd.frame_index
                qr.video_id = dd.video_id
            else:
                raise ValueError("No key found {}".format(r))
            qr.algorithm = dr.algorithm
            qr.rank = r.get('rank', rank)
            qr.distance = r.get('dist', rank)
            qr_batch.append(qr)
        if region_pk:
            event.finalize_query({"QueryResult":qr_batch},results={region_pk:{"retriever_state":index_retriever.findex}})
        else:
            event.finalize_query({"QueryResult":qr_batch},results={"retriever_state":index_retriever.findex})
        event.parent_process.results_available = True
        event.parent_process.save()
        return 0
