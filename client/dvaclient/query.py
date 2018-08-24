import base64, logging, time
from dvaclient import constants, visual_search_results, schema


class DVAQuery(object):

    def __init__(self, query_json=None, query_id=None, context=None):
        if query_json is None:
            self.query_json = {}
        else:
            self.query_json = query_json
        self.query_request = None
        self.context = context
        self.results = None
        self.query_id = query_id
        self.search_results = []

    def execute(self, context=None):
        if self.context is None:
            self.context = context
        if self.query_request is None:
            validator = schema.Validator(self.query_json)
            validator.validate()
            self.query_request = context.execute_query(self.query_json)
            self.query_id = self.query_request['id']
            self.context = context
        else:
            raise ValueError("Query already requested")

    def wait(self, timeout=3, max_attempts=60, verbose=False):
        i_timeout, i_max_attempts = timeout, max_attempts
        while not self.completed() and max_attempts > 0:
            msg = "\n\nQuery {qid} not completed, sleeping for {timeout} seconds and {attempts} attempts remaining\n".format(
                qid=self.query_id, timeout=timeout, attempts=max_attempts)
            logging.info(msg)
            if verbose:
                print msg
                print "Running/Pending/Completed tasks"
                self.context.list_events(verbose=True, query_id=self.query_id)
            max_attempts -= 1
            time.sleep(timeout)
        if max_attempts == 0:
            print  'Timed out after {} seconds'.format(i_max_attempts * i_timeout)
            return False
        return True

    def completed(self):
        if self.results is None:
            self.results = self.context.get_results(self.query_id)
        if not self.results['completed']:
            self.results = self.context.get_results(self.query_id)  # refresh results
            if all([t['completed'] for t in self.results['tasks']]):
                # dont wait for scheduler just check if all launched tasks have completed.
                self.results['completed'] = True
        return self.results['completed']

    def gather_search_results(self):
        if self.query_json['process_type'] != constants.QUERY:
            raise ValueError("Process is not of type query")
        else:
            for t in self.results['tasks']:
                if t['query_results']:
                    self.search_results.append(visual_search_results.VisualSearchResults(self, task=t))


class ProcessVideoURL(DVAQuery):
    def __init__(self, name, url):
        super(ProcessVideoURL, self).__init__()
        self.url = url
        self.name = name
        self.query_json = {
            "process_type": constants.PROCESS,
            "create": [

                {"MODEL": "Video",
                 "spec": {
                     "name": self.name,
                     "dataset": False,
                     "url": self.url
                 }

                 }
            ],
            "map": [{
                "operation": "perform_import",
                "video_id": "__created__0",
                "arguments": {
                    "name": self.name,
                    "map": [
                        {
                            "operation": "perform_video_segmentation",
                            "arguments": {
                                "map": [
                                    {
                                        "operation": "perform_video_decode",
                                        "arguments": {
                                            "segments_batch_size": 10,
                                            "rate": 30,
                                            "rescale": 0,
                                            "map": [
                                                {
                                                    "operation": "perform_indexing",
                                                    "arguments": {
                                                        "trainedmodel_selector":{"name":"inception"},
                                                        "target": "frames",
                                                        "filters": "__parent__"
                                                    }
                                                },
                                                {
                                                    "operation": "perform_detection",
                                                    "arguments": {
                                                        "filters": "__parent__",
                                                        "trainedmodel_selector":{"name":"coco"},
                                                        "map": [
                                                            {
                                                                "operation": "perform_indexing",
                                                                "arguments": {
                                                                    "trainedmodel_selector":{"name":"inception"},
                                                                    "target": "regions",
                                                                    "filters": {
                                                                        "event_id": "__parent_event__",
                                                                        "w__gte": 50,
                                                                        "h__gte": 50
                                                                    }
                                                                }
                                                            }
                                                        ]
                                                    }
                                                },
                                                {
                                                    "operation": "perform_detection",
                                                    "arguments": {
                                                        "filters": "__parent__",
                                                        "trainedmodel_selector":{"name":"face"},
                                                        "map": [
                                                            {
                                                                "operation": "perform_indexing",
                                                                "arguments": {
                                                                    "trainedmodel_selector":{"name":"facenet"},
                                                                    "target": "regions",
                                                                    "filters": {
                                                                        "event_id": "__parent_event__"
                                                                    }
                                                                }
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            }]
        }


class FindSimilarImages(DVAQuery):
    def __init__(self, query_image_path, indexer_pk, retriever_pk, n=20):
        super(FindSimilarImages, self).__init__()
        self.query_image_path = query_image_path
        self.query_json = {
            'process_type': constants.QUERY,
            'image_data_b64': base64.encodestring(file(self.query_image_path).read()),
            'map': [
                {
                    'operation': 'perform_indexing',
                    'arguments': {
                        "trainedmodel_selector": {"pk": indexer_pk},
                        'target': 'query',
                        'map': [
                            {'operation': 'perform_retrieval',
                             'arguments': {'count': n, "retriever_selector": {"pk": retriever_pk}}
                             }
                        ]
                    }

                }

            ]
        }


class DetectAndFindSimilarImages(DVAQuery):
    def __init__(self, query_image_path, detector_pk, indexer_pk, retriever_pk, n=20):
        super(DetectAndFindSimilarImages, self).__init__()
        self.query_image_path = query_image_path
        self.query_json = {
            'process_type': constants.QUERY,
            'image_data_b64': base64.encodestring(file(self.query_image_path).read()),
            'map': [
                {'operation': 'perform_detection',
                 'arguments': {
                     'target': 'query',
                     "trainedmodel_selector":{"pk":detector_pk},
                     'map': [
                         {'operation': 'perform_indexing',
                          'arguments': {
                              "trainedmodel_selector":{"pk":indexer_pk},
                              'target': 'query',
                              'map': [
                                  {'operation': 'perform_retrieval',
                                   'arguments': {'count': n,
                                                 "retriever_selector": {"pk": retriever_pk}
                                                 }
                                   }
                              ]
                          }

                          }

                     ]
                 }
                 }
            ]
        }
