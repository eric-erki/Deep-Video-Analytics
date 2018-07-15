#!/usr/bin/env python
import django, os, sys
sys.path.append('../server/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
import base64
from dvaapp.models import DVAPQL, Retriever, QueryResult
from dvaapp.processing import DVAPQLProcess
from django.conf import settings

if __name__ == '__main__':
    if settings.ENABLE_FAISS:
        algo = Retriever.FAISS
    else:
        algo = Retriever.EXACT
    query_dict = {
        'process_type': DVAPQL.QUERY,
        'image_data_b64': base64.encodestring(file('data/query.png').read()),
        'map': [
            {
                'operation': 'perform_indexing',
                'arguments': {
                    'trainedmodel_selector': {"name":'inception'},
                    'target': 'query',
                    'map': [
                        {'operation': 'perform_retrieval',
                         'arguments': {'count': 15, 'retriever_selector':{'pk':Retriever.objects.get(name='inception',
                                                                                          algorithm=algo,
                                                                                          approximator_shasum=None).pk}}
                         }
                    ]
                }

            },
            {
                'operation': 'perform_detection',
                'arguments': {
                    'trainedmodel_selector': {'name':'coco'},
                    'target': 'query',
                }

            }

        ]
    }
    qp = DVAPQLProcess()
    qp.create_from_json(query_dict)
    qp.launch()
    qp.wait_query(timeout=400)
    print QueryResult.objects.count()
