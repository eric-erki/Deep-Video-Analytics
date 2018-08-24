import sys
sys.path.append('client/') #add DVA client to python path
import dvaclient
import json
creds = json.load(file('creds.json'))
server, token = 'http://localhost:8000/api/', creds['token']
ctx = dvaclient.context.DVAContext(server=server,token=token)
for v in ctx.list_videos():
    print "{name} with ID: {vid}".format(name=v['name'],vid=v['id'])
_ = ctx.list_events(verbose=True)
video_processing_query = dvaclient.query.ProcessVideoURL(name="spectre",url="https://www.youtube.com/watch?v=ashLaclKCik")
video_processing_query.execute(ctx)
leo_query_image = 'tests/data/query_leo.png'
indexers = {r['name']:r for r in ctx.list_models() if r['model_type'] == 'I'}
retrievers = {r['name']:r for r in ctx.list_retrievers()}
inception_indexer_pk = indexers['inception']['id']
inception_retriever_pk = retrievers['inception']['id']
q = dvaclient.query.FindSimilarImages(leo_query_image,indexer_pk=inception_indexer_pk,retriever_pk=inception_retriever_pk)
q.execute(context=ctx)
q.wait()
q.gather_search_results()
q = dvaclient.query.FindSimilarImages('tests/data/query_car.png',indexer_pk=inception_indexer_pk,retriever_pk=inception_retriever_pk)
q.execute(context=ctx)
q.wait()
q.gather_search_results()

