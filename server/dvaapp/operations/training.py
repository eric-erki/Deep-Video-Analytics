import json, shutil, os
import numpy as np
from dvalib.trainers import lopq_trainer
from django.conf import settings
from dvaapp.models import TrainedModel, Retriever


def train_lopq(training_set,vectors_path,args,output_dirname):
    l = lopq_trainer.LOPQTrainer(name=args['source_indexer_shasum'],
                                 dirname=output_dirname,
                                 components=args['components'],m=args['m'],v=args['v'],sub=args['sub'],
                                 source_indexer_shasum=args['source_indexer_shasum'])
    data = np.load(vectors_path)
    l.train(data)
    j = l.save()
    m = TrainedModel(**j)
    m.training_set = training_set
    m.save()
    m.create_directory()
    for f in m.files:
        shutil.copy(f['url'],'{}/models/{}/{}'.format(settings.MEDIA_ROOT,m.pk,f['filename']))
    dr = Retriever.objects.create(name="lopq retriever",source_filters={},
                                  algorithm=Retriever.LOPQ, approximator_shasum=m.shasum)