import json, shutil, os, logging, copy
import numpy as np
from dvalib.trainers import lopq_trainer
from django.conf import settings
from dvaapp.models import TrainedModel, Retriever, TrainingSet, IndexEntries


def train_lopq(start,args):
    args = copy.deepcopy(args)
    dt = TrainingSet.objects.get(**args['training_set_selector'])
    m = TrainedModel()
    dirname = "{}/models/{}".format(settings.MEDIA_ROOT,m.uuid)
    m.create_directory()
    l = lopq_trainer.LOPQTrainer(name=args["name"],
                                 dirname=dirname,
                                 components=args['components'],m=args['m'],v=args['v'],sub=args['sub'],
                                 source_indexer_shasum=args['indexer_shasum'])
    index_list = []
    for f in dt.files:
        di = IndexEntries.objects.get(pk=f['pk'])
        vecs, _ = di.load_index()
        if di.count:
            index_list.append(np.atleast_2d(vecs))
            logging.info("loaded {}".format(index_list[-1].shape))
        else:
            logging.info("Ignoring {}".format(di.pk))
    data = np.concatenate(vecs).squeeze()
    logging.info("Final shape {}".format(data.shape))
    l.train(data,lopq_train_opts=args["lopq_train_opts"])
    j = l.save()
    m.name = j["name"]
    m.algorithm = j["algorithm"]
    m.model_type = j["model_type"]
    m.arguments = j["arguments"]
    m.shasum = j["shasum"]
    m.files = j["files"]
    m.event = start
    m.training_set = dt
    m.save()
    m.upload()
    _ = Retriever.objects.create(name="Retriever for approximator {}".format(m.pk),source_filters={}, algorithm=Retriever.LOPQ,
                                  approximator_shasum=m.shasum,indexer_shasum=args['indexer_shasum'])