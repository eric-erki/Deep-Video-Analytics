#!/usr/bin/env python
import django, json, sys, os, logging, time, random
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='../logs/init_fs.log',
                    filemode='a')
sys.path.append(os.path.dirname(__file__))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from django.conf import settings
from dvaui.models import ExternalServer
from dvaapp.models import TrainedModel, DVAPQL
from dvaapp.processing import DVAPQLProcess
from django.contrib.auth.models import User
from dvaapp.fs import get_path_to_file


def create_model(m):
    try:
        if m['model_type'] == TrainedModel.DETECTOR:
            dm, created = TrainedModel.objects.get_or_create(name=m['name'], algorithm=m['algorithm'], mode=m['mode'],
                                                             files=m.get('files', []), model_filename=m.get("filename", ""),
                                                             detector_type=m.get("detector_type", ""),
                                                             arguments=m.get("arguments", {}),
                                                             model_type=TrainedModel.DETECTOR, )
        else:
            dm, created = TrainedModel.objects.get_or_create(name=m['name'], mode=m.get('mode', TrainedModel.TENSORFLOW),
                                                             files=m.get('files', []),
                                                             algorithm=m.get('algorithm', ""),
                                                             arguments=m.get("arguments", {}),
                                                             shasum=m.get('shasum', None),
                                                             model_type=m['model_type'])
        if created:
            dm.download()
    except:
        logging.info("Failed to create model {}, it might already exist".format(m))
        pass


if __name__ == "__main__":
    if 'SUPERUSER' in os.environ and not User.objects.filter(is_superuser=True).exists():
        try:
            User.objects.create_superuser(username=os.environ['SUPERUSER'],
                                          password=os.environ['SUPERPASS'],
                                          email=os.environ['SUPEREMAIL'])
        except:
            logging.warning("Could not create Superuser, might be because one already exists in which "
                            "case please ignore.")
            pass
    for create_dirname in ['queries', 'exports', 'external', 'retrievers', 'ingest','training_sets']:
        if not os.path.isdir("{}/{}".format(settings.MEDIA_ROOT, create_dirname)):
            try:
                os.mkdir("{}/{}".format(settings.MEDIA_ROOT, create_dirname))
            except:
                pass
    if ExternalServer.objects.count() == 0:
        for e in json.loads(file("../configs/custom_defaults/external.json").read()):
            de,_ = ExternalServer.objects.get_or_create(name=e['name'],url=e['url'])
            de.pull()
    local_models_path = "../configs/custom_defaults/trained_models.json"
    if 'INIT_MODELS' in os.environ and os.environ['INIT_MODELS'].strip():
        remote_models_path = os.environ['INIT_MODELS']
        if not remote_models_path.startswith('/root/DVA/configs/custom_defaults/'):
            local_models_path = 'custom_models.json'
            get_path_to_file(remote_models_path, local_models_path)
        else:
            local_models_path = remote_models_path
    default_models = json.loads(file(local_models_path).read())
    if not settings.KUBE_MODE:
        for m in default_models:
            create_model(m)
    if 'LAUNCH_SERVER' in os.environ or 'LAUNCH_SERVER_NGINX' in os.environ:
        if settings.KUBE_MODE:
            # todo(akshay): This code is prone to race condition when starting the cluster.
            time.sleep(random.randint(15))
            for m in default_models:
                create_model(m)
        if 'INIT_PROCESS' in os.environ:
            path = os.environ.get('INIT_PROCESS',None)
            if path and path.strip():
                if not path.startswith('/root/DVA/configs/custom_defaults/'):
                    get_path_to_file(path,"temp.json")
                    path = 'temp.json'
                try:
                    jspec = json.load(file(path))
                except:
                    logging.exception("could not load : {}".format(path))
                else:
                    p = DVAPQLProcess()
                    if DVAPQL.objects.count() == 0:
                        p.create_from_json(jspec)
                        p.launch()