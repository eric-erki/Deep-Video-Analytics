#!/usr/bin/env python
import django, json, sys, os, logging, subprocess, base64

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='../logs/init_fs.log',
                    filemode='w')
sys.path.append(os.path.dirname(__file__))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from django.conf import settings
from dvaui.models import ExternalServer
from dvaapp.models import TrainedModel, DVAPQL, TEvent
from dvaapp.processing import DVAPQLProcess
from django.contrib.auth.models import User
from django.utils import timezone


def create_model(m, init_event):
    try:
        if m['model_type'] == TrainedModel.DETECTOR:
            dm, created = TrainedModel.objects.get_or_create(name=m['name'], algorithm=m['algorithm'], mode=m['mode'],
                                                             files=m.get('files', []),
                                                             model_filename=m.get("filename", ""),
                                                             detector_type=m.get("detector_type", ""),
                                                             arguments=m.get("arguments", {}),
                                                             event=init_event,
                                                             model_type=TrainedModel.DETECTOR, )
        else:
            dm, created = TrainedModel.objects.get_or_create(name=m['name'],
                                                             mode=m.get('mode', TrainedModel.TENSORFLOW),
                                                             files=m.get('files', []),
                                                             algorithm=m.get('algorithm', ""),
                                                             arguments=m.get("arguments", {}),
                                                             shasum=m.get('shasum', None),
                                                             event=init_event,
                                                             model_type=m['model_type'])
        if created:
            dm.download()
    except:
        logging.info("Failed to create model {}, it might already exist".format(m))
        pass


def init_models():
    # In Kube mode create models when scheduler is launched which is always the first container.
    if 'INIT_MODELS' in os.environ:
        default_models = json.loads(base64.decodestring(os.environ['INIT_MODELS']))['models']
        if settings.KUBE_MODE and 'LAUNCH_SCHEDULER' in os.environ:
            init_event = TEvent.objects.create(operation="perform_init", duration=0, started=True, completed=True
                                               , start_ts=timezone.now())
            for m in default_models:
                create_model(m, init_event)
        elif not settings.KUBE_MODE:
            init_event = TEvent.objects.create(operation="perform_init", duration=0, started=True, completed=True,
                                               start_ts=timezone.now())
            for m in default_models:
                create_model(m, init_event)


def init_process():
    if 'INIT_PROCESS' in os.environ:
        try:
            jspec = json.loads(base64.decodestring(os.environ['INIT_PROCESS']))
        except:
            logging.exception("could not decode : {}".format(os.environ['INIT_PROCESS']))
        else:
            p = DVAPQLProcess()
            if DVAPQL.objects.count() == 0:
                p.create_from_json(jspec)
                p.launch()


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
    for create_dirname in ['queries', 'exports', 'external', 'retrievers', 'ingest', 'training_sets']:
        if not os.path.isdir("{}/{}".format(settings.MEDIA_ROOT, create_dirname)):
            try:
                os.mkdir("{}/{}".format(settings.MEDIA_ROOT, create_dirname))
            except:
                pass
    if ExternalServer.objects.count() == 0 and 'INIT_MODELS' in os.environ:
        for e in json.loads(base64.decodestring(os.environ['INIT_MODELS']))['external']:
            de, _ = ExternalServer.objects.get_or_create(name=e['name'], url=e['url'])
            de.pull()
    init_models()
    if 'LAUNCH_SERVER' in os.environ or 'LAUNCH_SERVER_NGINX' in os.environ:
        init_process()