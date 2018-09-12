#!/usr/bin/env python
import django, os, sys, subprocess, shlex
sys.path.append(os.path.dirname(__file__))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from dvaapp.models import TrainedModel, Retriever
from django.conf import settings
import logging


QTYPE_TO_MODEL_TYPE = {
    'indexer':TrainedModel.INDEXER,
    'detector':TrainedModel.DETECTOR,
    'analyzer':TrainedModel.ANALYZER,
    'approximator':TrainedModel.APPROXIMATOR
}


def launch_model_queue_by_name(k):
    qtype, model_name = k.split('_')[-2:]
    if qtype == 'retriever':
        dm = Retriever.objects.filter(name=model_name).first()
        if dm is None:
            logging.error("Retriever with name {} not found. Not launching worker.".format(model_name))
        else:
            queue_name = 'q_retriever_{}'.format(dm.pk)
            _ = subprocess.Popen(['./startq.py', queue_name])
    else:
        dm = TrainedModel.objects.filter(name=model_name, model_type=QTYPE_TO_MODEL_TYPE[qtype]).first()
        if dm is None:
            logging.error("Trained Model with name {} and queue type {} not found. Not launching worker.".format(
                model_name,qtype))
        else:
            launch_trained_model_worker(dm)


def launch_model_queue_by_pk(k):
    qtype, pk = k.split('_')[-2:]
    if qtype == 'retriever':
        queue_name = 'q_retriever_{}'.format(pk)
        _ = subprocess.Popen(['./startq.py', queue_name])
    else:
        dm = TrainedModel.objects.get(pk=pk)
        if dm is None:
            logging.error("Trained Model with PK {}  not found. Not launching worker.".format(pk))
        else:
            launch_trained_model_worker(dm)


def launch_trained_model_worker(dm):
    queue_name = 'q_model_{}'.format(dm.pk)
    envs = os.environ.copy()
    if dm.mode == dm.PYTORCH:
        env_mode = "PYTORCH_MODE"
    elif dm.mode == dm.CAFFE:
        env_mode = "CAFFE_MODE"
    elif dm.mode == dm.MXNET:
        env_mode = "MXNET_MODE"
    else:
        env_mode = None
    if env_mode:
        envs[env_mode] = "1"
    _ = subprocess.Popen(['./startq.py', queue_name], env=envs)


def launch_named_queue(k):
    if k.strip() == 'LAUNCH_Q_qextract':
        queue_name = k.split('_')[-1]
        _ = subprocess.Popen(
            shlex.split(('./startq.py {} {}'.format(queue_name, os.environ['LAUNCH_Q_qextract']))))
    elif k.startswith('LAUNCH_Q_GLOBAL_RETRIEVER'):
        _ = subprocess.Popen(shlex.split(('./startq.py {}'.format(settings.GLOBAL_RETRIEVER))))
    elif k.startswith('LAUNCH_Q_GLOBAL_MODEL'):
        _ = subprocess.Popen(shlex.split(('./startq.py {}'.format(settings.GLOBAL_MODEL))))
    else:
        queue_name = k.split('_')[-1]
        _ = subprocess.Popen(shlex.split(('./startq.py {}'.format(queue_name))))


def launch_scheduler():
    if os.environ.get("LAUNCH_SCHEDULER", False):
        # Launch reducer tasks on same machine
        _ = subprocess.Popen(shlex.split('./startq.py {}'.format(settings.Q_REDUCER)))
        # Should be launched only once per deployment
        _ = subprocess.Popen(['./start_scheduler.py'])


def launch_notebook():
    if os.environ.get("LAUNCH_NOTEBOOK", False):
        _ = subprocess.Popen(['./run_jupyter.sh','--allow-root','--notebook-dir=/root/DVA/'],cwd="/")


def launch_manager(block_on_manager):
    if block_on_manager:  # the container process waits on the manager
        subprocess.check_call(['./startq.py','{}'.format(settings.Q_MANAGER)])
    else:
        _ = subprocess.Popen(shlex.split('./startq.py {}'.format(settings.Q_MANAGER)))


def launch_server():
    if 'LAUNCH_SERVER' in os.environ:
        subprocess.check_output(["python", "manage.py", "collectstatic", "--no-input"])
        p = subprocess.Popen(['python', 'manage.py', 'runserver', '0.0.0.0:80'])
        p.wait()
    elif 'LAUNCH_SERVER_NGINX' in os.environ:
        subprocess.check_output(["chmod", "0777", "-R", "/tmp"])
        subprocess.check_output(["python", "manage.py", "collectstatic", "--no-input"])
        subprocess.check_output(["chmod", "0777", "-R", "dva/staticfiles/"])
        # subprocess.check_output(["chmod","0777","-R","/root/media/"])
        try:
            subprocess.check_output(["cp", "../configs/nginx.conf", "/etc/nginx/"])
        except:
            print "warning assuming that the config was already moved"
            pass
        if 'ENABLE_BASICAUTH' in os.environ:
            try:
                subprocess.check_output(["cp", "../configs/nginx-app_password.conf", "/etc/nginx/sites-available/default"])
            except:
                print "warning assuming that the config was already moved"
                pass
        else:
            try:
                subprocess.check_output(["cp", "../configs/nginx-app.conf", "/etc/nginx/sites-available/default"])
            except:
                print "warning assuming that the config was already moved"
                pass
        try:
            subprocess.check_output(["cp", "../configs/supervisor-app.conf", "/etc/supervisor/conf.d/"])
        except:
            print "warning assuming that the config was already moved"
            pass
        p = subprocess.Popen(['supervisord', '-n'])
        p.wait()


if __name__ == '__main__':
    block_on_manager = sys.argv[-1] == '1'
    for k in os.environ:
        if k.startswith('LAUNCH_BY_NAME_'):
            launch_model_queue_by_name(k)
        if k.startswith('LAUNCH_BY_PK_'):
            launch_model_queue_by_pk(k)
        elif k.startswith('LAUNCH_Q_') and k != 'LAUNCH_Q_{}'.format(settings.Q_MANAGER):
            launch_named_queue(k)
    launch_scheduler()
    launch_notebook()
    launch_manager(block_on_manager)
    launch_server()