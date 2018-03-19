#!/usr/bin/env python
import argparse
import copy
import subprocess
import time
import urllib2
import os
import webbrowser
import shlex


def launch_gcp():
    command = 'gcloud beta compute --project "{project}" instances create "{name}" ' \
              '--zone "us-west1-b" --machine-type "custom-12-46080" --subnet "default" --maintenance-policy' \
              ' "TERMINATE" --service-account "{service-_ccount}"' \
              ' --scopes "https://www.googleapis.com/auth/devstorage.read_only",' \
              '"https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring.write",' \
              '"https://www.googleapis.com/auth/servicecontrol",' \
              '"https://www.googleapis.com/auth/service.management.readonly",' \
              '"https://www.googleapis.com/auth/trace.append" ' \
              '--accelerator type=nvidia-tesla-p100,count=2 --min-cpu-platform "Automatic" --image "{image_name}" ' \
              '--image-project "{image_project}" --boot-disk-size "128" ' \
              '--boot-disk-type "pd-ssd" --boot-disk-device-name "{name}"'
    print "running {}".format(command)
    subprocess.check_call(shlex.split(command))


def load_envs(path):
    return { line.split('=')[0]:line.split('=')[1].strip() for line in file(path)}


def start(deployment_type, gpu_count, init_process):
    print "Checking if docker-compose is available"
    max_minutes = 20
    if deployment_type == 'gpu':
        if gpu_count == 1:
            fname = 'docker-compose-gpu.yml'
        else:
            fname = 'docker-compose-{}-gpus.yml'.format(gpu_count)
    else:
        fname = 'docker-compose.yml'
    print "Starting deploy/{}/{}".format(deployment_type, fname)
    try:
        # Fixed to dev since deployment directory does not matters for checking if docker-compose exists.
        subprocess.check_call(["docker-compose", 'ps'],
                              cwd=os.path.join(os.path.dirname(__file__), 'deploy/dev'))
    except:
        raise SystemError("Docker-compose is not available")
    print "Pulling/Refreshing container images, first time it might take a while to download the image"
    try:
        if deployment_type == 'gpu':
            print "Trying to set persistence mode for GPU"
            try:
                subprocess.check_call(["sudo","nvidia-smi", '-pm', '1'])
            except:
                print "Error could not set persistence mode pleae manually run 'sudo nvidia-smi -pm 1'"
                pass
            subprocess.check_call(["docker", 'pull', 'akshayubhat/dva-auto:gpu'])
        else:
            subprocess.check_call(["docker", 'pull', 'akshayubhat/dva-auto:latest'])
    except:
        raise SystemError("Docker is not running / could not pull akshayubhat/dva-auto:latest image from docker hub")
    print "Trying to launch containers"
    try:
        args = ["docker-compose", '-f', fname, 'up', '-d']
        print " ".join(args)
        envs = copy.deepcopy(os.environ)
        envs['INIT_PROCESS'] = init_process
        if os.path.isfile(os.path.expanduser('~/aws.env')):
            envs.update(load_envs(os.path.expanduser('~/aws.env')))
        if os.path.isfile(os.path.expanduser('~/do.env')):
            envs.update(load_envs(os.path.expanduser('~/do.env')))
        compose_process = subprocess.Popen(args, cwd=os.path.join(os.path.dirname(__file__),
                                                                  'deploy/{}'.format(deployment_type)), env=envs)
    except:
        raise SystemError("Could not start container")
    while max_minutes:
        print "Checking if DVA server is running, waiting for another minute and at most {max_minutes} minutes".format(
            max_minutes=max_minutes)
        try:
            r = urllib2.urlopen("http://localhost:8000")
            if r.getcode() == 200:
                view_notebook_url()
                print "Open browser window and go to http://localhost:8000 to access DVA Web UI"
                print 'For windows you might need to replace "localhost" with ip address of docker-machine'
                webbrowser.open("http://localhost:8000")
                webbrowser.open("http://localhost:8888")
                break
        except:
            pass
        time.sleep(60)
        max_minutes -= 1
    compose_process.wait()


def stop(deployment_type, gpu_count, clean=False):
    if clean:
        extra_args = ['-v',]
    else:
        extra_args = []
    if deployment_type == 'gpu':
        if gpu_count == 1:
            fname = 'docker-compose-gpu.yml'
        else:
            fname = 'docker-compose-{}-gpus.yml'.format(gpu_count)
    else:
        fname = 'docker-compose.yml'
    print "Stopping deploy/{}/{}".format(deployment_type, fname)
    try:
        subprocess.check_call(["docker-compose", '-f', fname, 'down']+extra_args,
                              cwd=os.path.join(os.path.dirname(__file__), 'deploy/{}'.format(deployment_type)))
    except:
        raise SystemError("Could not stop containers")


def view_notebook_url():
    print 'Use following url containing pre-auth token to use jupyter notebook'
    print subprocess.check_output(["docker", "exec", "-it", "webserver", "jupyter", 'notebook', 'list'])


def view_uwsgi_logs():
    print 'Use following auth code to use jupyter notebook on  '
    print subprocess.check_output(
        ["docker", "exec", "-it", "webserver", "bash", '-c ', "'cat /var/log/supervisor/app-*'"])


def generate_multi_gpu_compose():
    skeleton = """
     version: '3'
     services:
       db:
         image: postgres:9.6.6
         container_name: dva-pg
         volumes:
          - dvapgdata:/var/lib/postgresql/data
         env_file:
           - ../common.env
       rabbit:
         image: rabbitmq
         container_name: dva-rmq
         env_file:
           - ../common.env
         volumes:
           - dvarabbit:/var/lib/rabbitmq
       redis:
         image: bitnami/redis:latest
         container_name: dva-redis
         env_file:
           - ../common.env
         volumes:
           - dvaredis:/bitnami       
       webserver:
         image: akshayubhat/dva-auto:gpu
         container_name: webserver
         env_file:
           - ../common.env
         environment:
           - LAUNCH_SERVER_NGINX=1
           - LAUNCH_NOTEBOOK=1
           - INIT_PROCESS={INIT_PROCESS}
         command: bash -c "git reset --hard && git pull && sleep 10 && ./start_container.py"
         ports:
           - "127.0.0.1:8000:80"
           - "127.0.0.1:8888:8888"
         depends_on:
           - db
           - redis       
           - rabbit
         volumes:
           - dvadata:/root/media
       non-gpu-workers:
         image: akshayubhat/dva-auto:gpu
         env_file:
           - ../common.env
         environment:
           - LAUNCH_BY_NAME_retriever_inception=1
           - LAUNCH_BY_NAME_retriever_facenet=1
           - LAUNCH_Q_qextract=1
           - LAUNCH_Q_qstreamer=1
           - LAUNCH_SCHEDULER=1
           - LAUNCH_Q_GLOBAL_RETRIEVER=1
         command: bash -c "git reset --hard && git pull && sleep 45 && ./start_container.py"
         depends_on:
           - db
           - redis       
           - rabbit
         volumes:
           - dvadata:/root/media
    {gpu_workers}
       global-model:
         image: akshayubhat/dva-auto:gpu
         env_file:
           - ../common.env
         environment:
           - GPU_AVAILABLE=1     
           - NVIDIA_VISIBLE_DEVICES={global_model_gpu_id}
           - GPU_MEMORY={global_model_memory_fraction}
           - LAUNCH_Q_GLOBAL_MODEL=1
         command: bash -c "git reset --hard && git pull && sleep 45 && ./start_container.py"
         depends_on:
           - db
           - redis       
           - rabbit
         volumes:
           - dvadata:/root/media
     volumes:
      dvapgdata:
      dvadata:
      dvarabbit:
      dvaredis:
    """

    block = """   {worker_name}:
         image: akshayubhat/dva-auto:gpu
         env_file:
           - ../common.env
         environment:
           - GPU_AVAILABLE=1
           - NVIDIA_VISIBLE_DEVICES={gpu_id}
           - GPU_MEMORY={memory_fraction}
           - {env_key}={env_value}
         command: bash -c "git reset --hard && git pull && sleep 45 && ./start_container.py"
         depends_on:
           - db
           - redis       
           - rabbit
         volumes:
           - dvadata:/root/media"""

    config = {
        "deploy/gpu/docker-compose-2-gpus.yml": {"global_model_gpu_id": 0,
                                      "global_model_memory_fraction": 0.1,
                                      "workers":
                                          [(0, 0.25, "LAUNCH_BY_NAME_indexer_inception", "inception"),
                                           (0, 0.2, "LAUNCH_BY_NAME_analyzer_crnn", "crnn"),
                                           (0, 0.5, "LAUNCH_BY_NAME_detector_coco", "coco"),
                                           (1, 0.5, "LAUNCH_BY_NAME_detector_textbox", "textbox"),
                                           (1, 0.19, "LAUNCH_BY_NAME_detector_face", "face"),
                                           (1, 0.15, "LAUNCH_BY_NAME_indexer_facenet", "facenet"),
                                           (1, 0.15, "LAUNCH_BY_NAME_analyzer_tagger", "tagger")]
                                      },
        "deploy/gpu/docker-compose-4-gpus.yml": {"global_model_gpu_id": 2,
                                      "global_model_memory_fraction": 0.29,
                                      "workers":
                                          [(0, 0.3, "LAUNCH_BY_NAME_indexer_inception", "inception"),
                                           (0, 0.4, "LAUNCH_BY_NAME_analyzer_tagger", "tagger"),
                                           (0, 0.2, "LAUNCH_BY_NAME_analyzer_crnn", "crnn"),
                                           (1, 1.0, "LAUNCH_BY_NAME_detector_coco", "coco"),
                                           (2, 0.7, "LAUNCH_BY_NAME_detector_face", "face"),
                                           (3, 0.5, "LAUNCH_BY_NAME_detector_textbox", "textbox"),
                                           (3, 0.45, "LAUNCH_BY_NAME_indexer_facenet", "facenet")
                                           ]
                                      },
    }
    for fname in config:
        blocks = []
        worker_specs = config[fname]['workers']
        for gpu_id, fraction, env_key, worker_name, in worker_specs:
            blocks.append(
                block.format(worker_name=worker_name, gpu_id=gpu_id, memory_fraction=fraction, env_key=env_key,
                             env_value=1))
        with open(fname, 'w') as out:
            out.write(skeleton.format(gpu_workers="\n".join(blocks),
                                      global_model_gpu_id=config[fname]['global_model_gpu_id'],
                                      global_model_memory_fraction=config[fname]['global_model_memory_fraction'],
                                      INIT_PROCESS='${INIT_PROCESS}'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("action",
                        help="Select action out of { start | stop | clean | clean_restart "
                             "| jupyter (view jupyter URL) | wsgi (view logs) | generate_multi_gpu_compose }")
    parser.add_argument("type", nargs='?', help="select deployment type { dev | test_rfs | cpu | gpu  }. If unsure "
                                                "choose cpu. Required for start, stop, clean, restart, clean_restart")
    parser.add_argument("--gpus", help="For GPU mode select number of P100 GPUs: 1, 2, 4. default is 1", default=1,
                        type=int)
    parser.add_argument("--init_process", help="Initial DVAPQL path default: configs/custom_defaults/init_process.json",
                        default="/root/DVA/configs/custom_defaults/init_process.json")
    args = parser.parse_args()
    if args.action == 'stop':
        stop(args.type, args.gpus)
    elif args.action == 'start':
        start(args.type, args.gpus, args.init_process)
    elif args.action == 'clean':
        stop(args.type, args.gpus, clean=True)
    elif args.action == 'restart':
        stop(args.type, args.gpus)
        start(args.type, args.gpus, args.init_process)
    elif args.action == 'clean_restart':
        stop(args.type, args.gpus, clean=True)
        start(args.type, args.gpus, args.init_process)
    elif args.action == 'jupyter':
        view_notebook_url()
    elif args.action == 'wsgi':
        view_uwsgi_logs()
    elif args.action == 'generate_multi_gpu_compose':
        generate_multi_gpu_compose()
    else:
        raise NotImplementedError("{} and {}".format(args.action,args.type))