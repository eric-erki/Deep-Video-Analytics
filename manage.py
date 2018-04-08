#!/usr/bin/env python
import argparse
import subprocess
import time
import urllib2
import os
import webbrowser
import shlex
import json
import base64


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
    return {line.split('=')[0]: line.split('=')[1].strip() for line in file(path)}


def create_custom_env(init_process, init_models, deployment_type):
    envs = {}
    envs['INIT_PROCESS'] = init_process
    envs['INIT_MODELS'] = init_models
    if deployment_type == 'test_rfs':
        envs['DISABLE_NFS'] = 1
        try:
            envs.update(load_envs(os.path.expanduser('~/media.env')))
        except:
            print '~/media.env not found. required for testing rfs mode.'
        try:
            envs.update(load_envs(os.path.expanduser('~/aws.env')))
        except:
            print '~/aws.env not found. required for testing rfs mode.'
    else:
        if os.path.isfile(os.path.expanduser('~/aws.env')):
            envs.update(load_envs(os.path.expanduser('~/aws.env')))
        else:
            print '{} not found. not passing AWS creds.'.format(os.path.expanduser('~/aws.env'))
        if os.path.isfile(os.path.expanduser('~/do.env')):
            envs.update(load_envs(os.path.expanduser('~/do.env')))
        else:
            print '{} not found. not passing Digital Ocean creds.'.format(os.path.expanduser('~/do.env'))
    with open('custom.env', 'w') as out:
        out.write(file('default.env').read())
        out.write('\n')
        for k, v in envs.items():
            out.write("{}={}\n".format(k, v))


def start(deployment_type, gpu_count, init_process, init_models):
    print "Checking if docker-compose is available"
    max_minutes = 20
    if deployment_type == 'gpu':
        if gpu_count == 1:
            fname = 'docker-compose-gpu.yml'
        else:
            fname = 'docker-compose-{}-gpus.yml'.format(gpu_count)
    else:
        fname = 'docker-compose.yml'
    create_custom_env(init_process, init_models, deployment_type)
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
                subprocess.check_call(["sudo", "nvidia-smi", '-pm', '1'])
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
        compose_process = subprocess.Popen(args, cwd=os.path.join(os.path.dirname(__file__),
                                                                  'deploy/{}'.format(deployment_type)))
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
        extra_args = ['-v', ]
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
        subprocess.check_call(["docker-compose", '-f', fname, 'down'] + extra_args,
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
    skeleton = """  version: '3'
  services:
   db:
     image: postgres:9.6.6
     container_name: dva-pg
     volumes:
      - dvapgdata:/var/lib/postgresql/data
     env_file:
       - ../../custom.env
   rabbit:
     image: rabbitmq
     container_name: dva-rmq
     env_file:
       - ../../custom.env
     volumes:
       - dvarabbit:/var/lib/rabbitmq
   redis:
     image: bitnami/redis:latest
     container_name: dva-redis
     env_file:
       - ../../custom.env
     volumes:
       - dvaredis:/bitnami       
   webserver:
     image: akshayubhat/dva-auto:gpu
     container_name: webserver
     env_file:
       - ../../custom.env
     environment:
       - LAUNCH_SERVER_NGINX=1
       - LAUNCH_NOTEBOOK=1
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
       - ../../custom.env
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
       - ../../custom.env
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
           - ../../custom.env
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
                                      global_model_memory_fraction=config[fname]['global_model_memory_fraction']))


def run_commands(command_list):
    for k in command_list:
        print "running {}".format(k)
        subprocess.check_call(shlex.split(k))


def launch_kube(gpu=False):
    setup_kube()
    init_commands = ['kubectl create -f deploy/kube/secrets.yml', 'kubectl create -f deploy/kube/postgres.yaml',
                     'kubectl create -f deploy/kube/rabbitmq.yaml', 'kubectl create -f deploy/kube/redis.yaml']
    run_commands(init_commands)
    print "sleeping for 120 seconds"
    time.sleep(120)
    webserver_commands = ['kubectl create -f deploy/kube/webserver.yaml', ]
    run_commands(webserver_commands)
    print "sleeping for 60 seconds"
    time.sleep(60)
    if gpu:
        deployment_commands = ['kubectl create -f deploy/kube/coco_gpu.yaml',
                               'kubectl create -f deploy/kube/extractor.yaml',
                               'kubectl create -f deploy/kube/face.yaml',
                               'kubectl create -f deploy/kube/facenet.yaml',
                               'kubectl create -f deploy/kube/facenet_retriever.yaml',
                               'kubectl create -f deploy/kube/inception.yaml',
                               'kubectl create -f deploy/kube/inception_retriever.yaml',
                               'kubectl create -f deploy/kube/global_retriever.yaml',
                               'kubectl create -f deploy/kube/textbox.yaml',
                               'kubectl create -f deploy/kube/scheduler.yaml,'
                               'kubectl create -f deploy/kube/crnn.yaml',
                               'kubectl create -f deploy/kube/tagger.yaml']
    else:
        deployment_commands = ['kubectl create -f deploy/kube/coco.yaml',
                               'kubectl create -f deploy/kube/extractor.yaml',
                               'kubectl create -f deploy/kube/face.yaml',
                               'kubectl create -f deploy/kube/facenet.yaml',
                               'kubectl create -f deploy/kube/facenet_retriever.yaml',
                               'kubectl create -f deploy/kube/inception.yaml',
                               'kubectl create -f deploy/kube/inception_retriever.yaml',
                               'kubectl create -f deploy/kube/global_retriever.yaml',
                               'kubectl create -f deploy/kube/textbox.yaml',
                               'kubectl create -f deploy/kube/scheduler.yaml,'
                               'kubectl create -f deploy/kube/crnn.yaml',
                               'kubectl create -f deploy/kube/tagger.yaml']
    run_commands(deployment_commands)


def delete_kube():
    delete_commands = ['kubectl delete -f deploy/kube/secrets.yml',
                       'kubectl delete -f deploy/kube/coco.yaml',
                       'kubectl delete -f deploy/kube/redis.yaml',
                       'kubectl delete -f deploy/kube/extractor.yaml',
                       'kubectl delete -f deploy/kube/face.yaml',
                       'kubectl delete -f deploy/kube/facenet.yaml',
                       'kubectl delete -f deploy/kube/facenet_retriever.yaml',
                       'kubectl delete -f deploy/kube/inception.yaml',
                       'kubectl delete -f deploy/kube/inception_retriever.yaml',
                       'kubectl delete -f deploy/kube/postgres.yaml',
                       'kubectl delete -f deploy/kube/rabbitmq.yaml',
                       'kubectl delete -f deploy/kube/textbox.yaml',
                       'kubectl delete -f deploy/kube/webserver.yaml',
                       'kubectl delete -f deploy/kube/scheduler.yaml',
                       'kubectl delete -f deploy/kube/crnn.yaml',
                       'kubectl delete -f deploy/kube/tagger.yaml',
                       'kubectl delete -f deploy/kube/global_retriever.yaml', ]
    run_commands(delete_commands)


def kube_gpu_setup():
    command = ['kubectl', 'create', '-f',
               'https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators'
               '/k8s-1.9/nvidia-driver-installer/cos/daemonset-preloaded.yaml']
    subprocess.check_call(command)


def erase_kube_bucket():
    config = get_kube_config()
    subprocess.check_call(['gsutil', '-m', 'rm', 'gs://{}/**'.format(config['mediabucket'])])


def get_kube_config():
    """
    # to set CORS on the bucket Can be * or specific website e.g. http://example.website.com
    :return:
    """
    if not os.path.isfile('kubeconfig.json'):
        print "kubeconfig.json not found, edit kubeconfig.example.json and store it as kubeconfig.json"
        raise EnvironmentError(
            "kubeconfig.json not found, edit kubeconfig.example.json and store it as kubeconfig.json")
    else:
        with open('kubeconfig.json') as fh:
            return json.load(fh)


def kube_create_premptible_node_pool():
    config = get_kube_config()
    command = 'gcloud beta container --project "{project_name}" node-pools create "{pool_name}"' \
              ' --zone "{zone}" --cluster "{cluster_name}" ' \
              '--machine-type "n1-standard-2" --image-type "COS" ' \
              '--disk-size "100" ' \
              '--scopes "https://www.googleapis.com/auth/compute",' \
              '"https://www.googleapis.com/auth/devstorage.read_write",' \
              '"https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring",' \
              '"https://www.googleapis.com/auth/servicecontrol",' \
              '"https://www.googleapis.com/auth/service.management.readonly",' \
              '"https://www.googleapis.com/auth/trace.append" ' \
              '--preemptible --num-nodes "{count}"  '
    command = command.format(project_name=config['project_name'],
                             pool_name="premptpool",
                             cluster_name=config['cluster_name'],
                             zone=config['zone'], count=5)
    print command
    subprocess.check_call(shlex.split(command))


def setup_kube():
    config = get_kube_config()
    print "attempting to create bucket"
    try:
        subprocess.check_call(shlex.split('gsutil mb -c regional -l {} gs://{}'.format(config['region'],
                                                                                       config['mediabucket'])))
    except:
        print "failed to create bucket, assuming it already exists"
    print "attempting to set public view permission on the bucket"
    try:
        subprocess.check_call(shlex.split('gsutil iam ch allUsers:objectViewer gs://{}'.format(config['mediabucket'])))
    except:
        print "failed to set permissions to public"
    with open('cors.json', 'w') as out:
        json.dump([
            {
                "origin": [config['cors_origin']],
                "responseHeader": ["Content-Type"],
                "method": ["GET", "HEAD"],
                "maxAgeSeconds": 3600
            }
        ], out)
    print "attempting to set bucket policy"
    try:
        subprocess.check_call(shlex.split('gsutil cors set cors.json gs://{}'.format(config['mediabucket'])))
    except:
        print "failed to set bucket policy"
    print "Attempting to create deploy/kube/secrets.yml from deploy/kube/secrets_template.yml and config."
    with open('deploy/kube/secrets_template.yml') as f:
        template = f.read()
    with open('deploy/kube/secrets.yml', 'w') as out:
        out.write(template.format(
            dbusername=base64.encodestring(config['dbusername']),
            dbpassword=base64.encodestring(config['dbpassword']),
            rabbithost=base64.encodestring(config['rabbithost']),
            rabbitpassword=base64.encodestring(config['rabbitpassword']),
            rabbitusername=base64.encodestring(config['rabbitusername']),
            awskey=base64.encodestring(config['awskey']),
            awssecret=base64.encodestring(config['awssecret']),
            secretkey=base64.encodestring(config['secretkey']),
            mediabucket=base64.encodestring(config['mediabucket']),
            mediaurl=base64.encodestring('http://{}.storage.googleapis.com/'.format(config['mediabucket'])),
            superuser=base64.encodestring(config['superuser']),
            superpass=base64.encodestring(config['superpass']),
            superemail=base64.encodestring(config['superemail']),
            cloudfsprefix=base64.encodestring(config['cloudfsprefix']),
            redishost=base64.encodestring(config['redishost']),
            redispassword=base64.encodestring(config['redispassword']),
        ).replace('\n\n', '\n'))


def clear_media_bucket():
    envs = load_envs(os.path.expanduser('~/media.env'))
    print "Erasing bucket {}".format(envs['MEDIA_BUCKET'])
    subprocess.check_call(['aws','s3','rm','--recursive','--quiet','s3://{}'.format(envs['MEDIA_BUCKET'])])
    print "Bucket erased"


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("action",
                        help="Select action out of { start | stop | clean | clean_restart "
                             "| notebook (view jupyter notebook URL) | wsgi (view WSGI logs) }")
    parser.add_argument("type", nargs='?',
                        help="select deployment type { dev | test_rfs | cpu | gpu | kube  }. If unsure "
                             "choose cpu. Required for start, stop, clean, restart, clean_restart")
    parser.add_argument("--gpus", help="For GPU mode select number of P100 GPUs: 1, 2, 4. default is 1", default=1,
                        type=int)
    parser.add_argument("--init_process", help="Initial DVAPQL path default: configs/custom_defaults/init_process.json",
                        default="/root/DVA/configs/custom_defaults/init_process.json")
    parser.add_argument("--init_models", help="Path to trained_models.json:",
                        default="/root/DVA/configs/custom_defaults/trained_models.json")
    args = parser.parse_args()
    if args.type and args.type == 'kube':
        if args.action == 'start':
            launch_kube()
        elif args.action == 'stop':
            delete_kube()
        else:
            raise NotImplementedError("Kubernetes management only suports start and stop actions")
    else:
        if args.type and args.type == 'gpu':
            generate_multi_gpu_compose()
        if args.action == 'stop':
            stop(args.type, args.gpus)
        elif args.action == 'start':
            start(args.type, args.gpus, args.init_process, args.init_models)
        elif args.action == 'clean':
            stop(args.type, args.gpus, clean=True)
            if args.type == 'test_rfs':
                clear_media_bucket()
        elif args.action == 'restart':
            stop(args.type, args.gpus)
            start(args.type, args.gpus, args.init_process, args.init_models)
        elif args.action == 'clean_restart':
            stop(args.type, args.gpus, clean=True)
            if args.type == 'test_rfs':
                clear_media_bucket()
            start(args.type, args.gpus, args.init_process, args.init_models)
        elif args.action == 'notebook':
            view_notebook_url()
        elif args.action == 'wsgi':
            view_uwsgi_logs()
        else:
            raise NotImplementedError("{} and {}".format(args.action, args.type))
