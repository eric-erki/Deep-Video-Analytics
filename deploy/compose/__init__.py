"""
Code in this file assumes that it is being run via dvactl and git repo root as current directory
"""
import subprocess
import time
import urllib2
import os
import json
import uuid

DOCKER_COMPOSE = 'docker-compose.exe' if 'WSL' in os.environ else 'docker-compose'

DOCKER = 'docker.exe' if 'WSL' in os.environ else 'docker'

DEFAULT_ENV = """GLOBAL_MODEL=1
DOCKER_MODE=true
RABBIT_HOST=rabbit
RABBIT_USER=dvauser
RABBIT_PASS=localpass
DB_HOST=db
DB_NAME=postgres
DB_USER=pgdbuser
DB_PASS=pgdbpass
SUPERUSER=admin
SUPERPASS=super
SUPEREMAIL=admin@test.com
DISABLE_DEBUG=1
SECRET_KEY=283y312bhv2b13v13
POSTGRES_USER=pgdbuser
POSTGRES_PASSWORD=pgdbpass
RABBITMQ_DEFAULT_USER=dvauser
RABBITMQ_DEFAULT_PASS=localpass
REDIS_PASSWORD=redispass
REDIS_HOST=redis
"""


def wait_to_start(max_minutes=10):
    while max_minutes:
        print "Checking if DVA server is running, waiting for another minute and at most {max_minutes} minutes".format(
            max_minutes=max_minutes)
        try:
            r = urllib2.urlopen("http://localhost:8000")
            if r.getcode() == 200:
                print "Open browser window and go to http://localhost:8000 to access DVA Web UI"
                print 'For windows you might need to replace "localhost" with ip address of docker-machine'
                break
        except:
            pass
        time.sleep(60)
        max_minutes -= 1


def generate_multi_gpu_compose(fname, config, cpu_image, gpu_image):
    blocks = []
    worker_specs = config['workers']
    for gpu_id, fraction, env_key, worker_name, in worker_specs:
        if fraction > 0:
            blocks.append(
                file('deploy/compose/gpu_block.yaml').read().format(worker_name=worker_name, gpu_id=gpu_id,
                                                                    memory_fraction=fraction, env_key=env_key,
                                                                    env_value=1, BRANCH='{BRANCH}', cpu_image=cpu_image,
                                                                    gpu_image=gpu_image))
        else:
            blocks.append(
                file('deploy/compose/gpu_cpu_block.yaml').read().format(worker_name=worker_name, env_key=env_key,
                                                                        env_value=1, BRANCH='{BRANCH}',
                                                                        cpu_image=cpu_image, gpu_image=gpu_image))
    with open(fname, 'w') as out:
        out.write(
            file('deploy/compose/gpu_skeleton.yaml').read().format(gpu_workers="\n".join(blocks), BRANCH='{BRANCH}',
                                                                   global_model_gpu_id=config['global_model_gpu_id'],
                                                                   global_model_memory_fraction=config[
                                                                       'global_model_memory_fraction'],
                                                                   cpu_image=cpu_image, gpu_image=gpu_image))


def generate_cpu_compose(cpu_image):
    with open('deploy/compose/docker-compose-cpu.yaml', 'w') as out:
        out.write(file('deploy/compose/docker-compose-cpu.yaml.template').read().format(cpu_image=cpu_image,
                                                                                        BRANCH='{BRANCH}'))
    with open('deploy/compose/docker-compose-dev.yaml', 'w') as out:
        out.write(file('deploy/compose/docker-compose-dev.yaml.template').read().format(cpu_image=cpu_image,
                                                                                        BRANCH='{BRANCH}'))


def load_envs(path):
    return {line.split('=')[0]: line.split('=')[1].strip() for line in file(path)}


def create_custom_env(init_process, init_models, cred_envs, branch):
    envs = {'INIT_PROCESS': init_process, 'INIT_MODELS': init_models}
    if branch == 'stable':
        envs['BRANCH'] = 'sleep 1'
    else:
        envs['BRANCH'] = "git checkout --track origin/{}".format(branch)
    envs.update(cred_envs)
    with open('custom.env', 'w') as out:
        out.write(DEFAULT_ENV)
        out.write('\n')
        for k, v in envs.items():
            out.write("{}={}\n".format(k, v))


def pull_latest_images(deployment_type, cpu_image, gpu_image):
    print "Pulling/Refreshing container images, first time it might take a while to download the image"
    try:
        if deployment_type == 'gpu':
            subprocess.check_call([DOCKER, 'pull', gpu_image])
        subprocess.check_call([DOCKER, 'pull', cpu_image])
    except:
        raise SystemError("Docker is not running / could not pull akshayubhat/dva-auto:latest image from docker hub")


def start_docker_compose(deployment_type, gpu_count, init_process, init_models, cred_envs, branch, refresh, cpu_image,
                         gpu_image):
    print "Checking if docker-compose is available"
    max_minutes = 20
    if deployment_type == 'gpu':
        print "Trying to set persistence mode for GPU"
        try:
            subprocess.check_call(["sudo", "nvidia-smi", '-pm', '1'])
        except:
            print "Error could not set persistence mode pleae manually run 'sudo nvidia-smi -pm 1'"
            pass
        fname = 'docker-compose-{}-gpus.yaml'.format(gpu_count)
    else:
        fname = 'docker-compose-{}.yaml'.format(deployment_type)
    create_custom_env(init_process, init_models, cred_envs, branch)
    print "Starting deploy/compose/{}".format(fname)
    try:
        # Fixed to dev since deployment directory does not matters for checking if docker-compose exists.
        subprocess.check_call([DOCKER_COMPOSE, '--help'],
                              cwd=os.path.join(os.path.dirname(os.path.curdir), 'deploy/compose/'))
    except:
        raise SystemError("Docker-compose is not available")
    if refresh:
        pull_latest_images(deployment_type, cpu_image, gpu_image)
    print "Trying to launch containers"
    try:
        args = [DOCKER_COMPOSE, '-f', fname, 'up', '-d']
        print " ".join(args)
        compose_process = subprocess.Popen(args, cwd=os.path.join(os.path.dirname(os.path.curdir), 'deploy/compose/'))
    except:
        raise SystemError("Could not start container")
    wait_to_start(max_minutes)
    compose_process.wait()


def stop_docker_compose(deployment_type, gpu_count, clean=False):
    if clean:
        extra_args = ['-v', ]
    else:
        extra_args = []
    if deployment_type == 'gpu':
        fname = 'docker-compose-{}-gpus.yaml'.format(gpu_count)
    else:
        fname = 'docker-compose-{}.yaml'.format(deployment_type)
    print "Stopping deploy/compose/{}".format(fname)
    try:
        subprocess.check_call([DOCKER_COMPOSE, '-f', fname, 'down'] + extra_args,
                              cwd=os.path.join(os.path.dirname(os.path.curdir),
                                               'deploy/compose'))
    except:
        raise SystemError("Could not stop containers")


def get_auth():
    token = subprocess.check_output([DOCKER, "exec", "webserver", "scripts/generate_testing_token.py"]).strip()
    server = 'http://localhost:8000/api/'
    with open('creds.json', 'w') as fh:
        json.dump({'server': server, 'token': token}, fh)
    print "token and server information are stored in creds.json"


def ingest(path):
    vuuid = str(uuid.uuid1()).replace('-', '_')
    temp_path = "/root/{}.{}".format(vuuid,path.split('.')[-1])
    container_path = "/ingest/{}.{}".format(vuuid,path.split('.')[-1])
    if container_path.endswith('.'):
        raise ValueError("{} appears to be a directory only files can be ingested".format(path))
    _ = subprocess.check_output([DOCKER, "cp", path, "webserver:{}".format(temp_path)]).strip()
    # This is required since cp fails when trying to copy a file inside volume
    _ = subprocess.check_output([DOCKER, "exec", "webserver", "cp", temp_path, "/root/media/{}".format(container_path)])
    return container_path


def handle_compose_operations(args, mode, gpus, init_process, init_models, cred_envs, gpu_compose_filename, gpu_config,
                              branch, cpu_image, gpu_image, refresh):
    if mode == 'gpu':
        generate_multi_gpu_compose(gpu_compose_filename, gpu_config, cpu_image, gpu_image)
    else:
        generate_cpu_compose(cpu_image)
    if args.action == 'stop':
        stop_docker_compose(mode, gpus)
    elif args.action == 'start':
        start_docker_compose(mode, gpus, init_process, init_models, cred_envs, branch, refresh, cpu_image, gpu_image)
        get_auth()
    elif args.action == 'auth':
        get_auth()
    elif args.action == 'ingest':
        print ingest(args.f)
    elif args.action == 'wait_to_start':
        wait_to_start()
    elif args.action == 'clean':
        stop_docker_compose(mode, gpus, clean=True)
    elif args.action == 'restart':
        stop_docker_compose(mode, gpus)
        start_docker_compose(mode, gpus, init_process, init_models, cred_envs, branch, refresh, cpu_image, gpu_image)
    else:
        raise NotImplementedError("{} and {}".format(args.action, mode))
