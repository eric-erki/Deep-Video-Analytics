"""
Code in this file assumes that it is being run via dvactl and git repo root as current directory
"""
import subprocess
import time
import urllib2
import os
import json
import webbrowser
from . import test, gpu


def create_custom_env(init_process, init_models, deployment_type):
    envs = {'INIT_PROCESS': init_process, 'INIT_MODELS': init_models}
    if deployment_type == 'test':
        envs['ENABLE_CLOUDFS'] = 1
        try:
            envs.update(test.load_envs(os.path.expanduser('~/media.env')))
        except:
            print '~/media.env not found. required for testing rfs mode.'
        try:
            envs.update(test.load_envs(os.path.expanduser('~/aws.env')))
        except:
            print '~/aws.env not found. required for testing rfs mode.'
    else:
        if os.path.isfile(os.path.expanduser('~/aws.env')):
            envs.update(test.load_envs(os.path.expanduser('~/aws.env')))
        else:
            print '{} not found. not passing AWS creds.'.format(os.path.expanduser('~/aws.env'))
        if os.path.isfile(os.path.expanduser('~/do.env')):
            envs.update(test.load_envs(os.path.expanduser('~/do.env')))
        else:
            print '{} not found. not passing Digital Ocean creds.'.format(os.path.expanduser('~/do.env'))
    with open('custom.env', 'w') as out:
        out.write(file('default.env').read())
        out.write('\n')
        for k, v in envs.items():
            out.write("{}={}\n".format(k, v))


def start_docker_compose(deployment_type, gpu_count, init_process, init_models):
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
    print "Starting deploy/compose/{}/{}".format(deployment_type, fname)
    try:
        # Fixed to dev since deployment directory does not matters for checking if docker-compose exists.
        subprocess.check_call(["docker-compose", 'ps'],
                              cwd=os.path.join(os.path.dirname(os.path.curdir), 'deploy/compose/dev'))
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
        compose_process = subprocess.Popen(args, cwd=os.path.join(os.path.dirname(os.path.curdir),
                                                                  'deploy/compose/{}'.format(deployment_type)))
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


def stop_docker_compose(deployment_type, gpu_count, clean=False):
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
    print "Stopping deploy/compose/{}/{}".format(deployment_type, fname)
    try:
        subprocess.check_call(["docker-compose", '-f', fname, 'down'] + extra_args,
                              cwd=os.path.join(os.path.dirname(os.path.curdir),
                                               'deploy/compose/{}'.format(deployment_type)))
    except:
        raise SystemError("Could not stop containers")


def view_notebook_url():
    print 'Use following url containing pre-auth token to use jupyter notebook'
    print subprocess.check_output(["docker", "exec", "-it", "webserver", "jupyter", 'notebook', 'list'])


def view_uwsgi_logs():
    print 'Use following auth code to use jupyter notebook on  '
    print subprocess.check_output(
        ["docker", "exec", "-it", "webserver", "bash", '-c ', "'cat /var/log/supervisor/app-*'"])


def get_auth():
    token = subprocess.check_output(["docker", "exec", "-it", "webserver", "scripts/generate_testing_token.py"]).strip()
    server = 'http://localhost:8000/api/'
    with open('creds.json','w') as fh:
        json.dump({'server':server,'token':token},fh)
    print "token and server information are stored in creds.json"


def handle_compose_operations(args,mode,gpus):
    if mode == 'gpu':
        gpu.generate_multi_gpu_compose()
    if args.action == 'stop':
        stop_docker_compose(mode, gpus)
    elif args.action == 'start':
        start_docker_compose(mode, gpus, args.init_process, args.init_models)
        get_auth()
    elif args.action == 'auth':
        get_auth()
    elif args.action == 'clean':
        stop_docker_compose(mode, gpus, clean=True)
        if mode == 'test':
            test.clear_media_bucket()
    elif args.action == 'restart':
        stop_docker_compose(mode, gpus)
        start_docker_compose(mode, gpus, args.init_process, args.init_models)
    elif args.action == 'clean_restart':
        stop_docker_compose(mode, gpus, clean=True)
        if mode == 'test':
            test.clear_media_bucket()
            start_docker_compose(mode, gpus, args.init_process, args.init_models)
    elif args.action == 'notebook':
        view_notebook_url()
    elif args.action == 'wsgi':
        view_uwsgi_logs()
    else:
        raise NotImplementedError("{} and {}".format(args.action, mode))
