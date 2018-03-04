#!/usr/bin/env python
import argparse
import copy
import subprocess
import time
import urllib2
import os
import webbrowser


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
        subprocess.check_call(["docker-compose", 'ps'],
                              cwd=os.path.join(os.path.dirname(__file__), 'deploy/{}'.format(deployment_type)))
    except:
        raise SystemError("Docker-compose is not available")
    print "Pulling/Refreshing container images, first time it might take a while to download the image"
    try:
        if deployment_type == 'gpu':
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
        compose_process = subprocess.Popen(args, cwd=os.path.join(os.path.dirname(__file__),
                                                                  'deploy/{}'.format(deployment_type)),env=envs)
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("action",
                        help="Select action out of { start | stop | clean | clean_restart "
                             "| jupyter (view jupyter URL) | wsgi (view logs) }")
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
    else:
        raise NotImplementedError("{} and {}".format(args.action,args.type))