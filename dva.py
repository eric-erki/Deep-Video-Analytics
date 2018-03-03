#!/usr/bin/env python
import argparse
import subprocess
import time
import urllib2
import os
import webbrowser


def start_cpu_dev(subdir):
    print "Checking if docker-compose is available"
    max_minutes = 20
    try:
        subprocess.check_call(["docker-compose", 'ps'],
                              cwd=os.path.join(os.path.dirname(__file__), 'deploy/{}'.format(subdir)))
    except:
        raise SystemError("Docker-compose is not available")
    print "Pulling/Refreshing container images, first time it might take a while to download the image"
    try:
        subprocess.check_call(["docker", 'pull', 'akshayubhat/dva-auto:latest'])
    except:
        raise SystemError("Docker is not running / could not pull akshayubhat/dva-auto:latest image from docker hub")
    print "Trying to launch containers"
    try:
        compose_process = subprocess.Popen(["docker-compose", 'up', '-d'],
                                           cwd=os.path.join(os.path.dirname(__file__), 'deploy/{}'.format(subdir)))
    except:
        raise SystemError("Could not start container")
    while max_minutes:
        print "Checking if DVA server is running, waiting for another minute and at most {max_minutes} minutes".format(
            max_minutes=max_minutes)
        try:
            r = urllib2.urlopen("http://localhost:8000")
            if r.getcode() == 200:
                print "Open browser window and go to http://localhost:8000 to access DVA Web UI"
                print 'Use following auth code to use jupyter notebook on  '
                print subprocess.check_output(["docker", "exec", "-it", "webserver", "jupyter", 'notebook', 'list'])
                print 'For windows you might need to replace "localhost" with ip address of docker-machine'
                webbrowser.open("http://localhost:8000")
                webbrowser.open("http://localhost:8888")
                break
        except:
            pass
        time.sleep(60)
        max_minutes -= 1
    compose_process.wait()


def start_gpu(gpu_count):
    print "Checking if docker-compose is available"
    max_minutes = 20
    if gpu_count == 1:
        fname = 'docker-compose-gpu.yml'
    else:
        fname = 'docker-compose-{}-gpus.yml'.format(gpu_count)
    try:
        subprocess.check_call(["docker-compose", 'ps'],
                              cwd=os.path.join(os.path.dirname(__file__), 'deploy/gpu'))
    except:
        raise SystemError("Docker-compose is not available")
    print "Pulling/Refreshing container images, first time it might take a while to download the image"
    try:
        subprocess.check_call(["docker", 'pull', 'akshayubhat/dva-auto:latest'])
    except:
        raise SystemError("Docker is not running / could not pull akshayubhat/dva-auto:latest image from docker hub")
    print "Trying to launch containers"
    try:
        compose_process = subprocess.Popen(["docker-compose", 'up', '-d', fname, ],
                                           cwd=os.path.join(os.path.dirname(__file__), 'deploy/gpu'))
    except:
        raise SystemError("Could not start container")
    while max_minutes:
        print "Checking if DVA server is running, waiting for another minute and at most {max_minutes} minutes".format(
            max_minutes=max_minutes)
        try:
            r = urllib2.urlopen("http://localhost:8000")
            if r.getcode() == 200:
                print "Open browser window and go to http://localhost:8000 to access DVA Web UI"
                print 'Use following auth code to use jupyter notebook on  '
                print subprocess.check_output(["docker", "exec", "-it", "webserver", "jupyter", 'notebook', 'list'])
                print 'For windows you might need to replace "localhost" with ip address of docker-machine'
                webbrowser.open("http://localhost:8000")
                webbrowser.open("http://localhost:8888")
                break
        except:
            pass
        time.sleep(60)
        max_minutes -= 1
    compose_process.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("action", help="Select action out of { start | stop | restart } ")
    parser.add_argument("--type", help="select deployment type { dev | cpu | gpu } . default is cpu", default="cpu")
    parser.add_argument("--gpus", help="For GPU mode select number of P100 GPUs: 1, 2, 4. default is 1", default=1,
                        type=int)
    args = parser.parse_args()
    if args.action == 'stop' or args.action == 'restart':
        pass
    if args.action == 'start' or args.action == 'restart':
        if args.type != 'gpu':
            start_cpu_dev(args.type)
        else:
            start_gpu(args.gpus)
