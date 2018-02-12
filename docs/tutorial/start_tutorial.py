#!/usr/bin/env python
import os, urllib2, subprocess, time, webbrowser

if __name__ == '__main__':
    print "Checking if docker-compose is available"
    max_minutes = 20
    try:
        subprocess.check_call(["docker-compose", 'ps'],
                              cwd=os.path.join(os.path.dirname(__file__), '../../deploy/cpu'))
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
                                           cwd=os.path.join(os.path.dirname(__file__), '../../deploy/cpu'))
    except:
        raise SystemError("Could not start container")
    while max_minutes:
        print "Checking if DVA server is running, waiting for another minute and at most {max_minutes} minutes".format(max_minutes=max_minutes)
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