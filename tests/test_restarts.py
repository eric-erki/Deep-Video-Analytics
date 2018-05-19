#!/usr/bin/env python
import subprocess
import time

if __name__ == '__main__':
    subprocess.check_call(["wget","https://raw.githubusercontent.com/VisualDataNetwork/root/master/map_reduce/video.json"])
    subprocess.check_call(["./dvactl","exec","-f","video.json"])
    time.sleep(120)
    subprocess.check_call(["docker","exec",'-u="root"','-it','webserver','./../tests/kill_workers.sh'])
