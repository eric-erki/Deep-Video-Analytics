#!/usr/bin/env python
import subprocess
import time
import os
import json

if __name__ == '__main__':
    config = json.load(file('config.json'))
    assert config['mode'] == 'test'
    subprocess.check_call(["wget","https://raw.githubusercontent.com/VisualDataNetwork/root/master/map_reduce/video.json"])
    subprocess.check_call(["./dvactl","exec","-f","video.json"])
    time.sleep(120)
    os.system('docker exec -u="root" -it inception ./scripts/kill_manager.sh')
