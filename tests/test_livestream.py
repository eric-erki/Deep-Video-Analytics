#!/usr/bin/env python
import django, sys, glob, os
sys.path.append('../server/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from dvaapp.models import TEvent, Video
from dvaapp.operations import livestreaming

if __name__ == '__main__':
    dv = Video(name="test",url="https://www.twitch.tv/wankilstudio")
    dv.save()
    start = TEvent(video=dv,operation="perform_stream_capture")
    start.save()
    l = livestreaming.LivestreamCapture(dv,start)
    l.start_process()
    l.poll()
    l.finalize()