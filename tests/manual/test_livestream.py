#!/usr/bin/env python
import django, sys, glob, os,time, logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='/root/DVA/logs/tests.log',
                    filemode='a')
sys.path.append('../../server/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from dvaapp.models import TEvent, Video
from dvaapp.tasks import perform_stream_capture

if __name__ == '__main__':
    dv = Video(name="test",url=sys.argv[-1])
    dv.save()
    start = TEvent(video=dv,operation="perform_stream_capture",arguments={"max_time":180})
    start.save()
    perform_stream_capture(start.pk)