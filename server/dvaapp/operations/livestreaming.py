import subprocess as sp
import os, time, logging, psutil
from ..models import Segment


def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()


class LivestreamCapture(object):

    def __init__(self,dv,event,wait_time=2,max_time=31536000,max_wait=20):
        self.pid = None
        self.event = event
        self.path = self.dv.url
        self.dv = dv
        self.capture = None
        self.wait_time = wait_time
        self.max_time = max_time
        self.last_segment_index = None
        self.segments_dir = self.dv.segments_dir()
        self.start_time = None
        self.processed_segments = set()
        self.max_wait = max_wait
        self.dv.create_directory()

    def start_process(self):
        self.start_time = time.time()
        self.capture = sp.Popen(['./scripts/consume_livestream.sh',self.path,self.segments_dir])
        logging.info("Started capturing {} using process {}".format(self.path,self.capture))

    def upload(self):
        new_segments = False
        for fname in os.listdir("{}.mp4".format(self.segments_dir)):
            if fname not in self.processed_segments:
                new_segments = True
                logging.info(fname)
                segment_index = int(fname.split('/')[-1].split('.')[0])
                ds = Segment(video=self.dv,segment_index=segment_index)
                ds.save()
                self.processed_segments.add(fname)
        return new_segments

    def poll(self):
        while (time.time() - self.start_time < self.max_time) and (self.capture.poll() is None):
            new_segments = self.upload()
            if not new_segments:
                time.sleep(self.wait_time)
                self.max_wait -= 1
            if self.max_wait == 0:
                logging.info("Killing capture process since max_wait is at 0")
                kill(self.capture.pid)
                break
        self.finalize()

    def finalize(self):
        pass