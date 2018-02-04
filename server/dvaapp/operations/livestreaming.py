import subprocess as sp


class LivestreamCapture(object):

    def __init__(self,dv,path,wait_time):
        self.pid = None
        self.path = path
        self.dv = dv
        self.capture = None
        self.wait_time = wait_time
        self.last_segment_index = None

    def start_process(self):
        segments_dir = self.dv.segments_dir()
        self.capture = sp.Popen(['./scripts/consume_livestream.sh',self.path,segments_dir])
