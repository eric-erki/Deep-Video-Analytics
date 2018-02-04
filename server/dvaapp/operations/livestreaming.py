import subprocess as sp
import os, time, logging, psutil, glob, shlex
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
        self.dv = dv
        self.path = self.dv.url
        self.capture = None
        self.wait_time = wait_time
        self.max_time = max_time
        self.last_segment_index = None
        self.segments_dir = self.dv.segments_dir()
        self.start_time = None
        self.processed_segments = set()
        self.max_wait = max_wait
        self.dv.create_directory()
        self.segment_frames_dict = {}
        self.start_index = 0
        self.csv_format = None

    def detect_csv_segment_format(self):
        format_path = "{}format.txt".format(self.segments_dir)
        self.csv_format = {}
        if not os.path.isfile(format_path):
            command ="ffprobe -i {}0.mp4 -show_frames -select_streams v:0 -print_format csv=nokey=0".format(self.segments_dir)
            csv_format_lines = sp.check_output(shlex.split(command))
            with open(format_path,'w') as formatfile:
                formatfile.write(csv_format_lines)
            logging.info("Generated csv format {}".format(self.csv_format))
        for line in file(format_path).read().splitlines():
            if line.strip():
                for i,kv in enumerate(line.strip().split(',')):
                    if '=' in kv:
                        k,v = kv.strip().split('=')
                        self.csv_format[k] = i
                    else:
                        self.csv_format[kv] = i
                break
        self.field_count = len(self.csv_format)
        self.pict_type_index = self.csv_format['pict_type']
        self.time_index = self.csv_format['best_effort_timestamp_time']

    def start_process(self):
        self.start_time = time.time()
        self.capture = sp.Popen(['./scripts/consume_livestream.sh',self.path,self.segments_dir],cwd="/root/DVA/server/")
        logging.info("Started capturing {} using process {}".format(self.path,self.capture))

    def parse_segment_framelist(self,segment_id, framelist):
        if self.csv_format is None:
            self.detect_csv_segment_format()
        frames = {}
        findex = 0
        for line in framelist.splitlines():
            if line.strip():
                entries = line.strip().split(',')
                if len(entries) == self.field_count:
                    frames[findex] = {'type': entries[self.pict_type_index], 'ts': float(entries[self.time_index])}
                    findex += 1
                else:
                    errro_message = "format used {} \n {} (expected) != {} entries in {} \n {} ".format(self.csv_format,self.field_count,len(entries),segment_id, line)
                    logging.error(errro_message)
                    raise ValueError, errro_message
        return frames

    def upload(self):
        new_segments = []
        for fname in glob.glob("{}*.mp4".format(self.segments_dir)):
            if fname not in self.processed_segments:
                logging.info(fname)
                segment_index = int(fname.split('/')[-1].split('.')[0])
                new_segments.append((segment_index,fname))
                self.processed_segments.add(fname)
        for segment_index,segment_file_name in sorted(new_segments):
            command = 'ffprobe -select_streams v -show_streams  -print_format json {}  '.format(segment_file_name)
            # logging.info(command)
            segment_json = sp.check_output(shlex.split(command), cwd=self.segments_dir)
            command = 'ffprobe -show_frames -select_streams v:0 -print_format csv {}'.format(segment_file_name)
            # logging.info(command)
            framelist = sp.check_output(shlex.split(command), cwd=self.segments_dir)
            with open("{}/{}.txt".format(self.segments_dir, segment_file_name.split('.')[0]), 'w') as framesout:
                framesout.write(framelist)
            self.segment_frames_dict[segment_index] = self.parse_segment_framelist(segment_index, framelist)
            start_time = 0.0
            end_time = 0.0
            ds = Segment()
            ds.segment_index = segment_index
            ds.start_time = start_time
            ds.start_index = self.start_index
            self.start_index += len(self.segment_frames_dict[segment_index])
            ds.frame_count = len(self.segment_frames_dict[segment_index])
            ds.end_time = end_time
            ds.video_id = self.dv.pk
            ds.event_id = self.start_index
            ds.metadata = segment_json
            ds.save()
        return new_segments

    def poll(self):
        while (time.time() - self.start_time < self.max_time) and (self.capture.poll() is None):
            try:
                new_segments = self.upload()
            except:
                kill(self.capture.pid)
                logging.exception("Failed to upload")
                break
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