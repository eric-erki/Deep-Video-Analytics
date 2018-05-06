import subprocess as sp
import os, time, logging, shlex
from ..models import Segment
from ..fs import upload_file_to_remote
from django.conf import settings
from ..processing import process_next

try:
    import psutil
except ImportError:
    pass


def kill(proc_pid):
    process = psutil.Process(proc_pid)
    for proc in process.children(recursive=True):
        proc.kill()
    process.kill()


class LivestreamCapture(object):

    def __init__(self,dv,event,wait_time=3,max_time=31536000,max_wait=120,segments_batch_size=5):
        self.pid = None
        self.event = event
        self.dv = dv
        self.path = self.dv.url
        self.capture = None
        self.wait_time = event.arguments.get('wait_time',wait_time)
        self.max_time = event.arguments.get('max_time',max_time)
        self.last_processed_segment_index = -1
        self.segments_dir = self.dv.segments_dir()
        self.start_time = None
        self.processed_segments = set()
        self.max_wait = event.arguments.get('max_wait',max_wait)
        self.dv.create_directory()
        self.dv.stream = True
        self.segment_frames_dict = {}
        self.start_index = 0
        self.csv_format = None
        self.segments_batch_size = event.arguments.get('segments_batch_size',segments_batch_size)
        self.segments_batch = set()
        self.last_segment_time = time.time()

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
        args = ['./scripts/consume_livestream.sh',self.path,self.segments_dir]
        logging.info(args)
        self.capture = sp.Popen(args,cwd="/root/DVA/server/")
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
                    frames[findex] = (entries[self.pict_type_index],float(entries[self.time_index]))
                    findex += 1
                else:
                    errro_message = "format used {} \n {} (expected) != {} entries in {} \n {} ".format(self.csv_format,self.field_count,len(entries),segment_id, line)
                    logging.error(errro_message)
                    raise ValueError, errro_message
        return frames

    def upload(self,final=False):
        segments_processed = False
        logging.info(self.last_processed_segment_index)
        if not final:
            while os.path.isfile('{}{}.mp4'.format(self.segments_dir,self.last_processed_segment_index+2)):
                segment_file_name = '{}{}.mp4'.format(self.segments_dir,self.last_processed_segment_index+1)
                segment_index = self.last_processed_segment_index + 1
                self.process_segment(segment_index, segment_file_name)
                segments_processed = True
        else:
            segment_file_name = '{}{}.mp4'.format(self.segments_dir, self.last_processed_segment_index + 1)
            segment_index = self.last_processed_segment_index + 1
            if os.path.isfile(segment_file_name):
                self.process_segment(segment_index, segment_file_name)
                segments_processed = True
        return segments_processed

    def process_segment(self, segment_index, segment_file_name):
        logging.info("processing {} {}".format(segment_index, segment_file_name))
        command = 'ffprobe -select_streams v -show_streams  -print_format json {}  '.format(segment_file_name)
        # logging.info(command)
        segment_json = sp.check_output(shlex.split(command), cwd=self.segments_dir)
        command = 'ffprobe -show_frames -select_streams v:0 -print_format csv {}'.format(segment_file_name)
        # logging.info(command)
        framelist = sp.check_output(shlex.split(command), cwd=self.segments_dir)
        self.segment_frames_dict[segment_index] = self.parse_segment_framelist(segment_index, framelist)
        start_time = 0.0
        end_time = 0.0
        ds = Segment()
        ds.segment_index = segment_index
        ds.start_time = start_time
        ds.start_index = self.start_index
        ds.framelist = self.segment_frames_dict[segment_index]
        self.start_index += len(self.segment_frames_dict[segment_index])
        ds.frame_count = len(self.segment_frames_dict[segment_index])
        ds.end_time = end_time
        ds.video_id = self.dv.pk
        ds.event_id = self.event.pk
        ds.metadata = segment_json
        ds.save()
        self.last_processed_segment_index = segment_index
        if settings.ENABLE_CLOUDFS:
            upload_file_to_remote(ds.path(""))
        self.dv.segments = self.last_processed_segment_index + 1
        self.dv.save()
        self.segments_batch.add(segment_index)
        self.last_segment_time = time.time()
        self.processed_segments.add(segment_file_name)
        if (self.last_processed_segment_index % self.segments_batch_size == 0):
            process_next(self.event,map_filters=[{'segment_index__in':list(self.segments_batch)}])
            self.segments_batch = set()

    def poll(self):
        while (time.time() - self.start_time < self.max_time) and (self.capture.poll() is None):
            try:
                new_segments = self.upload()
            except:
                logging.exception("Failed to upload")
                break
            if not new_segments:
                time.sleep(self.wait_time)
            if (time.time() - self.last_segment_time) > self.max_wait:
                logging.info("no new segment found in last {} seconds".format(self.max_wait))
                break
        logging.info("Killing capture process")
        kill(self.capture.pid)
        try:
            self.upload(final=True)
        except:
            pass

    def finalize(self):
        process_next(self.event, map_filters=[{'segment_index__in': list(self.segments_batch)}])