import os,zipfile,logging
from PIL import Image
from ..models import Frame, Region


class DatasetCreator(object):
    """
    Wrapper object for a  dataset
    """

    def __init__(self,dvideo,media_dir):
        self.dvideo = dvideo
        self.primary_key = self.dvideo.pk
        self.media_dir = media_dir
        self.local_path = dvideo.path()
        self.segments_dir = "{}/{}/segments/".format(self.media_dir,self.primary_key)
        self.duration = None
        self.width = None
        self.height = None
        self.metadata = {}
        self.segment_frames_dict = {}
        self.csv_format = None

    def extract(self,extract_event):
        self.extract_zip_dataset(extract_event)
        os.remove("{}/{}/video/{}.zip".format(self.media_dir, self.primary_key, self.primary_key))

    def extract_zip_dataset(self,event):
        zipf = zipfile.ZipFile("{}/{}/video/{}.zip".format(self.media_dir, self.primary_key, self.primary_key), 'r')
        zipf.extractall("{}/{}/frames/".format(self.media_dir, self.primary_key))
        zipf.close()
        i = 0
        df_list = []
        root_length = len("{}/{}/frames/".format(self.media_dir, self.primary_key))
        for subdir, dirs, files in os.walk("{}/{}/frames/".format(self.media_dir, self.primary_key)):
            if '__MACOSX' not in subdir:
                for ofname in files:
                    fname = os.path.join(subdir, ofname)
                    if fname.endswith('jpg') or fname.endswith('jpeg'):
                        i += 1
                        try:
                            im = Image.open(fname)
                            w, h = im.size
                        except IOError:
                            logging.info("Could not open {} skipping".format(fname))
                        else:
                            dst = "{}/{}/frames/{}.jpg".format(self.media_dir, self.primary_key, i)
                            os.rename(fname, dst)
                            df = Frame()
                            df.frame_index = i
                            df.video_id = self.dvideo.pk
                            df.h = h
                            df.w = w
                            df.event_id = event.pk
                            df.name = os.path.join(subdir[root_length:], ofname)
                            if not df.name.startswith('/'):
                                df.name = "/{}".format(df.name)
                            df_list.append(df)

                    else:
                        logging.warning("skipping {} not a jpeg file".format(fname))
            else:
                logging.warning("skipping {} ".format(subdir))
        self.dvideo.frames = len(df_list)
        self.dvideo.save()
        regions = []
        per_event_region_index = 0
        for i,f in enumerate(df_list):
            if f.name:
                a = Region()
                a.video_id = self.dvideo.pk
                a.frame_index = f.frame_index
                a.per_event_index = per_event_region_index
                per_event_region_index += 1
                if '/' in f.name:
                    a.metadata = {'labels':list({ l.strip() for l in f.name.split('/')[1:] if l.strip() })}
                    a.text = f.name.split('/')
                a.region_type = a.ANNOTATION
                a.object_name = 'directory_labels'
                a.event_id = event.pk
                regions.append(a)
        event.finalize({"Region":regions, "Frame":df_list})
