from collections import namedtuple
import logging
import base64
import requests

try:
    from PIL import Image as PImage
    from IPython.display import Image, display
except:
    logging.warning("Cannot import IPython display")

VSResult = namedtuple('VSResult',field_names=['rank','entry','region','frame_index','video_id'])


def crop_region_url(url,region):
    r = requests.get(url, stream=True)
    r.raw.decode_content = True
    img = PImage.open(r.raw)
    cropped = img.crop((region['x'], region['y'], region['x']+ region['w'], region['y'] + region['h']))
    with open('temp.jpg', 'w') as f:
        cropped.save(f, format="JPEG")
    r.close()
    return "temp.jpg"


class VisualSearchResults(object):

    def __init__(self,query, task = None, query_region = None):
        self.query = query
        self.similar_images = []
        if task:
            self.query_region = None
            self.description = "Task ID {task_id} operation: {operation} with retriever {pk} " \
                             "and max_results {count}".format(task_id=task['id'], operation=task['operation'],
                                                              pk=task['arguments']['retriever_selector'],
                                                              count=task['arguments']['count'])
            for r in task['query_results']:
                if r['region']:
                    region = self.query.context.get_region(r['region'])
                    self.similar_images.append((r['rank'],VSResult(rank=r['rank'],entry=r,frame_index=r['frame_index'],
                                                                   region=region,video_id=r['video'])))
                else:
                    self.similar_images.append((r['rank'],VSResult(rank=r['rank'],entry=r,frame_index=r['frame_index'],
                                                                   region=None,video_id=r['video'])))
            self.similar_images = sorted(self.similar_images)
        else:
            self.query_region = query_region
            for r in query_region['query_results']:
                pass

    def visualize(self):
        print "Query Image"
        with open('temp.png', 'w') as f:
            f.write(base64.decodestring(self.query.query_json['image_data_b64']))
            f.close()
        display(Image("temp.png", width=300))
        print self.description
        print "Results"
        for rank, r in self.similar_images:
            frame_media_url = "/{}/frames/{}.jpg".format(r.video_id,r.frame_index)
            if r.region:
                print "Rank {}, region".format(rank)
                display(Image(crop_region_url(self.fix_url(frame_media_url), r.region), width=300))
            else:
                print "Rank {}, full frame".format(rank)
                display(Image(self.fix_url(frame_media_url), width=300))
        print "\n\n\n"

    def fix_url(self,url):
        if url.startswith('/'):
            url = "{}{}".format(self.query.context.server.replace('/api/',''),url)
            return url
        else:
            return url