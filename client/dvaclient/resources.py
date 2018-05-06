import requests


class BaseResource(object):

    def __init__(self, pk, context, entry=None):
        self.pk = pk
        self.context = context
        self.entry = entry

    def refresh(self):
        pass


class Video(BaseResource):
    _path = 'video'

class Frame(BaseResource):
    _path = 'frame'

