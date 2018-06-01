import os, json, requests


class DVAContext(object):
    def __init__(self, server=None, token=None):
        if server:
            if not server.endswith('/'):
                server = "{}/".format(server)
            self.server = server
        else:
            self.server = 'http://localhost:8000/api/'
        if token:
            self.token = token
        elif 'DVA_TOKEN' in os.environ:
            self.token = os.environ['DVA_TOKEN']
        elif os.path.isfile('creds.json'):
            self.token = json.loads(file('creds.json').read())['token']
        self.headers = {'Authorization': 'Token {}'.format(self.token)}

    def iterate_paginated(self, url):
        r = requests.get(url, headers=self.headers)
        if r.ok:
            results = r.json()
            while True:
                for k in results['results']:
                    yield k
                if results['next']:
                    r = requests.get(results['next'], headers=self.headers)
                    if r.ok:
                        results = r.json()
                    else:
                        r.raise_for_status()
                else:
                    raise StopIteration
        else:
            r.raise_for_status()

    def list_videos(self):
        url = "{server}videos/".format(server=self.server)
        return list(self.iterate_paginated(url))

    def list_queries(self):
        url = "{server}queries/".format(server=self.server)
        return list(self.iterate_paginated(url))

    def list_models(self):
        url = "{server}models/".format(server=self.server)
        return list(self.iterate_paginated(url))

    def list_retrievers(self):
        url = "{server}retrievers/".format(server=self.server)
        return list(self.iterate_paginated(url))

    def list_events(self,verbose=False,query_id=None):
        url = "{server}events/".format(server=self.server)
        events = list(self.iterate_paginated(url))
        if verbose:
            for e in events:
                if query_id and e['parent_process'].strip('/').split('/')[-1] == str(query_id):
                    print e['created'], e['start_ts'], e['operation'], e['started'], e['completed'], e['duration']
                elif query_id is None:
                    print e['created'], e['start_ts'], e['operation'], e['started'], e['completed'], e['duration']
        return events

    def get_frame(self, frame_pk):
        r = requests.get("{server}frames/{frame_pk}".format(server=self.server,frame_pk=frame_pk),
                         headers=self.headers)
        if r.ok:
            return r.json()
        else:
            r.raise_for_status()

    def get_region(self, region_pk):
        r = requests.get("{server}regions/{region_pk}".format(server=self.server,region_pk=region_pk),
                         headers=self.headers)
        if r.ok:
            return r.json()
        else:
            r.raise_for_status()

    def execute_query(self, query):
        r = requests.post("{server}queries/".format(server=self.server), data={'script': json.dumps(query)},
                          headers=self.headers)
        if r.ok:
            return r.json()
        else:
            raise r.raise_for_status()

    def get_results(self, query_id):
        r = requests.get("{server}queries/{query_id}/".format(server=self.server, query_id=query_id),
                         headers=self.headers)
        if r.ok:
            return r.json()
        else:
            raise r.raise_for_status()
