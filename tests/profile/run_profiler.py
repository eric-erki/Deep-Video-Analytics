#!/usr/bin/env python
import os
import json
import shutil
import subprocess
import glob
import sys
sys.path.append('../../client/')
from dvaclient import context, query


def store_token_for_testing():
    subprocess.check_output(['python','generate_testing_token.py'],cwd="../../server/scripts")
    shutil.move('../../server/scripts/creds.json','creds.json')


if __name__ == '__main__':
    port = 80
    if not os.path.isfile('creds.json'):
        store_token_for_testing()
    token = json.loads(file('creds.json').read())['token']
    context = context.DVAContext(token=token)
    queries = []
    for fname in glob.glob('procesess/*.json'):
        q = query.DVAQuery(json.load(file(fname)))
        q.execute(context)
        queries.append(q)
    for q in queries:
        q.wait()