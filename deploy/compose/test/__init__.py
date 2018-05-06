"""
Code in this file assumes that it is being run via dvactl and git repo root as current directory
"""
import os
import subprocess


def load_envs(path):
    return {line.split('=')[0]: line.split('=')[1].strip() for line in file(path)}


def clear_media_bucket():
    envs = load_envs(os.path.expanduser('~/media.env'))
    print "Erasing bucket {}".format(envs['MEDIA_BUCKET'])
    subprocess.check_call(['aws','s3','rm','--recursive','--quiet','s3://{}'.format(envs['MEDIA_BUCKET'])])
    print "Bucket erased"
