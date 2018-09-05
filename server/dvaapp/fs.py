from django.conf import settings
import os
import shlex
import boto3
import shutil
import errno
import logging
import subprocess
import requests
import urlparse
from dva.in_memory import redis_client

try:
    from google.cloud import storage
except:
    logging.exception("Could not import gcloud storage client")
    pass
try:
    S3 = boto3.resource('s3')
except:
    logging.exception("Could not initialize S3")
    pass
try:
    GS = storage.Client()
except:
    # suppress the exception unless GCloud support is really required.
    if settings.MEDIA_BUCKET and settings.CLOUD_FS_PREFIX == 'gs':
        logging.exception("Could not initialize GS client")
    pass

if settings.MEDIA_BUCKET and settings.CLOUD_FS_PREFIX == 's3':
    S3_MODE = True
    GS_MODE = False
    BUCKET = S3.Bucket(settings.MEDIA_BUCKET)
elif settings.MEDIA_BUCKET and settings.CLOUD_FS_PREFIX == 'gs':
    S3_MODE = False
    GS_MODE = True
    BUCKET = GS.get_bucket(settings.MEDIA_BUCKET)
else:
    S3_MODE = False
    GS_MODE = False
    BUCKET = None

if 'DO_ACCESS_KEY_ID' in os.environ and 'DO_SECRET_ACCESS_KEY' and os.environ:
    do_session = boto3.session.Session()
    do_client = do_session.client('s3', region_name=os.environ.get('DO_REGION', 'nyc3'),
                                  endpoint_url='https://{}.digitaloceanspaces.com'.format(
                                      os.environ.get('DO_REGION', 'nyc3')),
                                  aws_access_key_id=os.environ['DO_ACCESS_KEY_ID'],
                                  aws_secret_access_key=os.environ['DO_SECRET_ACCESS_KEY'])
    do_resource = do_session.resource('s3', region_name=os.environ.get('DO_REGION', 'nyc3'),
                                  endpoint_url='https://{}.digitaloceanspaces.com'.format(
                                      os.environ.get('DO_REGION', 'nyc3')),
                                  aws_access_key_id=os.environ['DO_ACCESS_KEY_ID'],
                                  aws_secret_access_key=os.environ['DO_SECRET_ACCESS_KEY'])


def cacheable(path):
    return path.startswith('/queries/') or '/segments/' in path or '/regions/' in path \
           or ('/frames/' in path and (path.endswith('.jpg') or path.endswith('.png')))


def cache_path(path, payload=None, expire_in_seconds=600):
    if not path.startswith('/'):
        path = "/{}".format(path)
    if cacheable(path):
        if payload is None:
            with open('{}{}'.format(settings.MEDIA_ROOT, path), 'rb') as body:
                redis_client.set(path, body.read(), ex=expire_in_seconds, nx=True)
        else:
            redis_client.set(path, payload, ex=expire_in_seconds, nx=True)
        return True
    else:
        return False


def get_from_cache(path):
    """
    :param path:
    :return:
    """
    if not path.startswith('/'):
        path = "/{}".format(path)
    if cacheable(path):
        body = redis_client.get(path)
        return body
    return None


def get_from_remote_fs(src, path, dlpath, original_path, safe):
    if S3_MODE:
        try:
            BUCKET.download_file(src, dlpath)
        except:
            raise ValueError("{} to {}".format(path, dlpath))
    else:
        try:
            with open(dlpath, 'w') as fout:
                BUCKET.get_blob(src).download_to_file(fout)
        except:
            raise ValueError("{} to {}".format(src, dlpath))
    if safe:
        os.rename(dlpath, original_path)
    # checks and puts the object back in cache
    cache_path(path)


def mkdir_safe(dlpath):
    try:
        os.makedirs(os.path.dirname(dlpath))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def retrieve_video_via_url(dv, url):
    dv.create_directory(create_subdirs=True)
    output_dir = "{}/{}/{}/".format(settings.MEDIA_ROOT, dv.pk, 'video')
    command = "youtube-dl -f 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/mp4'  \"{}\" -o {}.mp4".format(url, dv.pk)
    logging.info(command)
    download = subprocess.Popen(shlex.split(command), cwd=output_dir)
    download.wait()
    if download.returncode != 0:
        raise ValueError("Could not download the video")


def copy_remote(dv, path):
    extension = path.split('.')[-1]
    source = '{}/{}'.format(settings.MEDIA_BUCKET, path.strip('/'))
    dest = '{}/video/{}.{}'.format(dv.pk, dv.pk, extension)
    dv.create_directory()  # for compatibility and to ensure that it sync does not fails.
    if S3_MODE:
        try:
            BUCKET.Object(dest).copy({'Bucket': settings.MEDIA_BUCKET, 'Key': path.strip('/')})
        except:
            raise ValueError("Could not copy from {} to {}".format(source, dest))
        S3.Object(settings.MEDIA_BUCKET, path.strip('/')).delete()
    elif GS_MODE:
        BUCKET.copy_blob(BUCKET.get_blob(path.strip('/')), BUCKET, new_name=dest)
        BUCKET.delete_blob(path.strip('/'))
    else:
        raise ValueError("NFS disabled and unknown cloud storage prefix")


def ensure(path, dirnames=None, media_root=None, safe=False, event_id=None):
    original_path = None
    if BUCKET is not None:
        if media_root is None:
            media_root = settings.MEDIA_ROOT
        if dirnames is None:
            dirnames = {}
        if path.startswith('/') or media_root.endswith('/'):
            dlpath = "{}{}".format(media_root, path)
        else:
            dlpath = "{}/{}".format(media_root, path)
        if safe:
            if not event_id is None:
                original_path = dlpath
                dlpath = "{}.{}".format(dlpath, event_id)
            else:
                raise ValueError("Safe ensure must be used with event id instead got {}".format(event_id))
        dirname = os.path.dirname(dlpath)
        if os.path.isfile(dlpath):
            return True
        else:
            if dirname not in dirnames and not os.path.exists(dirname):
                mkdir_safe(dlpath)
            src = path.strip('/')
            body = get_from_cache(path)
            if body:
                with open(dlpath, 'w') as fout:
                    fout.write(body)
                if safe:
                    os.rename(dlpath, original_path)
            else:
                get_from_remote_fs(src, path, dlpath, original_path, safe)


def get_path_to_file(path, local_path):
    """
    # resource.meta.client.download_file(bucket, key, ofname, ExtraArgs={'RequestPayer': 'requester'})
    :param remote_path: e.g. s3://bucket/asd/asdsad/key.zip or gs:/bucket_name/key .. or /
    :param local_path:
    :return:
    """
    if settings.ENABLE_CLOUDFS and path.startswith('/ingest/'):
        if S3_MODE:
            path = "s3://{}{}".format(settings.MEDIA_BUCKET, path)
        elif GS_MODE:
            path = "gs://{}{}".format(settings.MEDIA_BUCKET, path)
        else:
            raise ValueError("NFS disabled but neither GS or S3 enabled.")
    # avoid maliciously crafted relative imports outside media root
    if path.startswith('/ingest') and '..' not in path:
        shutil.move(os.path.join(settings.MEDIA_ROOT, path.strip('/')), local_path)
    # avoid maliciously crafted relative imports outside test dir
    elif path.startswith('/root/DVA/tests/ci/') and '..' not in path:
        shutil.move(path, local_path)
    elif path.startswith('http'):
        u = urlparse.urlparse(path)
        if u.hostname == 'www.dropbox.com' and not path.endswith('?dl=1'):
            r = requests.get(path + '?dl=1')
        else:
            r = requests.get(path, stream=True)
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        r.close()
    elif path.endswith('/'):
        raise ValueError("Cannot import directories {}".format(path))
    elif path.startswith('s3'):
        bucket_name = path[5:].split('/')[0]
        key = '/'.join(path[5:].split('/')[1:])
        remote_bucket = S3.Bucket(bucket_name)
        remote_bucket.download_file(key, local_path)
    elif path.startswith('gs'):
        bucket_name = path[5:].split('/')[0]
        key = '/'.join(path[5:].split('/')[1:])
        remote_bucket = GS.get_bucket(bucket_name)
        with open(local_path, 'w') as fout:
            remote_bucket.get_blob(key).download_to_file(fout)
    elif path.startswith('do'):
        bucket_name = path[5:].split('/')[0]
        key = '/'.join(path[5:].split('/')[1:])
        do_client.download_file(bucket_name, key, local_path)
    else:
        raise ValueError("Unknown file system {}".format(path))


def upload_file_to_path(local_path, remote_path, make_public=False):
    fs_type = remote_path[:2]
    bucket_name = remote_path[5:].split('/')[0]
    key = '/'.join(remote_path[5:].split('/')[1:])
    if remote_path.endswith('/'):
        raise ValueError("key/remote-path cannot end in a /")
    elif fs_type == 's3':
        with open(local_path, 'rb') as body:
            S3.Object(bucket_name, key).put(Body=body)
        if make_public:
            object_acl = S3.ObjectAcl(bucket_name, key)
            object_acl.put(ACL='public-read')
    elif fs_type == 'gs':
        remote_bucket = GS.get_bucket(bucket_name)
        blob = remote_bucket.blob(key.strip('/'))
        blob.upload_from_filename(local_path)
        if make_public:
            blob.make_public()
    elif fs_type == 'do':
        do_client.upload_file(local_path, bucket_name, key)
        if make_public:
            object_acl = do_resource.ObjectAcl(bucket_name, key)
            object_acl.put(ACL='public-read')
    else:
        raise ValueError("Unknown cloud file system : '{}'".format(remote_path))


def upload_file_to_remote(fpath, cache=True):
    if cache:
        cache_path(fpath)
    if S3_MODE:
        with open('{}{}'.format(settings.MEDIA_ROOT, fpath), 'rb') as body:
            S3.Object(settings.MEDIA_BUCKET, fpath.strip('/')).put(Body=body)
    else:
        fblob = BUCKET.blob(fpath.strip('/'))
        fblob.upload_from_filename(filename='{}{}'.format(settings.MEDIA_ROOT, fpath))


def download_video_from_remote_to_local(dv):
    logging.info("Download entire directory from remote fs for {}".format(dv.pk))
    if S3_MODE:
        dest = '{}/{}/'.format(settings.MEDIA_ROOT, dv.pk)
        src = 's3://{}/{}/'.format(settings.MEDIA_BUCKET, dv.pk)
        try:
            os.mkdir(dest)
        except:
            pass
        command = " ".join(['aws', 's3', 'sync', '--quiet', src, dest])
        syncer = subprocess.Popen(['aws', 's3', 'sync', '--quiet', '--size-only', src, dest])
        syncer.wait()
        if syncer.returncode != 0:
            raise ValueError("Error while executing : {}".format(command))
    else:
        dv.create_directory()
        for blob in BUCKET.list_blobs(prefix='{}/'.format(dv.pk)):
            dirname = os.path.dirname("{}/{}".format(settings.MEDIA_ROOT,blob.name))
            if 'events' in dirname and not os.path.isdir(dirname):
                try:
                    os.mkdir(dirname)
                except:
                    pass
            with open("{}/{}".format(settings.MEDIA_ROOT,blob.name), 'w') as fout:
                blob.download_to_file(fout)


def upload_video_to_remote(video_id):
    logging.info("Uploading entire directory to remote fs for {}".format(video_id))
    src = '{}/{}/'.format(settings.MEDIA_ROOT, video_id)
    if S3_MODE:
        dest = 's3://{}/{}/'.format(settings.MEDIA_BUCKET, video_id)
        command = " ".join(['aws', 's3', 'sync', '--quiet', src, dest])
        syncer = subprocess.Popen(['aws', 's3', 'sync', '--quiet', '--size-only', src, dest])
        syncer.wait()
        if syncer.returncode != 0:
            raise ValueError("Error while executing : {}".format(command))
    elif GS_MODE:
        root_length = len(settings.MEDIA_ROOT)
        for root, directories, filenames in os.walk(src):
            for filename in filenames:
                path = os.path.join(root, filename)
                logging.info("uploading {} with gcs version {}".format(path,storage.__version__))
                upload_file_to_remote(path[root_length:], cache=False)
    else:
        raise ValueError


def download_s3_dir(dist, local, bucket, client=None, resource=None):
    """
    Taken from http://stackoverflow.com/questions/31918960/boto3-to-download-all-files-from-a-s3-bucket
    :param client:
    :param resource:
    :param dist:
    :param local:
    :param bucket:
    :return:
    """
    if client is None and resource is None:
        client = boto3.client('s3')
        resource = boto3.resource('s3')
    paginator = client.get_paginator('list_objects')
    for result in paginator.paginate(Bucket=bucket, Delimiter='/', Prefix=dist, RequestPayer='requester'):
        if result.get('CommonPrefixes') is not None:
            for subdir in result.get('CommonPrefixes'):
                download_s3_dir(subdir.get('Prefix'), local, bucket, client, resource)
        if result.get('Contents') is not None:
            for ffile in result.get('Contents'):
                if not os.path.exists(os.path.dirname(local + os.sep + ffile.get('Key'))):
                    os.makedirs(os.path.dirname(local + os.sep + ffile.get('Key')))
                resource.meta.client.download_file(bucket, ffile.get('Key'), local + os.sep + ffile.get('Key'),
                                                   ExtraArgs={'RequestPayer': 'requester'})
