#!/usr/bin/env python
import django
import os, sys, glob
sys.path.append("../server/")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from django.core.files.uploadedfile import SimpleUploadedFile
from dvaui.view_shared import handle_uploaded_file
from dvaapp.models import Video, TEvent, DVAPQL, Retriever, TrainedModel, Export
from django.conf import settings
from dvaapp.processing import DVAPQLProcess
from dvaapp.tasks import perform_dataset_extraction, perform_indexing, perform_export, perform_import, \
    perform_detection, \
    perform_video_segmentation, perform_transformation

if __name__ == '__main__':
    for fname in glob.glob('data/citest*.mp4'):
        name = fname.split('/')[-1].split('.')[0]
        f = SimpleUploadedFile(fname, file(fname).read(), content_type="video/mp4")
        handle_uploaded_file(f, name)
    if settings.DEBUG:
        for fname in glob.glob('data/*.zip'):
            name = fname.split('/')[-1].split('.')[0]
            f = SimpleUploadedFile(fname, file(fname).read(), content_type="application/zip")
            handle_uploaded_file(f, name)
    for i, v in enumerate(Video.objects.all()):
        perform_import(TEvent.objects.get(video=v, operation='perform_import').pk)
        if v.dataset:
            arguments = {'sync': True}
            perform_dataset_extraction(TEvent.objects.create(video=v, arguments=arguments).pk)
        else:
            arguments = {'sync': True}
            perform_video_segmentation(TEvent.objects.create(video=v, arguments=arguments).pk)
        arguments = {'trainedmodel_selector' : {'name':'inception'}, 'target': 'frames'}
        perform_indexing(TEvent.objects.create(video=v, arguments=arguments).pk)
        if i == 1:  # save travis time by just running detection on first video
            # face_mtcnn
            arguments = {'trainedmodel_selector' : {'name':'face'}}
            dt = TEvent.objects.create(video=v, arguments=arguments)
            perform_detection(dt.pk)
            print "done perform_detection"
            arguments = {'filters': {'event_id': dt.pk}, }
            perform_transformation(TEvent.objects.create(video=v, arguments=arguments).pk)
            print "done perform_transformation"
            # coco_mobilenet
            arguments = {'trainedmodel_selector' : {'name':'coco'}}
            dt = TEvent.objects.create(video=v, arguments=arguments)
            perform_detection(dt.pk)
            print "done perform_detection"
            arguments = {'filters': {'event_id': dt.pk}, }
            perform_transformation(TEvent.objects.create(video=v, arguments=arguments).pk)
            print "done perform_transformation"
            # inception on crops from detector
            arguments = {'trainedmodel_selector' : {'name':'inception'}, 'target': 'regions',
                         'filters': {'event_id': dt.pk, 'w__gte': 50, 'h__gte': 50}}
            perform_indexing(TEvent.objects.create(video=v, arguments=arguments).pk)
            print "done perform_indexing"
        temp = TEvent.objects.create(arguments={'video_selector':{'pk':v.pk}})
        perform_export(temp.pk)
        fname = Export.objects.get(event=temp).url
        v.delete() # Delete exported video so that the uniqueness constraint is not violated.
        f = SimpleUploadedFile(fname, file(fname.replace(settings.MEDIA_URL,settings.MEDIA_ROOT)).read(),
                               content_type="application/zip")
        print fname
        vimported = handle_uploaded_file(f, fname)
        perform_import(TEvent.objects.get(video=vimported, operation='perform_import').pk)
