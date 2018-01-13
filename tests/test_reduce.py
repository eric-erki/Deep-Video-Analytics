#!/usr/bin/env python
import django, sys, glob, os
sys.path.append('../server/')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
django.setup()
from dvaui.view_shared import handle_uploaded_file
from dvaapp.tasks import perform_import, perform_frame_download
from django.core.files.uploadedfile import SimpleUploadedFile
from dvaapp.models import TEvent

if __name__ == '__main__':
    pass