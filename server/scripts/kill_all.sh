#!/usr/bin/env sh
set -xe
ps auxww | grep 'celery -A dva * ' | awk '{print $2}' | xargs kill -9
ps auxww | grep 'manage.py' | awk '{print $2}' | xargs kill -9