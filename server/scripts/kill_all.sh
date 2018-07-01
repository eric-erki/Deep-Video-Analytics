#!/usr/bin/env sh
set -xe
ps auxww | grep 'manage.py' | awk '{print $2}' | xargs kill -9
