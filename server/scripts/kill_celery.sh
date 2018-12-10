#!/usr/bin/env sh
set -xe
for pid in $(ps aux | grep "celery" | awk '{print $2}'); do kill -9 $pid && echo $pid; done