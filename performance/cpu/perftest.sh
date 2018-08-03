#!/usr/bin/env bash
set -x
apt-get update -y
apt-get install -y python2.7 wget
sleep 420
wget localhost:8000
wget webserver:80
which python
./dvactl wait_to_start
./dvactl auth
