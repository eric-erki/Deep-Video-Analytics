#!/usr/bin/env bash
set -x
apt-get update -y
apt-get install -y python2.7 wget
sleep 420
wget webserver:80
python2.7 ./dvactl wait_to_start
python2.7 ./dvactl auth
