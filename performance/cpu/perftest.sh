#!/usr/bin/env bash
set -xe
sudo apt-get update -y
sudo apt-get install -y python2.7 wget
which python
./dvactl wait_to_start
wget localhost:8000
wget webserver:80
./dvactl auth
