#!/usr/bin/env sh
set -xe
apt-get update -q && apt-get install -y wget xz-utils pkg-config python-dev python-opencv unzip libopencv-dev python-pip libav-tools libjpeg-dev  libpng-dev  libtiff-dev  libjasper-dev  python-numpy python-scipy  python-pycurl  python-opencv git nginx supervisor libpq-dev python-cffi build-essential libssl-dev libffi-dev python-dev libhdf5-dev cmake libopenblas-dev swig
apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8
echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main" > /etc/apt/sources.list.d/pgdg.list
apt-get update && apt-get install -y postgresql-client-9.6 zip
dpkg -L python-opencv
pip install pip --upgrade
pip install --upgrade setuptools
pip install --upgrade matplotlib
pip install --upgrade cffi
pip install --no-deps keras
pip install -r requirements.txt
pip install --no-cache-dir http://download.pytorch.org/whl/cpu/torch-0.3.1-cp27-cp27mu-linux_x86_64.whl
pip install --no-cache-dir mxnet==0.11.0
pip install torchvision
pip install --no-deps h5py
wget https://www.dropbox.com/s/bjyzb8hytdwp2tp/ffmpeg-release-64bit-static.tar.xz
tar xvfJ ffmpeg-release-64bit-static.tar.xz
mv ffmpeg*/* /bin/
shasum ffmpeg-release-64bit-static.tar.xz | awk '$1!="a93bce9e510afef02f7e2592f6b5d117dcd08854"{exit 1}'
wget https://yt-dl.org/downloads/latest/youtube-dl -O /bin/youtube-dl
chmod a+rx /bin/youtube-dl
youtube-dl -U
