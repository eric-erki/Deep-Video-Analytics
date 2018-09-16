#!/usr/bin/env sh
set -xe
cd ../../
mkdir models
cd models
wget https://www.dropbox.com/s/nzz26b2p4wxygg3/coco_mobilenet.pb
wget https://www.dropbox.com/s/ghxowdua65r82d6/checkpoint
wget https://www.dropbox.com/s/v9w4mhcks7a719k/VGGnet_fast_rcnn_iter_50000.ckpt.data-00000-of-00001
wget https://www.dropbox.com/s/2d0licy8npype6r/VGGnet_fast_rcnn_iter_50000.ckpt.index
wget https://www.dropbox.com/s/vwmz2ra9jovlxjd/VGGnet_fast_rcnn_iter_50000.ckpt.meta
wget https://www.dropbox.com/s/fc7li2vwn8lvsyu/network.pb
wget https://www.dropbox.com/s/jytpgw8et09ede9/facenet.pb
wget https://www.dropbox.com/s/l0vo83hmvv2aipn/crnn.pth
wget https://www.dropbox.com/s/umo0xtljm9an90x/open_images.ckpt
wget https://www.dropbox.com/s/f93axdtlb3ltj40/open_images.ckpt.meta
wget https://www.dropbox.com/s/2sd5nzpyhsj10w5/open_images_labelmap.txt
wget https://www.dropbox.com/s/yboqv4leem6oy01/open_images_dict.csv
wget https://www.dropbox.com/s/77kmt20jnh31jsd/eigenvals.npy
wget https://www.dropbox.com/s/925ol3y308s2ii5/eigenvecs.npy
wget https://www.dropbox.com/s/4om8ncgzg0d1vix/mean.npy
wget https://vdn.nyc3.digitaloceanspaces.com/models/PCAR128_IVF4096_SQ8_LFW.dva_model_export



