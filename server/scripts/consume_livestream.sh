#!/usr/bin/env bash
streamlink "$1" best -O  | ffmpeg -re -i - -c:v libx264 -c:a aac -ac 1 -strict -2 -crf 18 -profile:v baseline -maxrate 3000k -bufsize 1835k -pix_fmt yuv420p -flags -global_header -f segment -segment_time 0.1 $2/%d.mp4
