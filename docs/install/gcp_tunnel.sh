#/usr/bin/env bash
gcloud compute ssh $1 --zone us-west1-b -- -L 8000:localhost:8000 -L 8888:localhost:8888 -L 8889:localhost:8889
