# Docker-Compose single node deployment for development, testing and small-scale use

0. /dev contains a docker-compose file which maps repo root as a shared volume (between host/container) for
   interactive development and testing.

1. /test contains docker-compose file for testing cloud fs (s3,gs) sync between containers without shared volume.

2. /cpu contains docker-compose files for non-GPU single machine deployments on Linode, AWS, GCP etc.

3. /gpu contains docker-compose files for single machine with 1/2/4 GPU deployments on GCP & AWS etc.
