# Deep Video Analytics deployment configurations

#### Environment variables

- common.env contains environment variable used in both cpu and gpu/multi-gpu deployments.

#### Developing Deep Video Analytics & three deployment scenarios

- /dev contains a docker-compose file which maps local directory as a shared volume (between host/container) for
  interactively development and testing.

1. /cpu contains docker-compose files for non-GPU single machine deployments on Linode, AWS, GCP etc.

2. /gpu contains docker-compose files for single machine with 1/2/4 GPU deployments on GCP & AWS etc.

3. /kube contains files used for a scalable GKE + GCS setup, with both GPU and non-GPU node pools supported.

#### Container images

- /dockerfiles contains Dockerfiles required for building containers