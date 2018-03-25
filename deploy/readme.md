# Deep Video Analytics deployment configurations

#### Developing Deep Video Analytics & three deployment scenarios

0. /dev contains a docker-compose file which maps local directory as a shared volume (between host/container) for
   interactive development and testing.

1. /cpu contains docker-compose files for non-GPU single machine deployments on Linode, AWS, GCP etc.

2. /gpu contains docker-compose files for single machine with 1/2/4 GPU deployments on GCP & AWS etc.

3. /kube contains files used for a scalable GKE + GCS setup, with both GPU and non-GPU node pools supported.

4. /test_rfs contains docker-compose file for testing remote fs for sync between containers that don't share a volume.

#### Container images

- /dockerfiles contains Dockerfiles required for building containers
