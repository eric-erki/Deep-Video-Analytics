# Deployment on single machines with multiple GPUs

The docker-compose files in this repo are intended for single instance GPU deployment. 
E.g. A single EC2 or a GCP instance. For several one-off workloads this is good enough.

- docker-compose-gpu.yml : Docker compose file for single GPU with at last 12 Gb VRAM.

- docker-compose-<n>-gpus.yml : Docker compose file for multiple GPUs.

#### The docker-compose files use loop back interface (127.0.0.1:8000:80), we recommend forwarding the host OS port
(8000) over SSH tunnel when using cloud providers or VPS services such as Linode.