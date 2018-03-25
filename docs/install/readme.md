# GPU driver (CUDA + Docker + nvidia-docker) installation instructions for AWS & GCP

#### We provide a public AMI in us-east-1 region with Docker, nvidia-docker2 and DVA GPU container image : ami-642f631e


- packer_ami.json : Contains Packer script to automatically create AWS EC2 AMI using AWS Deep Learning AMI
                    in us-east-1 region.

- install_cuda_drivers.sh install CUDA along with drivers on GCP Ubuntu Xenial VM
  ( Not needed for AWS since the DL AMI contains pre-installed drivers.)

- fix_docker_compose.py : make nvidia-docker default runtime.

- install_docker.sh :  install compatible docker version, make sure you log out and log in.

- install_nvidia_docker.sh install nvidia docker and make it default runtime.