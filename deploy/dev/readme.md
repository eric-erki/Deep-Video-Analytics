## Deep Video Analytics Development environment

- The docker-compose.yml provides a seamless development environment by mounting the repo
("../../" relative to the compose file) as a volume inside the container.

- You can then use your favorite IDE on your host OS to edit files.

- I develop DVA on powerful cloud-instances using PyCharm with its auto-upload on save via SFTP/SCP feature.

- To test distributed non-NFS setup docker-compose-non-nfs.yml is used where S3/GCS is used for storing data instead.