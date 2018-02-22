# Deployment on a single machine without GPU

E.g. A desktop, linode, digital ocean droplet, aws ec2 instance or a gcp cloud vm.

- docker-compose.yml : for single node deployments.

- shell.sh : For bash into running container e.g. `./shell.sh`

- webserver_logs.sh : Get uwsgi logs from webserver

#### The docker-compose files use loopback interface (127.0.0.1:8000:80), we recommend forwarding the host OS port (8000)
over SSH tunnel when using cloud providers or VPS services such as Linode.