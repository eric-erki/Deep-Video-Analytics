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



Deployment with Kubernetes on Google Cloud Engine

Organization:

- deployments/
- launch.sh
- launch_gpu.sh


### Instructions for GPU using GKE Beta:

## Deployment steps:

####  0: Ensure that GKE cluster is setup and connect by following instructions from GCP web console.

**Make sure that you provide "Read+Write" access to storage. As shown in figure below**

![Permissions](figures/storage_permissions.png "permissions")

Once cluster has been created, log in using either cloud shell or in your local terminal. By following instruction
for connecting to cluster. Start kubproxy in background by runnin `kubectl proxy &`. Clone DeepVideoAnalytics
repo and go to deploy/kube.

#### 1. Create [config.py](config_example.py) (copy & edit config_example.py) which contains values for secrets_template.yml

#### 2. Run "[create_bucket.py](create_bucket.py)" to create and make Google Cloud storage bucket public.

Above command creates bucket to store media files (images, videos, indexes etc.) and **makes it public**. You might encounter error
if the bucket name is already taken.

#### 3. Run "[create_secrets.py](create_secrets.py)" to create secrets.yml

Above command creates secrets.yml which contains base64 encoded secrets.

#### 4. Run "[launch.sh](launch.sh)" to launch containers.

This will launch all replication controllers, create secrets and persistent disk claims.

####  5. Open kubeproxy and visit external IP listed in services.

You can also get the IP address for Webserver load balancer by running
```kubectl get svc```

####  6. To clean up once you are done run following command

```./delete.sh  && python erase_bucket.py```

Above command deletes all controllers, secrets and empties the bucket.
Note that the bucket itself is not deleted, you can manually deleted bucket
using gsutil.

#### 7. Shut down the container via GCP web console.

Ensure that the cluster is shutdown, so that you don't end up getting charged
for the GCE nodes.

## TODO:

[ ] Ensure that Postgres and RabbitMQ are "Stateful sets" / consider reusing a Helm Chart.

[ ] Enable / add example for HTTP/HTTPS ingress and create seperate multi-region bucket to serve static files.
