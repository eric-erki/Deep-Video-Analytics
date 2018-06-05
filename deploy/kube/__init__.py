"""
Code in this file assumes that it is being run via dvactl and git repo root as current directory
"""
import shlex
import subprocess
import time
import random
import json
import os
import base64
import glob

DEFAULT_WORKERS = [
    {'name':'coco',
     'worker_env':'LAUNCH_BY_NAME_detector_coco',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"4000Mi"
     },
    {'name':'crnn',
     'worker_env':'LAUNCH_BY_NAME_analyzer_crnn',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"4000Mi"
     },
    {'name':'extractor',
     'worker_env':'LAUNCH_Q_qextract',
     'max_cpu':1,
     'request_cpu':1,
     'request_memory':"1000Mi",
     'max_memory':"10000Mi"
     },
    {'name':'face',
     'worker_env':'LAUNCH_BY_NAME_detector_face',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"4000Mi"
     },
    {'name':'facenet',
     'worker_env':'LAUNCH_BY_NAME_indexer_facenet',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"4000Mi"
     },
    {'name':'globalmodel',
     'worker_env':'LAUNCH_Q_GLOBAL_MODEL',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"8000Mi"
     },
    {'name':'globalretriever',
     'worker_env':'LAUNCH_Q_GLOBAL_RETRIEVER',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"80000Mi"
     },
    {'name':'inception',
     'worker_env':'LAUNCH_BY_NAME_indexer_inception',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"4000Mi"
     },
    {'name':'retinception',
     'worker_env':'LAUNCH_BY_NAME_retriever_inception',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"80000Mi"
     },
    {'name':'streamer',
     'worker_env':'LAUNCH_Q_qstreamer',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"500Mi",
     'max_memory':"4000Mi"
     },
    {'name':'tagger',
     'worker_env':'LAUNCH_BY_NAME_analyzer_tagger',
     'max_cpu':4,
     'request_cpu':1,
     'request_memory':"2000Mi",
     'max_memory':"4000Mi"
     },
    {'name':'textbox',
     'worker_env':'LAUNCH_BY_NAME_detector_textbox',
     'max_cpu':8,
     'request_cpu':1,
     'request_memory':"3000Mi",
     'max_memory':"8000Mi"
     },
    {'name':'trainer',
     'worker_env':'LAUNCH_Q_qtrainer',
     'max_cpu':8,
     'request_cpu':1,
     'request_memory':"3000Mi",
     'max_memory':"8000Mi"
     },
]


CLUSTER_CREATE_COMMAND = """ gcloud beta container --project "{project_name}" clusters create 
"{cluster_name}" --zone "{zone}" --username "admin" --cluster-version "1.8.8-gke.0" --machine-type "{machine_type}"  
--image-type "COS" --disk-size "100" --num-nodes "{nodes}" 
--scopes "https://www.googleapis.com/auth/compute","https://www.googleapis.com/auth/devstorage.read_write","https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring","https://www.googleapis.com/auth/servicecontrol","https://www.googleapis.com/auth/service.management.readonly","https://www.googleapis.com/auth/trace.append" \
--network "default" --enable-cloud-logging --enable-cloud-monitoring --subnetwork "default" 
--addons HorizontalPodAutoscaling,HttpLoadBalancing,KubernetesDashboard --enable-autorepair
"""

PREMPTIBLE_CREATE_COMMAND = 'gcloud beta container --project "{project_name}" node-pools create "{pool_name}"' \
          ' --zone "{zone}" --cluster "{cluster_name}" ' \
          '--machine-type "{machine_type}" --image-type "COS" ' \
          '--disk-size "100" ' \
          '--scopes "https://www.googleapis.com/auth/compute",' \
          '"https://www.googleapis.com/auth/devstorage.read_write",' \
          '"https://www.googleapis.com/auth/logging.write","https://www.googleapis.com/auth/monitoring",' \
          '"https://www.googleapis.com/auth/servicecontrol",' \
          '"https://www.googleapis.com/auth/service.management.readonly",' \
          '"https://www.googleapis.com/auth/trace.append" ' \
          '--preemptible --num-nodes "{count}"  '

AUTH_COMMAND = "gcloud container clusters get-credentials {cluster_name} --zone {zone} --project {project_name}"

POD_COMMAND = "kubectl get -n {namespace} pods"

SERVER_COMMAND = "kubectl get -n {namespace} service --output json"

TOKEN_COMMAND = "kubectl exec -n {namespace} -it {pod_name} -c dvawebserver scripts/generate_testing_token.py"


def run_commands(command_list):
    for k in command_list:
        print "running {}".format(k)
        subprocess.check_call(shlex.split(k))


def get_namespace():
    return json.load(file('deploy/kube/namespace.json'))['metadata']['name']


def launch_kube():
    setup_kube()
    config = get_kube_config()
    namespace = get_namespace()
    try:
        print "Attempting to create namespace {}".format(namespace)
        run_commands(['kubectl create -f deploy/kube/namespace.json',])
    except:
        print "Could not create namespace {}, it might already exist".format(namespace)
    init_deployments = ['secrets.yml', 'postgres.yaml', 'rabbitmq.yaml', 'redis.yaml']
    init_commands = []
    for k in init_deployments:
        init_commands.append("kubectl create -n {} -f deploy/kube/{}".format(namespace,k))
    run_commands(init_commands)
    print "sleeping for 120 seconds"
    time.sleep(120)
    webserver_commands = ['kubectl create -n {} -f deploy/kube/webserver.yaml'.format(namespace), ]
    run_commands(webserver_commands)
    print "webserver launched, sleeping for 60 seconds"
    time.sleep(60)
    scheduler_commands = ['kubectl create -n {} -f deploy/kube/scheduler.yaml'.format(namespace), ]
    run_commands(scheduler_commands)
    print "scheduler launched, sleeping for 10 seconds"
    time.sleep(10)
    commands = []
    worker_template = file('./deploy/kube/worker.yaml.template').read()
    for k in DEFAULT_WORKERS:
        yaml_fname = './deploy/kube/{}.yaml'.format(k['name'])
        with open(yaml_fname, 'w') as out:
            k['common'] = config['common_env']
            k['command'] = config['command']
            out.write(worker_template.format(**k))
        commands.append("kubectl create -n {} -f {}".format(namespace,yaml_fname))
    run_commands(commands)
    print "Waiting another 200 seconds to get auth token and ingress IP address"
    time.sleep(200)
    get_auth()


def delete_kube():
    namespace = get_namespace()
    delete_commands = ['kubectl -n {} delete po,svc,pvc,deployment,statefulset,secrets --all'.format(namespace), ]
    run_commands(delete_commands)


def kube_gpu_setup():
    command = ['kubectl', 'create', '-f',
               'https://raw.githubusercontent.com/GoogleCloudPlatform/container-engine-accelerators'
               '/k8s-1.9/nvidia-driver-installer/cos/daemonset-preloaded.yaml']
    subprocess.check_call(command)


def erase_kube_bucket():
    config = get_kube_config()
    subprocess.check_call(['gsutil', '-m', 'rm', 'gs://{}/**'.format(config['mediabucket'])])


def get_kube_config():
    """
    # to set CORS on the bucket Can be * or specific website e.g. http://example.website.com
    :return:
    """
    with open('config.json') as fh:
        configs = json.load(fh)
    if 'GOOGLE_CLOUD_PROJECT' in os.environ:
        configs['project_name'] = os.environ['GOOGLE_CLOUD_PROJECT']
    else:
        EnvironmentError("Could not find GOOGLE_CLOUD_PROJECT in environment")
    with open('deploy/kube/common.yaml') as f:
        configs['common_env'] = f.read()
    if configs['branch'] == 'stable':
        configs['command'] = 'git reset --hard && git pull && sleep 15  && ./start_container.py'
    else:
        configs['command'] = 'git reset --hard && git checkout --track origin/master && git pull && sleep 60 && ./start_container.py'
    return configs


def kube_create_premptible_node_pool():
    config = get_kube_config()
    machine_type = 'n1-standard-2'
    count = 5
    v = raw_input("Please enter machine_type (current {}) or press enter to keep default >>".format(machine_type)).strip()
    if v:
        machine_type = v
    v = raw_input("Please enter machine count (current {}) or press enter to keep default >>".format(count)).strip()
    if v:
        count = int(v)
    command = PREMPTIBLE_CREATE_COMMAND.format(project_name=config['project_name'], pool_name="premptpool",
                                               cluster_name=config['cluster_name'], zone=config['zone'], count=count,
                                               machine_type=machine_type)
    print "Creating pre-emptible node pool"
    print command
    subprocess.check_call(shlex.split(command))


def generate_deployments():
    config = get_kube_config()
    for fname in glob.glob('./deploy/kube/*.template'):
        if 'worker.yaml' not in fname and 'worker_gpu.yaml' not in fname:
            with open(fname.replace('.template',''),'w') as out:
                out.write(file(fname).read().format(common=config['common_env'],command=config['command']))


def setup_kube():
    generate_deployments()
    config = get_kube_config()
    print "attempting to create bucket"
    region = '-'.join(config['zone'].split('-')[:2])
    try:
        subprocess.check_call(shlex.split('gsutil mb -c regional -l {} gs://{}'.format(region,
                                                                                       config['mediabucket'])))
    except:
        print "failed to create bucket, assuming it already exists"
    print "attempting to set public view permission on the bucket"
    try:
        subprocess.check_call(shlex.split('gsutil iam ch allUsers:objectViewer gs://{}'.format(config['mediabucket'])))
    except:
        print "failed to set permissions to public"
    with open('cors.json', 'w') as out:
        json.dump([
            {
                "origin": [config['cors_origin']],
                "responseHeader": ["Content-Type"],
                "method": ["GET", "HEAD"],
                "maxAgeSeconds": 3600
            }
        ], out)
    print "attempting to set bucket policy"
    try:
        subprocess.check_call(shlex.split('gsutil cors set cors.json gs://{}'.format(config['mediabucket'])))
    except:
        print "failed to set bucket policy"
    print "Attempting to create deploy/kube/secrets.yml from deploy/kube/secrets_template.yml and config."
    with open('deploy/kube/secrets_template.yml') as f:
        template = f.read()
    with open('deploy/kube/secrets.yml', 'w') as out:
        out.write(template.format(
            dbusername=base64.encodestring(config['dbusername']),
            dbpassword=base64.encodestring(config['dbpassword']),
            rabbithost=base64.encodestring(config['rabbithost']),
            rabbitpassword=base64.encodestring(config['rabbitpassword']),
            rabbitusername=base64.encodestring(config['rabbitusername']),
            awskey=base64.encodestring(config['awskey']),
            awssecret=base64.encodestring(config['awssecret']),
            secretkey=base64.encodestring(config['secretkey']),
            mediabucket=base64.encodestring(config['mediabucket']),
            mediaurl=base64.encodestring('http://{}.storage.googleapis.com/'.format(config['mediabucket'])),
            superuser=base64.encodestring(config['superuser']),
            superpass=base64.encodestring(config['superpass']),
            superemail=base64.encodestring(config['superemail']),
            cloudfsprefix=base64.encodestring(config['cloudfsprefix']),
            redishost=base64.encodestring(config['redishost']),
            redispassword=base64.encodestring(config['redispassword']),
        ).replace('\n\n', '\n'))


def create_cluster():
    """
    Create a GKE cluster
    :return:
    """
    config = get_kube_config()
    command = CLUSTER_CREATE_COMMAND.replace('\n','').format(cluster_name=config['cluster_name'],
                                                             project_name=config['project_name'],
                                                             machine_type=config['machine_type'],
                                                             nodes=config['nodes'],
                                                             zone=config['zone'])
    print "Creating cluster by running {}".format(command)
    subprocess.check_call(shlex.split(command))
    command = AUTH_COMMAND.replace('\n','').format(cluster_name=config['cluster_name'],
                                                             project_name=config['project_name'],
                                                             zone=config['zone'])
    print "Authenticating with cluster by running {}".format(command)
    subprocess.check_call(shlex.split(command))


def get_webserver_pod():
    namespace = get_namespace()
    output = subprocess.check_output(shlex.split(POD_COMMAND.format(namespace=namespace))).splitlines()
    for line in output:
        if line.startswith('dvawebserver'):
            return line.strip().split()[0]


def get_service_ip():
    namespace = get_namespace()
    output = json.loads(subprocess.check_output(shlex.split(SERVER_COMMAND.format(namespace=namespace))))
    for i in output['items']:
        if i['metadata']['name'] == 'dvawebserver':
            return i['status']['loadBalancer']['ingress'][0]['ip']
    raise ValueError("Service IP could not be found? Check if allocated.")


def get_auth():
    config = get_kube_config()
    pod_name = get_webserver_pod()
    namespace = get_namespace()
    token = subprocess.check_output(shlex.split(TOKEN_COMMAND.format(namespace=namespace,pod_name=pod_name))).strip()
    ip = get_service_ip()
    server = 'http://{}/api/'.format(ip)
    with open('creds.json','w') as fh:
        json.dump({'server':server,'token':token},fh)
    print "Token and server stored in creds.json"
    print "Visit web UI on http://{} \nusername: {}\npassword {}".format(ip, config['superuser'], config['superpass'])


def handle_kube_operations(args):
    if args.action == 'create':
        create_cluster()
    elif args.action == 'auth':
        get_auth()
    elif args.action == 'start':
        launch_kube()
    elif args.action == 'create_premptible':
        kube_create_premptible_node_pool()
    elif args.action == 'stop' or args.action == 'clean':
        delete_kube()
        if args.action == 'clean':
            erase_kube_bucket()
    else:
        raise NotImplementedError("Kubernetes management does not supports: {}".format(args.action))
