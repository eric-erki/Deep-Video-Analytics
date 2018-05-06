"""
Code in this file assumes that it is being run via dvactl and git repo root as current directory
"""
CONFIG = {
    "deploy/gpu/docker-compose-2-gpus.yml": {"global_model_gpu_id": 0,
                                             "global_model_memory_fraction": 0.1,
                                             "workers":
                                                 [(0, 0.25, "LAUNCH_BY_NAME_indexer_inception", "inception"),
                                                  (0, 0.2, "LAUNCH_BY_NAME_analyzer_crnn", "crnn"),
                                                  (0, 0.5, "LAUNCH_BY_NAME_detector_coco", "coco"),
                                                  (1, 0.5, "LAUNCH_BY_NAME_detector_textbox", "textbox"),
                                                  (1, 0.19, "LAUNCH_BY_NAME_detector_face", "face"),
                                                  (1, 0.15, "LAUNCH_BY_NAME_indexer_facenet", "facenet"),
                                                  (1, 0.15, "LAUNCH_BY_NAME_analyzer_tagger", "tagger")]
                                             },
    "deploy/gpu/docker-compose-4-gpus.yml": {"global_model_gpu_id": 2,
                                             "global_model_memory_fraction": 0.29,
                                             "workers":
                                                 [(0, 0.3, "LAUNCH_BY_NAME_indexer_inception", "inception"),
                                                  (0, 0.4, "LAUNCH_BY_NAME_analyzer_tagger", "tagger"),
                                                  (0, 0.2, "LAUNCH_BY_NAME_analyzer_crnn", "crnn"),
                                                  (1, 1.0, "LAUNCH_BY_NAME_detector_coco", "coco"),
                                                  (2, 0.7, "LAUNCH_BY_NAME_detector_face", "face"),
                                                  (3, 0.5, "LAUNCH_BY_NAME_detector_textbox", "textbox"),
                                                  (3, 0.45, "LAUNCH_BY_NAME_indexer_facenet", "facenet")
                                                  ]
                                             },
}

SKELETON = """  version: '3'
  services:
   db:
     image: postgres:9.6.6
     container_name: dva-pg
     volumes:
      - dvapgdata:/var/lib/postgresql/data
     env_file:
       - ../../../custom.env
   rabbit:
     image: rabbitmq
     container_name: dva-rmq
     env_file:
       - ../../../custom.env
     volumes:
       - dvarabbit:/var/lib/rabbitmq
   redis:
     image: bitnami/redis:latest
     container_name: dva-redis
     env_file:
       - ../../../custom.env
     volumes:
       - dvaredis:/bitnami       
   webserver:
     image: akshayubhat/dva-auto:gpu
     container_name: webserver
     env_file:
       - ../../../custom.env
     environment:
       - LAUNCH_SERVER_NGINX=1
       - LAUNCH_NOTEBOOK=1
     command: bash -c "git reset --hard && git pull && sleep 10 && ./start_container.py"
     ports:
       - "127.0.0.1:8000:80"
       - "127.0.0.1:8888:8888"
     depends_on:
       - db
       - redis       
       - rabbit
     volumes:
       - dvadata:/root/media
   non-gpu-workers:
     image: akshayubhat/dva-auto:gpu
     env_file:
       - ../../../custom.env
     environment:
       - LAUNCH_BY_NAME_retriever_inception=1
       - LAUNCH_BY_NAME_retriever_facenet=1
       - LAUNCH_Q_qextract=1
       - LAUNCH_Q_qstreamer=1
       - LAUNCH_SCHEDULER=1
       - LAUNCH_Q_GLOBAL_RETRIEVER=1
     command: bash -c "git reset --hard && git pull && sleep 45 && ./start_container.py"
     depends_on:
       - db
       - redis       
       - rabbit
     volumes:
       - dvadata:/root/media
{gpu_workers}
   global-model:
     image: akshayubhat/dva-auto:gpu
     env_file:
       - ../../../custom.env
     environment:
       - GPU_AVAILABLE=1     
       - NVIDIA_VISIBLE_DEVICES={global_model_gpu_id}
       - GPU_MEMORY={global_model_memory_fraction}
       - LAUNCH_Q_GLOBAL_MODEL=1
     command: bash -c "git reset --hard && git pull && sleep 45 && ./start_container.py"
     depends_on:
       - db
       - redis       
       - rabbit
     volumes:
       - dvadata:/root/media
  volumes:
   dvapgdata:
   dvadata:
   dvarabbit:
   dvaredis:
    """

BLOCK = """   {worker_name}:
         image: akshayubhat/dva-auto:gpu
         env_file:
           - ../../../custom.env
         environment:
           - GPU_AVAILABLE=1
           - NVIDIA_VISIBLE_DEVICES={gpu_id}
           - GPU_MEMORY={memory_fraction}
           - {env_key}={env_value}
         command: bash -c "git reset --hard && git pull && sleep 45 && ./start_container.py"
         depends_on:
           - db
           - redis       
           - rabbit
         volumes:
           - dvadata:/root/media"""


def generate_multi_gpu_compose():
    for fname in CONFIG:
        blocks = []
        worker_specs = CONFIG[fname]['workers']
        for gpu_id, fraction, env_key, worker_name, in worker_specs:
            blocks.append(
                BLOCK.format(worker_name=worker_name, gpu_id=gpu_id, memory_fraction=fraction, env_key=env_key,
                             env_value=1))
        with open(fname, 'w') as out:
            out.write(SKELETON.format(gpu_workers="\n".join(blocks),
                                      global_model_gpu_id=CONFIG[fname]['global_model_gpu_id'],
                                      global_model_memory_fraction=CONFIG[fname]['global_model_memory_fraction']))
