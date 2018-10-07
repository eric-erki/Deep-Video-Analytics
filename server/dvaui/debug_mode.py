from dvaapp import models
import subprocess


def restart_all_workers():
    for w in models.Worker.objects.all():
        w.alive = False
        w.shutdown = True
        w.save()
    try:
        subprocess.check_call(['sh','scripts/kill_celery.sh'])
    except:
        pass
    try:
        subprocess.check_call(['python', 'launch_from_env.py'])
    except:
        pass


def list_workers():
    output = []
    for line in subprocess.check_output(['ps','aux']).splitlines():
        if 'celery' in line:
            output.append(line)
    return "\n".join(output)