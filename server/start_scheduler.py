#!/usr/bin/env python
import django, os, subprocess, sys, logging, shlex
sys.path.append(os.path.dirname(__file__))

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
    django.setup()
    from django_celery_beat.models import PeriodicTask,IntervalSchedule
    from django.conf import settings
    di, created = IntervalSchedule.objects.get_or_create(every=os.environ.get('REFRESH_MINUTES',3),period=IntervalSchedule.MINUTES)
    _ = PeriodicTask.objects.get_or_create(name="monitoring",task="monitor_system",interval=di,queue='qscheduler')
    di, created = IntervalSchedule.objects.get_or_create(every=os.environ.get('REFRESH_MINUTES', 1),
                                                         period=IntervalSchedule.MINUTES)
    _ = PeriodicTask.objects.get_or_create(name="refresher",task="refresh_retriever",interval=di,
                                           queue=settings.Q_REFRESHER)
    p = subprocess.Popen(['./startq.py','qscheduler'])
    if os.path.isfile('celerybeat.pid'):
        # Remove stale celerybeat pidfile which happens in dev mode
        os.remove('celerybeat.pid')
    subprocess.check_call(shlex.split("celery -A dva beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler -f ../logs/beat.log"))