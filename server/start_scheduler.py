#!/usr/bin/env python
import django, os, subprocess, sys, shlex

sys.path.append(os.path.dirname(__file__))

if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dva.settings")
    django.setup()
    from django_celery_beat.models import PeriodicTask, IntervalSchedule

    every_three_minutes, _ = IntervalSchedule.objects.get_or_create(every=os.environ.get('REFRESH_MINUTES', 3),
                                                                    period=IntervalSchedule.MINUTES)
    every_minute, _ = IntervalSchedule.objects.get_or_create(every=os.environ.get('REFRESH_MINUTES', 1),
                                                             period=IntervalSchedule.MINUTES)
    _ = PeriodicTask.objects.get_or_create(name="refresher", task="monitor_retrievers", interval=every_minute,
                                           queue='qscheduler')
    _ = PeriodicTask.objects.get_or_create(name="monitoring", task="monitor_system", interval=every_three_minutes,
                                           queue='qscheduler')
    p = subprocess.Popen(['./startq.py', 'qscheduler'])
    # Remove stale celerybeat pidfile which happens in dev mode
    if os.path.isfile('celerybeat.pid'):
        os.remove('celerybeat.pid')
    subprocess.check_call(shlex.split(
        "celery -A dva beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler -f ../logs/beat.log"))
