import os
from celery import Celery
from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'merge_duplicate_companies.settings')

app = Celery('merge_duplicate_companies')

app.config_from_object('django.conf:settings', namespace='CELERY')
# app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
app.autodiscover_tasks()
