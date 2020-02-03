from builtins import range
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils import timezone
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.contrib.hooks.redis_hook import RedisHook
import json, os
from agoda_poc.source_crawlers import oneDrive, google, youtube, s3, http_ftp
from base64 import b64encode as b64e
from dotenv import load_dotenv

load_dotenv('agoda-poc-config')
load_dotenv('.env')
tempDownloadFolder = '/usr/local/airflow/webserver/tempDownloads'
aws_access_key_id = os.getenv('aws_access_key_id')
aws_secret_access_key = os.getenv('aws_secret_access_key')

seven_days_ago = datetime.combine(
        datetime.today() - timedelta(7), datetime.min.time())

default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': seven_days_ago,
        'email': ['mail.sajidrahman@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
      }

def getUniqueRunId(kwargs):
    run_id=kwargs["run_id"]
    dag=kwargs['dag']
    return "krawler_" + str(dag) + str(run_id)

def get_redis_data(kwargs):
    prefixRunId = getUniqueRunId(kwargs)
    redis_conn = RedisHook(redis_conn_id='redis_default').get_conn()
    return json.loads(redis_conn.get(prefixRunId))

def set_redis_data(data, **kwargs):
    prefixRunId = getUniqueRunId(kwargs)
    redis_conn = RedisHook(redis_conn_id='redis_default').get_conn()
    redis_conn.set(prefixRunId, json.dumps(data))

dag = DAG(
    dag_id='source-crawler', default_args=default_args,
    schedule_interval=None)

def consume_source(**kwargs):
    url = kwargs['dag_run'].conf['resource_key']
    # source = kwargs['dag_run'].conf.get('source')

    
    url = url.get('conf').get('resource_key')
    print(f"##############URL = {url}")


    set_redis_data({'url':url}, **kwargs)

    if url[:3] == 'ftp':
        source='ftp'
    elif url.split('/')[2] == '1drv.ms':
        source='OneDrive'
    elif url.split('/')[2] == 'drive.google.com':
        source='GoogleDrive'
    elif url.split('/')[2] == 'www.youtube.com':
        source='YouTube'
    elif url[:2] == 's3':
        source='s3'
    else:
        source='http'

    if source == 's3':
        return 's3_handler'
    elif source == 'GoogleDrive':
        return 'google_drive_handler'
    elif source == 'OneDrive':
        return 'one_drive_handler'
    elif source == 'YouTube':
        return 'youtube_handler'
    elif source == 'http':
        return 'http_handler'
    elif source == 'ftp':
        return 'ftp_handler'

def s3_handler(**kwargs):
    redis_data = get_redis_data(kwargs)
    bucket = redis_data['url'].split('/')[0].strip()
    key = '/'.join(redis_data['url'].split('/')[1:])
    s3Downloader = s3.S3Downloader(bucket, key, 
                            aws_id=aws_access_key_id, 
                            aws_key=aws_secret_access_key,
                            destPathPrefix=tempDownloadFolder)
    payload, status_code = s3Downloader.download()


def google_drive_handler(**kwargs):
    redis_data = get_redis_data(kwargs)
    fileId = redis_data['url'].split('/')[5]
    gdDownloader = google.GoogleDriveDownloader(fileId, tempDownloadFolder)
    payload, status_code = gdDownloader.download()


def one_drive_handler(**kwargs):
    redis_data = get_redis_data(kwargs)
    shareLink = redis_data['url']
    odDownloader = oneDrive.OneDriveDownloader(shareLink, tempDownloadFolder)
    payload, status_code = odDownloader.download()


def youtube_handler(**kwargs):
    redis_data = get_redis_data(kwargs)
    url = redis_data['url']
    ytDownloader = youtube.YoutubeDownloader(url, tempDownloadFolder)
    ytDownloader.download()


def http_handler(**kwargs):
    redis_data = get_redis_data(kwargs)
    url = redis_data['url']
    down = http_ftp.Download(url)
    down.download(destPathPrefix=tempDownloadFolder)


def ftp_handler(**kwargs):
    redis_data = get_redis_data(kwargs)
    url = redis_data['url']
    down = http_ftp.Download(url)
    down.download(destPathPrefix=tempDownloadFolder)


def clean_up(**kwargs):
    prefixRunId = getUniqueRunId(kwargs)
    redis_conn = RedisHook(redis_conn_id='redis_default').get_conn()
    if redis_conn.get(prefixRunId) != None:
        redis_conn.delete(prefixRunId)


consume_source_op = BranchPythonOperator(
    task_id='consume_source',
    provide_context=True,
    python_callable=consume_source,
    dag=dag)

s3_handler_op = PythonOperator(
    task_id='s3_handler',
    provide_context=True,
    python_callable=s3_handler,
    dag=dag)

google_drive_handler_op = PythonOperator(
    task_id='google_drive_handler',
    provide_context=True,
    python_callable=google_drive_handler,
    dag=dag)

one_drive_handler_op = PythonOperator(
    task_id='one_drive_handler',
    provide_context=True,
    python_callable=one_drive_handler,
    dag=dag)

youtube_handler_op = PythonOperator(
    task_id='youtube_handler',
    provide_context=True,
    python_callable=youtube_handler,
    dag=dag)

http_handler_op = PythonOperator(
    task_id='http_handler',
    provide_context=True,
    python_callable=http_handler,
    dag=dag)

ftp_handler_op = PythonOperator(
    task_id='ftp_handler',
    provide_context=True,
    python_callable=ftp_handler,
    dag=dag)

cleanup_handler_op = PythonOperator(
    task_id='clean_up',
    provide_context=True,
    python_callable=clean_up,
    trigger_rule='one_success',
    dag=dag)

consume_source_op >> [s3_handler_op, google_drive_handler_op, one_drive_handler_op, youtube_handler_op, ftp_handler_op, http_handler_op]
[s3_handler_op, google_drive_handler_op, one_drive_handler_op, youtube_handler_op, ftp_handler_op, http_handler_op] >> cleanup_handler_op