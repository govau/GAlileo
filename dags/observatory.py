import datetime
import six
import pandas as pd
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay

# pd.datetime is an alias for datetime.datetime
today = pd.datetime.today()

from airflow import models
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.datetime(2019, 5, 31),
}
DOCKER_IMAGE = 'gcr.io/dta-ga-bigquery/galileo'
DATA_DIR = '/home/airflow/gcs/data/'
GCS_BUCKET = 'us-east1-dta-airflow-b3415db4-bucket'
cf_username = Secret('env', 'CF_USERNAME', 'airflow-secrets', 'CF_USERNAME')
cf_password = Secret('env', 'CF_PASSWORD', 'airflow-secrets', 'CF_PASSWORD')
htpasswd = models.Variable.get('HTPASSWD', '')

with models.DAG(
        'observatory',
        schedule_interval=datetime.timedelta(days=7),
        default_args=default_dag_args) as dag:
    deploy_shiny = KubernetesPodOperator(task_id='deploy-shiny', name='deploy-shiny', namespace='default',
                                         secrets=[cf_username, cf_password],
                                         image_pull_policy="Always",
                                         image=DOCKER_IMAGE,
                                         cmds=['bash', '-c'],
                                         arguments=['git clone https://github.com/govau/GAlileo --depth=1 && '
                                                    'cp -rv GAlileo/shiny/observatory deploy &&'
                                                    'cd deploy && '
                                                    'gsutil cp -r gs://{GCS_BUCKET}/dags/shiny/observatory/* . && '
                                                    'htpasswd -b -c htpasswd observatory {HTPASSWD} && '
                                                    'cf login -a https://api.system.y.cld.gov.au -u $CF_USERNAME -p $CF_PASSWORD && '
                                                    'cf unmap-route observatory-green apps.y.cld.gov.au -n observatory &&'
                                                    'cf v3-apply-manifest -f manifest.yml'
                                                    'cf v3-push observatory-green &&'
                                                    'cf map-route observatory-green apps.y.cld.gov.au -n observatory &&'
                                                    'cf unmap-route observatory-blue apps.y.cld.gov.au -n observatory &&'
                                                    'cf v3-push observatory-blue &&'
                                                    'cf map-route observatory-blue apps.y.cld.gov.au -n observatory'
                                                    .format(GCS_BUCKET=GCS_BUCKET, HTPASSWD=htpasswd)])
