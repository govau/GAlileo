import datetime
import six
import pandas as pd
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay

# pd.datetime is an alias for datetime.datetime
today = pd.datetime.today()

from airflow import models
from airflow.contrib.kubernetes.secret import Secret
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.datetime(2019, 5, 31),
}
DOCKER_IMAGE = 'gcr.io/dta-ga-bigquery/galileo'
DATA_DIR = '/home/airflow/gcs/data/'
GCS_BUCKET = 'us-east1-dta-airflow-b3415db4-bucket'
connection = BaseHook.get_connection("cloudfoundry_default")
cf_username = Secret('env', 'CF_USERNAME', 'airflow-secrets', connection.username)
cf_password = Secret('env', 'CF_PASSWORD', 'airflow-secrets', connection.password)
htpasswd = models.Variable.get('HTPASSWD', '')

with models.DAG(
        'observatory',
        schedule_interval=datetime.timedelta(days=7),
        default_args=default_dag_args) as dag:


        graph_my_data = KubernetesPodOperator(task_id='graph-my-data',name='graph-my-data',namespace='default', secrets=[cf_username,cf_password],
                                              image=DOCKER_IMAGE, cmds=['bash', '-c'],
                                              arguments=['gsutil cp gs://{GCS_BUCKET}/data/{NAME}.csv . && '
                                                         'htpasswd -b -c htpasswd observatory {HTPASSWD} && '
                                                         'cf login -a https://api.system.b.cld.gov.au -o dta -s marketplace -u $CF_USERNAME -p $CF_PASSWORD'])
