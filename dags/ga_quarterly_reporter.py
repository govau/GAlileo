from __future__ import print_function
import datetime
import os
from tablib import Dataset

from airflow import models
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.email import send_email

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 5, 2),
}

DATA_DIR = '/home/airflow/gcs/data/'
if not os.path.isdir(DATA_DIR):
    DATA_DIR = '../../data/'


def send_report():
    datestamp = datetime.datetime.now().strftime('%d%b%Y')
    report_file = DATA_DIR + 'GA360-%s.csv' % datestamp

    table = Dataset().load(open(report_file, 'rt').read()).export('df').to_html()

    send_email(
        to=models.Variable.get('QUARTERLY_EMAIL_RECIPIENT', 'alex.sadleir@digital.gov.au'),
        cc=models.Variable.get('ANALYTICS_TEAM_EMAILS', []),
        subject='%s Automated Quarterly GA360 report [DO NOT RESPOND]' % datestamp,
        html_content=table,
        files=[report_file]
    )


with models.DAG(
        'ga_quarterly_reporter',
        schedule_interval=datetime.timedelta(days=90),
        default_args=default_dag_args) as dag:
    quarterly_report = KubernetesPodOperator(
        task_id='quarterly-report',
        name='quarterly-report',
        namespace='default',
        image='gcr.io/%s/galileo' % models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery'),
        cmds=['bash', '-c'],
        image_pull_policy="Always",
        arguments=['gsutil cp gs://%s/data/credentials.json . && ' % models.Variable.get('AIRFLOW_BUCKET',
                                                                                         'us-east1-dta-airflow-b3415db4-bucket') +
                   'gsutil cp gs://%s/dags/r_scripts/extractaccinfo.R . && ' % models.Variable.get('AIRFLOW_BUCKET',
                                                                                                   'us-east1-dta-airflow-b3415db4-bucket') +
                   'R -f extractaccinfo.R && '
                   'gsutil cp GA360*.csv gs://%s/data/' % models.Variable.get('AIRFLOW_BUCKET',
                                                                              'us-east1-dta-airflow-b3415db4-bucket')])

    email_summary = PythonOperator(
        task_id='email_summary',
        python_callable=send_report
    )
    quarterly_report >> email_summary
