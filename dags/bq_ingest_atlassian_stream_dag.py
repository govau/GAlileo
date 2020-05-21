from __future__ import print_function
import datetime
import pendulum
import os
import tablib
import pathlib

import json
import requests
import re
import six
import itertools
import io
import pytz

from requests.exceptions import HTTPError

from airflow import models
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

from galileo import galileo, searchconsole, ga


def get_streams():
    # Data streams sources - URL Queries
    query_number = ("1", "5", "6", "7", "8", "9", "10")
    #  url of dashboard from Atlassian
    url = "https://dashboard.platform.aus-gov.com.au"
    # Get the json response query
    for q in query_number:
        url_q = url + "/api/queries/" + q
        res = requests.get(url_q, auth=('', ''))
        # Extract dataset ID from query json
        data_res = res.json()
        data_id = data_res['latest_query_data_id']
        json_url = url + "/api/queries/" + q + \
            "/results/" + str(data_id) + ".json"
        try:
            # read the result json file
            data_stream = requests.get(
                json_url, auth=('', ''))
            data_stream.raise_for_status()
        except HTTPError as http_err:
            print(http_err)
            continue
        except ValueError as val_err:
            print(val_err)
            continue
        except Exception as err:
            print(err)
            continue
        json_stream = io.StringIO(data_stream.text)
        #  write the json file to destination
        write_bq_table(json_stream, q)


def write_bq_table(data_stream, query_number):
    client = bigquery.Client()
    table_id = "dta-ga-bigquery.dta_customers_ausgov" + \
        ".atlassian_dashboard_q" + query_number

    job_config = bigquery.LoadJobConfig(
        writeDisposition="WRITE_APPEND",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON"
    )

    job = client.load_table_from_file(
        data_stream, table_id, job_config=job_config
    )
    job.result()


default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 4, 8),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=10)
    # 'start_date': pendulum.create(2020, 1, 15, tz="Australia/Sydney")
}

with models.DAG(
        'ingestion_stream_atlassian',
        # schedule_interval=datetime.timedelta(days=1),
        schedule_interval='*/30 * * * *',
        catchup=False,
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')

    # Data stream ingestion method call
    ingest_stream = PythonOperator(
        task_id='ingest_stream',
        python_callable=get_streams,
        provide_context=False,
        dag=dag
    )

ingest_stream
