from __future__ import print_function
import datetime
import pendulum
import os
import tablib
import pathlib

from airflow import models
from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

from google.cloud import bigquery


from galileo import galileo, searchconsole, ga

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    # 'start_date': datetime.datetime(2020, 1, 15),
    'start_date': pendulum.create(2020, 1, 15, tz="Australia/Sydney")
}

with models.DAG(
        'pageviews_snapshot',
        schedule_interval=datetime.timedelta(days=1),
        # schedule_interval = '0 0 * * *',
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')
  
    query_pageviews_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_pageviews_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_pgvw_daily_snapshot_full").read_text(), use_legacy_sql=False)
    export_pageviews_snapshot_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_internalsearch_to_gcs',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_emp",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'pgviews_daily_snapshot_emp')],
        export_format='NEWLINE_DELIMITED_JSON')
    query_pageviews_snapshot >> export_pageviews_snapshot_to_gcs
