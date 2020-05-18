from __future__ import print_function
import datetime
# import pendulum
import os
# import tablib
import pathlib

from airflow import models
# from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

from google.cloud import bigquery

from galileo import galileo, searchconsole, ga

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2020, 5, 1),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5)
}

with models.DAG(
        'project_ursa_major_monthly_counter',
        # schedule_interval=datetime.timedelta(days=1),
        schedule_interval='0 20 1 * *',
        catchup=False,
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')

    # BigQuery Scripts
    # total unique websites and their agency count
    query_digital_services_counter = bigquery_operator.BigQueryOperator(
        task_id='query_digital_services_counter',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_websites_count_doi").read_text(), use_legacy_sql=False)

    # ============================================================================================================
    # ============================================================================================================
    # Export datasets
    # total unique visitors 90 days snapshot
    export_bq_to_gcs_json_counter = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_counter',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.ga_monthly_websites_counter",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'websites_agencies_monthly_counter')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_counter = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_counter',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.ga_monthly_websites_counter",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'websites_agencies_monthly_counter')],
        export_format='CSV')

    
    # ============================================================================================================
    query_digital_services_counter >> export_bq_to_gcs_json_counter
    query_digital_services_counter >> export_bq_to_gcs_csv_counter
