import datetime
from datetime import date, timedelta
import os
import pathlib

from airflow import models
# from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

from google.cloud import bigquery


default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2021, 2, 12),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=10)
}

# Government sites referral since October 2020 - BigQuery data export
with models.DAG(
        'govau_site_referral_since_oct2020',
        # schedule_interval=datetime.timedelta(days=1),
        schedule_interval='0 8 * * 1',
        # email=['analytics@dta.gov.au', 'mufaddal.taiyab-ali@dta.gov.au'],
        # email_on_failure=True,
        catchup=False,
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')

    export_bq_to_gcs_csv = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv',
        source_project_dataset_table="{{params.project_id}}.dta_customers.govau_site_referral_since_oct_2020_weekly",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/govau_site_referral/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'govau_site_referral_since_oct2020_weekly'
            )],
        export_format='CSV')

    export_bq_to_gcs_csv
