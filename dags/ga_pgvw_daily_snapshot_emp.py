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
    'start_date': datetime.datetime(2020, 1, 15)
    # 'start_date': pendulum.create(2020, 1, 15, tz="Australia/Sydney")
}

with models.DAG(
        'pageviews_snapshot_emp',
        schedule_interval=datetime.timedelta(days=1),
        # schedule_interval = '0 0 * * *',
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')
  
    # BigQuery Scripts
    # pageviews snapshot
    query_pageviews_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_pageviews_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_pgvw_daily_snapshot_full_emp").read_text(), use_legacy_sql=False)
    
    query_pageviews_snapshot_delta = bigquery_operator.BigQueryOperator(
        task_id='query_pageviews_snapshot_delta',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_pgvw_daily_snapshot_incremental_emp").read_text(), use_legacy_sql=False)

    # total visitors snapshot
    query_total_visitors_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_total_visitors_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_total_visitors_snapshot_emp").read_text(), use_legacy_sql=False)

    query_total_visitors_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_total_visitors_delta_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_total_visitors_snapshot_delta_emp").read_text(), use_legacy_sql=False)

    # device category snapshot
    query_device_category_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_category_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_devicecategory_daily_full_snapshot_emp").read_text(), use_legacy_sql=False)

    query_device_category_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_category_delta_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_devicecategory_daily_snapshot_delta_emp").read_text(), use_legacy_sql=False)

    # device browser snapshot
    query_device_browser_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_browser_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_devicebrowser_daily_full_snapshot_emp").read_text(), use_legacy_sql=False)

    query_device_browser_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_browser_delta_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts/dta_sql_devicebrowser_daily_snapshot_delta_emp").read_text(), use_legacy_sql=False)
    # ============================================================================================================
    # Export datasets
    # pageviews snapshot
    export_bq_to_gcs_json = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_increment_emp",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/json/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'pgviews_daily_snapshot_emp')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv',
    source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_increment_emp",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/csv/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'pgviews_daily_snapshot_emp')],
    export_format='CSV')

    # total visitors snapshot
    export_bq_to_gcs_json_totalvisitors = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_totalvisitors',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_totalvisitors_delta_emp",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/json/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'totalvisitors_daily_snapshot_emp')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_totalvisitors = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_totalvisitors',
    source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_totalvisitors_delta_emp",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/csv/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'totalvisitors_daily_snapshot_emp')],
    export_format='CSV')
    
    # device category snapshot
    export_bq_to_gcs_json_device_category = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_device_category',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_category_delta_emp",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/json/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_category_daily_snapshot_emp')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_device_category = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_device_category',
    source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_category_delta_emp",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/csv/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'device_category_daily_snapshot_emp')],
    export_format='CSV')

    # device browser snapshot
    export_bq_to_gcs_json_device_browser = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_device_browser',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_browser_delta_emp",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/json/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_browser_daily_snapshot_emp')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_device_browser = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_device_browser',
    source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_browser_delta_emp",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/csv/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'device_browser_daily_snapshot_emp')],
    export_format='CSV')
    # ============================================================================================================
    query_pageviews_snapshot >> query_pageviews_snapshot_delta >> export_bq_to_gcs_json
    query_pageviews_snapshot_delta >> export_bq_to_gcs_csv
    query_total_visitors_snapshot >> query_total_visitors_delta_snapshot >> export_bq_to_gcs_json_totalvisitors
    query_total_visitors_delta_snapshot >> export_bq_to_gcs_csv_totalvisitors
    query_device_category_snapshot >> query_device_category_delta_snapshot >> export_bq_to_gcs_json_device_category
    query_device_category_delta_snapshot >> export_bq_to_gcs_csv_device_category
    query_device_browser_snapshot >> query_device_browser_delta_snapshot >> export_bq_to_gcs_json_device_browser
    query_device_browser_delta_snapshot >> export_bq_to_gcs_csv_device_browser