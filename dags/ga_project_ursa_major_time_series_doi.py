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
    'start_date': datetime.datetime(2020, 4, 22),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5)
}

with models.DAG(
        'project_ursa_major_daily_time_series',
        # schedule_interval=datetime.timedelta(days=1),
        schedule_interval='0 20 * * *',
        catchup=True,
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')

    # BigQuery Scripts
    # total unique visitors 90 days snapshot
    query_unique_visitors_90days_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_unique_visitors_90days_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_unique_visitors_snapshot_90days_daily_doi").read_text(), use_legacy_sql=False)

    # total unique visitors 90 days hourly snapshot
    query_unique_visitors_90days_hourly_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_unique_visitors_90days_hourly_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_unique_visitors_snapshot_90days_hourly_doi").read_text(), use_legacy_sql=False)

    # total unique visitors 90 days hourly week split snapshot
    query_unique_visitors_90days_hourly_weeksplit_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_unique_visitors_90days_hourly_weeksplit_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts_ursa_major/dta_sql_unique_visitors_snapshot_90days_hourly_splitweek_doi").read_text(), use_legacy_sql=False)

    # browser past 12 months count daily snapshot
    query_browser_12months_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_browser_12months_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts_ursa_major/dta_sql_browser_snapshot_12months_daily_doi").read_text(), use_legacy_sql=False)

    # browser version past 12 months count daily snapshot
    query_browser_version_12months_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_browser_version_12months_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts_ursa_major/dta_sql_browser_ver_snapshot_12months_daily_doi").read_text(), use_legacy_sql=False)

    # operating system past 12 months count daily snapshot
    query_opsys_12months_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_opsys_12months_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_opsystem_snapshot_12months_daily_doi").read_text(), use_legacy_sql=False)

    # operating system version past 12 months count daily snapshot
    query_opsys_ver_12months_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_opsys_ver_12months_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_opsystem_ver_snapshot_12months_daily_doi").read_text(), use_legacy_sql=False)

    # device category past 12 months count daily snapshot
    query_device_category_12months_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_category_12months_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_devicecategory_snapshot_12months_daily_doi").read_text(), use_legacy_sql=False)

    # device screen resolution past 12 months count daily snapshot
    query_screen_resolution_12months_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_screen_resolution_12months_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_screen_res_snapshot_12months_daily_doi").read_text(), use_legacy_sql=False)

    # device brand past 12 months count daily snapshot
    query_device_brand_12months_daily_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_brand_12months_daily_snapshot',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_ursa_major/dta_sql_devicebrand_snapshot_12months_daily_doi").read_text(), use_legacy_sql=False)
    # ============================================================================================================
    # ============================================================================================================
    # Export datasets
    # total unique visitors 90 days snapshot
    export_bq_to_gcs_json_uniquevisitors = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_uniquevisitors',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.unique_visitors_90days_daily_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'uniquevisitors_90days_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_uniquevisitors = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_uniquevisitors',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.unique_visitors_90days_daily_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'uniquevisitors_90days_daily_snapshot_doi')],
        export_format='CSV')

    # total unique visitors 90 days hourly snapshot
    export_bq_to_gcs_json_uniquevisitors_hourly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_uniquevisitors_hourly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.unique_visitors_90days_hourly_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'uniquevisitors_90days_hourly_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_uniquevisitors_hourly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_uniquevisitors_hourly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.unique_visitors_90days_hourly_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'uniquevisitors_90days_hourly_snapshot_doi')],
        export_format='CSV')

    # total unique visitors 90 days hourly week split snapshot
    export_bq_to_gcs_json_uniquevisitors_hourly_weeksplit = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_uniquevisitors_hourly_weeksplit',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.unique_visitors_90days_hourly_weeksplit_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'uniquevisitors_90days_hourly_weeksplit_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_uniquevisitors_hourly_weeksplit = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_uniquevisitors_hourly_weeksplit',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.unique_visitors_90days_hourly_weeksplit_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'uniquevisitors_90days_hourly_weeksplit_snapshot_doi')],
        export_format='CSV')

    # browser past 12 months count daily snapshot
    export_bq_to_gcs_json_browser_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_browser_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_browser_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'browser_12months_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_browser_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_browser_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_browser_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'browser_12months_daily_snapshot_doi')],
        export_format='CSV')

    # browser version past 12 months count daily snapshot
    export_bq_to_gcs_json_browser_version_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_browser_version_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_browser_version_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'browser_version_12months_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_browser_version_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_browser_version_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_browser_version_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'browser_version_12months_daily_snapshot_doi')],
        export_format='CSV')

    # operating system past 12 months count daily snapshot
    export_bq_to_gcs_json_opsys_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_opsys_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_opsys_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'opsys_12months_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_opsys_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_opsys_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_opsys_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'opsys_12months_daily_snapshot_doi')],
        export_format='CSV')

    # operating system version past 12 months count daily snapshot
    export_bq_to_gcs_json_opsys_ver_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_opsys_ver_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_opsys_ver_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'opsys_version_12months_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_opsys_ver_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_opsys_ver_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_opsys_ver_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'opsys_version_12months_daily_snapshot_doi')],
        export_format='CSV')

    # device category past 12 months count daily snapshot
    export_bq_to_gcs_json_devicecategory_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_devicecategory_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_category_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_category_12months_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_devicecategory_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_devicecategory_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_category_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_category_12months_daily_snapshot_doi')],
        export_format='CSV')

    # device screen resolution past 12 months count daily snapshot
    export_bq_to_gcs_json_screenres_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_screenres_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_screenresolution_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'screen_resolution_12months_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_screenres_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_screenres_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_screenresolution_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'screen_resolution_12months_daily_snapshot_doi')],
        export_format='CSV')

    # device brand past 12 months count daily snapshot
    export_bq_to_gcs_json_devicebrand_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_devicebrand_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_brand_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_brand_12months_daily_snapshot_doi')],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_devicebrand_monthly = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_devicebrand_monthly',
        source_project_dataset_table="{{params.project_id}}.dta_project_ursa_major.device_brand_yearly_snapshot_month_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/project_ursa_major/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_brand_12months_daily_snapshot_doi')],
        export_format='CSV')
    # ============================================================================================================
    query_unique_visitors_90days_daily_snapshot >> export_bq_to_gcs_json_uniquevisitors
    query_unique_visitors_90days_daily_snapshot >> export_bq_to_gcs_csv_uniquevisitors
    query_unique_visitors_90days_hourly_snapshot >> export_bq_to_gcs_json_uniquevisitors_hourly
    query_unique_visitors_90days_hourly_snapshot >> export_bq_to_gcs_csv_uniquevisitors_hourly
    query_unique_visitors_90days_hourly_weeksplit_snapshot >> export_bq_to_gcs_json_uniquevisitors_hourly_weeksplit
    query_unique_visitors_90days_hourly_weeksplit_snapshot >> export_bq_to_gcs_csv_uniquevisitors_hourly_weeksplit
    query_browser_12months_daily_snapshot >> export_bq_to_gcs_json_browser_monthly
    query_browser_12months_daily_snapshot >> export_bq_to_gcs_csv_browser_monthly
    query_opsys_12months_daily_snapshot >> export_bq_to_gcs_json_opsys_monthly
    query_opsys_12months_daily_snapshot >> export_bq_to_gcs_csv_opsys_monthly
    query_opsys_ver_12months_daily_snapshot >> export_bq_to_gcs_json_opsys_ver_monthly
    query_opsys_ver_12months_daily_snapshot >> export_bq_to_gcs_csv_opsys_ver_monthly
    query_device_category_12months_daily_snapshot >> export_bq_to_gcs_json_devicecategory_monthly
    query_device_category_12months_daily_snapshot >> export_bq_to_gcs_csv_devicecategory_monthly
    query_browser_version_12months_daily_snapshot >> export_bq_to_gcs_json_browser_version_monthly
    query_browser_version_12months_daily_snapshot >> export_bq_to_gcs_csv_browser_version_monthly
    query_screen_resolution_12months_daily_snapshot >> export_bq_to_gcs_json_screenres_monthly
    query_screen_resolution_12months_daily_snapshot >> export_bq_to_gcs_csv_screenres_monthly
    query_device_brand_12months_daily_snapshot >> export_bq_to_gcs_json_devicebrand_monthly
    query_device_brand_12months_daily_snapshot >> export_bq_to_gcs_csv_devicebrand_monthly
