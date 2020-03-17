from __future__ import print_function
import datetime
from datetime import date, timedelta
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
    'start_date': datetime.datetime(2020, 3, 1),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=10)
}

# Dataset of Interest (DOI) delta snapshot
with models.DAG(
        'pageviews_delta_snapshot_doi',
        # schedule_interval=datetime.timedelta(days=1),
        schedule_interval='0 12 * * *',
        catchup=False,
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')

    # BigQuery Scripts
    # pageviews snapshot
    query_pageviews_snapshot_delta = bigquery_operator.BigQueryOperator(
        task_id='query_pageviews_snapshot_delta',
        priority='BATCH',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts_doi/bq_scripts_delta/dta_sql_pgvw_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # total visitors and total days snapshot
    query_total_visitors_days_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_total_visitors_days_delta_snapshot',
        priority='BATCH',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts_doi/bq_scripts_delta/dta_sql_total_visitors_days_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # device category snapshot
    query_device_category_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_category_delta_snapshot',
        priority='BATCH',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts_doi/bq_scripts_delta/dta_sql_devicecategory_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # device browser snapshot
    query_device_browser_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_device_browser_delta_snapshot',
        priority='BATCH',
        bql=pathlib.Path(galileo.DAGS_DIR+"/bq_scripts_doi/bq_scripts_delta/dta_sql_devicebrowser_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # # # device operating system snapshot
    # # query_device_ops_delta_snapshot = bigquery_operator.BigQueryOperator(
    # #     task_id='query_device_ops_delta_snapshot',
    # #     bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_doi/dta_sql_deviceops_daily_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # traffic source and medium snapshot
    query_traffic_src_medium_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_traffic_src_medium_delta_snapshot',
        priority='BATCH',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_doi/bq_scripts_delta/dta_sql_traffic_sourcemedium_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # country snapshot
    query_country_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_country_delta_snapshot',
        priority='BATCH',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_doi/bq_scripts_delta/dta_sql_geolocation_country_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # local city snapshot
    query_local_city_delta_snapshot = bigquery_operator.BigQueryOperator(
        task_id='query_local_city_delta_snapshot',
        priority='BATCH',
        bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_doi/bq_scripts_delta/dta_sql_geolocation_localcity_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)

    # # user session level engagement snapshot
    # query_user_session_delta_snapshot = bigquery_operator.BigQueryOperator(
    #     task_id='query_user_session_delta_snapshot',
    #     bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_doi/dta_sql_sessions_user_daily_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)
    
    # # device operating system and browser snapshot
    # query_device_opsbrowser_delta_snapshot = bigquery_operator.BigQueryOperator(
    #     task_id='query_device_opsbrowser_delta_snapshot',
    #     bql=pathlib.Path(galileo.DAGS_DIR + "/bq_scripts_doi/dta_sql_device_opsbrowser_daily_snapshot_delta_day_doi").read_text(), use_legacy_sql=False)
    # ===================================================================================================
    # Export datasets
    # pageviews snapshot
    export_bq_to_gcs_json_pgviews = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_pgviews',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'pgviews_daily_snapshot_doi_' + str(date.today()-timedelta(days=1))
                )],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_pgviews = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_pgviews',
    source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_delta_doi",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/delta_snapshot/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'pgviews_daily_snapshot_doi_' + str(date.today()-timedelta(days=1))
            )],
    export_format='CSV')
    
    # total visitors and total days snapshot
    export_bq_to_gcs_json_total_visitors_delta = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_total_visitors_delta',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_visitors_days_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                # date.strftime(date.today(), '%m%y'),
                'visitors_daily_snapshot_doi_' + str(date.today()-timedelta(days=1))
                )],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_total_visitors_delta = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_total_visitors_delta',
    source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_visitors_days_delta_doi",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/delta_snapshot/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            # date.strftime(date.today(), '%m%y'),
            'visitors_daily_snapshot_doi_' + str(date.today()-timedelta(days=1))
            )],
    export_format='CSV')


    # device category snapshot
    export_bq_to_gcs_json_device_category = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_device_category',
        source_project_dataset_table="{{params.project_id}}.dta_customers.device_category_delta_snapshot_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_category_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_device_category = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_device_category',
    source_project_dataset_table="{{params.project_id}}.dta_customers.device_category_delta_snapshot_doi",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/delta_snapshot/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'device_category_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
    export_format='CSV')

    # device browser snapshot
    export_bq_to_gcs_json_device_browser = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_device_browser',
        source_project_dataset_table="{{params.project_id}}.dta_customers.devicebrowser_daily_snapshot_delta_doi", 
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'device_browser_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_device_browser = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_device_browser',
    source_project_dataset_table="{{params.project_id}}.dta_customers.devicebrowser_daily_snapshot_delta_doi",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/delta_snapshot/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'device_browser_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
    export_format='CSV')

    # # device operating system snapshot
    # export_bq_to_gcs_json_device_ops = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    #     task_id='export_bq_to_gcs_json_device_ops',
    #     source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_ops_delta_doi",
    #     params={
    #         'project_id': project_id
    #     },
    #     destination_cloud_storage_uris=[
    #         "gs://%s/data/analytics/json/%s.json" % (
    #             models.Variable.get('AIRFLOW_BUCKET',
    #                                 'us-east1-dta-airflow-b3415db4-bucket'),
    #             'device_ops_daily_snapshot_doi')],
    #     export_format='NEWLINE_DELIMITED_JSON')

    # export_bq_to_gcs_csv_device_ops = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    # task_id='export_bq_to_gcs_csv_device_ops',
    # source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_ops_delta_doi",
    # params={
    #     'project_id': project_id
    # },
    # destination_cloud_storage_uris=[
    #     "gs://%s/data/analytics/csv/%s.csv" % (
    #         models.Variable.get('AIRFLOW_BUCKET',
    #                             'us-east1-dta-airflow-b3415db4-bucket'),
    #         'device_ops_daily_snapshot_doi')],
    # export_format='CSV')

    # traffic source and medium snapshot
    export_bq_to_gcs_json_traffic_src_medium = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_traffic_src_medium',
        source_project_dataset_table="{{params.project_id}}.dta_customers.pgvw_daily_traffic_srcmedium_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'traffic_src_medium_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_traffic_src_medium = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_traffic_src_medium',
    source_project_dataset_table="{{params.project_id}}.dta_customers.pgvw_daily_traffic_srcmedium_delta_doi",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/delta_snapshot/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'traffic_src_medium_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
    export_format='CSV')

    # local city snapshot
    export_bq_to_gcs_json_local_city = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_local_city',
        source_project_dataset_table="{{params.project_id}}.dta_customers.localcity_daily_snapshot_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'local_city_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_local_city = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    task_id='export_bq_to_gcs_csv_local_city',
    source_project_dataset_table="{{params.project_id}}.dta_customers.localcity_daily_snapshot_delta_doi",
    params={
        'project_id': project_id
    },
    destination_cloud_storage_uris=[
        "gs://%s/data/analytics/delta_snapshot/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'local_city_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
    export_format='CSV')

    # Country snapshot
    export_bq_to_gcs_json_country = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_json_country',
        source_project_dataset_table="{{params.project_id}}.dta_customers.country_daily_snapshot_delta_doi",
        params={
            'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.json" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                    'us-east1-dta-airflow-b3415db4-bucket'),
                'country_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
        export_format='NEWLINE_DELIMITED_JSON')

    export_bq_to_gcs_csv_country = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_bq_to_gcs_csv_country',
        source_project_dataset_table="{{params.project_id}}.dta_customers.country_daily_snapshot_delta_doi",
        params={
        'project_id': project_id
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/analytics/delta_snapshot/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET',
                                'us-east1-dta-airflow-b3415db4-bucket'),
            'country_daily_snapshot_doi_' + str(date.today()-timedelta(days=1)))],
    export_format='CSV')

    # # user session level engagement snapshot
    # export_bq_to_gcs_json_session_users = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    #     task_id='export_bq_to_gcs_json_session_users',
    #     source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_session_user_delta_doi",
    #     params={
    #         'project_id': project_id
    #     },
    #     destination_cloud_storage_uris=[
    #         "gs://%s/data/analytics/json/%s.json" % (
    #             models.Variable.get('AIRFLOW_BUCKET',
    #                                 'us-east1-dta-airflow-b3415db4-bucket'),
    #             'session_users_daily_snapshot_doi')],
    #     export_format='NEWLINE_DELIMITED_JSON')

    # export_bq_to_gcs_csv_session_users = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    # task_id='export_bq_to_gcs_csv_session_users',
    # source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_session_user_delta_doi",
    # params={
    #     'project_id': project_id
    # },
    # destination_cloud_storage_uris=[
    #     "gs://%s/data/analytics/csv/%s.csv" % (
    #         models.Variable.get('AIRFLOW_BUCKET',
    #                             'us-east1-dta-airflow-b3415db4-bucket'),
    #         'session_users_daily_snapshot_doi')],
    # export_format='CSV')

    # # device operating system and browser snapshot
    # export_bq_to_gcs_json_device_opsbrowser = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    #     task_id='export_bq_to_gcs_json_device_opsbrowser',
    #     source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_opsbrowser_delta_doi",
    #     params={
    #         'project_id': project_id
    #     },
    #     destination_cloud_storage_uris=[
    #         "gs://%s/data/analytics/json/%s.json" % (
    #             models.Variable.get('AIRFLOW_BUCKET',
    #                                 'us-east1-dta-airflow-b3415db4-bucket'),
    #             'device_opsbrowser_daily_snapshot_doi')],
    #     export_format='NEWLINE_DELIMITED_JSON')

    # export_bq_to_gcs_csv_device_opsbrowser = bigquery_to_gcs.BigQueryToCloudStorageOperator(
    # task_id='export_bq_to_gcs_csv_device_opsbrowser',
    # source_project_dataset_table="{{params.project_id}}.dta_customers.pageviews_daily_snapshot_device_opsbrowser_delta_doi", 
    # params={
    #     'project_id': project_id
    # },
    # destination_cloud_storage_uris=[
    #     "gs://%s/data/analytics/csv/%s.csv" % (
    #         models.Variable.get('AIRFLOW_BUCKET',
    #                             'us-east1-dta-airflow-b3415db4-bucket'),
    #         'device_opsbrowser_daily_snapshot_doi')],
    # export_format='CSV')
    # ============================================================================================================
    query_pageviews_snapshot_delta >> export_bq_to_gcs_json_pgviews
    query_pageviews_snapshot_delta >> export_bq_to_gcs_csv_pgviews
    query_total_visitors_days_delta_snapshot >> export_bq_to_gcs_json_total_visitors_delta
    query_total_visitors_days_delta_snapshot >> export_bq_to_gcs_csv_total_visitors_delta
    query_device_category_delta_snapshot >> export_bq_to_gcs_json_device_category
    query_device_category_delta_snapshot >> export_bq_to_gcs_csv_device_category
    query_device_browser_delta_snapshot >> export_bq_to_gcs_json_device_browser
    query_device_browser_delta_snapshot >> export_bq_to_gcs_csv_device_browser
    # query_device_ops_delta_snapshot >> export_bq_to_gcs_json_device_ops
    # query_device_ops_delta_snapshot >> export_bq_to_gcs_csv_device_ops
    query_traffic_src_medium_delta_snapshot >> export_bq_to_gcs_json_traffic_src_medium
    query_traffic_src_medium_delta_snapshot >> export_bq_to_gcs_csv_traffic_src_medium
    query_local_city_delta_snapshot >> export_bq_to_gcs_json_local_city
    query_local_city_delta_snapshot >> export_bq_to_gcs_csv_local_city
    query_country_delta_snapshot >> export_bq_to_gcs_json_country
    query_country_delta_snapshot >> export_bq_to_gcs_csv_country
    # query_user_session_delta_snapshot >> export_bq_to_gcs_json_session_users
    # query_user_session_delta_snapshot >> export_bq_to_gcs_csv_session_users
    # query_device_opsbrowser_delta_snapshot >> export_bq_to_gcs_json_device_opsbrowser
    # query_device_opsbrowser_delta_snapshot >> export_bq_to_gcs_csv_device_opsbrowser