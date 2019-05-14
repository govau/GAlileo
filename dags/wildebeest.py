import datetime
import six
import pandas as pd
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay

# pd.datetime is an alias for datetime.datetime
today = pd.datetime.today()

from airflow import models
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': datetime.datetime(2019, 5, 13),
}

project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')

# to get view ids query "SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA where schema_name < 'a'"
view_ids = set(['34154705', '100180008', '88992271', '71597546', '101713735', '69211100', '86149663',
                '99993137',  '34938005', '70635257', '80842702', '101163468', '90974611',
                '77664740', '104411629', '100832347', '95074916', '53715324', '95014024', '134969186',
                '31265425', '47586269', '95068310', '98362688', '104395490', '100095673', '5289745', '100136570',
                '77084214', '100095166', '85844330', '98349896', '129200625', '69522323', '98360372', '98349897'])


def generate_host_query(view_ids):
    start = (pd.datetime.today() - BDay(45)).strftime("%Y%m%d")  # 45 business days ago
    end = (pd.datetime.today() - BDay(1)).strftime("%Y%m%d")  # 1 business day ago
    temp_table = 'wildebeest_host_%s' % (end)

    subqueries = "UNION ALL".join(["""
    (SELECT
      trafficSource.source as from_hostname,
      h.page.hostname as to_hostname,
      COUNT(*) AS count
    FROM
      `dta-ga-bigquery.{}.ga_sessions_*`,
      UNNEST(hits) AS h
    WHERE
      _TABLE_SUFFIX BETWEEN '{}' AND '{}'
       AND trafficSource.source != '(direct)'
    GROUP BY
      to_hostname,
      from_hostname)
        
        """.format(vid, start, end) for vid in view_ids])

    query = """
    CREATE TABLE `{{params.project_id}}.tmp.{{ params.temp_table }}`
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    ) AS
                {{ params.subqueries }}
           """
    return query, subqueries, temp_table


def generate_url_query(view_ids):
    start = (pd.datetime.today() - BDay(6)).strftime("%Y%m%d")  # 6 business days ago
    end = (pd.datetime.today() - BDay(1)).strftime("%Y%m%d")  # 1 business day ago
    temp_table = 'wildebeest_url_%s' % (end)

    subqueries = "UNION ALL".join(["""
    (SELECT
      trafficSource.source as from_hostname,
      CONCAT(trafficSource.source,
      IF
        (trafficSource.medium = "referral",
          trafficSource.referralPath,
          '') ) as from_url,
          h.page.hostname as to_hostname,
      CONCAT(h.page.hostname, h.page.pagePath) as to_url,
      COUNT(*) AS count
    FROM
      `dta-ga-bigquery.{}.ga_sessions_*`,
      UNNEST(hits) AS h
    WHERE
      _TABLE_SUFFIX BETWEEN '{}' AND '{}'
       AND trafficSource.source != '(direct)'
    GROUP BY
      to_url,
      from_url,
      to_hostname,
      from_hostname)
        
        """.format(vid, start, end) for vid in view_ids])

    query = """
    CREATE TABLE `{{params.project_id}}.tmp.{{ params.temp_table }}`
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    ) AS
                {{ params.subqueries }}
           """
    return query, subqueries, temp_table


with models.DAG(
        'wildebeest',
        schedule_interval=datetime.timedelta(days=7),
        default_args=default_dag_args) as dag:

    query, subqueries, temp_table = generate_host_query(view_ids)
    query_wildebeest_host = bigquery_operator.BigQueryOperator(
        task_id='query_wildebeest_host' ,
        bql=query, use_legacy_sql=False, params={
            'project_id': project_id,
            'temp_table': temp_table,
            'subqueries': subqueries
        })
    export_wildebeest_to_gcs_host = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_wildebeest_to_gcs_host',
        source_project_dataset_table="%s.tmp.%s" % (project_id, temp_table),
        destination_cloud_storage_uris=["gs://%s/data/wildebeest/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET', 'us-east1-dta-airflow-b3415db4-bucket'), temp_table,)],
        export_format='CSV')

    query_wildebeest_host >> export_wildebeest_to_gcs_host

    query, subqueries, temp_table = generate_url_query(view_ids)
    query_wildebeest_url = bigquery_operator.BigQueryOperator(
        task_id='query_wildebeest_url',
        bql=query, use_legacy_sql=False, params={
            'project_id': project_id,
            'temp_table': temp_table,
            'subqueries': subqueries
        })
    export_wildebeest_to_gcs_url = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_wildebeest_to_gcs_url',
        source_project_dataset_table="%s.tmp.%s" % (project_id, temp_table),
        destination_cloud_storage_uris=["gs://%s/data/wildebeest/%s.csv" % (
            models.Variable.get('AIRFLOW_BUCKET', 'us-east1-dta-airflow-b3415db4-bucket'), temp_table,)],
        export_format='CSV')

    query_wildebeest_url >> export_wildebeest_to_gcs_url
