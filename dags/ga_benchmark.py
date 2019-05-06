import datetime

from airflow import models
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
}


with models.DAG(
        'ga_benchmark',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    project_id = models.Variable.get('GCP_PROJECT','dta-ga-bigquery')

    view_id = '69211100'
    timestamp = '20190425'
    temp_table = 'benchmark_%s_%s' % (view_id, timestamp)
    query = """
    CREATE TABLE `{{params.project_id}}.tmp.{{ params.temp_table }}`
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    ) AS
        SELECT
        fullVisitorId,
        visitId,
        visitNumber,
        hits.hitNumber AS hitNumber,
                          hits.page.pagePath AS pagePath
        FROM
        `{{params.project_id}}.{{ params.view_id }}.ga_sessions_{{ params.timestamp }}`,
                                                      UNNEST(hits) as hits
        WHERE
        hits.type="PAGE"
        ORDER BY
        fullVisitorId,
        visitId,
        visitNumber,
        hitNumber
    """

    query_benchmark = bigquery_operator.BigQueryOperator(
        task_id='query_benchmark',
        bql=query, use_legacy_sql=False, params={
            'project_id': project_id,
            'view_id': view_id,
            'timestamp': timestamp,
            'temp_table': temp_table
        })
    export_benchmark_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_benchmark_to_gcs',
        source_project_dataset_table="%s.tmp.%s" % (project_id, temp_table),
        destination_cloud_storage_uris=["gs://us-central1-maxious-airflow-64b78389-bucket/data/%s.csv" % (temp_table,)],
        export_format='CSV')
    query_benchmark >> export_benchmark_to_gcs
