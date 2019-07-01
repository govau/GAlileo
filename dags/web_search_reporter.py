from __future__ import print_function
import datetime
import time
import tablib

from airflow import models
from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

from galileo import domain_slug
from galileo.searchconsole import generate_web_search_query_report

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 7, 1),
}

with models.DAG(
        'search_reporter',
        schedule_interval=datetime.timedelta(days=7),
        default_args=default_dag_args) as dag:
    for domain in ["https://data.gov.au", "https://www.dta.gov.au", "https://www.domainname.gov.au/",
                   "https://marketplace.service.gov.au"]:
        web_searchqueries = python_operator.PythonOperator(
            task_id='web_searchqueries_' + domain_slug(domain),
            python_callable=generate_web_search_query_report,
            op_args=[domain])
    for d in {
        {"domain": "https://www.dta.gov.au", "view_id": 99993137, "search_param": "keys"},
        {"domain": "https://data.gov.au", "view_id": 69211100, "search_param": "q"}
    }.items():
        project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')
        timestamp = time.time()
        start = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y%m%d")
        end = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y%m%d")
        temp_table = 'wildebeest_host_%s' % (end)

        query = """
            CREATE TABLE `{{params.project_id}}.tmp.{{ params.temp_table }}`
            OPTIONS(
              expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            ) AS
                        SELECT lower(replace(REGEXP_EXTRACT(page.pagePath, r"{{ params.search_param }}=(.*?)(?:&|$)"),"+"," ")) query, 
                        count(*) impressions FROM
            FROM
              `dta-ga-bigquery.{}.ga_sessions_*`,
              UNNEST(hits)
        
            WHERE
              _TABLE_SUFFIX BETWEEN '{}' AND '{}'
        AND       REGEXP_CONTAINS(page.pagePath, r"{{ params.search_param }}=(.*?)(?:&|$)")
        group by query
        order by count(*) desc
                   """.format(d['view_id'], start, end)

        query_benchmark = bigquery_operator.BigQueryOperator(
            task_id='query_benchmark',
            bql=query, use_legacy_sql=False, params={
                'project_id': project_id,
                'view_id': d['view_id'],
                'timestamp': timestamp,
                'temp_table': temp_table
            })
        export_benchmark_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
            task_id='export_benchmark_to_gcs',
            source_project_dataset_table="%s.tmp.%s" % (project_id, temp_table),
            destination_cloud_storage_uris=[
                "gs://us-central1-maxious-airflow-64b78389-bucket/data/%s.csv" % (temp_table,)],
            export_format='CSV')
        query_benchmark >> export_benchmark_to_gcs
