from __future__ import print_function
import datetime
import os
import tablib

from airflow import models
from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

from galileo import galileo, searchconsole, ga

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 7, 4),
}


def export_search_events():
    searches = ga.get_events('impressions', '114274207', "ElasticSearch-Results", "Successful Search")
    search_clicks = ga.get_events('clicks', '114274207', "ElasticSearch-Results Clicks", "Page Result Click")
    from collections import defaultdict
    d = defaultdict(dict)
    for l in (searches, search_clicks):
        for elem in l:
            d[elem['query'].lower()].update(elem)
    data = tablib.Dataset(headers=['query', 'page', 'impressions', 'clicks'])
    for l in d.values():
        data.append((l['query'], '114274207', l.get('impressions'), l.get('clicks')))
    if not os.path.isdir(galileo.DATA_DIR + '/searchqueries'):
        os.mkdir(galileo.DATA_DIR + '/searchqueries')
    with open(galileo.DATA_DIR + '/searchqueries/114274207_internalsearch_' + datetime.datetime.now().strftime(
            '%Y%m%d') + '.csv',
              'wt', newline='') as f:
        f.write(data.csv)


with models.DAG(
        'search_reporter',
        schedule_interval=datetime.timedelta(days=7),
        default_args=default_dag_args) as dag:
    event_searchqueries = python_operator.PythonOperator(
        task_id='web_searchqueries_events',
        python_callable=export_search_events)

    for domain in ["https://data.gov.au", "https://www.dta.gov.au", "https://www.domainname.gov.au/",
                   "https://marketplace.service.gov.au"]:
        web_searchqueries = python_operator.PythonOperator(
            task_id='web_searchqueries_' + galileo.domain_slug(domain),
            python_callable=searchconsole.generate_web_search_query_report,
            op_args=[domain])
    for d in [
        {"domain": "https://www.dta.gov.au", "view_id": 99993137, "search_param": "keys"},
        {"domain": "https://data.gov.au", "view_id": 69211100, "search_param": "q"},
        {"domain": "https://marketplace.service.gov.au", "view_id": 130142010, "search_param": "keyword"}
    ]:
        project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')
        start = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y%m%d")
        end = (datetime.date.today() - datetime.timedelta(days=2)).strftime("%Y%m%d")
        temp_table = '%s_internalsearch_%s' % (galileo.domain_slug(d['domain']), end)

        query = """
            CREATE TABLE `{{params.project_id}}.tmp.{{params.temp_table}}_{{ ts_nodash }}`
            OPTIONS(
              expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            ) AS
                        SELECT lower(replace(REGEXP_EXTRACT(page.pagePath, r"{{ params.search_param }}=(.*?)(?:&|$)"),"+"," ")) query, 
                        "{{params.domain}}" page,
                        count(*) impressions 
            FROM
              `dta-ga-bigquery.{{params.view_id}}.ga_sessions_*`,
              UNNEST(hits)
        
            WHERE
              _TABLE_SUFFIX BETWEEN '{{params.start}}' AND '{{params.end}}'
            AND       REGEXP_CONTAINS(page.pagePath, r"{{ params.search_param }}=(.*?)(?:&|$)")
            group by query
            order by count(*) desc
                       """

        query_internalsearch = bigquery_operator.BigQueryOperator(
            task_id='query_internalsearch_' + galileo.domain_slug(d['domain']),
            bql=query, use_legacy_sql=False, params={
                'project_id': project_id,
                'view_id': d['view_id'],
                'start': start,
                'end': end,
                'temp_table': temp_table,
                'domain': d['domain'],
                'search_param': d['search_param']
            })
        export_internalsearch_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
            task_id='export_internalsearch_to_gcs_' + galileo.domain_slug(d['domain']),
            source_project_dataset_table="{{params.project_id}}.tmp.{{params.temp_table}}_{{ ts_nodash }}",
            params= {
                'project_id': project_id,
                    'temp_table': temp_table
            },
            destination_cloud_storage_uris=[
                "gs://%s/data/searchqueries/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET', 'us-east1-dta-airflow-b3415db4-bucket'), temp_table)],
            export_format='CSV')
        query_internalsearch >> export_internalsearch_to_gcs
