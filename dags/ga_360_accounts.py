from __future__ import print_function
import datetime, os

from airflow import models
from airflow.operators import python_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.contrib.operators import bigquery_operator

from galileo.ga import generate_accounts_views_index

DATA_DIR = '/home/airflow/gcs/data/'
if not os.path.isdir(DATA_DIR):
    DATA_DIR = '../data/'

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 26),
}


def compare_webcrawl_ga_with_accounts():
    from tablib import Dataset
    domain_names = Dataset().load(open(DATA_DIR + 'domain_names.csv').read())
    ga_domains = Dataset().load(open(DATA_DIR + 'ga_domains.csv').read())
    ga_billing_subscribers = Dataset().load(open(DATA_DIR + 'analytics_usage_201905.csv').read())
    ga_billing_subscriber_codes = [subscriber['ID'] for subscriber in ga_billing_subscribers.dict]
    ga_data_subscribers = Dataset().load(open(DATA_DIR + 'ga_accounts_views_index.csv').read())
    ga_data_subscribers_codes = [subscriber['property_id'] for subscriber in ga_data_subscribers.dict]
    ga_subscriber_codes = list(set().union(ga_billing_subscriber_codes, ga_data_subscribers_codes))
    agency_hostnames = {domain: agency for domain, agency in domain_names}

    subscriber_agencies = set()
    for domain in ga_domains.dict:
        agency = agency_hostnames.get(domain['hostname'],
                                      agency_hostnames.get(domain['domain_name'], "Unknown Agency"))
        if "GTM" not in domain['ga_code']:
            if domain['ga_code'] in ga_subscriber_codes:
                subscriber_agencies.add(agency)
    non_subscriber_websites = Dataset()
    non_subscriber_websites.headers = ["agency", "hostname", "ga_code"]
    subscriber_websites_not_subscribed = Dataset()
    subscriber_websites_not_subscribed.headers = ["agency", "hostname", "ga_code"]
    for domain in ga_domains.dict:
        agency = agency_hostnames.get(domain['hostname'],
                                      agency_hostnames.get(domain['domain_name'], "Unknown Agency"))
        if "GTM" not in domain['ga_code']:
            if domain['ga_code'] not in ga_subscriber_codes and agency not in subscriber_agencies:
                print("{}: {} has non-subscriber UA code: {}".format(agency, domain['hostname'], domain['ga_code']))
                non_subscriber_websites.append([agency, domain['hostname'], domain['ga_code']])
            elif domain['ga_code'] not in ga_subscriber_codes and agency in subscriber_agencies:
                print("{}: {} has non-subscriber UA code but is a subscribing agency: {}".format(agency,
                                                                                                 domain['hostname'],
                                                                                                 domain['ga_code']))
                subscriber_websites_not_subscribed.append([agency, domain['hostname'], domain['ga_code']])
    with open(DATA_DIR + '/non_subscriber_websites.csv', 'wt', newline='') as f:
        f.write(non_subscriber_websites.csv)
    with open(DATA_DIR + '/subscriber_websites_not_subscribed.csv', 'wt', newline='') as f:
        f.write(subscriber_websites_not_subscribed.csv)


with models.DAG(
        'ga_360_accounts',
        schedule_interval=datetime.timedelta(days=7),
        default_args=default_dag_args) as dag:
    generate_account_index = python_operator.PythonOperator(
        task_id='generate_account_index',
        python_callable=generate_accounts_views_index)

    project_id = models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')
    temp_table = 'ga_domains'
    query = """
                CREATE TABLE `{{params.project_id}}.tmp.{{params.temp_table}}_{{ ts_nodash }}`
                OPTIONS(
                  expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
                ) AS
                            select distinct domain_name, hostname, ga_code from web_crawl.url_resource, unnest(google_analytics) as ga_code
                           """
    query_webcrawl_ga_index = bigquery_operator.BigQueryOperator(
        task_id='query_webcrawl_ga_index',
        bql=query, use_legacy_sql=False, params={
            'project_id': project_id,
            'temp_table': temp_table,
        })
    export_webcrawl_ga_index_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_webcrawl_ga_index_to_gcs',
        source_project_dataset_table="{{params.project_id}}.tmp.{{params.temp_table}}_{{ ts_nodash }}",
        params={
            'project_id': project_id,
            'temp_table': temp_table
        },
        destination_cloud_storage_uris=[
            "gs://%s/data/%s.csv" % (
                models.Variable.get('AIRFLOW_BUCKET', 'us-east1-dta-airflow-b3415db4-bucket'), temp_table)],
        export_format='CSV')
    query_webcrawl_ga_index >> export_webcrawl_ga_index_to_gcs

    compare_webcrawl_ga_accounts = python_operator.PythonOperator(
        task_id='compare_webcrawl_ga_with_accounts',
        python_callable=compare_webcrawl_ga_with_accounts)
    generate_account_index >> compare_webcrawl_ga_accounts
    export_webcrawl_ga_index_to_gcs >> compare_webcrawl_ga_accounts

if __name__ == '__main__':
    compare_webcrawl_ga_with_accounts()
