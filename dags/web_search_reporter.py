from __future__ import print_function
import datetime
import re

from airflow import models
from airflow.operators import python_operator

from galileo import domain_slug
from galileo.searchconsole import generate_search_query_report

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 7, 1),
}

with models.DAG(
        'web_search_reporter',
        schedule_interval=datetime.timedelta(days=7),
        default_args=default_dag_args) as dag:
    for domain in ["https://data.gov.au", "https://www.dta.gov.au", "https://www.domainname.gov.au/",
                   "https://marketplace.service.gov.au"]:
        gen_searchqueries = python_operator.PythonOperator(
            task_id='gen_searchqueries_' + domain_slug(domain),
            python_callable=generate_search_query_report,
            op_args=[domain])
