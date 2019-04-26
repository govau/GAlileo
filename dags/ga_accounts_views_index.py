from __future__ import print_function
import datetime

from airflow import models
from airflow.operators import python_operator

from galileo.ga import generate_accounts_views_index

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 26),
}

with models.DAG(
        'ga_accounts_views_index',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    gen_index = python_operator.PythonOperator(
        task_id='gen_index',
        python_callable=generate_accounts_views_index)
