from __future__ import print_function
import datetime
import glob
import os

from airflow import models
from airflow.operators import python_operator
from airflow.contrib.operators import slack_webhook_operator
from airflow.contrib.operators import dataflow_operator

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2019, 4, 26),
    # http://airflow.apache.org/_api/airflow/contrib/operators/dataflow_operator/index.html
    'dataflow_default_options': {
        'project': models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery'),
        'region': 'us-central1',
        'zone': 'us-central1-b',
        'tempLocation': 'gs://staging.%s.appspot.com/' % models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery')
    }
}

DATA_DIR = '/home/airflow/gcs/data/'
if not os.path.isdir(DATA_DIR):
    DATA_DIR = '../../data/'


def combine_tally():
    from tablib import Dataset
    data = Dataset()
    for f in glob.glob(DATA_DIR + 'tally_69211100_20190425.csv-*'):
        d = Dataset().load(open(f, 'rt').read())
        for row in d:
            data.append(row)

    with open(DATA_DIR + 'tally_69211100_20190425.csv', 'wt', newline='')  as f:
        f.write('path,hits\n')
        f.write(data.csv)


def generate_plotly_chart():
    from tablib import Dataset

    df = Dataset().load(open(DATA_DIR + 'tally_69211100_20190425.csv', 'r').read()).df.sort_values(by=['hits'])
    df = df[df['hits'] > 30]

    import plotly
    import plotly.graph_objs as go
    plotly.offline.plot({
        "data": [go.Bar(x=df.path, y=df.hits)]}, filename=DATA_DIR + "temp-plot.html", auto_open=False)


def generate_graph():
    import igraph
    g = igraph.Graph()
    g.add_vertices(3)
    g.add_edges([(0, 1), (1, 2)])
    print(g)
    g.write_graphml(DATA_DIR + "social_network.graphml")


def find_number_one():
    from tablib import Dataset

    df = Dataset().load(open(DATA_DIR + 'tally_69211100_20190425.csv', 'r').read()).df.sort_values(by=['hits'])

    return df.values[-1][0], df.values[-1][1]


def tell_slack(context):
    o = slack_webhook_operator.SlackWebhookOperator(task_id="tell_slack", http_conn_id='slack_default',
                                                    message="Number one page today is %s (%s hits)" % (
                                                        find_number_one()))
    return o.execute(context)


with models.DAG(
        'ga_daily_reporter',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    benchmark_tally = dataflow_operator.DataFlowPythonOperator(task_id='benchmark_tally',
                                                               py_file='/home/airflow/gcs/dags/pipelines/benchmark_tally.py')
    combine_tally = python_operator.PythonOperator(
        task_id='combine_tally',
        python_callable=combine_tally,
        on_success_callback=tell_slack)
    # on_success_callback is a hack to delay generating the slack message
    # https://stackoverflow.com/questions/52054427/how-to-integrate-apache-airflow-with-slack
    tell_slack = slack_webhook_operator.SlackWebhookOperator(task_id="tell_slack", http_conn_id='slack_default',
                                                             message="A new report is out: "
                                                                     "https://%s/data/tally_69211100_20190425.csv" % (
                                                                         models.Variable.get('AIRFLOW_BUCKET',
                                                                                             'us-east1-dta-airflow-b3415db4-bucket')))

    generate_graph = python_operator.PythonOperator(
        task_id='generate_graph',
        python_callable=generate_graph)

    generate_plotly_chart = python_operator.PythonOperator(
        task_id='generate_plot.ly_chart',
        python_callable=generate_plotly_chart)

    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

    kubetest = KubernetesPodOperator(
        task_id='pod-ex-minimum',
        name='pod-ex-minimum',
        namespace='default',
        image='gcr.io/%s/galileo' % models.Variable.get('GCP_PROJECT', 'dta-ga-bigquery'),
        image_pull_policy="Always",
        cmds=['bash', '-c'],
        arguments=['gsutil cp gs://%s/data/tally_69211100_20190425.csv . && ' % models.Variable.get('AIRFLOW_BUCKET',
                                                                                                    'us-east1-dta-airflow-b3415db4-bucket') +
                   'gsutil cp gs://%s/dags/r_scripts/csvggplot.R . && ' % models.Variable.get('AIRFLOW_BUCKET',
                                                                                              'us-east1-dta-airflow-b3415db4-bucket') +
                   'R -f csvggplot.R && '
                   'gsutil cp tally_69211100_20190425.png gs://%s/data/' % models.Variable.get('AIRFLOW_BUCKET',
                                                                                               'us-east1-dta-airflow-b3415db4-bucket')], )

    benchmark_tally >> combine_tally
    combine_tally >> generate_plotly_chart
    combine_tally >> generate_graph
    combine_tally >> kubetest
    combine_tally >> tell_slack
