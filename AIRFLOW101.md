# Airflow 101

## Our Environment

Our analytics pipeline runs on open source [Apache Airflow](http://airflow.apache.org/tutorial.html) which is written in Python. This means we can deploy it to other clouds or inhouse if we need.

We have some special configuration:
- using latest beta version composer-1.6.1-airflow-1.10.1
- using Python 3
- google-api-python-client, tablib, python-igraph, plotly pypi packages preinstalled
- slack_default and bigquery_default connection
- special docker image that includes google cloud SDK and R with tidyverse/ggplot2
- sendgrid enabled to allow warning and other messages to be sent.

## Getting Started 
To access "Cloud Composer" (the Google branding for Airflow), visit https://console.cloud.google.com/composer/environments
From this page you can access the Airflow webserver and the DAGs folder

Read https://cloud.google.com/composer/docs/ for more information.

## How to write a new workflow

### DAGs
Each pipeline is defined in a DAG file. (A Directed Acyclical Graph is a graph describing a process that goes step by step forward only with no infinite recursion or "cycles".)
DAG files are technically Python code but use some special keywords and operators to describe data processes. Each pipeline can have a schedule and a SLA (maximum expected run time).

DAG files are made up of Tasks that run Operators and can draw from Connections (via Hooks) and Variables. Definitions @ http://airflow.apache.org/concepts.html

Tutorials http://airflow.apache.org/tutorial.html and https://cloud.google.com/composer/docs/how-to/using/writing-dags and https://cloud.google.com/blog/products/gcp/how-to-aggregate-data-for-bigquery-using-apache-airflow

Tips for designing a workflow: https://en.wikipedia.org/wiki/SOLID

### Header 

 
Set email_on_failure to True to send an email notification when an operator in the DAG fails. 
```
default_dag_args = {

    # Email whenever an Operator in the DAG fails.
    'email': models.Variable.get('email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}
```

Schedule, start date and SLA

**WARNING: if the start date is in the past, it will try to catch up running jobs for the schedule period (eg. daily) the first time the DAG is loaded **

If the task takes longer than the SLA, an alert email is triggered.

```
with models.DAG(
        'ga_quarterly_reporter',
        schedule_interval=datetime.timedelta(days=90),
        sla=datetime.timedelta(hours=1),
        default_args=default_dag_args) as dag:
```

 ### Variables
 
 Variables are configured via the webserver under Admin -> Variables. A variable can be a string, Python list/array or Python dict.
 The second parameter of the .get() function is the default value if the variable isn't found.
 You can use variables in the python string formatting functions https://docs.python.org/3/library/string.html#formatexamples 
```
from airflow import models

'dataflow_default_options': {
    'project': models.Variable.get('GCP_PROJECT','dta-ga-bigquery'),
    'tempLocation': 'gs://staging.%s.appspot.com/' % models.Variable.get('GCP_PROJECT','dta-ga-bigquery')
 }
```

### Operators

Full listing at http://airflow.apache.org/_api/airflow/operators/index.html and http://airflow.apache.org/_api/airflow/contrib/operators/index.html includes operators for Bash scripts, JIRA, S3, SQL databases etc. 

**Our favourite operators:**

- PythonOperator
http://airflow.apache.org/howto/operator/python.html

- BigQueryOperator and BigQueryToCloudStorageOperator

Our environment automatically has a connection to bigquery so no credentials are needed.

http://airflow.apache.org/_api/airflow/contrib/operators/bigquery_operator/index.html

- KubernetesPodOperator 
Perfect for running an R script or a Python script that needs system packages like chart/graph rendering.

We run a custom docker image with extra R packages described in docker/Dockerfile

https://airflow.apache.org/_api/airflow/contrib/operators/kubernetes_pod_operator/index.html#airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator

**Honorable Mentions:**

- DataFlowOperator

uses the google cloud branded implementation of Apache Beam, another 
- SlackWebHookOperator

- EmailOperator
Gmail seems to take 5 or 6 minutes to virus scan attachments before they appear.


## Dependencies and Deployment

At the end of the file, in the indented "with DAG:" section you can define dependencies between operators (else they will all run concurrently):
```
A >> B >> C

or 
A >> B
B >> C

or 

A >> B
C << B

```

Once you have a DAG, can drag drop it into the folder via the web browser and soon it will be visible in the webserver. When updating a DAG, there is also a Refresh (recycling icon) button.
You can either trigger the whole DAG or "clear" a task to make that task and all dependent tasks be retried.

Once it is good, check it into Git!