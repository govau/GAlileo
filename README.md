= Airflow 101 =
Our analytics pipeline runs on open source [Apache Airflow](http://airflow.apache.org/tutorial.html) which is written in Python. This means we can deploy it to other clouds or inhouse if we need.

We have some special configuration:
- using latest beta version composer-1.6.1-airflow-1.10.1
- google-api-python-client, tablib, python-igraph, plotly pypi packages preinstalled
- slack_default and bigquery_default connection
- special docker image that includes google cloud SDK and R with tidyverse/ggplot2
- sendgrid enabled to allow warning and other messages to be sent.

To access "Cloud Composer" (the Google branding for Airflow), visit https://console.cloud.google.com/composer/environments
From this page you can access the Airflow webserver and the DAGs folder

Each pipeline is defined in a DAG file. (A Directed Acyclical Graph is a graph describing a process that goes step by step forward only with no infinite recursion or "cycles".)
DAG files are technically Python but use some special keywords and operators to describe processes. Each pipeline can have a schedule and a SLA (maximum expected run time).

DAG files are made up of Tasks that run Operators and can draw from Connections (via Hooks) and Variables.

Tips for designing a workflow: https://en.wikipedia.org/wiki/SOLID

Our favourite operators:

- PythonOperator

- BigQueryOperator and BigQueryToCloudStorageOperator

- KubernetesPodOperator 

(ie. run an R script or a Python script that needs system packages like chart/graph rendering)

We run a custom docker image with extra R packages described in docker/Dockerfile

Honorable Mentions:
- DataFlowOperator

- SlackWebHookOperator

- EmailOperator