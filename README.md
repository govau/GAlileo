to push to GCP:
```
gcloud composer environments update ENVIRONMENT-NAME \\
--update-pypi-packages-from-file requirements.txt \\
--location LOCATION

gcloud composer environments storage dags import \
    --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source dags/*
```


Our analytics pipeline runs on open source Apache Airflow which is written in Python. This means we can deploy it to other clouds or inhouse if we need.
Some special configuration:
- using latest beta version composer-1.6.1-airflow-1.10.1
- google-api-python-client, tablib, python-igraph, plotly pypi packages preinstalled
- slack_default connection

Each pipeline is defined in a DAG file. (A Directed Acyclical Graph is a graph describing a process that goes step by step forward only with no infinite recursion or "cycles".)
DAG files are technically Python but use some special keywords and operators to describe processes. Each pipeline can have a schedule and a SLA (maximum expected run time).

DAG files are made up of Operators and can draw from Connections (via Hooks) and Variables.

Our favourite operators:

Tips for designing a workflow:
https://en.wikipedia.org/wiki/SOLID