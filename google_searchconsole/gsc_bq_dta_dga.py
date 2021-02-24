import datetime
import json
import os
import re

import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

########### SET YOUR PARAMETERS HERE ####################################
PROPERTIES = ["https://www.dta.gov.au", "https://data.gov.au"]
BQ_DATASET_NAME = 'search_console'
BQ_TABLE_NAME = 'test_table'
SERVICE_ACCOUNT_FILE = 'test-credentials.json'
SERVICE_ACCOUNT_FILE_BQ = 'dta-ga-bigquery-fe73cf8a69f6.json'
################ END OF PARAMETERS ######################################


# Authenticate and construct service.
SCOPES = ['https://www.googleapis.com/auth/webmasters']
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

service = build(
    'webmasters',
    'v3',
    credentials=credentials
)


def domain_slug(domain):
    return re.sub(r"http(s)|:|\/|www.?|\.", "", domain)


def get_sc_df(site_url, start_date, end_date, start_row):
    """Grab Search Console data for the specific property and send it to BigQuery."""

    request = {
        'startDate': start_date,
        'endDate': end_date,
        # uneditable to enforce a nice clean dataframe at the end!
        'dimensions': ['query', 'device', 'page', 'date'],
        'rowLimit': 25000,
        'startRow': start_row
    }

    response = service.searchanalytics().query(
        siteUrl=site_url, body=request).execute()

    if len(response) > 1:

        x = response['rows']

        df = pd.DataFrame.from_dict(x)

        # split the keys list into columns
        df[['query', 'device', 'page', 'date']] = pd.DataFrame(
            df['keys'].values.tolist(), index=df.index)

        # Drop the key columns
        result = df.drop(['keys'], axis=1)

        print(result)

        # Add a website identifier
        result['website'] = site_url

        # establish a BigQuery client
        client = bigquery.Client.from_service_account_json(
            SERVICE_ACCOUNT_FILE_BQ)
        dataset_id = BQ_DATASET_NAME
        table_name = BQ_TABLE_NAME
        # create a job config
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True
        )
        # Set the destination table
        table_ref = client.dataset(dataset_id).table(table_name)
        # job_config.destination = table_ref
        # job_config.autodetect =TRUE
        # job_config.write_disposition = 'WRITE_APPEND'

        load_job = client.load_table_from_dataframe(
            result, table_ref, job_config=job_config)
        load_job.result()

        return result
    else:
        print("There are no more results to return.")


# Loop through all defined properties, for up to 100,000 rows of data in each
for p in PROPERTIES:
    for x in range(0, 100000, 25000):
        y = get_sc_df(p, "2021-01-01", "2021-01-31", x)
        if len(y) < 25000:
            break
        else:
            continue
