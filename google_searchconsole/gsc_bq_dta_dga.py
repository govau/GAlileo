import datetime
import re

import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import parameters as pe

# Authenticate and construct service.
credentials = service_account.Credentials.from_service_account_file(
    pe.SERVICE_ACCOUNT_FILE, scopes=pe.SCOPES)

service = build(
    'webmasters',
    'v3',
    credentials=credentials
)


def get_sc_df(site_url, start_row, end_date=datetime.date.today(), days=90):
    """Grab Search Console data for the specific property and send it to BigQuery."""

    start_date = (end_date - datetime.timedelta(days=days)
                  ).strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
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
            pe.SERVICE_ACCOUNT_FILE_BQ)
        dataset_id = pe.BQ_DATASET_NAME
        table_name = pe.BQ_TABLE_NAME
        # create a job config
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            # write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
        # Set the destination table
        table_ref = client.dataset(dataset_id).table(table_name)

        load_job = client.load_table_from_dataframe(
            result, table_ref, job_config=job_config)
        load_job.result()

        return result
    else:
        print("There are no more results to return.")


# Loop through all defined properties, for up to 1000,000 rows of data in each
for p in pe.PROPERTIES:
    for x in range(0, 1000000, 25000):
        y = get_sc_df(p, x)
        if len(y) < 25000:
            break
        else:
            continue
