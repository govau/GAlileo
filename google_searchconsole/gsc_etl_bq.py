# Python 3 script methods
# authorisation Google Search Console account
# Access the verified list of sites
# Read Google search console datasets
# Ingest datasets into BigQuery

# Note: create logging folder in current directory for logging errors

import datetime
import re

import pandas as pd
import logging

from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
# from oauth2client.service_account import ServiceAccountCredentials

import httplib2

from apiclient import errors
from oauth2client.client import OAuth2WebServerFlow

import parameters_bigquery as pb
import parameters_auth as gsc


# Authenticate and construct service method

def construct_service():
    # construct service using OAuth2.0 desktop client
    # Run through the OAuth flow and retrieve credentials
    flow = OAuth2WebServerFlow(
        gsc.CLIENT_ID, gsc.CLIENT_SECRET, gsc.OAUTH_SCOPE, gsc.REDIRECT_URI)
    authorize_url = flow.step1_get_authorize_url()
    print('Go to the following link in your browser: ' + authorize_url)
    code = input('Enter verification code: ').strip()
    credentials = flow.step2_exchange(code)

    # Create an httplib2.Http object and authorize it with our credentials
    http = httplib2.Http()
    http = credentials.authorize(http)

    webmasters_service = build('searchconsole', 'v1', http=http)

    if webmasters_service:
        return webmasters_service
    else:
        print("Authorisation failed")


def domain_slug(domain):
    return re.sub(r"http(s)|:|\/|www.?|\.", "", domain)


def get_sc_df(webmasters_service, site_url, start_row, end_date=datetime.date.today(), days=90):
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

    response = webmasters_service.searchanalytics().query(
        siteUrl=site_url, body=request).execute()

    if (not response) and len(response) > 1:

        x = response['rows']

        df = pd.DataFrame.from_dict(x)

        # split the keys list into columns
        df[['query', 'device', 'page', 'date']] = pd.DataFrame(
            df['keys'].values.tolist(), index=df.index)

        # Drop the key columns
        result = df.drop(['keys'], axis=1)

        # Add a website identifier
        result['website'] = site_url
        logging.info(result)

        # establish a BigQuery client
        client = bigquery.Client.from_service_account_json(
            pb.SERVICE_ACCOUNT_FILE_BQ)
        dataset_id = gsc.BQ_DATASET_NAME
        table_name = 'gsc_' + domain_slug(site_url)
        # create a job config
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
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
# Retrieve list of properties in account
def main():
    webmasters_service = construct_service()

    site_list = webmasters_service.sites().list().execute()

    # Filter for verified websites
    verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                           if s['permissionLevel'] != 'siteUnverifiedUser'
                           and s['siteUrl'][:4] == 'http']

    for site_url in verified_sites_urls:
        for x in range(0, 1000000, 25000):
            y = get_sc_df(webmasters_service, site_url, x)
            if (bool(y)) and len(y) < 25000:
                break
            else:
                continue


if __name__ == '__main__':
    logging.basicConfig(filename='logging/gsc.log', filemode='w',
                        level=logging.ERROR)
    main()
