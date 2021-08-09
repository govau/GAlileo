# Python 3 script methods
# authorisation Google Search Console account
# Access the verified list of sites
# Read Google search console datasets
# Ingest datasets into BigQuery

import datetime
from http.client import HTTPException
import re
import threading

import pandas as pd
import logging

import json

from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
# from oauth2client.service_account import ServiceAccountCredentials

import httplib2

from apiclient import errors
from oauth2client.client import OAuth2WebServerFlow

import parameters_dta_dga as pe
import parameters_gsc as gsc

# from datetime import datetime

# Authenticate and construct service methods


def construct_service_old():
    # construct service using service account created in GCP
    credentials = service_account.Credentials.from_service_account_file(
        pe.SERVICE_ACCOUNT_FILE_BQ, scopes=pe.SCOPES)

    service = build(
        'webmasters',
        'v3',
        credentials=credentials
    )


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


webmasters_service = construct_service()


def domain_slug(domain):
    return re.sub(r"http(s)|:|\/|www.?|\.", "", domain)


def store_ingestcount(rownum, siteurl):
    tracker = {"url": siteurl, "startrow": rownum}
    filename = 'tracker_' + domain_slug(siteurl) + '.json'
    with open(('data_store/' + filename), 'w') as of:
        json.dump(tracker, of)


def read_ingestcount(siteurl):
    # ingestion tracker function to read the row counter
    filename = 'tracker_' + domain_slug(siteurl) + '.json'
    try:
        with open('data_store/' + filename) as rfile:
            tracker = json.load(rfile)
            rfile.close()
        if tracker["startrow"] != '':
            return tracker["startrow"]
    except FileNotFoundError:
        print("tracker file does not created")
        return 1


def write_bigQuery(result, write_option='WRITE_APPEND'):
    # establish a BigQuery client
    client = bigquery.Client.from_service_account_json(
        pe.SERVICE_ACCOUNT_FILE_BQ)
    dataset_id = pe.BQ_DATASET_NAME
    table_name = pe.BQ_TABLE_NAME_SUMMARY
    # create a job config
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_option,
        autodetect=True
    )
    # Set the destination table
    table_ref = client.dataset(dataset_id).table(table_name)

    load_job = client.load_table_from_dataframe(
        result, table_ref, job_config=job_config)
    load_job.result()


def get_sc_df(site_url, start_row, end_date=datetime.date.today(), days=180):
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

    logging.info(site_url + '\t start row:' + str(start_row) + '\n')
    print(site_url + '\t start row:' + str(start_row) + '\n')

    try:
        response = webmasters_service.searchanalytics().query(
            siteUrl=site_url, body=request).execute()
    except Exception as e:
        logging.error('Error in extracting data:' + str(e))
        print('Error in extracting data, please lookup log file for details')
        return 0

    if response is not None:
        x = response['rows']
        df = pd.DataFrame.from_dict(x)

        store_ingestcount(start_row, site_url)
        print("data available in response object")

        # split the keys list into columns
        df[['query', 'device', 'page', 'date']] = pd.DataFrame(
            df['keys'].values.tolist(), index=df.index)

        # Drop the key columns
        result = df.drop(['keys'], axis=1)

        # Add a website identifier
        result['website'] = site_url
        logging.info('%s : %s', datetime.date.today, result)

        # print(result)
        # establish a BigQuery client
        client = bigquery.Client.from_service_account_json(
            pe.SERVICE_ACCOUNT_FILE_BQ)
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
        store_ingestcount(start_row, site_url)
        return 1
    else:
        print(site_url + ": There are no more results to return.")
        return 0


# def extract_stream(verified_sites_urls, service):
#     for site_url in verified_sites_urls:
#         for x in range(0, 1000000000, 25000):
#             y = get_sc_df(service, site_url, x)
#             if (bool(y)) and len(y) < 25000:
#                 break
#             else:
#                 continue

# Loop through all defined properties, for up to 1000,000,000 rows of data in each
# Retrieve list of properties in account


def main():

    site_list = webmasters_service.sites().list().execute()

    # Filter for verified websites
    verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                           if s['permissionLevel'] != 'siteUnverifiedUser'
                           and s['siteUrl'][:4] == 'http']
    logging.info('%s : %s', datetime.date.today, verified_sites_urls)

    # extract_stream(verified_sites_urls, webmasters_service)

    site_url_i = input(
        'Enter siteurl for data extract (hit enter for all sites funtion deactivated): ').strip()

    print(site_url_i)

    if site_url_i != "":
        startnum = input(
            'Press enter for existing stream to read range start number from file  \n 0 to start from beginning or other starting number:').strip()

        if startnum != 0 or startnum == '':
            startnum_read = read_ingestcount(site_url_i)
            logging.info("read ingestion tracker file successfully!")
            if startnum_read != 1:
                startnum = int(startnum_read)
            else:
                startnum = int(startnum)

        for x in range(startnum, 1000000000, 25000):
            y = get_sc_df(site_url_i, x)
            if y == 0:
                break
            else:
                continue
    else:
        print("Function deactivated for all sites.\nPlease rerun the program and enter single siteurl for data extract.\n")

    # if site_url_i == "":
    #     for site_url in verified_sites_urls:
    #         # debug
    #         print(site_url)
    #         for x in range(0, 1000000000, 25000):
    #             y = get_sc_df(site_url, x)
    #             if y == 0:
    #                 break
    #             else:
    #                 continue

    logging.info('%s : %s', 'Ended at: ', datetime.date.today)


if __name__ == '__main__':
    # log filename construct
    fname = 'logging/gsc_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S') + '.log'
    logging.basicConfig(filename=fname, filemode='w',
                        level=logging.INFO)
    main()
