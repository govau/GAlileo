import os
import re
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

DATA_DIR = '/home/airflow/gcs/data/'
if not os.path.isdir(DATA_DIR):
    DATA_DIR = '../../data/'


def get_service(api_name, api_version, scopes):
    if scopes == ['https://www.googleapis.com/auth/webmasters.readonly']:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            DATA_DIR + '/test-credentials.json', scopes=scopes)
    else:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            DATA_DIR + '/credentials.json', scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def domain_slug(domain):
    return re.sub(r"http(s)|:|\/|www.?|\.", "", domain)
