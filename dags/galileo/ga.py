from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import os
import tablib

DATA_DIR = '/home/airflow/gcs/data/'
if not os.path.isdir(DATA_DIR):
    DATA_DIR = '../../data/'

def get_service(api_name, api_version, scopes):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        DATA_DIR+'/credentials.json', scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def generate_accounts_views_index():
    # Authenticate and construct service.
    service = get_service(
        api_name='analytics',
        api_version='v3',
        scopes=['https://www.googleapis.com/auth/analytics.readonly'])
    data = tablib.Dataset()
    data.headers = ["account_id", "account_name", "property_id", "property_name", "property_level", "property_website",
                    "property_default_view", "view_id", "view_name"]
    accounts = service.management().accounts().list().execute()

    for account in accounts.get('items'):
        #print 'account', account['id'], account['name']
        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
            accountId=account['id']).execute()

        for property in properties.get('items'):
            #print '  property', property['id'],property['name']
            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(
                accountId=account['id'],
                webPropertyId=property['id']).execute()

            for view in profiles.get('items'):
                #print '    view',view['id'], view['name']
                # return the first view (profile) id.
                data.append([account['id'], account['name'], property['id'], property['name'],
                             property.get('level'), property.get('websiteUrl'),
                             property.get('defaultProfileId'), view['id'], view['name']])
    with open(DATA_DIR+'/ga_accounts_views_index.csv', 'wb') as f:
        f.write(data.csv)


if __name__ == '__main__':
    generate_accounts_views_index()