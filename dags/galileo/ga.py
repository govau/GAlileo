import tablib

try:
    from . import galileo
except ImportError:
    import galileo


def generate_accounts_views_index():
    # Authenticate and construct service.
    service = galileo.get_service(
        api_name='analytics',
        api_version='v3',
        scopes=['https://www.googleapis.com/auth/analytics.readonly'])
    data = tablib.Dataset()
    data.headers = ["account_id", "account_name", "property_id", "property_name", "property_level", "property_website",
                    "property_default_view", "view_id", "view_name"]
    accounts = service.management().accounts().list().execute()

    for account in accounts.get('items'):
        # print 'account', account['id'], account['name']
        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
            accountId=account['id']).execute()

        for property in properties.get('items'):
            # print '  property', property['id'],property['name']
            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(
                accountId=account['id'],
                webPropertyId=property['id']).execute()

            for view in profiles.get('items'):
                # print '    view',view['id'], view['name']
                # return each view ID
                data.append([account['id'], account['name'], property['id'], property['name'],
                             property.get('level'), property.get('websiteUrl'),
                             property.get('defaultProfileId'), view['id'], view['name']])
    with open(galileo.DATA_DIR + '/ga_accounts_views_index.csv', 'wt', newline='')  as f:
        f.write(data.csv)


def get_events(name, view_id, category, action):
    print('fetching', name, 'for', view_id)
    service = galileo.get_service(api_name='analyticsreporting', api_version='v4',
                                  scopes=['https://www.googleapis.com/auth/analytics.readonly'])
    response = service.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': view_id,
                    'dateRanges': [{'startDate': '30daysAgo', 'endDate': 'today'}],
                    'metrics': [{'expression': 'ga:totalEvents'}],
                    'dimensions': [{'name': 'ga:eventLabel'}],
                    'orderBys': [{"fieldName": 'ga:totalEvents', "sortOrder": "DESCENDING"}],
                    'filtersExpression': 'ga:totalEvents>10;ga:eventCategory==' + category + ';ga:eventAction==' + action,
                    'pageSize': 100000
                }]
        }
    ).execute()
    result = []
    for row in response.get('reports', [])[0].get('data', {}).get('rows', []):
        if row:
            # print(row['dimensions'][0], row['metrics'][0]['values'][0])
            result.append({"query": row['dimensions'][0], name: int(row['metrics'][0]['values'][0])})

    return result


if __name__ == '__main__':
    # generate_accounts_views_index()

    searches = get_events('impressions', '114274207', "ElasticSearch-Results", "Successful Search")
    search_clicks = get_events('clicks', '114274207', "ElasticSearch-Results Clicks", "Page Result Click")
    from collections import defaultdict

    d = defaultdict(dict)
    for l in (searches, search_clicks):
        for elem in l:
            d[elem['query'].lower()].update(elem)
    data = tablib.Dataset(headers=['query', 'impressions', 'clicks'])
    for l in d.values():
        data.append((l['query'], l.get('impressions'), l.get('clicks')))
    import datetime

    with open(galileo.DATA_DIR + 'internalsearch_114274207_' + datetime.datetime.now().strftime('%Y%m%d') + '.csv',
              'wt', newline='')  as f:
        f.write(data.csv)
