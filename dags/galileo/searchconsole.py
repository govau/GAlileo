import tablib
import datetime
import os

try:
    from . import galileo
except ImportError:
    import galileo


def generate_web_search_query_report(property_uri,
                                     days=10,
                                     end_date=datetime.date.today()):
    # Authenticate and construct service.
    service = galileo.get_service(
        api_name='searchconsole',
        api_version='v1',
        scopes=['https://www.googleapis.com/auth/webmasters.readonly'])
    start_date = (end_date - datetime.timedelta(days=days)
                  ).strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    print(property_uri)
    # First run a query to learn which dates we have data for. You should
    # always check which days in a date range have data
    # before running your main query.
    # This query shows data for the entire range, grouped and sorted by day,
    # descending; any days without data will be missing from the results.
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['date']
    }
    response = service.searchanalytics().query(
        siteUrl=property_uri, body=request).execute()
    # print(response)
    # data_start_date = response['rows'][-days-1]['keys'][0]
    data_start_date = response['rows'][0]['keys'][0]
    data_end_date = response['rows'][-1]['keys'][0]
    print("loading from", data_start_date, "to",
          data_end_date)  # , "(", days, "days )")
    data = tablib.Dataset()
    data.headers = ["query", "page", "clicks", "impressions",
                    "click_thru_ratio", "search_result_position"]
    page_start = 0
    last_rows = 1
    while last_rows > 0:
        request = {
            'startDate': data_start_date,
            'endDate': data_end_date,
            'dimensions': ['query', 'page'],
            'rowLimit': 25000,
            'startRow': page_start
        }
        response = service.searchanalytics().query(
            siteUrl=property_uri, body=request).execute()

        last_rows = 0
        if 'rows' in response:
            for row in response['rows']:
                last_rows += 1
                query = row['keys'][0].encode(
                    "ascii", errors="ignore").decode()
                page = row['keys'][1].encode("ascii", errors="ignore").decode()
                data.append([query, page, row['clicks'],
                             row['impressions'], row['ctr'], row['position']])
            print(page_start + last_rows, "received...")
            page_start += 25000
        else:
            print("done ", property_uri)
    if not os.path.isdir(galileo.DATA_DIR + '/searchqueries'):
        os.mkdir(galileo.DATA_DIR + '/searchqueries')
    with open(galileo.DATA_DIR + '/searchqueries/{}_websearch_{}_{}.csv'.format(
            galileo.domain_slug(property_uri),
            data_start_date.replace('-', ''),
            data_end_date.replace('-', ''),
    ), 'wt', newline='') as f:
        f.write(data.csv)


if __name__ == '__main__':
    generate_search_query_report("https://data.gov.au")
    generate_search_query_report("https://www.dta.gov.au")
    generate_search_query_report("https://www.domainname.gov.au/")
    generate_search_query_report("https://marketplace.service.gov.au")
