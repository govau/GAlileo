# Pyhton 3.6.4 script to ingest data streams from Atlassian dashboard to DTA data warehouse

# schedule interval for datasets are as follows
#  Q1: Total number of unique users​ : 300s
#  Q5: Unique users that trigger action : 1800s
#  Q6: Recieved message : 300s
#  Q7: Total message in plus out, include all : 300s
#  Q8: Hourly active users : 1800s
#  Q9: Unique users split by amount of messages sent : 1800s
#  Q10: Daily active users : 1800s

import json
import requests
import re
import six
import logging
from requests.exceptions import HTTPError
# import pandas

# Data streams sources - URL Queries
query_number = ("1", "5", "6", "7", "8", "9", "10")

#  url of dashboard from Atlassian
url = "https://dashboard.platform.aus-gov.com.au"
# Get the json response query
for q in query_number:
    url_q = url + "/api/queries/" + q
    # logging.info(url_q)
    res = requests.get(url_q, auth=('username', 'password'))
    # res.raise_for_status()
    # Extract dataset ID from query json
    data_res = res.json()
    data_id = data_res['latest_query_data_id']
    print(data_id)
    try:
        json_url = url + "/api/queries/" + q + \
            "/results/" + str(data_id) + ".json"
        # read the result json file
        data_jres = requests.get(json_url, auth=('username', 'password'))
        data_jres.raise_for_status()
    except HTTPError as http_err:
        print(http_err)
        continue
    except ValueError as val_err:
        print(val_err)
        continue
    except Exception as err:
        print(err)
        continue
        # res.raise_for_status()
        #  write the json file to destination
        # dataframe = pandas.DataFrame(json_stream)
        # dataframe_text = pandas.DataFrame(data_jres)
        # print(dataframe_text)
    outfilename = str(data_id) + "_q" + q + ".json"
    f = open(outfilename, "w")
    f.write(data_jres.text)
    f.close()
    # print('file written successfully')
