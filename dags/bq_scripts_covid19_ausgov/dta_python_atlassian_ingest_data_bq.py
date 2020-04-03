# Pyhton 2.7 script to ingest data streams from Atlassian dashboard to DTA data warehouse

# schedule interval for datasets are as follows
#  Total number of unique users​ : 300s
#  Unique users that trigger action : 1800s
#  Recieved message : 300s
#  Total message in plus out, include all : 300s
#  Hourly active users : 1800s
#  Unique users split by amount of messages sent : 1800s
#  Daily active users : 1800s


import json
import requests
import re
import six
import itertools

# Data streams sources - URL Queries

query_number = ("1", "5", "6", "7", "8", "9", "10")

#  url of dashboard from Atlassian
url = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au"


# Get the json response query
data_id = []

for q in query_number:
    url_q = url + "/api/queries/" + q
    req = requests.get(url_q).text
    data_q = json.loads(req)
# Extract dataset ID from query json
    data_id.append(data_q['latest_query_data_id'])

# Format the URL for json file number to fetch json data
# https://dashboard.platform.aus-gov.com.au/api/queries/<query_number>/results/<json_number>.json

for (i, q) in zip(data_id, query_number):
    json_url = url + "/api/queries/" + q + "/results/" + str(i) + ".json"
# read the result json file ​
    data_j = requests.get(json_url).text
    # data_j = json.loads(req)
# change the format of json to newline_delimited_json for ingestion into BQ
    # result = [json.dumps(record) for record in json.load(req)]
​    
#  write the json file to destination


​
