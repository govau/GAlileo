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
import itertools
import urllib
from urllib import Request as Re

# Data streams sources - URL Queries
query_number = ("1", "5", "6", "7", "8", "9", "10")

#  url of dashboard from Atlassian
url2 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au"
url = "https://dashboard.platform.aus-gov.com.au"

# Get the json response query
data_id = []

for q in query_number:
    url_q = url + "/api/queries/" + q
    # req = urllib2.Request(url_q)
    # print(req)
    res = urllib.urlopen(url_q)
    print(res.read())
    # res = requests.get(req)
    # data_q = json.loads(req)
    data_res = res.read()
# Extract dataset ID from query json
    data_id_ = re.search("\"latest_query_data_id\": ([0-9]+),", data_res)
    # data_id.append(data_q['latest_query_data_id'])
    data_id.append(data_id_)
    print(data_id)

# Format the URL for json file number to fetch json data
# https://dashboard.platform.aus-gov.com.au/api/queries/<query_number>/results/<json_number>.json

for (i, q) in zip(data_id, query_number):
    json_url = url + "/api/queries/" + q + "/results/" + str(i) + ".json"
# read the result json file ​
    # data_j = requests.get(json_url).text
    # data_j = json.loads(req)
    data_j = urllib.urlopen(json_url)
# change the format of json to newline_delimited_json for ingestion into BQ
    # result = [json.dumps(record) for record in json.load(req)]
    #  write the json file to destination
    f = open("temp_json.txt", "w")
    f.write(data_j)
