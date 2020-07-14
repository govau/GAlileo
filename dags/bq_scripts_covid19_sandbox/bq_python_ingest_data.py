# Pyhton 2.7 script to ingest data streams from Atlassian dashboard to DTA data warehouse

import json
import requests
import re
import six

# Data streams sources - URL Queries

query_number = ("1", "5", "6", "7", "8", "9", "10")

#  url of dashboard from Atlassian
url = "https://username:password@dashboard.platform.aus-gov.com.au"
​

for q in query_number:
    url_q = url + "/api/queries/" + q


#  Total number of unique users​
url_q1 = url + "/api/queries/1"

#  Unique users that trigger action
url_q5 = url + "/api/queries/5"
​
#  Recieved message
url_q6 = url + "/api/queries/6"
​
#  Total message in plus out, include all
url_q7 = url + "/api/queries/7"
​
#  Hourly active users
url_q8 = url + "/api/queries/8"
​
#  Unique users split by amount of messages sent
url_q9 = url + "/api/queries/9"
​
#  Daily active users
url_q10 = url + "/api/queries/10"
​
​
# Get the json response from the Query
# req1 = urllib2. Request()
req1 = requests.get(url_q1).text
​
req5 = requests.get(url_q5).text

req6 = requests.get(url_q6).text

req7 = requests.get(url_q7).text

req8 = requests.get(url_q8).text

req10 = requests.get(url_q10).text
​
# Extract dataset ID from query json
​
data_1 = json.loads(req1)
data_id_1 = re.search("\"latest_query_data_id\": ([0-9]+),", data_1)
# data_id_1 = data_1['latest_query_data_id']
​
print("Latest query ID for query 1:", data_id_1)

data_5 = json.loads(req5)
data_id_5 = data_5['latest_query_data_id']

data_6 = json.loads(req6)
data_id_6 = data_6['latest_query_data_id']

data_7 = json.loads(req7)
data_id_7 = data_7['latest_query_data_id']

data_8 = json.loads(req8)
data_id_8 = data_8['latest_query_data_id']

data_9 = json.loads(req9)
data_id_9 = data_1['latest_query_data_id']

data_10 = json.loads(req10)
data_id_10 = data_1['latest_query_data_id']

​
# Format the URL for json file number to fetch json data
​
# https://dashboard.platform.aus-gov.com.au/api/queries/1/results/<json_number>.json
​
json_url_1 = url + "/api/queries/1/results/" + str(data_id_1) + ".json"
​
# https://dashboard.platform.aus-gov.com.au/api/queries/5/results/<json_number>.json
​
json_url_5 = url + "/api/queries/5/results/" + str(data_id_5) + ".json"

# https://dashboard.platform.aus-gov.com.au/api/queries/6/results/<json_number>.json
​
json_url_6 = url + "/api/queries/6/results/" + str(data_id_6) + ".json"

# https://dashboard.platform.aus-gov.com.au/api/queries/7/results/<json_number>.json

json_url_7 = url + "/api/queries/7/results/" + str(data_id_7) + ".json"
​
# https://dashboard.platform.aus-gov.com.au/api/queries/8/results/<json_number>.json

json_url_8 = url + "/api/queries/8/results/" + str(data_id_8) + ".json"​
# https://dashboard.platform.aus-gov.com.au/api/queries/9/results/<json_number>.json
​
json_url_9 = url + "/api/queries/9/results/" + str(data_id_9) + ".json"
# https://dashboard.platform.aus-gov.com.au/api/queries/10/results/<json_number>.json
​
json_url_10 = url + "/api/queries/10/results/" + str(data_id_10) + ".json"
​
# read the result json file ​
# change the format of json to newline_delimited_json for ingestion into BQ
​
result = [json.dumps(record) for record in json.load(in_json)]
​
#  write the json file to destination