import urllib2
import json


url = "https://dashboard.platform.aus-gov.com.au"

url_q1 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au/api/queries/1"
url_q5 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au/api/queries/5"
url_q6 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au/api/queries/6"
url_q7 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au/api/queries/7"
url_q8 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au/api/queries/8"
url_q9 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au/api/queries/9"
url_q10 = "https://dta:graphs-are-awesome@dashboard.platform.aus-gov.com.au/api/queries/10"

req1 = urllib2.Request(url_q1)
res1 = urllib2.urlopen(req1)

print res1.read()