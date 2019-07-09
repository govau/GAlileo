# Observatory - Alpha

## About
### What is it?
The Observatory aims to quantify interactions with government services and support delivery teams to improve their own products and services. 

### How?
Using anonymous analytics data, we are creating prototypes that visualise user intent, language, behaviour and service outcomes. We want to show where individual services fit in to the broader environment.

### Why?
We want government to deliver the best services it possibly can, and part of that is [measuring performance](https://www.dta.gov.au/help-and-advice/digital-service-standard/digital-service-standard-criteria/11-measure-performance). The Observatory will help to give context and quantifiable measures to delivery teams striving to improve.

## Privacy
The Observatory does not:
- use any personally identifiable information (PII),
- sell data,
- provide data access to third parties unless strictly required under our [Terms of Service](https://www.dta.gov.au/our-projects/google-analytics-government/dta-terms-service-google-analytics-360).

## Subscribing
If you're working in government and would like to join or learn more, please visit the [Observatory](https://www.dta.gov.au/our-projects/google-analytics-government) on [dta.gov.au](https://www.dta.gov.au/).

## Running your own
### Using Observatory data (internal staff only)
To run the HTML version, download augov.gexf from /data on Google Cloud Storage, put it in html/observatory/data and run html/observatory/run.sh

### Using your own data 
We currently use a customised version of Gephi's .gexf file format, adding a "domain" column. 
You may be able to use a tool like [HttpGraph](https://gephi.org/plugins/#/plugin/httpgraph) as a starting point to create your own.
