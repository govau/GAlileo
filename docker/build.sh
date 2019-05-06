#!/usr/bin/env bash
# need to run "gcloud auth configure-docker" once
docker build -t galileo . && docker tag galileo gcr.io/dta-ga-bigquery/galileo && docker push gcr.io/dta-ga-bigquery/galileo

