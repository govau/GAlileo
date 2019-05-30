#!/usr/bin/env bash
# need to run "gcloud auth configure-docker" once
docker build -t warcraider . && docker tag warcraider gcr.io/dta-ga-bigquery/warcraider && docker push gcr.io/dta-ga-bigquery/warcraider
