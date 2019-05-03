#!/usr/bin/env bash
# need to run "gcloud auth configure-docker" once
docker build -t galileo . && docker tag galileo gcr.io/api-project-993139374055/galileo && docker push gcr.io/api-project-993139374055/galileo

