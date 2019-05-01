#!/usr/bin/env bash
docker build -t galileo .
gcloud auth configure-docker
docker tag galileo gcr.io/api-project-993139374055/galileo
docker push gcr.io/api-project-993139374055/galileo