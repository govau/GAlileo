to push to GCP:
```
gcloud composer environments update ENVIRONMENT-NAME \\
--update-pypi-packages-from-file requirements.txt \\
--location LOCATION

gcloud composer environments storage dags import \
    --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source dags/*
```