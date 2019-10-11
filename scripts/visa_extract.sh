#!/bin/bash
BUCKET=us-east1-dta-airflow-b3415db4-bucket
PROJECT=dta-ga-bigquery
for Q in 'visa_extract_ausgov' 'visa_extract_immi'
do
  bq query --destination_table "$PROJECT:tmp.$Q"  -n 1 < "$Q.sql";
  bq extract --destination_format CSV --print_header=true "$PROJECT:tmp.$Q" "gs://$BUCKET/data/$Q.csv";
done
