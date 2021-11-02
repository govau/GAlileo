-- BigQuery script for producing each agency billable hits report based on month aggregation
-- Data extract from GA360 account in csv format and loaded into BigQuery tables

SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JULY 2020' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202007_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'AUGUST 2020' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202008_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'SEPTEMBER 2020' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202009_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'OCTOBER 2020' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202010_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'NOVEMBER 2020' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202011_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'DECEMBER 2020' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202012_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JANUARY 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202101_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'FEBRUARY 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202102_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'MARCH 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202103_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'APRIL 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202104_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'MAY 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202105_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JUNE 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202106_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JULY 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202107_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'AUGUST 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202108_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'SEPTEMBER 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202109_report_detail` 
-- WHERE agency_name = 'HA'
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'OCTOBER 2021' as month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202110_report_detail` 
-- WHERE agency_name = 'HA';