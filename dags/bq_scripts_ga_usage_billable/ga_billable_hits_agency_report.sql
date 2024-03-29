-- BigQuery script for producing each agency billable hits report based on month aggregation
-- Data extract from GA360 account in csv format and loaded into BigQuery tables

SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JANUARY 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202001_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'FEBRUARY 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202002_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'MARCH 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202003_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'APRIL 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202004_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'MAY 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202005_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JUNE 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202006_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JULY 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202007_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'AUGUST 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202008_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'SEPTEMBER 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202009_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'OCTOBER 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202010_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'NOVEMBER 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202011_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'DECEMBER 2020' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202012_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JANUARY 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202101_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'FEBRUARY 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202102_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'MARCH 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202103_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'APRIL 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202104_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'MAY 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202105_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JUNE 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202106_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JULY 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202107_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'AUGUST 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202108_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'SEPTEMBER 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202109_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'OCTOBER 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202110_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'NOVEMBER 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202111_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- DTA DGA Account
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'DECEMBER 2021' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202112_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- GA Property ID
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'JANUARY 2022' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202201_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- GA Property ID
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'FEBRUARY 2022' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202201_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- GA Property ID
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'MARCH 2022' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202201_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- GA Property ID
union all
SELECT 
agency_name,
ga_id,
service_name,
total_service_hits,
total_agency_hits,
'APRIL 2022' as month_year,
type as account_type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202201_report_detail` 
WHERE regexp_contains(ga_id,'00000000') -- GA Property ID
;
