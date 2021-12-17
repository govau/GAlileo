/*
*/

SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202111 b
where dv.ga_id = b.id
and dv.month_year = 'November 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202110 b
where dv.ga_id = b.id
and dv.month_year = 'October 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202109 b
where dv.ga_id = b.id
and dv.month_year = 'September 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202108 b
where dv.ga_id = b.id
and dv.month_year = 'August 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202107 b
where dv.ga_id = b.id
and dv.month_year = 'July 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202106 b
where dv.ga_id = b.id
and dv.month_year = 'June 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202105 b
where dv.ga_id = b.id
and dv.month_year = 'May 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202104 b
where dv.ga_id = b.id
and dv.month_year = 'April 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202103 b
where dv.ga_id = b.id
and dv.month_year = 'March 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202102 b
where dv.ga_id = b.id
and dv.month_year = 'February 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202101 b
where dv.ga_id = b.id
and dv.month_year = 'January 2021'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202012 b
where dv.ga_id = b.id
and dv.month_year = 'December 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202011 b
where dv.ga_id = b.id
and dv.month_year = 'November 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202010 b
where dv.ga_id = b.id
and dv.month_year = 'October 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202009 b
where dv.ga_id = b.id
and dv.month_year = 'September 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202008 b
where dv.ga_id = b.id
and dv.month_year = 'August 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202007 b
where dv.ga_id = b.id
and dv.month_year = 'July 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202006 b
where dv.ga_id = b.id
and dv.month_year = 'June 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202005 b
where dv.ga_id = b.id
and dv.month_year = 'May 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202004 b
where dv.ga_id = b.id
and dv.month_year = 'April 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202003 b
where dv.ga_id = b.id
and dv.month_year = 'March 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202002 b
where dv.ga_id = b.id
and dv.month_year = 'February 2020'
union all
SELECT 
ga_id,
ga_service,
total_visitors,
unique_visitors,
Total_Hit_Volume,
Billable_Hit_Volume,
Type as account_type,
Name as ga_name,
month_year
FROM `dta-ga-bigquery.dta_ga360_usage_billing.digi_inv_ato_sa_jan2020_nov2021` dv,
dta_ga360_usage_billing.analytics_usage_202001 b
where dv.ga_id = b.id
and dv.month_year = 'January 2020'
;