-- Google Analytics data usage reporting script
-- To use the scirpt replace the table name signature and execute 
-- Replace table names in 'create' and 'from' statements with latest billable usage table and latest agency mapping table

-- Year 2018
-- 01
create or replace table `dta_ga360_usage_billing.analytics_usage_201801_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201801` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201801_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201801` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 02
create or replace table `dta_ga360_usage_billing.analytics_usage_201802_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201802` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201802_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201802` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 03 
create or replace table `dta_ga360_usage_billing.analytics_usage_201803_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201803` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201803_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201803` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 04
create or replace table `dta_ga360_usage_billing.analytics_usage_201804_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201804` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;


create or replace table `dta_ga360_usage_billing.analytics_usage_201804_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201804` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;

-- 05
create or replace table `dta_ga360_usage_billing.analytics_usage_201805_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201805` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201805_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201805` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 06
create or replace table `dta_ga360_usage_billing.analytics_usage_201806_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201806` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201806_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201806` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 07
create or replace table `dta_ga360_usage_billing.analytics_usage_201807_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201807` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201807_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201807` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


--08 
create or replace table `dta_ga360_usage_billing.analytics_usage_201808_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201808` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201808_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201808` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 09
create or replace table `dta_ga360_usage_billing.analytics_usage_201809_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201809` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201809_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201809` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 10
create or replace table `dta_ga360_usage_billing.analytics_usage_201810_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201810` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201810_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201810` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 11
create or replace table `dta_ga360_usage_billing.analytics_usage_201811_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201811` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201811_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201811` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


-- 12
create or replace table `dta_ga360_usage_billing.analytics_usage_201812_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201812` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201812_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201812` ba
left join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;