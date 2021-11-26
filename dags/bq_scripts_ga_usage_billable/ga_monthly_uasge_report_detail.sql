-- Google Analytics data usage reporting script
-- To use the scirpt replace the table name signature and execute 
-- Replace table names in 'create' and 'from' statements with latest billable usage table and latest agency mapping table

-- Year 2017
-- 01
create or replace table `dta_ga360_usage_billing.analytics_usage_201701_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201701` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201701_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201701` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201702_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201702` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201702_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201702` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201703_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201703` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201703_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201703` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201704_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201704` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;


create or replace table `dta_ga360_usage_billing.analytics_usage_201704_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201704` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201705_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201705` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201705_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201705` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201706_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201706` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201706_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201706` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201707_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201707` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201707_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201707` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201708_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201708` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201708_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201708` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201709_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201709` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201709_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201709` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201710_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201710` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201710_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201710` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201711_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201711` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201711_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201711` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201712_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201712` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201712_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201712` ba
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