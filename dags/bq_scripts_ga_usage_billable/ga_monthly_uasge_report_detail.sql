-- Google Analytics data usage reporting script
-- To use the scirpt replace the table name signature and execute 
-- Replace table names in 'create' and 'from' statements with latest billable usage table and latest agency mapping table


-- ABC and other exclusion report
-- Data reporting billable hits of subscribed agencies excluding ABC for a month
SELECT  
type, sum(Billable_Hit_Volume) as billable_hits_sum
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202111` 
where not regexp_contains(name,'ABC ')
group by type
;


-- Data report of a property that does not have proper agency name against it.
select * 
from `dta_ga360_usage_billing.analytics_usage_202111_report_detail`
where regexp_contains( agency_name,  'XXX')
or agency_name is null;




-- Year 2015
-- 01
create or replace table `dta_ga360_usage_billing.analytics_usage_201501_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201501` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201501_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201501` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201502_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201502` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201502_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201502` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201503_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201503` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201503_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201503` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201504_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201504` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;


create or replace table `dta_ga360_usage_billing.analytics_usage_201504_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201504` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201505_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201505` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201505_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201505` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201506_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201506` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201506_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201506` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201507_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201507` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201507_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201507` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201508_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201508` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201508_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201508` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201509_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201509` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201509_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201509` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201510_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201510` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201510_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201510` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201511_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201511` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201511_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201511` ba
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
create or replace table `dta_ga360_usage_billing.analytics_usage_201512_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201512` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_201512_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_201512` ba
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