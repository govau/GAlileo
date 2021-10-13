-- Google Analytics data usage reporting script
-- To use the scirpt replace the table name signature and execute 
-- Replace table names in 'create' and 'from' statements with latest billable usage table and latest agency mapping table

create or replace table `dta_ga360_usage_billing.analytics_usage_202109_report_detail`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    sum( Billable_Hit_Volume ) over (partition by ba.Name order by ba.Name) as total_service_hits,
    sum( Billable_Hit_Volume ) over (partition by am.agency order by am.agency) as total_agency_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202109` ba
right join `dta_ga360_usage_billing.ua_agency_mapping_latest` am
on am.property_id = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_202109_report`
as
SELECT  
    coalesce(am.agency,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    am.type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202109` ba
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



-- Data reporting billable hits of subscribed agencies excluding ABC for a month
SELECT  
type, sum(Billable_Hit_Volume) as billable_hits_sum
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202106` 
where not regexp_contains(name,'ABC ')
group by type
;


-- Data report of a property that does not have proper agency name against it.
select * 
from `dta_ga360_usage_billing.analytics_usage_202107_report_detail`
where regexp_contains( agency_name,  'XXX')
or agency_name is null;

