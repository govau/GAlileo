-- Google Analytics 4 data usage reporting script
-- To use the scirpt replace the table name signature and execute 
-- Replace table names in 'create' and 'from' statements with latest billable usage table and latest agency mapping table


create or replace table `dta_ga4_usage_billing.analytics_ga4_usage_202201_report_allaccounts`
as
SELECT  
    coalesce(am.agency,"Agency not linked","") as agency_name,
    ba.ID as ga_id,
    coalesce(ba.Name,"") as service_name,
    Total_Event_Volume as service_events_hits,
    Billable_Event_Volume as billable_service_hits,
    sum( Total_Event_Volume ) over (partition by am.agency, ba.type order by am.agency,ba.type) as total_agency_event_hits,
    sum( Billable_Event_Volume ) over (partition by am.agency, ba.type order by am.agency,ba.type) as total_agency_billable_hits
    ,ba.type
FROM `dta-ga-bigquery.dta_ga4_usage_billing.analytics_ga4_usage_202201` ba
right join `dta_ga4_usage_billing.ga4_agency_mapping_latest` am
on am.propertyid = ba.id
where 
ba.ID is not null
order by type, agency
;


create or replace table `dta_ga4_usage_billing.analytics_ga4_usage_202201_report`
as
SELECT  
    coalesce(am.agency,"Agency not linked","") as agency_name,
    sum( Total_Event_Volume ) as total_hits,
    sum( Billable_Event_Volume ) as total_billable_hits
    ,Type
FROM `dta-ga-bigquery.dta_ga4_usage_billing.analytics_ga4_usage_202201` ba
left join `dta_ga4_usage_billing.ga4_agency_mapping_latest` am
on am.propertyid = ba.id
-- where Billable_Event_Volume <> 0
group by 
        agency_name, Type
order by 
        agency_name, Type
        ;
