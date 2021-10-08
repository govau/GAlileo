create or replace table `dta_ga360_usage_billing.analytics_usage_202004_report_detail`
as
SELECT  
    am.string_field_1 as agency_name,
    ba.ID as ga_id,
    ba.Name as service_name,
    sum( Total_Hit_Volume ) over (partition by am.string_field_1 order by am.string_field_1) as total_hits,
    sum( Billable_Hit_Volume ) over (partition by am.string_field_1 order by am.string_field_1) as total_billable_hits,
    type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202004` ba
right join `dta_ga360_usage_billing.ua_to_agency_mapping` am
on am.string_field_0 = ba.id
where Billable_Hit_Volume <> 0;



create or replace table `dta_ga360_usage_billing.analytics_usage_202004_report`
as
SELECT  
    coalesce(am.string_field_1,"") as agency_name,
    sum( Total_Hit_Volume ) as total_hits,
    sum( Billable_Hit_Volume ) as total_billable_hits,
    type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202004` ba
left join `dta_ga360_usage_billing.ua_to_agency_mapping` am
on am.string_field_0 = ba.id
where Billable_Hit_Volume <> 0
group by 
        agency_name,
        type
order by 
        type desc,
        agency_name
        ;


