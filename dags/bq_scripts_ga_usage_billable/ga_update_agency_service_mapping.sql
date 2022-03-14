-- BigQuery script to update agencies name to property services
-- Data sourced from following
-- 1 sharepoint https://dta1.sharepoint.com/:x:/g/DDD/DPS/DSA/Ed9Pk3zZ-6RItmv8vB4g2wUBrFokwEpwNgM2SUGIhVRCaA?e=TdhPKJ
-- 2 BigQuery table ua_agency_mapping_latest
-- 3 Latest month usage billing csv extract and loaded into BigQuery table analytics_usage_<yearmonth>

-- The script needs to updated with latest billable usage table name and latest agency mapping table name 
-- for e.g. 
    -- replace 'analytics_usage_202111' with latest one
    -- reference latest mapping table with signature 'ua_agency_mapping_latest'


-- Data report of a property that does not have proper agency name against it.
select * 
from `dta_ga360_usage_billing.analytics_usage_202111_report_detail`
where regexp_contains( agency_name,  'XXX')
or agency_name is null;

-- Agency mapping script to extract base ID and match service with agency name
-- Case 1
with map as (
    select 
        DISTINCT
            coalesce(cast(base_UA as string),'') as Base_UA,
            coalesce(agency,'') as agency,
            coalesce(agency_long_name,'') as agency_long_name
    from    `dta_ga360_usage_billing.ua_agency_mapping_latest`
    where   agency <> ''
)
SELECT 
    map.agency, 
    map.agency_long_name,
    map.base_UA,
    ub.Name as property_name,
    ub.ID as property_id,
    ub.Type
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202111` as ub
 left join map
 ON regexp_substr(ub.id,'^UA-([0-9]+)-[0-9]+$') = map.Base_UA
 where  base_UA is not null
 order by agency, base_UA
;

-- Case 2 with blank agency name having base UA ID
with map as (
    select 
        DISTINCT
            coalesce(cast(base_UA as string),'') as Base_UA,
            coalesce(agency,'') as agency,
            coalesce(agency_long_name,'') as agency_long_name
    from    `dta_ga360_usage_billing.ua_agency_mapping_latest`
)
SELECT 
    map.agency, 
    map.agency_long_name,
    map.base_UA,
    ub.*
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202111` as ub
 left join map
 ON regexp_substr(ub.id,'^UA-([0-9]+)-[0-9]+$') = map.Base_UA
 where  base_UA is not null
;


-- Script to extract agencies' properties that have blank agency name 
with map as (
    select 
        DISTINCT
            coalesce(cast(base_UA as string),'') as Base_UA,
            coalesce(agency,'') as agency,
            coalesce(agency_long_name,'') as agency_long_name
    from    `dta_ga360_usage_billing.ua_agency_mapping_latest`
)
SELECT 
    map.agency, 
    map.agency_long_name,
    map.base_UA,
    regexp_substr(ub.id,'^UA-([0-9]+)-[0-9]+$') as unmap_base_UA,
    ub.*
FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202111` as ub
 left join map
 ON regexp_substr(ub.id,'^UA-([0-9]+)-[0-9]+$') = map.Base_UA
 where  base_UA is null
;


SELECT * FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202111` 
WHERE regexp_contains(id,'6986514')
and id not in(
SELECT ga_id FROM `dta-ga-bigquery.dta_ga360_usage_billing.analytics_usage_202111_report_detail` 
WHERE regexp_contains(ga_id,'6986514')
);
