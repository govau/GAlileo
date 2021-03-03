/*
Dimensions: Referral sources to gov.au domains
Metrics: users
Date Range: 09 February 2021 to current date
Data Slice: Hourly
*/

create or replace table dta_customers.govau_site_referral_feb_2021_fb
          OPTIONS (
            description = "Gov.AU site referral in February 2021 to current date for whole of government"
    )
as
select 
  traffic_referral,
  total_visitors,
  unique_visitors,
  visit_hour,
  -- visit_week_iso as visit_week,
  visit_date,
  visit_year,
  rank() over (partition by visit_date order by total_visitors desc) as peak_traffic_source
from (
  select
        COUNT(fullVisitorId) as total_visitors,
        COUNT(distinct fullVisitorId) as unique_visitors,
        sum(hit_count) as total_hits,
        traffic_referral,
        extract(HOUR from timestamp_seconds(visitStartTime) at Time Zone 'Australia/Sydney') as visit_hour,
        -- extract(ISOWEEK FROM date(timestamp_seconds(visitStartTime))) as visit_week_iso,
        format_date("%b-%d", date(timestamp_seconds(visitStartTime), 'Australia/Sydney')) as visit_date,
        format_date("%Y", date(timestamp_seconds(visitStartTime), 'Australia/Sydney')) as visit_year
    from
    (
/* Start - Datasets of Interest websites
    Insert Here Google Analytics Dataset of Websites of Interest and 'Union All' query result sets to get final result set
 */
 /*** aeaguide.education.gov.au/ ***/
   select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `79438793.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** trove.nla.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `23233927.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** designsystem.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `170387771.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** jobsearch.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `72008433.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** https://www.idpwd.com.au/ ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `34154705.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** mychild.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `100180008.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.jobjumpstart.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `111564569.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** igt.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `212190958.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** whatsnext.employment.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `100585217.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** ebs.tga.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `88992271.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.employment.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `77614012.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.fsc.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `174497994.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** data.wgea.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `93868316.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** army.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `122418128.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** osb.homeaffairs.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `110162521.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** Australia.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `71597546.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** data.gov.au - all data ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `69211100.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** abs.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `73191096.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** disabilityadvocacyfinder.dss.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `86149663.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** domainname.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `169220999.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** asic.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `39020822.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** onlineservices.ato.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `121638199.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.dta.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `99993137.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** [STRUCT(dta, )] ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `99993137.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** health.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `169499927.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.asd.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `121386494.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** familyrelationships.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `34938005.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** webarchive.nla.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `70635257.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** trove.nla.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `199921542.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** ga.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `80842702.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** ato.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `114274207.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** ABRWeb ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `178007804.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** catologue.nla.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `6592309.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.aqf.edu.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `149444086.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** cd.defence.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `178909235.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.studentsfirst.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `80703744.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** consultation.business.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `48099294.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** https://serviceproviders.dss.gov.au/ ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `101163468.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** australianjobs.employment.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `124827135.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** engage.dss.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `90974611.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.ihpa.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `82020118.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** nla.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `2802109.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.learningpotential.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `106413345.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** safeworkaustralia.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `179394289.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** beta.abs.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `186366587.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.artc.com.au\nAustralian Rail Track Corporation ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `225642503.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** [STRUCT(agency, artc)] ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `225642503.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.studyassist.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `53678167.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** govdex.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `77664740.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** covid19inlanguage.homeaffairs.gov.au (UA-61305954-25) – (View ID: 215803896) ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `215803896.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** maps.inlandrail.com.au/b2g-dec-2018#/\ninland rail map ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `186233756.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** [STRUCT(agency, inland_rail_map)] ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `186233756.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** airforce.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `122829809.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.tradesrecognitionaustralia.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `175869519.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** abcc.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `6533313.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** docs.education.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `77559172.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** jobaccess.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `104411629.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** eduportal.education.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `117867575.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** joboutlook.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `86630641.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** intercountryadoption.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `100832347.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** moneysmart.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `37548566.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** defence.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `5426088.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.education.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `77562775.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** ablis.business.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `78700159.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** defenceindustry.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `162370350.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** https://formerministers.dss.gov.au/ ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `53715324.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** communitybusinesspartnership.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `95014024.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** afsa.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `75255162.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** minister.homeaffairs.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `116763821.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** govcms ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `134969186.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** eduportal.education.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `117865571.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** video defence.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `122841309.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** m.directory.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `70856817.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** scamwatch.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `103904192.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** immi.homeaffairs.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `177457874.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** api.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `185106319.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** FWBC On Site ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `115980641.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** industry.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `175671120.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** http://www.companioncard.gov.au/ ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `31265425.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** humanservices.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `47586269.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** abr.business.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `94174429.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** librariesaustralia.nla.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `73966990.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** business.dmz.test.tga.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `98362688.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** myato ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `135414613.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** https://plan4womenssafety.dss.gov.au/ ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `104395490.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** news.defence.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `135989789.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** abf.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `177476111.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** Homeaffairs.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `100095673.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** betterschools.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `63623150.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.asbfeo.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `118336527.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** Style Manual ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `225103137.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** humanservices.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `5289745.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** superfundlookup.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `94178846.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** rba.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `191126238.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** covid19.homeaffairs.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `214546690.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** dss.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `77084214.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** immi.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `100095166.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** minister.defence.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `6059849.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** guides.dss.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `85844330.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** data.wgea.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `94241432.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** tga.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `129200625.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** banknotes.rba.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `203109603.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** innovation.govspace.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `69522323.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** business.dmz.development.tga.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `98360372.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** business.tga.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `98349897.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** myGov_beta ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `218817760.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.business.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `133849100.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** Career Pathways ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `222282547.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** www.tisnational.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `74070468.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** atrc.com.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `89766970.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
            union all
/*** marketplace.service.gov.au ***/
select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                CONCAT(trafficSource.medium, ' | ' ,trafficSource.source) as traffic_referral
            from
              `130142010.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between '20210209' and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
/* End - Datasets of Interest websites */
    )
     group by traffic_referral,visit_date,visit_year,visit_hour
  )
    order by visit_year, visit_date, visit_hour, peak_traffic_source 
;