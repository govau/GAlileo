-- Let's create the matrix that we need for a logistic regression

SELECT
  user_pseudo_id,
  sum(if(event_name = 'link_click',1,0)) as count_link_click,
  sum(if(event_name = 'scroll',1,0)) as count_scroll,
  sum(if(event_name = 'first_visit',1,0)) as count_first_visit,
  sum(if(event_name = 'click',1,0)) as count_click,
  sum(if(event_name = 'pageview',1,0)) as count_pageview,
  sum(if(event_name = 'external_click',1,0)) as count_external_click,
  sum(if(event_name = 'file_download',1,0)) as count_file_download,
  sum(if(event_name = 'user_engagement',1,0)) as count_user_engagement,
    -- event names link_click, scroll, first_visit, click, pageview, external_click, file_download, user_engagement
  --case when event_param_key= 'page_title' then event_param_value else null end as page_title,
  sum(session_engaged) as sum_engaged_count,
  count(session_engaged) as session_engaged_count
FROM(
  SELECT
    event_date, 
    user_pseudo_id,
    event_name,
    case when params.key= 'session_engaged' 
         then ifnull(params.value.int_value,0) + ifnull(cast(params.value.string_value as int64),0) 
         else null end as session_engaged
  FROM
    -- Change this to your Google Analytics 4 export location in BigQuery
    `analytics_264036411.events_*` as t1,
    UNNEST(event_params) AS params
  WHERE
    -- Define static and/or dynamic start and end date
    _table_suffix BETWEEN '20210414'
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  )
GROUP BY
user_pseudo_id
;

-- make a seperate table for the device category, browser and country so that we can join them on in python after finding the max
select
  user_pseudo_id,
  event_name,
  device.category as device_category,
  device.web_info.browser as browser,
  geo.country as country,
  traffic_source.medium as traffic_medium,
  traffic_source.name as traffic_source
  --event_name,
  --STRING_AGG(DISTINCT traffic_source, "_") AS traffic_agg,
  --(select traffic_source.name from ((select distinct t1.user_pseudo_id, ( 
  --  select traffic_source.name 
  --  from `analytics_264036411.events_*` t2
  --  where t2.user_pseudo_id = t1.user_pseudo_id) 
  --  group by traffic_source.name
  --  order by count(*) desc
  --  limit 1) as traffic_source 
  --  from `analytics_264036411.events_*` t1 )) as traffic_source, 
  --ANY_VALUE(device_category) as device_category,
  --ANY_VALUE(browser) as browser,
  --ANY_VALUE(country) as country,
  from
    -- Change this to your Google Analytics 4 export location in BigQuery
    `analytics_264036411.events_*` as t1,
    UNNEST(event_params) AS params
    WHERE
    -- Define static and/or dynamic start and end date
    _table_suffix BETWEEN '20210414'
    AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))