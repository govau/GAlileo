-- Let's create the matrix that we need for a logistic regression
-- to have aggregated event data and also to have the demographic user data is apparently really tricky, 
-- so i have dont the aggregation on the user data in python to save time and stress

SELECT
  -- the data will be aggregated by user_pseudo_id so we will look at users and what made them engaged
  user_pseudo_id,
  -- We will aggregate the event data for each user. The events we will be focusing on will be:
  -- link clicks
  sum(if(event_name = 'link_click',1,0)) as count_link_click,
  -- scroll events
  sum(if(event_name = 'scroll',1,0)) as count_scroll,
  -- first visit events
  sum(if(event_name = 'first_visit',1,0)) as count_first_visit,
  -- clicks
  sum(if(event_name = 'click',1,0)) as count_click,
  -- pageviews
  sum(if(event_name = 'pageview',1,0)) as count_pageview,
  -- external clicks
  sum(if(event_name = 'external_click',1,0)) as count_external_click,
  -- file downloads
  sum(if(event_name = 'file_download',1,0)) as count_file_download,
  -- user engagement events - i still am not entirely sure what these are and if they will mess with the analysis
  sum(if(event_name = 'user_engagement',1,0)) as count_user_engagement,
  --case when event_param_key= 'page_title' then event_param_value else null end as page_title,
  -- each user may or may not have a session_engaged parameter within an event - yes this is not actually session specific apparently
  sum(session_engaged) as sum_engaged_count,
  count(session_engaged) as session_engaged_count
  -- you can comment the above or below to see the engaged session counts and sums to demonstrate the point i make
  -- we will use engagement rate as the thing we are trying to predict
  -- sum(session_engaged)/count(session_engaged) as engagement_rate
FROM(
  SELECT
    -- we basically need this subquery to define the session_engaged metric, if you need to do anything to the key value params, do it here
    event_date, 
    user_pseudo_id,
    event_name,
    -- apparently when session_engaged triggers, sometimes it is in the int_value column, and sometimes it is in string_value?
    -- I got around this by adding them and casting the string to an integer
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

-- I tried really hard to do this in bigquery but a few of the functions that could help do not work in bigquery :(
-- This is a seperate table for the device category, browser and country so that we can join them on in python after finding the mode
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