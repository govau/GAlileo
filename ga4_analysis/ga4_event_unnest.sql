SELECT
  event_date, 
  user_pseudo_id,
  event_name,
  params.key AS event_param_key,
  params.value AS event_param_value,
FROM
  -- Change this to your Google Analytics 4 export location in BigQuery
  `analytics_264036411.events_*`,
  UNNEST(event_params) AS params
WHERE
  -- Define static and/or dynamic start and end date
  _table_suffix BETWEEN '20210414'
  AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
ORDER BY
  event_date