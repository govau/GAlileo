/*
BigQuery script to parse the GA4 meta schema for a property
reference link: https://gist.githubusercontent.com/jhnvdw/bf799dedcdf6c0acbd8e6e5f811d2401/raw/4be29e47340991f754f4de3fc330210973684bee/event_query.sql
*/

SELECT
  event_name,
  params.key AS event_parameter_key,
  CASE
    WHEN params.value.string_value IS NOT NULL THEN 'string'
    WHEN params.value.int_value IS NOT NULL THEN 'int'
    WHEN params.value.double_value IS NOT NULL THEN 'double'
    WHEN params.value.float_value IS NOT NULL THEN 'float'
END
  AS event_parameter_value
FROM
  -- Change this to your Google Analytics 4 export location in BigQuery
  `analytics_262101314.events_*`,
  UNNEST(event_params) AS params
WHERE
  -- Define static and/or dynamic start and end date
  _table_suffix BETWEEN '20210201'
  AND FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
GROUP BY
  1,
  2,
  3
ORDER BY
  1,
  2