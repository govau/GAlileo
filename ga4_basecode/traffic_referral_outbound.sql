-- Google Analytics 4 
-- Page referrals, page location and outbound pages statistics

-- page referrals count
select
    -- user_pseudo_id,
    (select value.string_value from unnest(event_params) where key = 'page_referrer' ) as traffic_from_url, 
    (select case when trim((split((select value.string_value from unnest(event_params) where key = 'page_location' ),'/')[safe_ordinal(4)])) = ''
        then (split((select value.string_value from unnest(event_params) where key = 'page_location' ),'/')[safe_ordinal(3)])
        else (split((select value.string_value from unnest(event_params) where key = 'page_location' ),'/')[safe_ordinal(4)])
        end) as traffic_to_page,
    count(*) as referral_count,
    (select value.string_value from unnest(event_params) where key = 'page_location' ) as page_url
from
    -- google analytics 4 export location in bigquery
    `analytics_264036411.events_*`
    where
    -- define static and/or dynamic start and end date
    _table_suffix between '20210301' and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
    and (select value.string_value from unnest(event_params) where key = 'page_referrer' ) is not null
    group by 
        -- user_pseudo_id,
        page_url,
        page_visited,
        pagereferral_url
    order by referral_count desc;




-- outbound page count
select
    -- user_pseudo_id,
    (select value.string_value from unnest(event_params) where key = 'page_location') as outbound_url, count(*) as outbound_count
from
    -- google analytics 4 export location in bigquery
    `analytics_264036411.events_*`
    where
    -- define static and/or dynamic start and end date
    _table_suffix between '20210301' and format_date('%Y%m%d',date_sub(current_date(), interval 1 day))
    and (select value.string_value from unnest(event_params) where key = 'outbound')  = 'true'
    group by 
        -- user_pseudo_id,
        outbound_url
    order by outbound_count desc;