/*
BigQuery script for adding columns to GA account profile for ATO and Services Australia accounts
*/
select
        COUNT(fullVisitorId) as total_visitors,
        COUNT(distinct fullVisitorId) as unique_visitors,
        APPROX_COUNT_DISTINCT(fullVisitorId) as unique_visitors_approx,
        format_date('%B %Y',datetime(timestamp_seconds(visitStartTime))) as month_year,
        -- net.reg_domain(hostname) as reg_domain,
        ga_service,
        ga_id
        -- user_type

    from
    (
/* Start - Datasets of Interest websites
    Insert Here Google Analytics Dataset of Websites of Interest and 'Union All' query result sets to get final result set
 */
        /*** myato ***/
        select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                hits.page.hostname as hostname,
                'myato' as ga_service,
                'UA-72006902-3' as ga_id,
                -- User Type (dimension)
                -- CASE
                --   WHEN totals.newVisits = 1 THEN 'new visitor'
                -- ELSE 
                --     'returning visitor'
                -- END AS user_type,
                type           
            from
              `135414613.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB('2021-12-01', INTERVAL 23 MONTH)) and FORMAT_DATE('%Y%m%d','2021-12-01')
        union all
        /*** community.ato.gov.au ***/
        select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                hits.page.hostname as hostname,
                'community.ato.gov.au' as ga_service,
                'UA-72006902-2' as ga_id,
                -- User Type (dimension)
                -- CASE
                --   WHEN totals.newVisits = 1 THEN 'new visitor'
                -- ELSE 
                --     'returning visitor'
                -- END AS user_type,
                type           
            from
              `139405429.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB('2021-12-01', INTERVAL 23 MONTH)) and FORMAT_DATE('%Y%m%d','2021-12-01')
        union all
        /*** onlineservices.ato.gov.au ***/
        select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                hits.page.hostname as hostname,
                'onlineservices.ato.gov.au' as ga_service,
                'UA-76958125-2' as ga_id,
                -- User Type (dimension)
                -- CASE
                --   WHEN totals.newVisits = 1 THEN 'new visitor'
                -- ELSE 
                --     'returning visitor'
                -- END AS user_type,
                type           
            from
              `121638199.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB('2021-12-01', INTERVAL 23 MONTH)) and FORMAT_DATE('%Y%m%d','2021-12-01')
        union all
        /*** ato.gov.au ***/
        select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                hits.page.hostname as hostname,
                'ato.gov.au' as ga_service,
                'UA-72006902-1' as ga_id,
                -- User Type (dimension)
                -- CASE
                --   WHEN totals.newVisits = 1 THEN 'new visitor'
                -- ELSE 
                --     'returning visitor'
                -- END AS user_type,
                type           
            from
              `114274207.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB('2021-12-01', INTERVAL 23 MONTH)) and FORMAT_DATE('%Y%m%d','2021-12-01')
        union all
        /*** humanservices.gov.au new active ***/
        select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                hits.page.hostname as hostname,
                'humanservices.gov.au' as ga_service,
                'UA-24200153-1' as ga_id,
                -- User Type (dimension)
                -- CASE
                --   WHEN totals.newVisits = 1 THEN 'new visitor'
                -- ELSE 
                --     'returning visitor'
                -- END AS user_type,
                type           
            from
              `47586269.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB('2021-12-01', INTERVAL 23 MONTH)) and FORMAT_DATE('%Y%m%d','2021-12-01')
        union all
        /*** humanservices.gov.au old deactivated ***/
        select
                fullVisitorId,
                visitStartTime,
                totals.hits as hit_count,
                hits.page.hostname as hostname,
                'humanservices.gov.au old' as ga_service,
                'UA-2785808-1' as ga_id,
                -- User Type (dimension)
                -- CASE
                --   WHEN totals.newVisits = 1 THEN 'new visitor'
                -- ELSE 
                --     'returning visitor'
                -- END AS user_type,
                type           
            from
              `5289745.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits
            where regexp_contains( hits.page.hostname, "^.*.gov.au$") = TRUE
            and    type = 'PAGE'
            and   totals.visits = 1
            and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB('2021-12-01', INTERVAL 23 MONTH)) and FORMAT_DATE('%Y%m%d','2021-12-01')
    )
    group by  month_year, ga_service, ga_id
    -- reg_domain,
    ;