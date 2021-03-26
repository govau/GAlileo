# Collecting data for the CPP analysis
# we want to look at the past 90 days and averages across all the different government domains that we have the datasets for


select
    visit_date
    total_visitors,
    unique_visitors,
    total_new_users,
    percentage_new_visitors,
    #total_sessions,
    total_pageviews,
    cast(avg_time_on_page as numeric) as avg_time_on_page,
    total_bounces,
    # traffic mediums
    traffic_medium_referral,
    traffic_medium_organic,
    traffic_medium_direct,
    traffic_medium_cpm,
    traffic_medium_email,
    total_sessions - traffic_medium_referral - traffic_medium_organic - traffic_medium_direct - traffic_medium_cpm - traffic_medium_email as traffic_medium_other,
    total_gov_referral as traffic_medium_gov_referral,
    total_other_gov_referral as traffic_medium_other_gov_referral,
    # take total time on page and divide by total number of that type of traffic source
    case when traffic_medium_referral <> 0 then t_o_p_referral/traffic_medium_referral else 0 end as avg_t_o_p_referral,
    case when total_gov_referral <> 0 then t_o_p_gov_referral/total_gov_referral else 0 end as avg_t_o_p_gov_referral,
    case when total_other_gov_referral <> 0 then t_o_p_other_gov_referral/total_other_gov_referral else 0 end as avg_t_o_p_other_gov_referral,
    case when traffic_medium_organic <> 0 then t_o_p_organic/traffic_medium_organic else 0 end as avg_t_o_p_organic,
    case when traffic_medium_direct <> 0 then t_o_p_direct/traffic_medium_direct else 0 end as avg_t_o_p_direct,
    # take total of pageviews and divide by total number of that type of traffic to find average pages per session
    case when traffic_medium_referral <> 0 then pageviews_referral/traffic_medium_referral else 0 end as pageviews_per_session_referral,
    case when total_gov_referral <> 0 then pageviews_gov_referral/total_gov_referral else 0 end as pageviews_per_session_gov_referral,
    case when total_other_gov_referral <> 0 then pageviews_other_gov_referral/total_other_gov_referral else 0 end as pageviews_per_session_other_gov_referral,
    case when traffic_medium_organic <> 0 then pageviews_organics/traffic_medium_organic else 0 end as pageviews_per_session_organic,
    case when traffic_medium_direct <> 0 then pageviews_direct/traffic_medium_direct else 0 end as pageviews_per_session_direct,
    # bounce rate
    case when traffic_medium_referral <> 0 then bounces_referral/traffic_medium_referral else 0 end as bounce_r_referral,
    case when total_gov_referral <> 0 then bounces_gov_referral/total_gov_referral  else 0 end as bounce_r_gov_referral,
    case when total_other_gov_referral <> 0 then bounces_other_gov_referral/total_other_gov_referral else 0 end as bounce_r_other_gov_referral,
    case when traffic_medium_organic <> 0 then bounces_organic/traffic_medium_organic else 0 end as bounce_r_organic,
    case when traffic_medium_direct <> 0 then bounces_direct/traffic_medium_direct else 0 end as bounce_r_direct
    #cast(current_timestamp as date) as t_date
from
    (
    select
        COUNT(fullVisitorId) as total_visitors,
        COUNT(DISTINCT fullVisitorId) as unique_visitors,
        SUM(newUsers)/(SUM(returningUsers)+SUM(newUsers)) as percentage_new_visitors,
        SUM(newUsers) as total_new_users,
        COUNT(visitId) as total_sessions,
        sum(pageviews) as total_pageviews,
        sum(traffic_medium_referral) as traffic_medium_referral,
        sum(traffic_medium_organic) as traffic_medium_organic,
        sum(traffic_medium_direct) as traffic_medium_direct,
        sum(traffic_medium_cpm) as traffic_medium_cpm,
        sum(traffic_medium_email) as traffic_medium_email,
        sum(is_gov_referral) as total_gov_referral,
        sum(is_state_local_gov_referral) as total_other_gov_referral,
        sum(bounces) as total_bounces,
        sum(t_o_p_referral) as t_o_p_referral,
        sum(t_o_p_gov_referral) as t_o_p_gov_referral,
        sum(t_o_p_other_gov_referral) as t_o_p_other_gov_referral,
        sum(t_o_p_organic) as t_o_p_organic,
        sum(t_o_p_direct) as t_o_p_direct,
        sum(pageviews_referral) as pageviews_referral,
        sum(pageviews_gov_referral) as pageviews_gov_referral,
        sum(pageviews_other_gov_referral) as pageviews_other_gov_referral,
        sum(pageviews_organics) as pageviews_organics,
        sum(pageviews_direct) as pageviews_direct,
        sum(bounces_referral) as bounces_referral,
        sum(bounces_gov_referral) as bounces_gov_referral,
        sum(bounces_other_gov_referral) as bounces_other_gov_referral,
        sum(bounces_organic) as bounces_organic,
        sum(bounces_direct) as bounces_direct,
        visit_date,
        # time on page
        avg(time_on_page) as avg_time_on_page
        from
            (
            select
                fullVisitorId,
                time_on_page,
                visitId,
                pageviews,
                traffic_source,
                newUsers,
                returningUsers,
                pagePath,
                medium,
                traffic_medium_referral,
                traffic_medium_organic,
                traffic_medium_direct,
                traffic_medium_cpm,
                traffic_medium_email,
                is_gov_referral,
                is_state_local_gov_referral,
                bounces,
                visit_date,
                # average time on pages for referrals
                case when medium = 'referral' then time_on_page else 0 end as t_o_p_referral,
                case when medium = 'referral' and regexp_contains(traffic_source, "^.*.gov.au$") = TRUE then time_on_page else 0 end as t_o_p_gov_referral,
                case when medium = 'referral' and 
                    regexp_contains(traffic_source, "^.*.(nsw.gov.au)|(vic.gov.au)|(qld.gov.au)|(tas.gov.au)|(sa.gov.au)|(wa.gov.au)|(nt.gov.au)|(act.gov.au)$") = TRUE 
                    then time_on_page else 0 end as t_o_p_other_gov_referral,
                case when medium = 'organic' then time_on_page else 0 end as t_o_p_organic,
                case when medium = 'direct' then time_on_page else 0 end as t_o_p_direct,
                # total pageviews for different referrals
                case when medium = 'referral' then pageviews else 0 end as pageviews_referral,
                case when medium = 'referral' and regexp_contains(traffic_source, "^.*.gov.au$") = TRUE then pageviews else 0 end as pageviews_gov_referral,
                case when medium = 'referral' and 
                    regexp_contains(traffic_source, "^.*.(nsw.gov.au)|(vic.gov.au)|(qld.gov.au)|(tas.gov.au)|(sa.gov.au)|(wa.gov.au)|(nt.gov.au)|(act.gov.au)$") = TRUE 
                    then pageviews else 0 end as pageviews_other_gov_referral,
                case when medium = 'organic' then pageviews else 0 end as pageviews_organics,
                case when medium = 'direct' then pageviews else 0 end as pageviews_direct,
                # bounce rate
                case when medium = 'referral' then bounces else 0 end as bounces_referral,
                case when medium = 'referral' and regexp_contains(traffic_source, "^.*.gov.au$") = TRUE then bounces else 0 end as bounces_gov_referral,
                case when medium = 'referral' and 
                    regexp_contains(traffic_source, "^.*.(nsw.gov.au)|(vic.gov.au)|(qld.gov.au)|(tas.gov.au)|(sa.gov.au)|(wa.gov.au)|(nt.gov.au)|(act.gov.au)$") = TRUE 
                    then bounces else 0 end as bounces_other_gov_referral,
                case when medium = 'organic' then bounces else 0 end as bounces_organic,
                case when medium = 'direct' then bounces else 0 end as bounces_direct,
            from (

            select
                fullVisitorId,
                visitId,
                pageviews,
                traffic_source,
                case when newVisits=1 then 1 else 0 end as newUsers,
                case when newVisits is null then 1 else 0 end as returningUsers,
                pagePath,
                # traffic mediums
                medium,
                case when medium = 'referral' then 1 else 0 end as traffic_medium_referral,
                case when medium = 'organic' then 1 else 0 end as traffic_medium_organic,
                case when medium = '(none)' then 1 else 0 end as traffic_medium_direct,
                case when medium = 'cpm' then 1 else 0 end as traffic_medium_cpm,
                case when medium = 'email' then 1 else 0 end as traffic_medium_email,
                case when medium = 'referral' and regexp_contains(traffic_source, "^.*.gov.au$") = TRUE then 1 else 0 end as is_gov_referral,
                case when medium = 'referral' and 
                    regexp_contains(traffic_source, "^.*.(nsw.gov.au)|(vic.gov.au)|(qld.gov.au)|(tas.gov.au)|(sa.gov.au)|(wa.gov.au)|(nt.gov.au)|(act.gov.au)$") = TRUE 
                    then 1 else 0 end as is_state_local_gov_referral,

                visit_date,
                hit_time,
                type,
                isExit,
                bounces,
                case
                    when isExit is not null then last_interaction - hit_time
                    else next_pageview - hit_time
                end as time_on_page
            from 
                (
                select
                    fullVisitorId,
                    visitId,
                    pageviews,
                    newVisits,
                    pagePath,
                    medium,
                    traffic_source,
                    visit_date,
                    hit_time,
                    type,
                    isExit,
                    bounces,
                    last_interaction,
                    lead(hit_time) over (partition by fullVisitorId, visitStartTime order by hit_time) as next_pageview
                from
                    (
                    select
                        fullVisitorId,
                        visitId,
                        pageviews,
                        newVisits,
                        hostname,
                        pagePath,
                        medium,
                        traffic_source,
                        visit_date,
                        hit_time,
                        type,
                        isExit,
                        bounces,
                        last_interaction,
                        visitStartTime
                        from
                        (
    /* Start - Datasets of employment websites
        Insert Here Google Analytics Dataset of Websites of Interest and 'Union All' query result sets to get final result set
    */
                        select
                                # the domain is myato
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `135414613.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is ABRWeb
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `178007804.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is govcms
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `134969186.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is ga.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `80842702.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is abf.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `177476111.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is abs.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `73191096.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is api.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `185106319.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is ato.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `114274207.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is dss.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `77084214.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is igt.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `212190958.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is myGov_beta
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `218817760.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is nla.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `2802109.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is rba.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `191126238.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is tga.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `129200625.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is abcc.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `6533313.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is afsa.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `75255162.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is army.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `122418128.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is asic.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `39020822.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is atrc.com.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `89766970.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is immi.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `100095166.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is FWBC On Site
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `115980641.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is Style Manual
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `225103137.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is govdex.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `77664740.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is health.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `169499927.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is defence.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `5426088.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is ebs.tga.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `88992271.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is mychild.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `100180008.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.aqf.edu.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `149444086.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.asd.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `121386494.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.dta.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `99993137.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.fsc.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `174497994.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is Career Pathways
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `222282547.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is [STRUCT(dta, )]
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `99993137.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is airforce.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `122829809.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is beta.abs.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `186366587.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is industry.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `175671120.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.ihpa.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `82020118.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is Australia.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `71597546.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is data.wgea.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `94241432.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is jobaccess.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `104411629.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is jobsearch.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `72008433.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is scamwatch.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `103904192.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is trove.nla.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `199921542.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is cd.defence.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `178909235.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is domainname.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `169220999.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is engage.dss.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `90974611.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is guides.dss.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `85844330.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is joboutlook.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `86630641.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is moneysmart.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `37548566.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.asbfeo.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `118336527.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is Homeaffairs.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `100095673.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is m.directory.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `70856817.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is abr.business.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `94174429.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is business.tga.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `98349897.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is designsystem.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `170387771.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is news.defence.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `135989789.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.business.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `133849100.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is banknotes.rba.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `203109603.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is betterschools.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `63623150.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is catologue.nla.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `6592309.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is humanservices.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `5289745.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is video defence.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `122841309.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.education.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `77562775.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is ablis.business.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `78700159.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is docs.education.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `77559172.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is webarchive.nla.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `70635257.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.employment.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `77614012.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is [STRUCT(agency, artc)]
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `225642503.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is data.gov.au - all data
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `69211100.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is defenceindustry.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `162370350.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is osb.homeaffairs.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `110162521.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is superfundlookup.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `94178846.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.studyassist.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `53678167.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.tisnational.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `74070468.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is immi.homeaffairs.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `177457874.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is minister.defence.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `6059849.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.jobjumpstart.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `111564569.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is safeworkaustralia.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `179394289.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.studentsfirst.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `80703744.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is https://www.idpwd.com.au/
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `34154705.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is onlineservices.ato.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `121638199.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is aeaguide.education.gov.au/
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `79438793.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is covid19.homeaffairs.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `214546690.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is eduportal.education.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `117865571.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is familyrelationships.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `34938005.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is innovation.govspace.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `69522323.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is marketplace.service.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `130142010.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is intercountryadoption.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `100832347.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is minister.homeaffairs.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `116763821.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is whatsnext.employment.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `100585217.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is business.dmz.test.tga.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `98362688.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is consultation.business.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `48099294.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.learningpotential.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `106413345.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is librariesaustralia.nla.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `73966990.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is australianjobs.employment.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `124827135.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is http://www.companioncard.gov.au/
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `31265425.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is [STRUCT(agency, inland_rail_map)]
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `186233756.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is business.dmz.development.tga.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `98360372.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is communitybusinesspartnership.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `95014024.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is disabilityadvocacyfinder.dss.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `86149663.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is https://formerministers.dss.gov.au/
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `53715324.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is https://serviceproviders.dss.gov.au/
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `101163468.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is https://plan4womenssafety.dss.gov.au/
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `104395490.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.tradesrecognitionaustralia.gov.au
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `175869519.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is www.artc.com.au\nAustralian Rail Track Corporation
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `225642503.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is maps.inlandrail.com.au/b2g-dec-2018#/\ninland rail map
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `186233756.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                                union all
                        select
                                # the domain is covid19inlanguage.homeaffairs.gov.au (UA-61305954-25) – (View ID: 215803896)
                                fullVisitorId,
                                visitId,
                                totals.pageviews as pageviews,
                                visitStartTime,
                                totals.newVisits as newVisits,
                                hits.page.hostname as hostname,
                                hits.page.pagePath,
                                trafficSource.medium as medium,
                                trafficSource.source as traffic_source,
                                date(timestamp_seconds(visitStartTime), 'Australia/Sydney') as visit_date,
                                hits.type,
                                coalesce(cast(hits.isExit as string),"") as isExit,
                                hits.time/1000 as hit_time,
                                totals.bounces as bounces,
                                max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                                (partition by fullVisitorId, visitStartTime) as last_interaction
                                from
                                `215803896.ga_sessions_*` AS GA,
                                UNNEST(GA.hits) AS hits
                                where type = 'PAGE'
                                    and regexp_contains(hits.page.hostname,".*.gov.au") = true
                                    and totals.visits = 1
                                    and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',CURRENT_DATE())
                        )
                    )    
                )
            )
        )
    GROUP BY visit_date

    # , total_visitors, unique_visitors, total_new_users, percentage_new_visitors, total_sessions, total_pageviews, total_time_on_page, traffic_medium 
    )
    order by visit_date
    ;