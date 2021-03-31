# Collecting data for the CPP analysis
# 


select
    domain,
    total_visitors,
    unique_visitors,
    total_new_users,
    percentage_new_visitors,
    #total_sessions,
    total_pageviews,
    total_pageviews/total_sessions as pages_per_session,
    cast(avg_time_on_page as numeric) as avg_time_on_page,
    total_bounces,
    # traffic mediums
    traffic_medium_referral,
    traffic_medium_organic,
    traffic_medium_direct,
    traffic_medium_cpm,
    traffic_medium_email,
    total_sessions - traffic_medium_referral - traffic_medium_organic - traffic_medium_direct - traffic_medium_cpm - traffic_medium_email as traffic_medium_other,
    total_gov_referral as traffic_medium_all_gov_referral,
    total_fed_gov_referral as traffic_medium_fed_gov_referral,
    total_other_gov_referral as traffic_medium_other_gov_referral,
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
        sum(is_fed_gov_referral) as total_fed_gov_referral,
        sum(is_state_local_gov_referral) as total_other_gov_referral,
        sum(bounces) as total_bounces,
        domain,
        # time on page
        avg(time_on_page) as avg_time_on_page
        from
            (

            select
                fullVisitorId,
                visitId,
                pageviews,
                traffic_source,
                case when newVisits=1 then 1 else 0 end as newUsers,
                case when newVisits is null then 1 else 0 end as returningUsers,
                pagePath,
                domain,
                # traffic mediums
                medium,
                case when medium = 'referral' then 1 else 0 end as traffic_medium_referral,
                case when medium = 'organic' then 1 else 0 end as traffic_medium_organic,
                case when medium = '(none)' then 1 else 0 end as traffic_medium_direct,
                case when medium = 'cpm' then 1 else 0 end as traffic_medium_cpm,
                case when medium = 'email' then 1 else 0 end as traffic_medium_email,
                case when medium = 'referral' and regexp_contains(traffic_source, "^.*.gov.au$") = TRUE then 1 else 0 end as is_gov_referral,
                case when medium = 'referral' and regexp_contains(traffic_source, "^.*.gov.au$") = TRUE and
                    regexp_contains(traffic_source, "^.*.(nsw.gov.au)|(vic.gov.au)|(qld.gov.au)|(tas.gov.au)|(sa.gov.au)|(wa.gov.au)|(nt.gov.au)|(act.gov.au)$") = FALSE
                    then 1 else 0 end as is_fed_gov_referral,
                case when medium = 'referral' and 
                    regexp_contains(traffic_source, "^.*.(nsw.gov.au)|(vic.gov.au)|(qld.gov.au)|(tas.gov.au)|(sa.gov.au)|(wa.gov.au)|(nt.gov.au)|(act.gov.au)$") = TRUE 
                    then 1 else 0 end as is_state_local_gov_referral,

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
                    domain,
                    pagePath,
                    medium,
                    traffic_source,
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
                        domain,
                        pagePath,
                        medium,
                        traffic_source,
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'myato' as domain
            from
               `135414613.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'ABRWeb' as domain
            from
               `178007804.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'govcms' as domain
            from
               `134969186.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'ga.gov.au' as domain
            from
               `80842702.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'abf.gov.au' as domain
            from
               `177476111.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'abs.gov.au' as domain
            from
               `73191096.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'api.gov.au' as domain
            from
               `185106319.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'ato.gov.au' as domain
            from
               `114274207.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'dss.gov.au' as domain
            from
               `77084214.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'igt.gov.au' as domain
            from
               `212190958.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'myGov_beta' as domain
            from
               `218817760.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'nla.gov.au' as domain
            from
               `2802109.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'rba.gov.au' as domain
            from
               `191126238.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'tga.gov.au' as domain
            from
               `129200625.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'abcc.gov.au' as domain
            from
               `6533313.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'afsa.gov.au' as domain
            from
               `75255162.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'army.gov.au' as domain
            from
               `122418128.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'asic.gov.au' as domain
            from
               `39020822.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'atrc.com.au' as domain
            from
               `89766970.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'immi.gov.au' as domain
            from
               `100095166.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'FWBC On Site' as domain
            from
               `115980641.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'Style Manual' as domain
            from
               `225103137.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'govdex.gov.au' as domain
            from
               `77664740.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'health.gov.au' as domain
            from
               `169499927.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'defence.gov.au' as domain
            from
               `5426088.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'ebs.tga.gov.au' as domain
            from
               `88992271.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'mychild.gov.au' as domain
            from
               `100180008.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.aqf.edu.au' as domain
            from
               `149444086.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.asd.gov.au' as domain
            from
               `121386494.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.dta.gov.au' as domain
            from
               `99993137.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.fsc.gov.au' as domain
            from
               `174497994.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'Career Pathways' as domain
            from
               `222282547.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            '[STRUCT(dta, )]' as domain
            from
               `99993137.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'airforce.gov.au' as domain
            from
               `122829809.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'beta.abs.gov.au' as domain
            from
               `186366587.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'industry.gov.au' as domain
            from
               `175671120.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.ihpa.gov.au' as domain
            from
               `82020118.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'Australia.gov.au' as domain
            from
               `71597546.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'data.wgea.gov.au' as domain
            from
               `94241432.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'jobaccess.gov.au' as domain
            from
               `104411629.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'jobsearch.gov.au' as domain
            from
               `72008433.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'scamwatch.gov.au' as domain
            from
               `103904192.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'trove.nla.gov.au' as domain
            from
               `199921542.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'cd.defence.gov.au' as domain
            from
               `178909235.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'domainname.gov.au' as domain
            from
               `169220999.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'engage.dss.gov.au' as domain
            from
               `90974611.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'guides.dss.gov.au' as domain
            from
               `85844330.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'joboutlook.gov.au' as domain
            from
               `86630641.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'moneysmart.gov.au' as domain
            from
               `37548566.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.asbfeo.gov.au' as domain
            from
               `118336527.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'Homeaffairs.gov.au' as domain
            from
               `100095673.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'm.directory.gov.au' as domain
            from
               `70856817.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'abr.business.gov.au' as domain
            from
               `94174429.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'business.tga.gov.au' as domain
            from
               `98349897.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'designsystem.gov.au' as domain
            from
               `170387771.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'news.defence.gov.au' as domain
            from
               `135989789.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.business.gov.au' as domain
            from
               `133849100.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'banknotes.rba.gov.au' as domain
            from
               `203109603.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'betterschools.gov.au' as domain
            from
               `63623150.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'catologue.nla.gov.au' as domain
            from
               `6592309.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'humanservices.gov.au' as domain
            from
               `5289745.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'video defence.gov.au' as domain
            from
               `122841309.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.education.gov.au' as domain
            from
               `77562775.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'ablis.business.gov.au' as domain
            from
               `78700159.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'docs.education.gov.au' as domain
            from
               `77559172.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'webarchive.nla.gov.au' as domain
            from
               `70635257.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.employment.gov.au' as domain
            from
               `77614012.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            '[STRUCT(agency, artc)]' as domain
            from
               `225642503.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'data.gov.au - all data' as domain
            from
               `69211100.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'defenceindustry.gov.au' as domain
            from
               `162370350.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'osb.homeaffairs.gov.au' as domain
            from
               `110162521.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'superfundlookup.gov.au' as domain
            from
               `94178846.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.studyassist.gov.au' as domain
            from
               `53678167.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.tisnational.gov.au' as domain
            from
               `74070468.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'immi.homeaffairs.gov.au' as domain
            from
               `177457874.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'minister.defence.gov.au' as domain
            from
               `6059849.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.jobjumpstart.gov.au' as domain
            from
               `111564569.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'safeworkaustralia.gov.au' as domain
            from
               `179394289.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.studentsfirst.gov.au' as domain
            from
               `80703744.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'https://www.idpwd.com.au/' as domain
            from
               `34154705.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'onlineservices.ato.gov.au' as domain
            from
               `121638199.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'aeaguide.education.gov.au/' as domain
            from
               `79438793.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'covid19.homeaffairs.gov.au' as domain
            from
               `214546690.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'eduportal.education.gov.au' as domain
            from
               `117865571.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'familyrelationships.gov.au' as domain
            from
               `34938005.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'innovation.govspace.gov.au' as domain
            from
               `69522323.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'marketplace.service.gov.au' as domain
            from
               `130142010.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'intercountryadoption.gov.au' as domain
            from
               `100832347.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'minister.homeaffairs.gov.au' as domain
            from
               `116763821.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'whatsnext.employment.gov.au' as domain
            from
               `100585217.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'business.dmz.test.tga.gov.au' as domain
            from
               `98362688.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'consultation.business.gov.au' as domain
            from
               `48099294.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.learningpotential.gov.au' as domain
            from
               `106413345.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'librariesaustralia.nla.gov.au' as domain
            from
               `73966990.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'australianjobs.employment.gov.au' as domain
            from
               `124827135.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'http://www.companioncard.gov.au/' as domain
            from
               `31265425.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            '[STRUCT(agency, inland_rail_map)]' as domain
            from
               `186233756.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'business.dmz.development.tga.gov.au' as domain
            from
               `98360372.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'communitybusinesspartnership.gov.au' as domain
            from
               `95014024.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'disabilityadvocacyfinder.dss.gov.au' as domain
            from
               `86149663.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'https://formerministers.dss.gov.au/' as domain
            from
               `53715324.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'https://serviceproviders.dss.gov.au/' as domain
            from
               `101163468.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'https://plan4womenssafety.dss.gov.au/' as domain
            from
               `104395490.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.tradesrecognitionaustralia.gov.au' as domain
            from
               `175869519.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'www.artc.com.au\nAustralian Rail Track Corporation' as domain
            from
               `225642503.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'maps.inlandrail.com.au/b2g-dec-2018#/\ninland rail map' as domain
            from
               `186233756.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
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
            (partition by fullVisitorId, visitStartTime) as last_interaction,
            'covid19inlanguage.homeaffairs.gov.au (UA-61305954-25) – (View ID: 215803896)' as domain
            from
               `215803896.ga_sessions_*` AS GA,
               UNNEST(GA.hits) AS hits
             where type = 'PAGE'
                 and regexp_contains(hits.page.hostname,".*.gov.au") = true
                 and totals.visits = 1
                 and _table_suffix between '20210101' and '20210331'
                 
            ))
        ))
    GROUP BY domain
    )
    order by domain
    ;