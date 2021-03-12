    /*
      apigovau - executive basics
      BigQuery script delivering following outputs for analytics prototype tool
      Basics for executive - Quarterly
      -Pageviews
      -Sessions
      -New users
      -Bounce rate
      -Time on page
      -Returning users
      -Top 10 top pages
      -Top 10 pages with highest growth
    */
    BEGIN
      create temp table t_exec_basics_prototype_qtr_1
      as
       select
          reg_domain,
          newUsers,
          returningUsers,
          FORMAT_DATE('%d%m%Y',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) as qtr_start,
          FORMAT_DATE('%d%m%Y',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as qtr_end
        from
          (
            select
              reg_domain,
              SUM(newUsers) AS newUsers,
              SUM(returningUsers) AS returningUsers
            from
            (
              select
              fullVisitorId,
              coalesce(net.reg_domain(hostname),'') as reg_domain,
              newUsers,
              returningUsers
            from
            (
        /* Start - Datasets of DTA websites */
                  select
                      fullVisitorId,
                      visitStartTime,
                      hits.page.hostname as hostname,
                      case when totals.newVisits=1 then 1 else 0 end as newUsers,
                      case when totals.newVisits is null then 1 else 0 end as returningUsers
                    from
                      `185106319.ga_sessions_*` AS GA,
                      UNNEST(GA.hits) AS hits 
                    WHERE
                      type = 'PAGE'
                      and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
            )
          )
          group by  reg_domain
          )
        ; 	


      create temp table t_exec_basics_prototype_qtr_2
      as
       select
          reg_domain,
          unique_visitors,
          pageviews,
          avg_time_on_page as time_on_page,
          CASE
            WHEN sessions = 0 THEN 0
            ELSE bounces / sessions
          END AS bounce_rate,
          bounces,
          sessions,
          case when unique_visitors > 0 then sessions/unique_visitors 
          else 0
          end as aveSession,
          case when sessions > 0 then pageviews/sessions
          else 0
          end as pagesPerSession,
           case when sessions > 0 then total_time_on_page/sessions
          else 0
          end as aveSessionDuration,
          FORMAT_DATE('%d%m%Y',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) as quarter_start,
          FORMAT_DATE('%d%m%Y',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) as quarter_end
        from
          (
            select
              reg_domain,
              COUNT(distinct fullVisitorId) as unique_visitors,
              count(*) as pageviews,
              sum(time_on_page) as total_time_on_page,
              avg(time_on_page) as avg_time_on_page,
              SUM(bounces) AS bounces,
              SUM(sessions) AS sessions
            from
            (
              select
              fullVisitorId,
              visitStartTime,
              hit_time,
              isExit,
              case
                when isExit is not null then last_interaction - hit_time
                else next_pageview - hit_time
              end as time_on_page,
              coalesce(net.reg_domain(hostname),'') as reg_domain,
              bounces,
              sessions
            from 
            (
              select
              fullVisitorId,
              visitStartTime,
              hostname,
              hit_time,
              isExit,
              last_interaction,
              lead(hit_time) over (partition by fullVisitorId, visitStartTime order by hit_time) as next_pageview,
              bounces,
              sessions
              from
              (
                select
                  fullVisitorId,
                  visitStartTime,
                  hostname,
                  hit_time,
                  isExit,
                  last_interaction,
                  CASE
                  WHEN hitNumber = first_interaction THEN bounces
                  ELSE 0
                  END AS bounces,
                  CASE
                  WHEN hitNumber = first_hit THEN visits
                  ELSE 0
                  END AS sessions
                from
                  (
        /* Start - Datasets of DTA websites */
                  select
                      fullVisitorId,
                      visitStartTime,
                      hits.page.hostname as hostname,
                      hits.hitNumber,
                      totals.bounces,
                      totals.visits,
                      coalesce(cast(hits.isExit as string),"") as isExit,
                      hits.time/1000 as hit_time,
                      max( if( hits.isInteraction is not null, hits.time/1000, 0 ) ) over
                      (partition by fullVisitorId, visitStartTime) as last_interaction,
                      MIN(IF(hits.isInteraction IS NOT NULL,
                        hits.hitNumber,0)) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_interaction,
                      MIN(hits.hitNumber) OVER (PARTITION BY fullVisitorId, visitStartTime) AS first_hit
                    from
                      `185106319.ga_sessions_*` AS GA,
                      UNNEST(GA.hits) AS hits 
                    WHERE
                      type = 'PAGE'
                      and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
          )
              )))
          group by  reg_domain
          )
        ; 	

create temp table t_prototype_qtr_pagetitle_top10
as
    select
      reg_domain,
      pagePath,
      pagetitle,
      count(*) as pageviews
    from
    (
      select
      pagePath,
      pagetitle,
      coalesce(net.reg_domain(hostname),'') as reg_domain
     from 
     (
      select
      hostname,
      pagePath,
      pagetitle
      from
      (
        select
          fullVisitorId,
          visitStartTime,
          hostname,
          pagePath,
          pagetitle
        from
          (
/* Start - Datasets of DTA websites */
           select
              fullVisitorId,
              visitStartTime,
              hits.page.hostname as hostname,
              hits.page.pagePath,
              hits.page.pagetitle
            from
              `185106319.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits 
            WHERE
              type = 'PAGE'
               and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) and FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
            )
        )
        )
    )
    group by  pagepath ,pagetitle, reg_domain;


create temp table t_prototype_preqtr_pagetitle_top10
as
    select
      reg_domain,
      pagePath,
      pagetitle,
      count(*) as pageviews
    from
    (
      select
      pagePath,
      pagetitle,
      coalesce(net.reg_domain(hostname),'') as reg_domain
     from 
     (
      select
      hostname,
      pagePath,
      pagetitle
      from
      (
        select
          fullVisitorId,
          visitStartTime,
          hostname,
          pagePath,
          pagetitle
        from
          (
/* Start - Datasets of DTA websites */
           select
              fullVisitorId,
              visitStartTime,
              hits.page.hostname as hostname,
              hits.page.pagePath,
              hits.page.pagetitle
            from
              `185106319.ga_sessions_*` AS GA,
              UNNEST(GA.hits) AS hits 
            WHERE
              type = 'PAGE'
            and _table_suffix between FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY)) and FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 91 DAY))
            )
        )
        )
    )
    group by  pagepath ,pagetitle, reg_domain;


create or replace table dta_customers.exec_basics_prototype_quarterly_apigovau
          OPTIONS (
            description = "Quarterly executive basics dimensions"
  )
    as
        select
              exec_1.reg_domain as hostname,
              unique_visitors as users,
              newUsers,
              returningUsers,
              exec_2.pageviews,
              time_on_page,
              bounce_rate*100 as bounce_rate,
              sessions,
              aveSession,
              pagesPerSession ,
              aveSessionDuration,
              tpgs.pageviews as pageviews_toprank,
              tpgs.pagetitle,
              tpgs.pagepath as pageurl,
              exec_1.qtr_start,
              exec_1.qtr_end
        from 
          t_exec_basics_prototype_qtr_1 exec_1
          inner join t_exec_basics_prototype_qtr_2 exec_2
            on exec_2.reg_domain = exec_1.reg_domain  
          inner join t_prototype_qtr_pagetitle_top10 tpgs
            on tpgs.reg_domain = exec_1.reg_domain
        order by pageviews_toprank desc, users desc
;

END;
    