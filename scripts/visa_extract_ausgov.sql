-- noinspection SqlNoDataSourceInspectionForFile
SELECT
    date,
    fullVisitorId,
    visitId,
    time,
    hitNumber,
    type as hitType,
    isEntrance,
    isExit,
    referer,
    page.pagePath,
    eventInfo.eventLabel as outboundLinkURL
FROM
    `dta-ga-bigquery.71597546.ga_sessions_*`,
    unnest(hits)
where
    fullVisitorId in (
        select
            fullVisitorId
        from
            `dta-ga-bigquery.71597546.ga_sessions_*`,
            unnest(hits)
        where
            page.pagePath like '%visa%'
            AND _TABLE_SUFFIX BETWEEN '20191001' AND '20191010'
    )
    and (
        eventInfo.eventCategory is null
        or eventInfo.eventCategory='Outbound links'
    )
    AND
    _TABLE_SUFFIX BETWEEN '20191001' AND '20191010'
order by fullVisitorId, visitId, hitNumber;