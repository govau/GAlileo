-- noinspection SqlNoDataSourceInspectionForFile
SELECT
    date,
    fullVisitorId,
    visitId,
    time as time_in_session,
    hitNumber,
    type as hitType,
    isEntrance,
    isExit,
    CONCAT(trafficSource.source,
    IF         (trafficSource.medium = "referral",
    trafficSource.referralPath,
    '') ) as from_url,
    page.pagePath
FROM
    `dta-ga-bigquery.177457874.ga_sessions_*`,
    unnest(hits)
where
    _TABLE_SUFFIX BETWEEN '20191001' AND '20191010'
    AND       trafficSource.source = 'australia.gov.au'
order by fullVisitorId, visitId, hitNumber;