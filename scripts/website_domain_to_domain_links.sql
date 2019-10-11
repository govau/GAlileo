SELECT from_hostname, to_hostname, COUNT(*) as link_count FROM
  (SELECT
  REGEXP_REPLACE(hostname, r"www?\.","") AS from_hostname, # remove www.
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        replace(lower(NET.HOST(link)),",",".") # extract host, make all lower case and replace commas with periods
        , r"(www?\.|&quot;|%??)", "") # remove www. and html escaped special characters like &quot; and %20
      , r"\.gov\.au.*$",".gov.au") # remove anything after .gov.au at the end of the name ie where a starting / has been forgotten in the link URL
      ,r"[^a-z0-9\.\-]", "" # remove any other non-alphanumeric characters except periods and dashes
    ) AS to_hostname
  FROM `dta-ga-bigquery.web_crawl.url_resource` wc, UNNEST(wc.links) as link
  WHERE NOT REGEXP_CONTAINS(link, r"tel:|mailto:|javascript:") ) # remove non http: links
WHERE to_hostname NOT IN ( "mailto", "tel", "javascript", "http", "ascd35053", "ed%75c%61t%69on%2Ego%76.au", "twitter.com", "t.co", "linkedin.com", "facebook.com", "google.com", "localhost", "mailchi.mp", "gmail.com", "flickr.com", "instagram.com", "youtube.com", "reddit.com", "digg.com", "bit.ly", "goo.gl", "bigpond.com", "adobe.com", "itunes.apple.com", "apple.com", "m.me", "apple.news", "plus.google.com", "support.google.com", "code.google.com", "play.google.com", "maps.google.com", "eepurl.com", "blinklist.com", "sphinn.com", "tumblr.com", "stumbleupon.com", "reddit.com", "news.ycombinator.com", "reporter.es.msn.com", "posterous.com", "pinterest.com", "delicious.com", "del.icio.us", "myspace.com", "stumbleupon.com", "addthis.com", "feedity.com", "outlook.com", "addtoany.com", "au.linkedin.com", "linkedin.com", "feeds.feedburner.com","soundcloud.com","ning.com","static.ning.com","mp.weixin.qq.com") # remove social networks
  AND from_hostname not in ("ifp.mychild.gov.au", "printsandprintmaking.gov.au", "link.aiatsis.gov.au") # remove large database sites
  AND from_hostname != to_hostname # remove links that loop back to the same host
GROUP BY from_hostname, to_hostname
HAVING link_count > 1 # find links with more than 1 instance