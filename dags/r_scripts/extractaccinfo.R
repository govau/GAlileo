library(tidyverse)
library(googleAuthR)
library(googleAnalyticsR)

#setting permissions to be able to use the google api
options(googleAuthR.scopes.selected = "https://www.googleapis.com/auth/analytics.readonly")
gar_auth_service("credentials.json")

gg <- ga_account_list() %>%
  select(accountId, internalWebPropertyId,websiteUrl, webPropertyId, level, type, viewId, viewName) %>%
  filter(level=="PREMIUM" & type == "WEB") %>%
  distinct(webPropertyId, .keep_all = TRUE)

name <- paste0("GA360-", format(Sys.time(), '%d%b%Y'), ".csv")
write.csv(gg,name)
