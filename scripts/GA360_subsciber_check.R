library(tidyverse)
#Set your working directory
setwd("~/Documents/usage search/GA Subscriber usage")

# Importing datasets, you will need to download the raw data 
# from Google Marketing platform, organisation settings/usage
# ua_to_agency_mapping is a csv of Agency name and their associated range of UA id's
ua_to_agency_mapping <- read.csv("data/ua_to_agency_mapping.csv", stringsAsFactors = FALSE)
au_201810 <- read.csv("data/analytics_usage_201810.csv", stringsAsFactors = FALSE)
au_201811 <- read.csv("data/analytics_usage_201811.csv", stringsAsFactors = FALSE)
au_201812 <- read.csv("data/analytics_usage_201812.csv", stringsAsFactors = FALSE)
au_201901 <- read.csv("data/analytics_usage_201901.csv", stringsAsFactors = FALSE)
au_201902 <- read.csv("data/analytics_usage_201902.csv", stringsAsFactors = FALSE)
au_201903 <- read.csv("data/analytics_usage_201903.csv", stringsAsFactors = FALSE)
au_201904 <- read.csv("data/analytics_usage_201904.csv", stringsAsFactors = FALSE)
au_201905 <- read.csv("data/analytics_usage_201905.csv", stringsAsFactors = FALSE)
au_201906 <- read.csv("data/analytics_usage_201906.csv", stringsAsFactors = FALSE)
au_201907 <- read.csv("data/analytics_usage_201907.csv", stringsAsFactors = FALSE)
au_201908 <- read.csv("data/analytics_usage_201908.csv", stringsAsFactors = FALSE)
au_201909 <- read.csv("data/analytics_usage_201909.csv", stringsAsFactors = FALSE)

#Amalgamate datasets and selecting on billable hit volume
GaMerge <- as.data.frame(unique(au_201908$ID))
colnames(GaMerge) <- "ID"
GaMerge <- left_join(x = GaMerge,y = au_201810[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201811[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201812[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201901[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201902[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201903[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201904[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201905[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201906[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201907[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201908[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)
GaMerge <- left_join(x = GaMerge,y = au_201909[,c("ID", "Billable.Hit.Volume")] , by = "ID", all = TRUE)

#Assigning Column Names
colnames(GaMerge) <- c("ID", "Oct18","Nov18", "Dec18", "Jan19", "Feb19", "Mar19", "Apr19", "May19", "Jun19", "Jul19", "Aug19", "Sept19")

GaMerge$Type <- au_201908$Type
#GaMerge[is.na(GaMerge)] <- 0 
#Filtering by Premium only
GaFilt <- filter(GaMerge, Type == "Premium")
GaFilt[is.na(GaFilt)] <- 0 

GaFilt <- left_join(GaFilt, ua_to_agency_mapping, by = "ID")
#rearrange column names
GaFilt$agency_mean <- as.integer(rowMeans(GaFilt[,2:13]))
GaFilt$agency_sum <- as.numeric(rowSums(GaFilt[,2:13]))

write.csv(GaFilt, "201810_201909GAusage_check.csv")
output<-GaFilt %>% group_by(Agency) %>% summarise(Total_hits = sum(agency_sum), Mean_hits = sum(as.numeric(agency_mean)))
write.csv(output, "201810_201909GAusage_check.csv")
