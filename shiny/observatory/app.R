library(shiny)
library(dplyr)
library(magrittr)
library(ggplot2)
library(lubridate)
library(stringr)
library(readxl)
library(igraph)
library(networkD3)
library(lazyeval)
library(r2d3)

# Define UI ----
# User interface ----
source("agencycomparison.R");
source("siteuserjourneys.R");
source("userjourneys.R");
ui <- function() {
  suppressDependencies("bootstrap");
  htmlTemplate(
  "template.html",
  # Application title
  title = "Observatory",
  subline = "To quantify interactions with every government service",
  # https://support.dominodatalab.com/hc/en-us/articles/360015932932-Increasing-the-timeout-for-Shiny-Server
  keepalive= textOutput("keepAlive"),
  main = navbarPage("", siteUserJourneys, agencyComparison, userJourneys)
  )
}

# Define server logic to summarize and view selected dataset ----
server <- function(input, output) {
  agencycomparison_server(input, output);
  userjourneys_server(input, output);
  siteuserjourneys_server(input, output);

  output$keepAlive <- renderText({
    req(input$count)
    paste("keep alive ", input$count)
  })
}

shinyApp(ui = ui, server = server)
