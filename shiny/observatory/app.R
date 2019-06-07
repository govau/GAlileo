library(shiny)
library(r2d3)

# Define UI ----
# User interface ----
source("shinytester.R");
source("painpoints.R");
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
  main = navbarPage("", userJourneys, shinyTester, painPoints)
  )
}

# Define server logic to summarize and view selected dataset ----
server <- function(input, output) {
  shinytester_server(input, output);
  painpoints_server(input, output);
  userjourneys_server(input, output);
}

shinyApp(ui = ui, server = server)
