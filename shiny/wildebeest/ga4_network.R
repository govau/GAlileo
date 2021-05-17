# network theory in R

raw_data <- read.csv('~/Documents/network_diagram_ga4/nodes_data.csv')

library(stringr)
# clean up a bit
raw_data$from_hostname <- gsub( "www.", "", raw_data$from_hostname, ignore.case = T)
raw_data$from_hostname <- gsub( "m.facebook.com", "facebook.com", raw_data$from_hostname, ignore.case = T)
raw_data$from_hostname <- gsub( "l.facebook.com", "facebook.com", raw_data$from_hostname, ignore.case = T)
raw_data$from_hostname <- gsub( "lm.facebook.com", "facebook.com", raw_data$from_hostname, ignore.case = T)
raw_data$from_hostname <- gsub( "pinterest", "pinterest.com.mx", raw_data$from_hostname, ignore.case = T)



library(shiny)
library(networkD3)
library(DT)
library(dplyr)

# Define UI ----
# User interface ----
ui <- shinyUI(fluidPage(
  titlePanel(h1("DTA")),
  
  sidebarLayout(
    position = "left",
    sidebarPanel(width = 2,
                 h2("Controls"),
                 #selectInput("var",
                 #            label = "Choose a user journey",
                 #            choices = c("all journeys", "carers journey",
                 #                        "start a business journey", "enrol in uni journey"),
                 #            selected = "all journeys"),
                 sliderInput("opacity", "Node Opacity", 0.6, min = 0.1,
                             max = 1, step = .1),
                 sliderInput("events", "minimum events", 1500, min = 0,
                             max = 20000, step = 50)
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Network Graph", simpleNetworkOutput("simple"), width = 13),
        tabPanel("Dyad table", DT::dataTableOutput("table")),
        tabPanel("Referring domain user count", DT::dataTableOutput("refer")),
        tabPanel("Receiving domain user count", DT::dataTableOutput("receive"))
      )
    ))))



# Define server logic ----
server <-
  function(input, output) {
    networkData <- reactive(raw_data %>%
                              filter(events > input$events) %>%
                              select(from_hostname, to_hostname))
    refer_domain <- raw_data %>%
      group_by(from_hostname) %>%
      summarise(total = sum(events)) %>%
      arrange(desc(total))
    
    receive_domain <- raw_data %>%
      group_by(to_hostname) %>%
      summarise(total = sum(events)) %>%
      arrange(desc(total))
    
    output$simple <- renderSimpleNetwork({
      simpleNetwork(networkData(), opacity = input$opacity, zoom = T)
    })
    output$table <- DT::renderDataTable({
      DT::datatable(raw_data[,c("from_hostname", "to_hostname", "events")],
                    options = list(lengthMenu = c(10, 25, 50, 100), pageLength = 10))
    })
    output$refer <- DT::renderDataTable({
      DT::datatable(receive_domain,
                    options = list(lengthMenu = c(10, 25, 50, 100), pageLength = 10))
    })
    output$receive <- DT::renderDataTable({
      DT::datatable(refer_domain,
                    options = list(lengthMenu = c(10, 25, 50, 100), pageLength = 10))
    })
  }

# Run the app ----
app <- shinyApp(ui = ui, server = server)
if (Sys.getenv("PORT") != "") {
  runApp(app, host = "0.0.0.0", port = strtoi(Sys.getenv("PORT")))
} 
runApp(app)
