library(shiny)
library(networkD3)
# Define UI ----
# User interface ----
ui <- shinyUI(fluidPage(
  titlePanel("Gov.AU Map"),
  
  sidebarLayout(
    position = "left",
    sidebarPanel(
      h2("Controls"),
      selectInput("var", 
                  label = "Choose a user journey",
                  choices = c("all journeys", "carers journey",
                              "start a business journey", "enrol in uni journey"),
                  selected = "all journeys"),
      sliderInput("opacity", "Node Opacity", 0.6, min = 0.1,
                  max = 1, step = .1),
      sliderInput("user", "minimum user", 1500, min = 0,
                  max = 10000, step = 100)
    ),
    mainPanel(
      h1("Network Graphs"),
      mainPanel("Simple Network", simpleNetworkOutput("simple"), width = 12)
        )
  )))
      


sna_data <- read.csv("bq_data.csv", stringsAsFactors = F)
sna_data$from_url <- gsub( "www.", "", sna_data$from_url, ignore.case = T)
sna_data$from_url <- gsub( "m.facebook.com", "facebook.com", sna_data$from_url, ignore.case = T)
sna_data$from_url <- gsub( "l.facebook.com", "facebook.com", sna_data$from_url, ignore.case = T)
sna_data$from_url <- gsub( "lm.facebook.com", "facebook.com", sna_data$from_url, ignore.case = T)
sna_data$to_url <- gsub( "www.", "", sna_data$to_url, ignore.case = T)
#filter by user jouney

# Define server logic ----
server <- 
  function(input, output) {
    networkData <- reactive( subset(sna_data, sna_data$count > input$user, c('from_hostname', 'to_hostname')))

    output$simple <- renderSimpleNetwork({
      simpleNetwork(networkData(), opacity = input$opacity, zoom = T)
      })
}


# Run the app ----
app <- shinyApp(ui = ui, server = server)
runApp(app, host="0.0.0.0", port=strtoi(Sys.getenv("PORT")))
  






