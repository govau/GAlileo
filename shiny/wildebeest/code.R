library(shiny)
library(networkD3)
library(DT)
library(dplyr)

# Define UI ----
# User interface ----
ui <- shinyUI(fluidPage(
titlePanel(h1("Gov.AU Observatory")),
sidebarLayout(
position = "left",
sidebarPanel(width = 3,
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
tabsetPanel(
tabPanel("Network Graph", simpleNetworkOutput("simple"), width = 13),
tabPanel("Dyad table", DT::dataTableOutput("table")),
tabPanel("Referring domain user count", DT::dataTableOutput("refer")),
tabPanel("Receiving domain user count", DT::dataTableOutput("receive"))
)
))))



sna_data <- read.csv("bq_data.csv", stringsAsFactors = F)
sna_data$from_hostname <- gsub("www.", "", sna_data$from_hostname, ignore.case = T)
sna_data$from_hostname <- gsub("m.facebook.com", "facebook.com", sna_data$from_hostname, ignore.case = T)
sna_data$from_hostname <- gsub("l.facebook.com", "facebook.com", sna_data$from_hostname, ignore.case = T)
sna_data$from_hostname <- gsub("lm.facebook.com", "facebook.com", sna_data$from_hostname, ignore.case = T)
sna_data$to_hostname <- gsub("www.", "", sna_data$to_hostname, ignore.case = T)
#filter by user jouney

# Define server logic ----
server <-
function(input, output) {
    networkData <- reactive(subset(sna_data, sna_data$count > input$user, c('from_hostname', 'to_hostname')))


    refer_domain <- sna_data %>%
        group_by(from_hostname) %>%
        summarise(total = sum(count)) %>%
        arrange(desc(total))

    receive_domain <- sna_data %>%
        group_by(to_hostname) %>%
        summarise(total = sum(count)) %>%
        arrange(desc(total))

    output$simple <- renderSimpleNetwork({
        simpleNetwork(networkData(), opacity = input$opacity, zoom = T)
    })
    output$table <- DT::renderDataTable({
        DT::datatable(sna_data[, c("from_hostname", "to_hostname", "count")],
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
runApp(app, host = "0.0.0.0", port = strtoi(Sys.getenv("PORT")))







