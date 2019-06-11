ujig.copy <- read.graph("uj_all.rdata", format = "pajek")
ujbusiness.copy <- read.graph("uj_business.rdata", format = "pajek")

userJourneys <- tabPanel(
  "Cross-site User Journeys", # Sidebar with a slider input for number of bins
  # Sidebar layout with input and output definitions ----
  sidebarLayout(
    # Sidebar panel for inputs ----
    sidebarPanel(
      # Input: Selector for choosing dataset ----
      selectInput(
        inputId = "journeys",
        label = "Choose an agency:",
        choices = c("business", "all")
      ),
      width = 3 ),
    # Main panel for displaying outputs ----
    main = mainPanel(
      # Output: Formatted text for caption ----
      h3(textOutput("uj-caption", container = span)),
      # Output: Verbatim text for data summary ----
      verbatimTextOutput("uj-summary"),
      # Output: HTML table with requested number of observations ----
      fluidRow(forceNetworkOutput("force")))
  )
)


userjourneys_server <- function (input, output) {
  dataSet <- reactive({
    switch(input$journeys,
           "all" = ujig.copy,
           "business"= ujbusiness.copy)})

  wc <- reactive({cluster_walktrap(dataSet())})
  members <- reactive({membership(wc())})

  graph_data <- reactive({igraph_to_networkD3(dataSet(), group = members())})



  output$force <- renderForceNetwork({
    forceNetwork(Links = graph_data()$links, Nodes = graph_data()$nodes,
                 Source = "source", Target = "target",
                 Value = "value", NodeID = "name",
                 Group = "group", zoom = TRUE, linkWidth = (graph_data()$links$value)/50, opacity = .8)
  })
}

shinyApp(ui = userJourneys, server = userjourneys_server)
