userJourneys <- tabPanel(
  "User Journeys", # Sidebar with a slider input for number of bins
  # Sidebar layout with input and output definitions ----
  sidebarLayout(
    # Sidebar panel for inputs ----
    sidebarPanel(
      sliderInput("bar_steps", label = "Steps:",
                  min = 1, max = 6, value = 6, step = 1),
      sliderInput("bar_journeys", label = "Journeys:",
                  min = 1, max = 6, value = 5, step = 1)
    )
    ,
    # Main panel for displaying outputs ----
    main = mainPanel(
d3Output("d3", width = "100%", height = 800)
    )
  )
)

userjourneys_server <- function (input, output) {
  output$d3 <- renderD3({
    r2d3(
      replicate(input$bar_journeys, list(data.frame(title = replicate(input$bar_steps,"")))),
      script = "userjourneys.js"
    )
  })
}
