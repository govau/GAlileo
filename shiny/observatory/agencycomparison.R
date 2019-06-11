if (!exists("agency")){
  agency <- read.csv("agency-1000plus.csv", stringsAsFactors = FALSE)[,-1]
}

if (!exists("all_agencies")){
  all_agencies <- read_xlsx("hostnames.xlsx", sheet = 1, col_names = F,  col_types = "text")
  colnames(all_agencies) <- c("hostnames")
}


if (!exists("large_agencies")){
  large_agencies <- read_xlsx("hostnames.xlsx", sheet = 2, col_names = F, col_types = "text")
  colnames(large_agencies) <- c("hostnames")
}

if (!exists("med_agencies")){
  med_agencies <- read_xlsx("hostnames.xlsx", sheet = 3, col_names = F, col_types = "text")
  colnames(med_agencies) <- c("hostnames")
}

if (!exists("small_agencies")){
  small_agencies <- read_xlsx("hostnames.xlsx", sheet = 4, col_names = F, col_types = "text")
  colnames(small_agencies) <- c("hostnames")
}


agencyComparison <- tabPanel(
  "Agency Comparison", # Sidebar with a slider input for number of bins
  # Sidebar layout with input and output definitions ----
  sidebarLayout(
    # Sidebar panel for inputs ----
    sidebarPanel(
      # Input: Selector for choosing dataset ----
      selectInput(
        inputId = "agencies",
        label = "Choose an agency:",
        choices = c("dta.gov.au", "humanservices.gov.au", "casa.gov.au")
      ),

      selectInput(
        inputId = "groups",
        label = "Compare with:",
        choices = c("small", "medium", "large", "all"),
      ),
      width = 3)
    ,
    # Main panel for displaying outputs ----
    main = mainPanel(
      # Output: Formatted text for caption ----
      h3(textOutput("ac_caption", container = span)),
      # Output: Verbatim text for data summary ----
      verbatimTextOutput("ac_summary"),
      # Output: HTML table with requested number of observations ----
      fluidRow(
        splitLayout(cellWidths = c("50%", "50%"), plotOutput("view1"), plotOutput("view2"))
      )
    )
  )
)


agencycomparison_server <- function (input, output) {
  # Return the requested dataset ----
  # By declaring datasetInput as a reactive expression we ensure
  # that:
  #
  # 1. It is only called when the inputs it depends on changes
  # 2. The computation and result are shared by all the callers,
  #    i.e. it only executes a single time
  group_filter <- reactive({
    switch(input$groups,
           "small" = small_agencies$hostnames,
           "medium" = med_agencies$hostnames,
           "large"= large_agencies$hostnames,
           "all" = all_agencies$hostnames)})

  datasetInput <- reactive({
    agency1  <- filter(agency, hostname == input$agencies) %>%
      group_by(sourceurl) %>%
      arrange(desc(tot_source))
  })

  DatasetCompare <- reactive({
    agency <- filter(agency, hostname %in% group_filter()) %>%
      group_by(sourceurl) %>%
      arrange(desc(tot_source))
  })

  output$ac_caption <- renderText({
    paste(input$agencies, " compared to", input$groups, "agencies")
  })


  output$view1 <- renderPlot({
    datasetInput() %>%
      top_n(5) %>%
      ggplot(aes(x="", y=tot_source, color=sourceurl)) +
      geom_bar(width = 1, stat = "identity")+
      coord_polar("y", start = 0)+
      theme_minimal() +
      labs(title = NULL)

  })

  output$view2 <- renderPlot({
    DatasetCompare() %>%
      top_n(5) %>%
      ggplot(aes(x="", y=tot_source)) +
      geom_bar(width = 1, stat = "identity")+
      coord_polar("y", start = 0)+
      theme_minimal()+
      labs(title = NULL)
  })


}

shinyApp(ui = agencyComparison, server = agencycomparison_server)
