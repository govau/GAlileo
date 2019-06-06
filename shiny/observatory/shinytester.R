shinyTester <- tabPanel(
  "Shiny Tester", # Sidebar with a slider input for number of bins
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
        choices = c("small", "medium", "large", "all")
      )
     )
    ,
    # Main panel for displaying outputs ----
    main = mainPanel(
      # Output: Formatted text for caption ----
      h3(textOutput("caption", container = span)),
      # Output: Verbatim text for data summary ----
      verbatimTextOutput("summary"),
      # Output: HTML table with requested number of observations ----
      fluidRow(
        splitLayout(cellWidths = c("50%", "50%"), plotOutput("view1"), plotOutput("view2"))
        )
    )
  )
)

shinytester_server <- function (input, output) {
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
      data %>% filter(hostname == input$agencies) %>% 
      group_by(hostname, source, city) %>%
      summarise(total = n()) 
  })
  
  DatasetCompare <- reactive({
    data %>% 
      filter(hostname %in% group_filter()) %>% 
      group_by(hostname, source, city) %>%
      summarise(total = n()) 
  })

  output$caption <- renderText({
    paste(input$agencies, " compared to", input$groups, "agencies")
  }) 
  

  output$view1 <- renderPlot({
    datasetInput() %>% 
      group_by(source) %>% 
      summarise(tot_source=sum(total)) %>% 
      top_n(5) %>% 
      ggplot(aes(x="", y=tot_source, fill = source)) +
      geom_bar(width = 1, stat = "identity")+
      coord_polar("y", start = 0)+
      theme_minimal() +
      labs(title = NULL)
      
  })

  output$view2 <- renderPlot({
    DatasetCompare() %>% 
      group_by(source) %>% 
      summarise(tot_source=sum(total)) %>% 
      top_n(5) %>% 
      ggplot(aes(x="", y=tot_source, fill = source)) +
      geom_bar(width = 1, stat = "identity")+
      coord_polar("y", start = 0)+
      theme_minimal()+
      labs(title = NULL)
  })
  
    output$keepAlive <- renderText({
    req(input$count)
    paste("keep alive ", input$count)
  })
}


