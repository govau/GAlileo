if (!exists("agency")){
  agency <- read.csv("agency.csv", stringsAsFactors = FALSE)
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
        choices = c("small", "medium", "large", "all"),
      ), 
      width = 3)
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
    agency  <- filter(agency, hostname == input$agencies) 
    agency %>% 
      group_by(sourceurl) %>% 
      summarise_(tot_source= interp(~sum(x), x = as.name(total))) %>% 
      top_n(5)
  })
  
  DatasetCompare <- reactive({
    agency <- filter(agency, hostname %in% group_filter()) 

  })

  output$caption <- renderText({
    paste(input$agencies, " compared to", input$groups, "agencies")
  }) 
  
  
  output$view1 <- renderPlot({
    datasetInput() %>%
      ggplot(aes(x="", y=tot_source)) +
      geom_bar(width = 1, stat = "identity")+
      coord_polar("y", start = 0)+
      theme_minimal() +
      labs(title = NULL)
    
  })
  
  output$view2 <- renderPlot({
    DatasetCompare() %>% 
      ggplot(aes(x="", y=tot_source)) +
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