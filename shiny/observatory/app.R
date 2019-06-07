library(shiny)
library(r2d3)

if (!exists("retirement")){
  retirement <- read.csv("retirement_lexicon.csv", header  = TRUE, stringsAsFactors = FALSE)
}

if (!exists("carer")){
  carers <- read.csv("carer_lexicon.csv", header = TRUE, stringsAsFactors = FALSE)
}

if (!exists("business")){
  business <- read.csv("business_lexicon.csv", header = TRUE, stringsAsFactors = FALSE)
}


score.sentiment = function(sentences, pos.words, neg.words, .progress='none')
{
  require(plyr)
  require(stringr)
  
  # we got a vector of sentences. plyr will handle a list or a vector as an "l" for us
  # we want a simple array of scores back, so we use "l" + "a" + "ply" = laply:
  scores = laply(sentences, function(sentence, pos.words, neg.words) {
    
    # clean up sentences with R's regex-driven global substitute, gsub():
    sentence = gsub('[[:punct:]]', '', sentence)
    sentence = gsub('[[:cntrl:]]', '', sentence)
    sentence = gsub('\\d+', '', sentence)
    # and convert to lower case:
    sentence = tolower(sentence)
    
    # split into words. str_split is in the stringr package
    word.list = str_split(sentence, '\\s+')
    # sometimes a list() is one level of hierarchy too much
    words = unlist(word.list)
    
    # compare our words to the dictionaries of positive & negative terms
    pos.matches = match(words, pos.words)
    neg.matches = match(words, neg.words)
    
    # match() returns the position of the matched term or NA
    # we just want a TRUE/FALSE:
    pos.matches = !is.na(pos.matches)
    neg.matches = !is.na(neg.matches)
    
    # and conveniently enough, TRUE/FALSE will be treated as 1/0 by sum():
    score = sum(pos.matches) - sum(neg.matches)
    
    return(score)
  }, pos.words, neg.words, .progress=.progress )
  
  scores.df = data.frame(score=scores, text=sentences)
  return(scores.df)
}




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
