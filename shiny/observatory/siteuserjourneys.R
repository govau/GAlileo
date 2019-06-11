siteUserJourneys <- tabPanel(
"User Journeys",
d3Output("d3", width = "100%", height = 800)
)

siteuserjourneys_server <- function (input, output) {
    output$d3 <- renderD3({
        r2d3(
        list(data.frame(title = c("DTA","About Us","Join Our Team","Recruiterbox"),
        href=c("https://google.com","https://google.com","https://google.com","")),
        data.frame(title = c("DTA","Blogs", "Help and Advice", "Guides and Tools", "Homepage - Digital Guides", "Topics", "Roles", "What each role does","Content guide","New to content design?"),
        href=c("https://google.com","https://google.com","https://google.com"," "," "," "," "," "," "," ")),
        data.frame(title = c("Domain Names","Guidelines", "Name Server Change"),
        href=c("https://google.com","https://google.com","https://google.com")),
        data.frame(title = c("Design System","Components", "Templates"),
        href=c("https://google.com","https://google.com","https://google.com")),
        data.frame(title = c("Design System","Get Started", "Download", "Community"),
        href=c("https://google.com","https://google.com","https://google.com", ""))
        ),
        script = "siteuserjourneys.js"
        )
    })
}
shinyApp(ui = siteUserJourneys, server = siteuserjourneys_server)
