FROM rocker/tidyverse

## prepare to install google cloud sdk
RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(lsb_release -c -s) main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN sudo apt install -y curl gnupg
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
RUN sudo apt-get update
## install cloudfoundry tools
RUN curl -v -L -o cf-cli_amd64.deb 'https://cli.run.pivotal.io/stable?release=debian64&source=github' && sudo dpkg -i cf-cli_amd64.deb

## install google cloud SDK and common R packages
RUN sudo apt install -y google-cloud-sdk r-cran-ggplot2 r-cran-stringr r-cran-plyr r-cran-magrittr r-cran-lubridate apache2-utils
## install some R packages directly from CRAN
RUN install2.r --error \
    -r 'http://cran.rstudio.com' \
    googleAuthR \
    googleAnalyticsR \
    searchConsoleR \
    gmailr \
    googleCloudStorageR \
    bigQueryR

RUN install2.r --error \
     -r 'http://cran.rstudio.com' \
     rsconnect

## clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/ && rm -rf /tmp/downloaded_packages/ /tmp/*.rds

CMD ["gsutil","list"]
