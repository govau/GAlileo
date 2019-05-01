FROM rocker/tidyverse

RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(lsb_release -c -s) main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN sudo apt install -y curl gnupg
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
RUN sudo apt-get update
RUN sudo apt install -y google-cloud-sdk r-cran-ggplot2 r-cran-stringr

CMD ["gsutil","list"]
