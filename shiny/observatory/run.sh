#!/usr/bin/env bash
if [[ ! -f setup_complete ]]; then
	for dir in /home/vcap/deps/0/apt/usr/lib/R/site-library/*; do
		if [[ ! "$dir" =~ (htmlwidgets|shiny|httpuv) ]]; then
		   echo installing ${dir};
		   R CMD INSTALL ${dir} ;
		fi
	done
	wait
	/home/vcap/deps/0/apt/usr/sbin/nginx -V
	touch setup_complete
fi
R -e "options(shiny.port = 3838); shiny::runApp(getwd())"
