#!/usr/bin/env bash
#cp airflow.cfg.template airflow.cfg
#python generate_config.py

airflow initdb
#python create_user.py

airflow webserver -p ${PORT}