#!/usr/bin/env python
from cfenv import AppEnv

env = AppEnv()

redis = env.get_service(label='redis')
postgres = env.get_service(label='postgres')

with open('airflow.cfg') as fd1, open('airflow.cfg', 'w') as fd2:
    for line in fd1:
        line = line.replace('REDIS_URL', redis.credentials['url'])
        line = line.replace('POSTGRES_URL', postgres.credentials['url'].replace('postgres:', "postgresql:"))
        fd2.write(line)
