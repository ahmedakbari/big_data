#!/bin/bash
docker-compose down -v
docker-compose up -d zookeeper kafka elasticsearch redis kibana cassandra clickhouse
sleep 5
docker-compose up -d --build twitter_fetcher
sleep 5
docker-compose up -d --build twitter_preprocess
sleep 5
docker-compose up -d --build kafka_to_elastic
sleep 90
docker-compose up -d --build kafka_to_cassandra
sleep 5
docker-compose up -d --build kafka_to_redis
docker-compose up -d --build kafka_to_clickhouse


sleep 5
docker-compose up -d --build fastapi_to_redis

#sleep 5
#docker exec -it superset superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin

#docker exec -it superset superset db upgrade
#docker exec -it superset superset init
