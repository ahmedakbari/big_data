#!/bin/bash
docker-compose down -v
docker-compose up -d zookeeper kafka elasticsearch redis kibana cassandra
sleep 5
docker-compose up -d --build twitter_fetcher
sleep 5
docker-compose up -d --build twitter_preprocess
sleep 5
docker-compose up -d --build kafka_to_elastic
sleep 90
docker-compose up -d --build kafka_to_cassandra
