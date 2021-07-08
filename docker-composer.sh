#!/bin/bash
docker-compose down -v
docker-compose up -d zookeeper kafka redis
sleep 5
docker-compose up -d --build twitter_fetcher
sleep 5
docker-compose up --build twitter_preprocess

