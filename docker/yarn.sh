#!/bin/bash

master_name=$(docker ps --filter "name=master" --format "{{.Names}}")
slave_name=$(docker ps --filter "name=slave" --format "{{.Names}}")

docker exec -it $master_name /usr/local/app/scripts/init.sh
docker cp code/spark.jar $master_name:/usr/local/app/scripts/
docker exec -it $master_name /usr/local/app/scripts/yarn.sh
docker exec -it $master_name /usr/local/app/scripts/get_results.sh

