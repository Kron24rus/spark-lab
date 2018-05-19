#!/bin/bash

master_name=$(docker ps --filter "name=spark_master" --format "{{.Names}}")
#echo $master_name
docker exec -it $master_name /usr/local/app/scripts/init.sh
docker cp code/spark.jar $master_name:/usr/local/app/scripts/
docker exec -it $master_name /usr/local/app/scripts/standalone.sh
docker exec -it $master_name /usr/local/app/scripts/get_results.sh

