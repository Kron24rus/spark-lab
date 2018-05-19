#!/bin/bash

# Check for sudo/root
printf "Checking for root permissions..."
if [[ $(id -u) -ne 0 ]]; then
	>&2 echo "Error: Root permissions required"
	exit 1
else
	echo "OK"
fi

sh compile.sh

#Deploy stack
docker stack deploy --compose-file docker/docker-compose.yml spark

master_started=$(docker ps --filter "name=spark_master" --filter status=running --format "{{.Names}}")
slave_started=$(docker ps --filter "name=spark_slave" --filter status=running --format "{{.Names}}")

counter=1

while [[ (-z "${master_started}") && (-z "${slave_started}") && ($counter -lt 30) ]]
do
	echo "Container strting..."
	counter=$counter+1
	sleep 5
	master_started=$(docker ps --filter "name=spark_master" --filter status=running --format "{{.Names}}")
    slave_started=$(docker ps --filter "name=spark_slave" --filter status=running --format "{{.Names}}")
done

#Start process if containers started
if [[ $counter -eq 30 ]]
then echo "not started"
else sh standalone.sh
fi
