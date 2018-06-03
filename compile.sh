#!/bin/bash

if [ ! -f docker/code/spark.jar ]; then
	cd spark/
	mvn clean install
	mv target/spark-1.0-SNAPSHOT-jar-with-dependencies.jar ../docker/code/spark.jar
fi
