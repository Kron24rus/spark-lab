#!/bin/bash
mkdir -p /usr/local/app/result

hdfs dfs -copyToLocal /task1 /usr/local/app/result
hdfs dfs -copyToLocal /task2 /usr/local/app/result
hdfs dfs -copyToLocal /task3 /usr/local/app/result

hdfs dfs -rm -r -f /task1 /task2 /task3

