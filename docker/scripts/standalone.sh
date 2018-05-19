#!/bin/bash

start-master.sh
start-slaves.sh

spark-submit --class com.fireway.spark.LogsAnalyzer spark.jar master /logs
