#!/bin/bash

start-yarn.sh

spark-submit --class com.fireway.spark.LogsAnalyzer \
             --master yarn \
             --deploy-mode client \
             spark.jar master /logs

