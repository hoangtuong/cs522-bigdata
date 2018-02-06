#!/bin/bash

set -x

# Copy input into HDFS
hadoop fs -rm -r /user/cloudera/project2/input
hadoop fs -mkdir /user/cloudera/project2 /user/cloudera/project2/input
hadoop fs -put input/* /user/cloudera/project2/input

# Create output folder on local file system
rm -rf output
mkdir output

# Run the jar file with Spark
spark-submit --class bigdata.project2.Main project2.jar

