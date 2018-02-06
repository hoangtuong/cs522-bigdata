#!/bin/bash

set -x

# Copy input into hadoop
hadoop fs -rm -r /user/cloudera/average/input
hadoop fs -mkdir /user/cloudera/average /user/cloudera/average/input
hadoop fs -put input/average/* /user/cloudera/average/input

# Create output folder on local machine
rm -rf output/average
mkdir output output/average

# Run Original Algorithm
hadoop fs -rm -r /user/cloudera/average/output
hadoop jar project1.jar bigdata.project1.average.AverageComputation /user/cloudera/average/input /user/cloudera/average/output
hadoop fs -cat /user/cloudera/average/output/* > output/average/average.out.txt

# Run In Mapper Algorithm
hadoop fs -rm -r /user/cloudera/average/output
hadoop jar project1.jar bigdata.project1.average.InMapperAverageComputation /user/cloudera/average/input /user/cloudera/average/output
hadoop fs -cat /user/cloudera/average/output/* > output/average/InMapperAverage.out.txt

