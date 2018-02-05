#!/bin/bash

set -x

# Copy input into hadoop
hadoop fs -rm -r /user/cloudera/wordcount/input
hadoop fs -mkdir /user/cloudera/wordcount /user/cloudera/wordcount/input
hadoop fs -put input/wordcount/* /user/cloudera/wordcount/input

# Create output folder on local machine
rm -rf output/wordcount
mkdir output output/wordcount

# Run Orginal algorithm
hadoop fs -rm -r /user/cloudera/wordcount/output
hadoop jar project1.jar bigdata.project1.wordcount.WordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output
hadoop fs -cat /user/cloudera/wordcount/output/* > output/wordcount/WordCount.out.txt

# Run In Mapper Combining algorithm
hadoop fs -rm -r /user/cloudera/wordcount/output
hadoop jar project1.jar bigdata.project1.wordcount.InMapperWordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output
hadoop fs -cat /user/cloudera/wordcount/output/* > output/wordcount/InMapperWordCount.out.txt

