#!/bin/bash

hadoop fs -rm -r /user/cloudera/project2/input
hadoop fs -mkdir /user/cloudera/project2
hadoop fs -put input /user/cloudera/project2/

#spark-submit project2.jar --class bigdata.loganalyzer.Main input/access_log output.txt
spark-submit --class bigdata.loganalyzer.Main project2.jar

#rm -rf output/spark/
#mkdir output output/spark
#hadoop fs -cat /user/cloudera/project2/output/* > result.txt
