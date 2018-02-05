#!/bin/bash

set -x

# Remove output file (if existed)
rm -rf output/frequency/pair.out.txt

# Run Pair Approach algorithms
hadoop fs -rm -r /user/cloudera/frequency/output
hadoop jar project1.jar bigdata.project1.frequency.PairRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/pair.out.txt

