#!/bin/bash

set -x

# Create output folder on local machine
rm -rf output/frequency/hybrid.out.txt

# Run Hybrid
hadoop fs -rm -r /user/cloudera/frequency/output
hadoop jar project1.jar bigdata.project1.frequency.HybridRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/hybrid.out.txt
