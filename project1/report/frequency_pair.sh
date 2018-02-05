#!/bin/bash

set -x

# Remove output files on both local and HDFS (if existed)
rm -rf output/frequency/pair.out.txt
hadoop fs -rm -r /user/cloudera/frequency/output

# For execution time measument
start=$(date +%s.%N)

# Run Pair Approach algorithms
hadoop jar project1.jar bigdata.project1.frequency.PairRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output

duration=$(echo "$(date +%s.%N) - $start" | bc)
printf "Execution time: %.6f seconds" $duration
echo "Pair: $duration seconds" >> output/time.out.txt


# Cat output from HDFS to local file
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/pair.out.txt

