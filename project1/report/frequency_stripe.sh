#!/bin/bash

set -x

# Remove output files on both local and HDFS (if existed)
rm -rf output/frequency/stripe.out.txt
hadoop fs -rm -r /user/cloudera/frequency/output

# For execution time measument
start=$(date +%s.%N)

# Run Stripe Approach algorithms
hadoop jar project1.jar bigdata.project1.frequency.StripeRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output

duration=$(echo "$(date +%s.%N) - $start" | bc)
printf "Execution time: %.6f seconds" $duration
echo "Stripe: $duration seconds" >> output/time.out.txt


# Cat output from HDFS to local file
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/stripe.out.txt

