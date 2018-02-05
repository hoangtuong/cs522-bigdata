#!/bin/bash

set -x

# Copy input into hadoop
hadoop fs -rm -r /user/cloudera/frequency/input
hadoop fs -mkdir /user/cloudera/frequency /user/cloudera/frequency/input
hadoop fs -put input/frequency/* /user/cloudera/frequency/input

# Create output folder on local machine
rm -rf output/frequency
mkdir output output/frequency




# Run Pair
hadoop fs -rm -r /user/cloudera/frequency/output

# Store the start time
startTimePair=$(date +%s.%N)

hadoop jar bigdata.jar mum.cs.bigdata.frequency.PairRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output

# Calculate and print out the execution duration
durationPair=$(echo "$(date +%s.%N) - $startTimePair" | bc)
printf "Execution time: %.6f seconds" $durationPair

echo "Execution time" > output/comparision.txt
echo "Pair: $durationPair seconds" >> output/comparision.txt

hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/pair.txt





# Run Stripe
hadoop fs -rm -r /user/cloudera/frequency/output

# Store the start time
startTimeStripe=$(date +%s.%N)

hadoop jar bigdata.jar mum.cs.bigdata.frequency.StripeRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output

# Calculate and print out the execution duration
durationStripe=$(echo "$(date +%s.%N) - $startTimeStripe" | bc)
printf "Execution time: %.6f seconds" $durationStripe

echo "Stripe: $durationStripe seconds" >> output/comparision.txt

hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/stripe.txt





# Run Hybrid
hadoop fs -rm -r /user/cloudera/frequency/output

# Store the start time
startTimeHybrid=$(date +%s.%N)

hadoop jar bigdata.jar mum.cs.bigdata.frequency.HybridRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output

# Calculate and print out the execution duration
durationHybrid=$(echo "$(date +%s.%N) - $startTimeHybrid" | bc)
printf "Execution time: %.6f seconds" $durationHybrid

echo "Hybrid: $durationHybrid seconds" >> output/comparision.txt

hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/hybrid.txt
