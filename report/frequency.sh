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
hadoop jar bigdata.jar mum.cs.bigdata.frequency.PairRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/pair.txt

# Run Stripe
hadoop fs -rm -r /user/cloudera/frequency/output
hadoop jar bigdata.jar mum.cs.bigdata.frequency.StripeRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/stripe.txt

# Run Hybrid
hadoop fs -rm -r /user/cloudera/frequency/output
hadoop jar bigdata.jar mum.cs.bigdata.frequency.HybridRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/hybrid.txt
