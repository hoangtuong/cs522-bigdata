#!/bin/bash

set -x

# Copy input into hadoop
hadoop fs -rm -r /user/cloudera/frequency/input
hadoop fs -mkdir /user/cloudera/frequency /user/cloudera/frequency/input
hadoop fs -put input/frequency/* /user/cloudera/frequency/input

# Create output folder on local file system
rm -rf output/frequency
rm -rf output/time.out.txt
mkdir output output/frequency

# Run Pair script
sh ./frequency_pair.sh

# Run Stripe script
sh ./frequency_stripe.sh

# Run Hybrid
sh ./frequency_hybrid.sh

