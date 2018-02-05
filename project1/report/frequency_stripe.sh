#!/bin/bash

set -x

# Create output folder on local machine
rm -rf output/frequency/stripe.out.txt

# Run Stripe
hadoop fs -rm -r /user/cloudera/frequency/output
hadoop jar project1.jar bigdata.project1.frequency.StripeRelativeFrequency /user/cloudera/frequency/input /user/cloudera/frequency/output
hadoop fs -cat /user/cloudera/frequency/output/* > output/frequency/stripe.out.txt

