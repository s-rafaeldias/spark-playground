#!/bin/bash
set -eo pipefail
IFS=$'\n\t'

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 main.py

# Create setup for kafka using confluent cli
if [[ -z $1 ]]; then
    echo "Starting new local kafka cluster"
    confluent local kafka start
    sleep 20

    confluent configuration update disable_feature_flags true

    echo "Creating topic kafka"
    confluent local kafka topic create example
fi


kafka_port=$(confluent local kafka broker list | awk -F '|' '{print $4}' | tail -1 | xargs)
kafka_port=$(echo "$kafka_port-1" | bc)

python main.py "$kafka_port"
