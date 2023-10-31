#!/bin/bash
set -eo pipefail
IFS=$'\n\t'


kafka_port=$(confluent local kafka broker list | awk -F '|' '{print $4}' | tail -1 | xargs)
kafka_port=$(echo "$kafka_port-1" | bc)

python data.py "$kafka_port" "$1" "$2" "$3"
