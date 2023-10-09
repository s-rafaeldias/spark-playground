#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# | Kafka REST Port | 8082  |
# | Plaintext Ports | 50797 |
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 main.py
