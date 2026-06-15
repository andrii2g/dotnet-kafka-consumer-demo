#!/usr/bin/env bash
set -euo pipefail

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list >/dev/null 2>&1; do
  sleep 2
done

/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic orders.created --partitions 6 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic orders.created.retry --partitions 6 --replication-factor 1
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic orders.created.dlq --partitions 6 --replication-factor 1
