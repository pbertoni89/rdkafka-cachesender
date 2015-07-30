#!/bin/bash
../kafka_2.11-0.8.2.1/bin/kafka-topics.sh --delete --topic wc --zookeeper localhost:2181
../kafka_2.11-0.8.2.1/bin/kafka-topics.sh --create --topic wc --partition 3 --replication-factor 1 --zookeeper localhost:2181