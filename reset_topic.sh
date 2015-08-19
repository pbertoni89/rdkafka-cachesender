#!/bin/bash

re_string='^[0-9]+$'
re_negative='^-?[0-9]+([.][0-9]+)?$'

if [ "$#" -ne 1 ] || ! [[ $1 =~ $re_string ]] || ! [[ $1 =~ $re_negative ]] ; then
	echo "usage: reset_topic.sh <no_of_partitions>"
else
	../kafka_2.11-0.8.2.1/bin/kafka-topics.sh --delete --topic wc --zookeeper localhost:2181
	../kafka_2.11-0.8.2.1/bin/kafka-topics.sh --create --topic wc --partition $1 --replication-factor 1 --zookeeper localhost:2181
fi
