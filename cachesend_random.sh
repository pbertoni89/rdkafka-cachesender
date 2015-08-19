#!/bin/bash

if [ "$#" -ne 1 ] ; then
	echo "	usage: cachesend_random.sh <partition>"
else
	echo "Must be present random.txt first !"
	echo "Assuming broker is on machine oracolo (10.20.10.141)"
	./rdkafka-cachesender -t "wc" -p $1 -b "10.20.10.141:9092" -f "random.txt" -s 1
fi
