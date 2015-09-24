#!/bin/bash

if [ "$#" -ne 2 ] ; then
	echo "	usage: cachesend.sh <file> <partition>"
else
	echo "Assuming broker is on machine oracolo (10.20.10.141)"
	./rdkafka-cachesender -t "wc" -p $2 -b "10.20.10.141:9092" -f $1 -s 200 -l 400 -n 10
fi
