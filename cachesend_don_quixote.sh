#!/bin/bash

if [ "$#" -ne 1 ] ; then
	echo "	usage: cachesend_don_quixote.sh <partition>"
else
	echo "Assuming broker is on machine oracolo (10.20.10.141)"
	./rdkafka-cachesender -t "wc" -p $1 -b "10.20.10.141:9092" -f "quixote.txt" -s 1
fi
