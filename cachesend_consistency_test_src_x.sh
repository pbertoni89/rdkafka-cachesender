#!/bin/bash
re_string='^[0-9]+$'
re_negative='^-?[0-9]+([.][0-9]+)?$'

if [ "$#" -ne 2 ] || ! [[ $2 =~ $re_string ]] || ! [[ $2 =~ $re_negative ]] ; then
        echo "usage: cachesend_consistency_test_src_x.sh <source> <partition>"
else
	./rdkafka-cachesender -t "wc" -p $2 -b "10.20.10.141:9092" -f $1".txt" -s 1 -m 150000 -n 10
fi
