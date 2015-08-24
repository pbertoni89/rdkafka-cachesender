#!/bin/bash

re_string='^[0-9]+$'
re_negative='^-?[0-9]+([.][0-9]+)?$'

if [ "$#" -ne 1 ] || ! [[ $1 =~ $re_string ]] || ! [[ $1 =~ $re_negative ]] ; then
	echo "usage: wordgenerator_x.sh <no_of_words>"
else
	rm random.txt
	./wordgenerator $1 >> random.txt
fi
