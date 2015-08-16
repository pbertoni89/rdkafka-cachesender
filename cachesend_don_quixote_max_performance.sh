#!/bin/bash
./rdkafka-cachesender -t "wc" -p 0 -b "10.20.10.141:9092" -f "quixote.txt" -s 0
