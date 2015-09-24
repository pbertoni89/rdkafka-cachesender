#!/bin/bash
echo "This file is intended to fed directly a WordCounter, not producing for a Kafka broker but actually at localhost:30000"
./cachesend_to_port -f random.txt -p 30000 -b 0 -l 100 -n 50
