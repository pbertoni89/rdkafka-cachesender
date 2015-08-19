#!/bin/bash
g++ rdkafka-cachesender.c -g -O3 -o rdkafka-cachesender -Wno-write-strings -fpermissive -lrdkafka -lz -lpthread -lrt -I ../librdkafka/src -L ../librdkafka/src
