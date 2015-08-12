#!/bin/bash
g++ rdkafka-cachesender.c -g -o rdkafka-cachesender -Wno-write-strings -fpermissive -lrdkafka -lz -lpthread -lrt -I ../librdkafka/src -L ../librdkafka/src
