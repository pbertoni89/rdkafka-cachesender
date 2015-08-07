#!/bin/bash
g++ rdkafka-cachesender.c -g -o rdkafka-cachesender -Wno-write-strings -fpermissive -lrdkafka -lz -lpthread -lrt -I /usr/local/include/librdkafka -L /usr/local/lib
