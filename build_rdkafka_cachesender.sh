#!/bin/bash
g++ rdkafka_cachesender.c -g -o rdkafka_cachesender -Wno-write-strings -fpermissive -lrdkafka -lz -lpthread -lrt -I /usr/local/include/librdkafka -L /usr/local/lib
