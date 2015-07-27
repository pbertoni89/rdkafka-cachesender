#!/bin/bash
g++ rdkafka_cachesender.c -o rdkafka_cachesender -fpermissive -lrdkafka -lz -lpthread -lrt -I /usr/local/include/librdkafka -L /usr/local/lib
