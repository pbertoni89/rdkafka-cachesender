#!/bin/bash
g++ rdkafka_consumer.c -g -O3 -o rdkafka_consumer -Wno-write-strings -fpermissive -lrdkafka -lz -lpthread -lrt -I ../librdkafka/src -L ../librdkafka/src
