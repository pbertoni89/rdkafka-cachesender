KAFKA SETUP
-------------------

First of all, move to `kafka_version` directory. Mine is `kafka_2.11-0.8.2.1`

    delete.topic.enable=true
    advertised.host.name=<broker_ip>
    advertised.port=9092

Then launch, in this order,

    ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
    ./bin/kafka-server-start.sh ./config/server.properties

And create your topic *e.g.* `wc` with script

    ./bin/kafka-topics.sh --create --topic wc --partition 3 --replication-factor 1 --zookeeper localhost:2181

Btw, I used a `partition` value of `3` for our test topology. Check topic creation with

    bin/kafka-topics.sh --list --zookeeper localhost:2181

