SCRIPTS
-------------------
Within kafka: run

    bin/kafka-topics.sh --create --topic wc --partition 1 --replication-factor 1 --zookeeper localhost:2181

And check topic creation

    bin/kafka-topics.sh --list --zookeeper localhost:2181
