```bash
cd /usr/hdp/current/kafka-broker

# Delete and recreate all topics
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.input.v1 2>/dev/null || true
sleep 5
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.input.v1



bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.config.v1 2>/dev/null || true
sleep 5
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.config.v1

bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.output.v1 2>/dev/null || true
sleep 5
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.output.v1


  # 1. List all ETL topics (verify they exist)
  cd /usr/hdp/current/kafka-broker
  bin/kafka-topics.sh --list --zookeeper clu01.softnet.tuc.gr:2182 | grep etl

  # 2. Check record count in each topic (should be 0)
  # For etl.input.v1
  bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
      --broker-list clu02.softnet.tuc.gr:6667 \
      --topic etl.input.v1 \
      --time -1 | awk -F: '{sum += $3} END {print "etl.input.v1: " sum " records"}'

  # For etl.config.v1
  bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
      --broker-list clu02.softnet.tuc.gr:6667 \
      --topic etl.config.v1 \
      --time -1 | awk -F: '{sum += $3} END {print "etl.config.v1: " sum " records"}'

  # For etl.output.v1
    bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list clu02.softnet.tuc.gr:6667 \
        --topic etl.output.v1 \
        --time -1 | awk -F: '{sum += $3} END {print "etl.output.v1: " sum " records"}'
```

```bash
cd /home/avoutas/boutasThesis

java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.input.topic=etl.input.v1 \
    --producer.mode=random \
    --producer.max.records=2000000


./send-config-to-kafka.sh config-elements.json
./send-config-to-kafka.sh config-clean-data.json
./send-config-to-kafka.sh config-high-light.json
./send-config-to-kafka.sh config-high-temp.json


./run-benchmark.sh 4