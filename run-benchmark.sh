#!/bin/bash
################################################################################
# ETL Flink Benchmark - Pure Processing Time
#
# All-in-one: resets topics, submits configs, produces data, starts Flink,
# measures pure processing time (excludes Flink init).
#
# Usage: ./run-benchmark.sh <parallelism> [records]
# Example: ./run-benchmark.sh 4 2000000
################################################################################

KAFKA_BROKER="clu02.softnet.tuc.gr:6667"
KAFKA_BROKERS="clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667"
ZOOKEEPER="clu01.softnet.tuc.gr:2182"
KAFKA_DIR="/usr/hdp/current/kafka-broker"
FLINK_DIR="/usr/local/flink"
JAR_DIR="/home/avoutas/boutasThesis"
CONSUMER_GROUP="etl-flink-consumer"
INPUT_TOPIC="etl.input.v1"
CONFIG_TOPIC="etl.config.v1"
OUTPUT_TOPIC="etl.output.v1"

PARALLELISM=${1:?"Usage: $0 <parallelism> [records]  Example: $0 4 2000000"}
RECORDS=${2:-2000000}

echo "=========================================="
echo "  ETL FLINK BENCHMARK"
echo "  Parallelism: $PARALLELISM"
echo "  Target records: $RECORDS"
echo "=========================================="

################################################################################
# PHASE 1: SETUP (not timed)
################################################################################

echo ""
echo "[1/7] Stopping existing Flink jobs..."
for job in $($FLINK_DIR/bin/flink list -r 2>/dev/null | awk 'NR>3 {print $4}'); do
    $FLINK_DIR/bin/flink cancel $job 2>/dev/null || true
done
sleep 3
echo "  Done"

echo "[2/7] Resetting topics..."
$KAFKA_DIR/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $INPUT_TOPIC 2>/dev/null || true
$KAFKA_DIR/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $CONFIG_TOPIC 2>/dev/null || true
$KAFKA_DIR/bin/kafka-topics.sh --delete --zookeeper $ZOOKEEPER --topic $OUTPUT_TOPIC 2>/dev/null || true
sleep 5
$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 2 --partitions 4 --topic $INPUT_TOPIC
$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 2 --partitions 4 --topic $CONFIG_TOPIC
$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER --replication-factor 2 --partitions 4 --topic $OUTPUT_TOPIC
sleep 3
echo "  Done"

echo "[3/7] Starting API & submitting configs..."
pkill -f "etl-api-1.0.0.jar" 2>/dev/null || true
sleep 2
java -jar $JAR_DIR/etl-api-1.0.0.jar \
    --kafka.bootstrap.servers=$KAFKA_BROKERS \
    --kafka.config.topic=$CONFIG_TOPIC \
    --server.port=8080 > /dev/null 2>&1 &
API_PID=$!
sleep 10
curl -s -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @$JAR_DIR/config-clean-data.json > /dev/null
curl -s -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @$JAR_DIR/config-elements.json > /dev/null
curl -s -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @$JAR_DIR/config-9.json > /dev/null
curl -s -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @$JAR_DIR/config-high-light.json > /dev/null
echo "  4 configs submitted"

echo "[4/7] Producing $RECORDS records..."
java -jar $JAR_DIR/etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=$KAFKA_BROKERS \
    --kafka.input.topic=$INPUT_TOPIC \
    --producer.max.records=$RECORDS \
    --producer.rate.per.sec=50000 > /dev/null 2>&1

TOTAL=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER --topic $INPUT_TOPIC --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')
echo "  Done: $TOTAL records in topic"

echo "[5/7] Deleting consumer group..."
$KAFKA_DIR/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER \
    --group $CONSUMER_GROUP --delete 2>/dev/null || true
sleep 2
echo "  Done"

################################################################################
# PHASE 2: MEASURE (only this part is timed)
################################################################################

echo ""
echo "[6/7] Starting Flink job..."
$FLINK_DIR/bin/flink run -p $PARALLELISM -d $JAR_DIR/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers $KAFKA_BROKERS \
    --kafka.config.topic $CONFIG_TOPIC \
    --kafka.input.topic $INPUT_TOPIC \
    --kafka.output.topic $OUTPUT_TOPIC 2>&1 | grep -i "submitted" || true

echo "[7/7] Measuring pure processing time..."
echo "  Waiting for Flink to start consuming data..."

# Wait until Flink starts consuming data records (offset > 0 on input topic)
# This excludes Flink init time (JVM startup, config loading, task deployment)
while true; do
    OFFSET=$($KAFKA_DIR/bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP --describe 2>/dev/null \
        | grep "$INPUT_TOPIC" | awk '{sum+=$3}END{print sum}')
    [ -n "$OFFSET" ] && [ "$OFFSET" -gt 0 ] 2>/dev/null && break
    sleep 0.5
done

# START timing: Flink is now actively processing data
START_TIME=$(date +%s)
echo "  Processing started at $(date '+%H:%M:%S')"

# Poll until all input records are consumed
while true; do
    OFFSET=$($KAFKA_DIR/bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP --describe 2>/dev/null \
        | grep "$INPUT_TOPIC" | awk '{sum+=$3}END{print sum}')
    [ -z "$OFFSET" ] && sleep 1 && continue

    PERCENT=$((OFFSET * 100 / TOTAL))
    printf "\r  Progress: %d / %d (%d%%)" "$OFFSET" "$TOTAL" "$PERCENT"

    [ "$OFFSET" -ge "$TOTAL" ] && break
    sleep 1
done

# END timing: all input records consumed
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "  Processing finished at $(date '+%H:%M:%S')"

# Wait for last window to flush, then count output
sleep 5
OUTPUT_COUNT=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER --topic $OUTPUT_TOPIC --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')

if [ "$DURATION" -gt 0 ]; then
    THROUGHPUT=$((TOTAL / DURATION))
else
    THROUGHPUT="N/A (< 1s)"
fi

echo ""
echo "=========================================="
echo "  RESULTS"
echo "=========================================="
echo "  Parallelism:      $PARALLELISM"
echo "  Input records:    $TOTAL"
echo "  Output records:   $OUTPUT_COUNT"
echo "  Processing time:  ${DURATION}s"
echo "  Throughput:       $THROUGHPUT rec/sec"
echo "=========================================="

# Cleanup
kill $API_PID 2>/dev/null || true
