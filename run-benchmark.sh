#!/bin/bash
################################################################################
# ETL Flink Benchmark - Pure Processing Time
#
# Prerequisite: topics, configs, and data must already be set up (see run.md).
# This script ONLY submits the Flink job and measures pure processing time.
#
# Usage: ./run-benchmark.sh <parallelism>
# Example: ./run-benchmark.sh 4
################################################################################

KAFKA_BROKER="clu02.softnet.tuc.gr:6667"
KAFKA_BROKERS="clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667"
KAFKA_DIR="/usr/hdp/current/kafka-broker"
FLINK_DIR="/usr/local/flink"
JAR_DIR="/home/avoutas/boutasThesis"
CONSUMER_GROUP="etl-flink-consumer"
INPUT_TOPIC="etl.input.v1"
CONFIG_TOPIC="etl.config.v1"
OUTPUT_TOPIC="etl.output.v1"

PARALLELISM=${1:?"Usage: $0 <parallelism>  Example: $0 4"}

echo "=========================================="
echo "  ETL FLINK BENCHMARK"
echo "  Parallelism: $PARALLELISM"
echo "=========================================="

################################################################################
# STEP 1: Stop existing Flink jobs
################################################################################

echo ""
echo "[1/3] Stopping existing Flink jobs..."
for job in $($FLINK_DIR/bin/flink list -r 2>/dev/null | awk 'NR>3 {print $4}'); do
    $FLINK_DIR/bin/flink cancel $job 2>/dev/null || true
done
sleep 3
echo "  Done"

################################################################################
# STEP 2: Delete consumer group (clean offsets)
################################################################################

echo "[2/3] Deleting consumer group..."
$KAFKA_DIR/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER \
    --group $CONSUMER_GROUP --delete 2>/dev/null || true
sleep 2
echo "  Done"

################################################################################
# STEP 3: Submit Flink job & measure pure processing time
################################################################################

# Count total records in input topic
TOTAL=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER --topic $INPUT_TOPIC --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')

if [ -z "$TOTAL" ] || [ "$TOTAL" -le 0 ] 2>/dev/null; then
    echo "  ERROR: No records in input topic. Run setup first (see run.md)."
    exit 1
fi
echo "  Input topic has $TOTAL records"

echo ""
echo "[3/3] Starting Flink job..."
$FLINK_DIR/bin/flink run -p $PARALLELISM -d $JAR_DIR/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers $KAFKA_BROKERS \
    --kafka.config.topic $CONFIG_TOPIC \
    --kafka.input.topic $INPUT_TOPIC \
    --kafka.output.topic $OUTPUT_TOPIC 2>&1 | grep -i "submitted" || true

echo "  Waiting for Flink to start consuming..."

# Wait until Flink starts consuming (offset > 0) — excludes JVM init, task deployment
WAIT_COUNT=0
MAX_WAIT=120  # 60 seconds timeout (120 * 0.5s)
while true; do
    OFFSET=$($KAFKA_DIR/bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP --describe 2>/dev/null \
        | grep "$INPUT_TOPIC" | awk '{sum+=$3}END{print sum}')
    [ -n "$OFFSET" ] && [ "$OFFSET" -gt 0 ] 2>/dev/null && break
    WAIT_COUNT=$((WAIT_COUNT + 1))
    if [ $WAIT_COUNT -ge $MAX_WAIT ]; then
        echo "  ERROR: Flink did not start consuming within 60s. Check Flink dashboard."
        exit 1
    fi
    sleep 0.5
done

# START timer — Flink is now actively processing
START_TIME=$(date +%s)
echo "  Processing started at $(date '+%H:%M:%S')"

# Poll until all input records consumed
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

# STOP timer
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
