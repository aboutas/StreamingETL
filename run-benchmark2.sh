#!/bin/bash
################################################################################
# ETL Flink Benchmark 2 - Read + Transform Time (excludes output write)
#
# Prerequisite: topics, configs, and data must already be set up (see run.md).
# Measures: START when Flink begins consuming → STOP when all input consumed.
# This captures read + transform time only (output write happens in parallel
# but any remaining writes after input is consumed are NOT included).
#
# Usage: ./run-benchmark2.sh <parallelism>
# Example: ./run-benchmark2.sh 4
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
echo "  ETL FLINK BENCHMARK 2 (read + transform)"
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
# STEP 2: Delete consumer group + reset output topic
################################################################################

echo "[2/3] Resetting for clean run..."

# Delete consumer group
$KAFKA_DIR/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER \
    --group $CONSUMER_GROUP --delete 2>/dev/null || true

# Reset output topic (delete + recreate) for accurate output count
$KAFKA_DIR/bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic $OUTPUT_TOPIC 2>/dev/null || true
sleep 3
$KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic $OUTPUT_TOPIC 2>/dev/null
sleep 2
echo "  Done"

################################################################################
# STEP 3: Submit Flink job & measure read + transform time
################################################################################

# Count total records in input topic
TOTAL=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER --topic $INPUT_TOPIC --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')

# Count configs
CONFIGS=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER --topic $CONFIG_TOPIC --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')

if [ -z "$TOTAL" ] || [ "$TOTAL" -le 0 ] 2>/dev/null; then
    echo "  ERROR: No records in input topic. Run setup first (see run.md)."
    exit 1
fi
echo "  Input: $TOTAL records | Configs: $CONFIGS"

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
START_TIME=$(date +%s%N)
echo "  Processing started at $(date '+%H:%M:%S')"

# Poll until all input records consumed
while true; do
    OFFSET=$($KAFKA_DIR/bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP --describe 2>/dev/null \
        | grep "$INPUT_TOPIC" | awk '{sum+=$3}END{print sum}')
    [ -z "$OFFSET" ] && sleep 1 && continue

    PERCENT=$((OFFSET * 100 / TOTAL))
    printf "\r  Input progress: %d / %d (%d%%)" "$OFFSET" "$TOTAL" "$PERCENT"

    [ "$OFFSET" -ge "$TOTAL" ] && break
    sleep 1
done

# STOP timer — all input consumed + transformed (via backpressure)
END_TIME=$(date +%s%N)

# Calculate duration in milliseconds
DURATION_MS=$(( (END_TIME - START_TIME) / 1000000 ))
DURATION_S=$((DURATION_MS / 1000))
DURATION_FRAC=$((DURATION_MS % 1000))

echo ""
echo "  All input consumed at $(date '+%H:%M:%S')"

# Wait for output to stabilize, then count
sleep 5
OUTPUT_COUNT=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER --topic $OUTPUT_TOPIC --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')

# Calculate throughput
if [ "$DURATION_S" -gt 0 ]; then
    THROUGHPUT=$((TOTAL * 1000 / DURATION_MS))
else
    THROUGHPUT="N/A (< 1s)"
fi

# Processing ratio
if [ "$TOTAL" -gt 0 ]; then
    RATIO=$((OUTPUT_COUNT * 100 / TOTAL))
else
    RATIO=0
fi

echo ""
echo "=========================================="
echo "  RESULTS (read + transform, no write)"
echo "=========================================="
echo "  Parallelism:        $PARALLELISM"
echo "  Configs:            $CONFIGS"
echo "  Input records:      $TOTAL"
echo "  Output records:     $OUTPUT_COUNT"
echo "  Processing ratio:   ${RATIO}%"
echo "  Processing time:    ${DURATION_S}.${DURATION_FRAC}s"
echo "  Throughput:         $THROUGHPUT rec/sec"
echo "=========================================="
