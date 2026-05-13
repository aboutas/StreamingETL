#!/bin/bash
################################################################################
# ETL Flink Benchmark - Pure Kafka Read Time
#
# Prerequisite: topics and data must already be set up (see run.md).
# This script measures ONLY the time to read all records from the input topic,
# with no ETL processing. Uses kafka-consumer-perf-test for accurate timing.
#
#   START: consumer begins fetching records
#   STOP:  all N records consumed
#
# Usage: ./run-benchmark-read.sh <parallelism>
# Example: ./run-benchmark-read.sh 4
################################################################################

KAFKA_BROKER="clu02.softnet.tuc.gr:6667"
KAFKA_BROKERS="clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667"
KAFKA_DIR="/usr/hdp/current/kafka-broker"
READ_CONSUMER_GROUP="etl-read-benchmark"
INPUT_TOPIC="etl.input.v1"

PARALLELISM=${1:?"Usage: $0 <parallelism>  Example: $0 4"}

echo "=========================================="
echo "  ETL READ BENCHMARK"
echo "  Parallelism (threads): $PARALLELISM"
echo "=========================================="

################################################################################
# STEP 1: Count input records
################################################################################

echo ""
echo "[1/3] Checking input topic..."

TOTAL=$($KAFKA_DIR/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER --topic $INPUT_TOPIC --time -1 2>/dev/null \
    | awk -F: '{sum += $3} END {print sum}')

if [ -z "$TOTAL" ] || [ "$TOTAL" -le 0 ] 2>/dev/null; then
    echo "  ERROR: No records in input topic. Run setup first (see run.md)."
    exit 1
fi
echo "  Input: $TOTAL records"

################################################################################
# STEP 2: Delete consumer group for clean run
################################################################################

echo ""
echo "[2/3] Resetting consumer group..."

$KAFKA_DIR/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER \
    --group $READ_CONSUMER_GROUP --delete 2>/dev/null || true
sleep 2

CG_CHECK=$($KAFKA_DIR/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER --list 2>/dev/null | grep -c "$READ_CONSUMER_GROUP" || true)
if [ "$CG_CHECK" -gt 0 ]; then
    echo "  WARNING: Consumer group still exists, retrying delete..."
    $KAFKA_DIR/bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $READ_CONSUMER_GROUP --delete 2>/dev/null || true
    sleep 3
fi
echo "  Done"

################################################################################
# STEP 3: Run kafka-consumer-perf-test and capture results
################################################################################

echo ""
echo "[3/3] Running read benchmark..."
echo "  Reading $TOTAL records with $PARALLELISM thread(s)..."
echo ""

# Output format (2 lines): header CSV + values CSV
# start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg,
# nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
RESULT=$($KAFKA_DIR/bin/kafka-consumer-perf-test.sh \
    --broker-list $KAFKA_BROKERS \
    --topic $INPUT_TOPIC \
    --messages $TOTAL \
    --threads $PARALLELISM \
    --group $READ_CONSUMER_GROUP 2>/dev/null | tail -1)

if [ -z "$RESULT" ]; then
    echo "  ERROR: kafka-consumer-perf-test produced no output."
    exit 1
fi

# Parse CSV columns (1-indexed)
DATA_MB=$(echo "$RESULT"       | awk -F',' '{print $3}' | tr -d ' ')
REBALANCE_MS=$(echo "$RESULT"  | awk -F',' '{print $7}' | tr -d ' ')
FETCH_TIME_MS=$(echo "$RESULT" | awk -F',' '{print $8}' | tr -d ' ')
FETCH_MBSEC=$(echo "$RESULT"   | awk -F',' '{print $9}' | tr -d ' ')
FETCH_MSGSEC=$(echo "$RESULT"  | awk -F',' '{print $10}'| tr -d ' ')

# Convert fetch.time.ms to seconds
FETCH_S=$((FETCH_TIME_MS / 1000))
FETCH_FRAC=$((FETCH_TIME_MS % 1000))

echo "=========================================="
echo "  RESULTS"
echo "=========================================="
echo "  Parallelism (threads): $PARALLELISM"
echo "  Input records:         $TOTAL"
echo "  Data consumed:         ${DATA_MB} MB"
echo "  Rebalance time:        ${REBALANCE_MS} ms"
echo "  Read time (fetch):     ${FETCH_S}.${FETCH_FRAC}s"
echo "  Read throughput:       ${FETCH_MSGSEC} rec/sec"
echo "  Read throughput:       ${FETCH_MBSEC} MB/sec"
echo "=========================================="
