#!/bin/bash

################################################################################
# ETL Flink Benchmark
################################################################################
# Usage: ./run-benchmark.sh <parallelism>
# Example: ./run-benchmark.sh 4
################################################################################

KAFKA_BROKER="clu02.softnet.tuc.gr:6667"
KAFKA_BROKERS="clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667"
CONSUMER_GROUP="etl-flink-consumer"

PARALLELISM=$1

if [ -z "$PARALLELISM" ]; then
    echo "Usage: $0 <parallelism>"
    echo "Example: $0 4"
    exit 1
fi

echo "=========================================="
echo "BENCHMARK - Parallelism: $PARALLELISM"
echo "=========================================="

# STEP 1: Stop existing Flink jobs
echo "[1/4] Stopping Flink jobs..."
cd /usr/local/flink
for job in $(./bin/flink list -r 2>/dev/null | awk 'NR>3 {print $4}'); do
    ./bin/flink cancel $job 2>/dev/null || true
done
sleep 5
echo "  Done"

# STEP 2: Delete consumer group
echo "[2/4] Deleting consumer group..."
/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh \
    --bootstrap-server $KAFKA_BROKER \
    --group $CONSUMER_GROUP \
    --delete 2>/dev/null || true
sleep 5
echo "  Done"

# STEP 3: Check input topic
echo "[3/4] Checking input topic..."
cd /usr/hdp/current/kafka-broker
TOTAL_RECORDS=$(bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list $KAFKA_BROKER \
    --topic etl.input.v1 \
    --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}')

if [ -z "$TOTAL_RECORDS" ] || [ "$TOTAL_RECORDS" = "0" ]; then
    echo "  ERROR: No records in etl.input.v1"
    exit 1
fi
echo "  Records: $TOTAL_RECORDS"

# STEP 4: Submit Flink job
echo "[4/4] Submitting Flink job..."
cd /usr/local/flink
./bin/flink run -p $PARALLELISM -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers $KAFKA_BROKERS \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1 2>&1 | grep -i "submitted" || true
echo "  Done"

# Wait for job to initialize
sleep 10

# MONITOR
echo ""
echo "Monitoring..."
echo "=========================================="

cd /usr/hdp/current/kafka-broker
FIRST_TIME=0

while true; do
    OFFSET=$(bin/kafka-consumer-groups.sh \
        --bootstrap-server $KAFKA_BROKER \
        --group $CONSUMER_GROUP \
        --describe 2>/dev/null | grep etl.input.v1 | awk '{sum+=$3}END{print sum}')

    [ -z "$OFFSET" ] && sleep 1 && continue

    NOW=$(date +%s)

    # First time we see offset > 0
    if [ "$FIRST_TIME" -eq 0 ] && [ "$OFFSET" -gt 0 ]; then
        FIRST_TIME=$NOW
    fi

    PERCENT=$((OFFSET * 100 / TOTAL_RECORDS))
    echo "$(date '+%H:%M:%S'): Offset=$OFFSET / $TOTAL_RECORDS | Progress=$PERCENT%"

    # Done when offset reaches total
    if [ "$OFFSET" -ge "$TOTAL_RECORDS" ]; then
        LAST_TIME=$NOW
        break
    fi

    sleep 1
done

# RESULT FROM MARKERS
echo ""
echo "Reading markers from output topic..."

# Get all marker lines directly (no temp file - more reliable)
MARKERS=$(timeout 180 bin/kafka-console-consumer.sh \
    --bootstrap-server $KAFKA_BROKER \
    --topic etl.output.v1 \
    --from-beginning \
    --timeout-ms 60000 2>/dev/null | grep -i "benchmark" || true)

# Find START and END markers
START_LINE=$(echo "$MARKERS" | grep -i "benchmark_start" | head -1)
END_LINE=$(echo "$MARKERS" | grep -i "benchmark_end" | tail -1)

# Extract processedAt values (format: "processedAt":1234567890.123)
START_EPOCH=$(echo "$START_LINE" | grep -o '"processedAt":[0-9]*' | cut -d':' -f2)
END_EPOCH=$(echo "$END_LINE" | grep -o '"processedAt":[0-9]*' | cut -d':' -f2)

echo "START marker epoch: $START_EPOCH"
echo "END marker epoch:   $END_EPOCH"

if [ -n "$START_EPOCH" ] && [ -n "$END_EPOCH" ]; then
    PURE_TIME=$((END_EPOCH - START_EPOCH))

    # Calculate measured records (total minus warmup)
    MEASURED_RECORDS=$((TOTAL_RECORDS - 5002))  # 5000 warmup + 2 markers

    if [ "$PURE_TIME" -gt 0 ]; then
        THROUGHPUT=$((MEASURED_RECORDS / PURE_TIME))
    else
        THROUGHPUT=0
    fi

    echo ""
    echo "=========================================="
    echo "RESULT (Pure Flink Processing Time)"
    echo "=========================================="
    echo "Total records:    $TOTAL_RECORDS"
    echo "Measured records: $MEASURED_RECORDS (excluding warmup)"
    echo "Pure time:        $PURE_TIME seconds"
    echo "Throughput:       $THROUGHPUT rec/sec"
    echo "=========================================="
else
    echo ""
    echo "=========================================="
    echo "ERROR: Could not find markers in output"
    echo "=========================================="
    echo "Make sure to delete topics before running!"
    echo "START found: $([ -n "$START_EPOCH" ] && echo 'YES' || echo 'NO')"
    echo "END found:   $([ -n "$END_EPOCH" ] && echo 'YES' || echo 'NO')"
fi
