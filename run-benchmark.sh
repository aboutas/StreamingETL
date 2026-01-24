cd /home/avoutas/boutasThesis

cat > run-benchmark.sh << 'SCRIPT'
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

# RESULT
PURE_TIME=$((LAST_TIME - FIRST_TIME))
if [ "$PURE_TIME" -gt 0 ]; then
    THROUGHPUT=$((TOTAL_RECORDS / PURE_TIME))
else
    THROUGHPUT=0
fi

echo ""
echo "=========================================="
echo "RESULT"
echo "=========================================="
echo "Records:      $TOTAL_RECORDS"
echo "Pure time:    $PURE_TIME seconds"
echo "Throughput:   $THROUGHPUT rec/sec"
echo "=========================================="
SCRIPT

chmod +x run-benchmark.sh
