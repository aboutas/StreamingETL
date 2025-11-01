## STEP-BY-STEP TESTING PROCEDURE

### **1. Prepare clean topics (job stopped):**

```bash
cd /usr/hdp/current/kafka-broker

# IMPORTANT: Reset consumer group offsets first
bin/kafka-consumer-groups.sh --bootstrap-server clu02.softnet.tuc.gr:6667 --group etl-flink-consumer --delete 2>/dev/null || true

# Delete and recreate all topics
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.input.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.config.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.output.v1 2>/dev/null || true

sleep 5

bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.input.v1
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.config.v1
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

### **2. Pre-populate DATA topic (variable record count for different tests):**

```bash
cd /home/avoutas/boutasThesis

# Change --producer.max.records parameter for different test scenarios
# Example: 100000, 500000, 1000000, etc.

java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.input.topic=etl.input.v1 \
    --producer.mode=random \
    --producer.max.records=100000

# Wait for completion (logs: "Random data production COMPLETED")
```

### **3. Create helper scripts (ONE-TIME SETUP):**

**Script 1: Send configs to Kafka**

```bash
cd /home/avoutas/boutasThesis

cat > send-config-to-kafka.sh << 'SCRIPT'
#!/bin/bash
CONFIG_FILE=$1

if [ -z "$CONFIG_FILE" ]; then
    echo "Usage: $0 <config-file.json>"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found: $CONFIG_FILE"
    exit 1
fi

# Extract jobId from config file (handles spaces around colon)
JOB_ID=$(grep -o '"jobId"[[:space:]]*:[[:space:]]*"[^"]*"' $CONFIG_FILE | sed 's/.*"\([^"]*\)".*/\1/')

if [ -z "$JOB_ID" ]; then
    echo "Error: Could not extract jobId from config file"
    exit 1
fi

# Read config JSON (remove newlines for single-line transmission)
JSON=$(cat $CONFIG_FILE | tr -d '\n')

# Send to Kafka with jobId as key
echo "$JOB_ID:$JSON" | \
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh \
    --broker-list clu02.softnet.tuc.gr:6667 \
    --topic etl.config.v1 \
    --property "parse.key=true" \
    --property "key.separator=:"

echo "✅ Config sent to Kafka: jobId=$JOB_ID"
SCRIPT

chmod +x send-config-to-kafka.sh
```

**Script 2: Benchmark runner (starts job + monitors)**

```bash
cd /home/avoutas/boutasThesis

cat > run-benchmark.sh << 'SCRIPT'
#!/bin/bash

# Usage: ./run-benchmark.sh <parallelism>
# Example: ./run-benchmark.sh 4

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <parallelism>"
    echo "Example: $0 4"
    exit 1
fi

PARALLELISM=$1

echo "=========================================="
echo "Starting benchmark test"
echo "Parallelism: $PARALLELISM"
echo "=========================================="
echo ""

# Stop any existing Flink jobs first
echo "Stopping any existing Flink jobs..."
cd /usr/local/flink
for job in $(./bin/flink list -r 2>/dev/null | grep -v "No running jobs" | awk 'NR>3 {print $4}'); do
    echo "Cancelling job: $job"
    ./bin/flink cancel $job
done
sleep 3

# Reset consumer group offsets for clean start
echo "Resetting consumer group offsets..."
/usr/hdp/current/kafka-broker/bin/kafka-consumer-groups.sh --bootstrap-server clu02.softnet.tuc.gr:6667 --group etl-flink-consumer --delete 2>/dev/null || true
sleep 2

# Record start time
START_TIME=$(date +%s)
echo "Start time: $(date -d @$START_TIME)"

# Start Flink job
cd /usr/local/flink
echo "Starting NEW Flink job with parallelism=$PARALLELISM..."

./bin/flink run -p $PARALLELISM -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1

echo "Job submitted!"
echo ""
echo "Monitoring consumer lag..."

# Switch to Kafka directory
cd /usr/hdp/current/kafka-broker

CONSUMER_GROUP="etl-flink-consumer"
PREV_OFFSET=0
STABLE_COUNT=0
COMPLETION_TIME=0

sleep 2  # Give job time to start

while true; do
    # Get current consumer offset (total records consumed so far)
    CURRENT_OFFSET=$(bin/kafka-consumer-groups.sh \
        --bootstrap-server clu02.softnet.tuc.gr:6667 \
        --group $CONSUMER_GROUP \
        --describe 2>/dev/null | \
        grep etl.input.v1 | \
        awk '{sum += $3} END {print sum}')

    # Skip if consumer group not yet created
    if [ -z "$CURRENT_OFFSET" ]; then
        sleep 1
        continue
    fi

    # Get current lag
    TOTAL_LAG=$(bin/kafka-consumer-groups.sh \
        --bootstrap-server clu02.softnet.tuc.gr:6667 \
        --group $CONSUMER_GROUP \
        --describe 2>/dev/null | \
        grep etl.input.v1 | \
        awk '{sum += $5} END {print sum}')

    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))

    echo "$(date): Offset = $CURRENT_OFFSET | Lag = $TOTAL_LAG (elapsed: ${ELAPSED}s)"

    # Detect when offset stops increasing (job finished consuming)
    if [ "$CURRENT_OFFSET" == "$PREV_OFFSET" ] && [ "$CURRENT_OFFSET" != "0" ]; then
        STABLE_COUNT=$((STABLE_COUNT + 1))

        # Record completion time on first detection
        if [ $STABLE_COUNT -eq 1 ]; then
            COMPLETION_TIME=$CURRENT_TIME
        fi

        # Confirm stable for 3 checks (3 seconds)
        if [ $STABLE_COUNT -ge 3 ]; then
            DURATION=$((COMPLETION_TIME - START_TIME))
            echo ""
            echo "============================================"
            echo "✅ ALL INPUT RECORDS CONSUMED!"
            echo "Total records processed: $CURRENT_OFFSET"
            echo "Duration: $DURATION seconds"
            echo "Parallelism: $PARALLELISM"
            echo "Throughput: $(awk "BEGIN {printf \"%.2f\", $CURRENT_OFFSET/$DURATION}") records/sec"
            echo "============================================"
            break
        fi
    else
        STABLE_COUNT=0
    fi

    PREV_OFFSET=$CURRENT_OFFSET
    sleep 1
done
SCRIPT

chmod +x run-benchmark.sh
```

### **4. Send configs to Kafka (VARIES BY TEST):**

```bash
cd /home/avoutas/boutasThesis

# Example 1: Test with 1 config
./send-config-to-kafka.sh config-elements.json

# Example 2: Test with 2 configs
./send-config-to-kafka.sh config-elements.json
./send-config-to-kafka.sh config-test123.json

# Example 3: Test with 4 configs
./send-config-to-kafka.sh config-elements.json
./send-config-to-kafka.sh config-test123.json
./send-config-to-kafka.sh config-cold-humid-rooms.json
./send-config-to-kafka.sh config-light-high-humidity.json
```

### **5. Verify topics have data (OPTIONAL):**

```bash
cd /usr/hdp/current/kafka-broker

# Check config topic
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.config.v1 \
    --from-beginning \
    --timeout-ms 3000

# Check input topic record count
bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list clu02.softnet.tuc.gr:6667 \
    --topic etl.input.v1 \
    --time -1 | awk -F: '{sum += $3} END {print "etl.input.v1: " sum " records"}'
```

### **6. Run benchmark test (SINGLE COMMAND - NO TERMINAL SWITCHING!):**

```bash
cd /home/avoutas/boutasThesis

# Usage: ./run-benchmark.sh <parallelism>

# Examples:
./run-benchmark.sh 4      # Parallelism=4
./run-benchmark.sh 8      # Parallelism=8
./run-benchmark.sh 16     # Parallelism=16
```

**ONE script, ONE command - no terminals, no log files, just the result!**
**Shows: Total records, Duration, Parallelism, Throughput**

---

## COMPLETE WORKFLOW SUMMARY

**Simple 6-step process:**

1. **Clean topics** (Step 1)
2. **Populate input** with X records (Step 2)
3. **Create scripts** - one-time setup (Step 3)
4. **Send configs** - 1, 2, 4, or 8 configs (Step 4)
5. **Verify** - optional (Step 5)
6. **Run benchmark** - `./run-benchmark.sh <parallelism>` (Step 6)

**Test parameters:**

| Parameter | Where to Change | Examples |
|-----------|----------------|----------|
| **Input Records** | Step 2: `--producer.max.records` | 100000, 500000, 1000000 |
| **Config Count** | Step 4: Number of `./send-config-to-kafka.sh` calls | 1, 2, 4, 8 configs |
| **Parallelism** | Step 6: `./run-benchmark.sh X` | 4, 8, 16 |

**Example tests:**
- `./run-benchmark.sh 4` → 100k records, 1 config, p=4
- `./run-benchmark.sh 8` → 100k records, 4 configs, p=8
- `./run-benchmark.sh 16` → 500k records, 8 configs, p=16