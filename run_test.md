## STEP-BY-STEP TESTING PROCEDURE

### **1. Prepare clean topics (job stopped):**

```bash
cd /usr/hdp/current/kafka-broker

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

### **2. Pre-populate DATA topic (100k records):**

```bash
cd /home/avoutas/boutasThesis

java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.input.topic=etl.input.v1 \
    --producer.mode=random \
    --producer.max.records=100000

# Wait for completion (logs: "Random data production COMPLETED")
```

### **3. Create helper scripts (ONE-TIME SETUP):**

**Create script to send configs to Kafka:**

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

**Create monitoring script for Step 6:**

```bash
cd /home/avoutas/boutasThesis

cat > monitor-completion.sh << 'SCRIPT'
#!/bin/bash

cd /usr/hdp/current/kafka-broker

START_TIME=$(cat /home/avoutas/boutasThesis/start_time.txt)

echo "Job start time loaded: $(date -d @$START_TIME)"
echo "Starting monitoring..."

PREV_COUNT=0
STABLE_COUNT=0

while true; do
    # Get input topic total records
    INPUT_COUNT=$(bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list clu02.softnet.tuc.gr:6667 \
        --topic etl.input.v1 \
        --time -1 | awk -F: '{sum += $3} END {print sum}')

    # Get output topic records
    OUTPUT_COUNT=$(bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list clu02.softnet.tuc.gr:6667 \
        --topic etl.output.v1 \
        --time -1 | awk -F: '{sum += $3} END {print sum}')

    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    echo "$(date): Input records = $INPUT_COUNT | Output records = $OUTPUT_COUNT (elapsed: ${ELAPSED}s)"

    # Check if stable for 3 iterations (30 seconds)
    if [ "$OUTPUT_COUNT" == "$PREV_COUNT" ] && [ "$OUTPUT_COUNT" != "0" ]; then
        STABLE_COUNT=$((STABLE_COUNT + 1))
        if [ $STABLE_COUNT -ge 3 ]; then
            END_TIME=$CURRENT_TIME
            DURATION=$((END_TIME - START_TIME))
            echo ""
            echo "============================================"
            echo "✅ PROCESSING COMPLETE!"
            echo "Duration: $DURATION seconds"
            echo "Input records: $INPUT_COUNT"
            echo "Output records: $OUTPUT_COUNT"
            echo "============================================"
            break
        fi
    else
        STABLE_COUNT=0
    fi

    PREV_COUNT=$OUTPUT_COUNT
    sleep 10
done
SCRIPT

chmod +x monitor-completion.sh
```

**Send configs (do this for each test):**

```bash
cd /home/avoutas/boutasThesis

# Send one or more configs:
./send-config-to-kafka.sh config-elements.json
./send-config-to-kafka.sh config-test123.json
./send-config-to-kafka.sh config-cold-humid-rooms.json
```

### **4. Verify both topics have data (job still NOT running):**

```bash
# Check config topic
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.config.v1 \
    --from-beginning \
    --timeout-ms 3000

# Check input topic (first 5)
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.input.v1 \
    --from-beginning
    --max-messages 5
```

### **5. NOW start Flink job with timing:**

```bash
cd /usr/local/flink

# Record start time to file (so it can be read from different terminal tabs)
START_TIME=$(date +%s)
echo $START_TIME > /home/avoutas/boutasThesis/start_time.txt
echo "Job started at: $(date -d @$START_TIME)"

./bin/flink run -p 4 -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1
```

**Note:** You can run Step 6 immediately or later - timing is always calculated from this start time.

### **6. Monitor output topic to detect completion (AUTOMATIC):**

**Run this in a different terminal tab (no need to hurry after Step 5):**

```bash
cd /home/avoutas/boutasThesis

# Simply run the monitoring script
./monitor-completion.sh