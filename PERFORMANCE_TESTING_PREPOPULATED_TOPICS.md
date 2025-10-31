# Performance Testing with Pre-populated Topics

## Overview

This guide explains how to measure Flink job consumption performance by pre-populating Kafka topics with data, then starting the job to measure processing time.

**Use Case:** Measure how long it takes to consume and process X number of records at different parallelism levels.

---

## THE PROBLEM WITH CURRENT IMPLEMENTATION

With CoFlatMap, you **CANNOT** just pre-populate data and measure consumption time because:
- Data consumed immediately when job starts
- Config arrives later → race condition
- Most events dropped before config arrives

---

## THE SOLUTION: Pre-populate BOTH topics!

### Testing Strategy:

```
1. Pre-populate BOTH config AND data topics (job NOT running)
   ↓
2. Start Flink job (reads both from beginning)
   ↓
3. Measure time from job start until all records processed
```

**This works because:**
- `auto.offset.reset=earliest` → reads from beginning
- Config topic has 1 record → consumed in milliseconds
- Data topic has N records → consumed and processed
- **Config arrives BEFORE data events get processed!** ✅

---

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

### **3. Create helper script and pre-populate CONFIG topic:**

**First, create a reusable script to send configs to Kafka:**

```bash
cd /home/avoutas/boutasThesis

# Create the helper script (one-time setup)
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

# Extract jobId from config file
JOB_ID=$(grep -o '"jobId":"[^"]*"' $CONFIG_FILE | cut -d'"' -f4)

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

# Make it executable
chmod +x send-config-to-kafka.sh
```

**Now use the script to send any config:**

```bash
cd /home/avoutas/boutasThesis

# Send config-elements.json
./send-config-to-kafka.sh config-elements.json

# Or send config-test123.json
./send-config-to-kafka.sh config-test123.json

# Or any other config file
./send-config-to-kafka.sh config-cold-humid-rooms.json
```

**Benefits of this approach:**
- ✅ Directly references your config files (no copy-paste JSON)
- ✅ Automatically extracts jobId as Kafka key
- ✅ Reusable for all config files
- ✅ Clean and maintainable

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
    --from-beginning \
    --max-messages 5
```

### **5. NOW start Flink job with timing:**

```bash
cd /usr/local/flink

# Record start time
START_TIME=$(date +%s)
echo "Job started at: $(date)"

./bin/flink run -p 11 -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1
```

### **6. Monitor output topic to detect completion:**

```bash
cd /usr/hdp/current/kafka-broker

# Count messages in output (run in loop)
while true; do
    COUNT=$(bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --broker-list clu02.softnet.tuc.gr:6667 \
        --topic etl.output.v1 \
        --time -1 | awk -F: '{sum += $3} END {print sum}')

    echo "$(date): Output records = $COUNT"

    # Stop when count stabilizes (no new records for 10 seconds)
    sleep 10
done

# When done, record end time
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "Processing completed in $DURATION seconds"
```

---

## PERFORMANCE METRICS

**You can measure:**

1. **Total Processing Time:** Start → Finish (seconds)
2. **Throughput:** Records / Time (events/sec)
3. **Parallelism Impact:** Test with -p 4, 11, 22, 33
4. **Scalability:** Test with 10k, 100k, 1M records

**Example Results:**
```
Test 1: 100k records, -p 11 → 25 seconds → 4,000 events/sec
Test 2: 100k records, -p 22 → 15 seconds → 6,666 events/sec
Test 3: 100k records, -p 33 → 12 seconds → 8,333 events/sec
```

---

## TESTING DIFFERENT PARALLELISM LEVELS

For each test:

```bash
# 1. Stop previous job
cd /usr/local/flink
./bin/flink list
./bin/flink cancel <JOB-ID>

# 2. Reset consumer group offsets (re-read same data)
cd /usr/hdp/current/kafka-broker
bin/kafka-consumer-groups.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --group etl-flink-consumer \
    --topic etl.input.v1 \
    --reset-offsets --to-earliest \
    --execute

bin/kafka-consumer-groups.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --group etl-flink-consumer \
    --topic etl.config.v1 \
    --reset-offsets --to-earliest \
    --execute

# 3. Clean output topic
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.output.v1
sleep 5
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.output.v1

# 4. Start job with new parallelism and time it
cd /usr/local/flink
START_TIME=$(date +%s)
./bin/flink run -p 22 -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1

# 5. Monitor and record end time (as above)
```

---

## TESTING DIFFERENT DATA SIZES

```bash
# Small: 10,000 records
--producer.max.records=10000

# Medium: 100,000 records
--producer.max.records=100000

# Large: 1,000,000 records
--producer.max.records=1000000
```

For each size:
1. Reset all topics (Step 1)
2. Pre-populate with new size (Step 2)
3. Pre-populate config (Step 3)
4. Start job and measure (Steps 5-6)

---

## ALTERNATIVE APPROACH: Broadcast State Pattern

**If the pre-populate approach has race conditions:**

The current CoFlatMap implementation might still have timing issues where config and data race depending on partition distribution.

**Better solution:** Modify code to use **Broadcast State Pattern**
- Config loaded as broadcast state (guaranteed to all subtasks before processing)
- Eliminates race condition entirely
- More complex implementation but bulletproof

**Trade-offs:**
- **Pre-populate approach:** Simple, works with existing code ✅
- **Broadcast state:** More reliable, requires code changes ⚠️

---

## TROUBLESHOOTING

**Problem:** Job processes zero events even with pre-populated data

**Cause:** Consumer group offsets already committed

**Solution:** Reset offsets before starting job:
```bash
bin/kafka-consumer-groups.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --group etl-flink-consumer \
    --delete
```

---

**Problem:** Config arrives after some data already processed

**Cause:** Config topic read slower than data topic

**Solution:**
1. Use smaller data batches initially to verify
2. Consider broadcast state pattern for production testing

---

## EXPECTED RESULTS

With 100,000 records and element-only config (temperature > 30°C):
- **Input:** 100,000 events
- **Temperature events:** ~16,666 (1/6 of total)
- **After filter:** ~3,000 outputs (18% pass threshold)
- **Processing time:** Varies by parallelism

Performance should scale linearly with parallelism up to the number of Kafka partitions (4 in our case).
