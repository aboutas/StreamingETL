################################################################################
# ETL FLINK PROJECT - RUN GUIDE
# Copy-paste commands as needed. Not a script file.
################################################################################

################################################################################
# PHASE 1: BUILD PROJECT (Ubuntu VM)
################################################################################

cd ~/Documents/etl-flink-project
git pull origin cluster
mvn clean package -DskipTests

# Creates:
# - flink/target/etl-flink-1.0.0.jar
# - services/etl-api/target/etl-api-1.0.0.jar
# - services/etl-file-producer/target/etl-file-producer-1.0.0.jar

################################################################################
# PHASE 2: TRANSFER TO CLUSTER (From VM)
################################################################################

cd ~/Documents/etl-flink-project
scp flink/target/etl-flink-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp services/etl-api/target/etl-api-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp services/etl-file-producer/target/etl-file-producer-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp config-*.json avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp run-benchmark.sh avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/

################################################################################
# PHASE 3: CONNECT TO CLUSTER
################################################################################

ssh avoutas@clu04.softnet.tuc.gr
cd /home/avoutas/boutasThesis
chmod +x run-benchmark.sh

################################################################################
# PHASE 4: RUN AUTOMATED BENCHMARK (RECOMMENDED)
################################################################################
#
# run-benchmark.sh does everything automatically:
#   1. Stops existing Flink jobs
#   2. Resets all Kafka topics (delete + recreate)
#   3. Starts API & submits 4 configs
#   4. Produces N records at max speed
#   5. Deletes consumer group for clean start
#   6. Starts Flink job
#   7. Measures pure processing time (excludes Flink init)
#
# Usage: ./run-benchmark.sh <parallelism> [records]
# Default records: 2000000

cd /home/avoutas/boutasThesis

./run-benchmark.sh 4               # p=4,  2M records
./run-benchmark.sh 4 500000        # p=4,  500K records
./run-benchmark.sh 8 1000000       # p=8,  1M records
./run-benchmark.sh 11 2000000      # p=11, 2M records
./run-benchmark.sh 22 2000000      # p=22, 2M records

# Output shows:
#   - Input records (actual count from topic)
#   - Output records
#   - Pure processing time (seconds)
#   - Throughput (records/sec)

# FOR SUBSEQUENT TESTS: just run again, it resets everything automatically.

################################################################################
# MANUAL STEP-BY-STEP (for debugging or custom tests)
################################################################################
#
# Use this when you need more control (custom configs, verify topics, etc.)
#

### Step 1: Reset topics

cd /usr/hdp/current/kafka-broker

# Reset consumer group
bin/kafka-consumer-groups.sh --bootstrap-server clu02.softnet.tuc.gr:6667 --group etl-flink-consumer --delete 2>/dev/null || true

# Delete and recreate all topics
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.input.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.config.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.output.v1 2>/dev/null || true
sleep 5
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.input.v1
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.config.v1
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.output.v1

### Step 2: Verify topics are empty (optional)

cd /usr/hdp/current/kafka-broker
bin/kafka-topics.sh --list --zookeeper clu01.softnet.tuc.gr:2182 | grep etl

bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list clu02.softnet.tuc.gr:6667 \
    --topic etl.input.v1 \
    --time -1 | awk -F: '{sum += $3} END {print "etl.input.v1: " sum " records"}'

bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list clu02.softnet.tuc.gr:6667 \
    --topic etl.config.v1 \
    --time -1 | awk -F: '{sum += $3} END {print "etl.config.v1: " sum " records"}'

bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list clu02.softnet.tuc.gr:6667 \
    --topic etl.output.v1 \
    --time -1 | awk -F: '{sum += $3} END {print "etl.output.v1: " sum " records"}'

### Step 3: Start API & submit configs

cd /home/avoutas/boutasThesis

# Start API in background
java -jar etl-api-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic=etl.config.v1 \
    --server.port=8080 &
sleep 10

# Submit configs (choose how many you need for the test)
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-clean-data.json
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-elements.json
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-9.json
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-high-light.json

# Expected response per config: {"jobId":"...","status":"REGISTERED",...}

### Step 4: Produce data

cd /home/avoutas/boutasThesis

# Change --producer.max.records for different test sizes
# Producer generates 48 records/cycle (8 locations x 6 sensor types)
# With rate=50000 it produces at maximum speed
java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.input.topic=etl.input.v1 \
    --producer.max.records=500000 \
    --producer.rate.per.sec=50000

# Wait for completion (logs: "Random data production COMPLETED")
# Producer exits automatically when max records reached

### Step 5: Stop existing Flink jobs

cd /usr/local/flink
./bin/flink list
./bin/flink cancel <JOB-ID>

### Step 6: Delete consumer group (clean offsets)

cd /usr/hdp/current/kafka-broker
bin/kafka-consumer-groups.sh --bootstrap-server clu02.softnet.tuc.gr:6667 --group etl-flink-consumer --delete 2>/dev/null || true

### Step 7: Start Flink job

cd /usr/local/flink
./bin/flink run -p 4 -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1

# Verify: ./bin/flink list  (should show RUNNING)
# Dashboard: http://clu01.softnet.tuc.gr:8081

### Step 8: Monitor processing

cd /usr/hdp/current/kafka-broker

# Watch consumer lag (offset vs end-offset)
bin/kafka-consumer-groups.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --group etl-flink-consumer \
    --describe

# Watch output topic
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.output.v1 \
    --from-beginning

################################################################################
# HELPER: SEND CONFIG DIRECTLY TO KAFKA (without API)
################################################################################
#
# Alternative to using the API. Sends config JSON directly to Kafka topic.
# Useful when API is not running or for quick tests.
#
# ONE-TIME SETUP: Create this script on the cluster

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

# Extract jobId from config file
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

echo "Config sent to Kafka: jobId=$JOB_ID"
SCRIPT

chmod +x send-config-to-kafka.sh

# Usage:
./send-config-to-kafka.sh config-elements.json
./send-config-to-kafka.sh config-9.json

################################################################################
# STOP SERVICES
################################################################################

# Stop Flink job
cd /usr/local/flink
./bin/flink list
./bin/flink cancel <JOB-ID>

# Stop ETL API
ps aux | grep etl-api
kill <PID>

# File Producer stops automatically when --producer.max.records=N is set
# Only kill if running in continuous mode (--producer.max.records=-1):
# ps aux | grep etl-file-producer
# kill <PID>

################################################################################
# PARALLELISM OPTIONS
################################################################################
#
#   -p 4   = Minimal (for testing)
#   -p 11  = Baseline (1 task per worker)
#   -p 22  = High load (2 tasks per worker)
#   -p 33  = Maximum (3 tasks per worker, all slots)
#
# Change: cancel job and resubmit with different -p value

################################################################################
# TEST PARAMETERS SUMMARY
################################################################################
#
# | Parameter     | Where to Change                              | Examples                |
# |---------------|----------------------------------------------|-------------------------|
# | Records       | run-benchmark.sh 2nd arg / --producer.max.records | 100000, 500000, 2000000 |
# | Configs       | run-benchmark.sh has 4 hardcoded / manual: choose | 1, 2, 4 configs         |
# | Parallelism   | run-benchmark.sh 1st arg / flink run -p N    | 4, 8, 11, 22           |
#
# Producer properties:
#   --producer.max.records=N     Number of records to produce (-1 = unlimited)
#   --producer.rate.per.sec=N    Target rate (50000 = max speed, default 10)

################################################################################
# TROUBLESHOOTING
################################################################################
#
# Job keeps restarting / Data Source FAILED:
#   -> Check Flink dashboard: http://clu01.softnet.tuc.gr:8081
#   -> Verify command-line arguments
#   -> Check TaskManager logs for Kafka connection errors
#
# Permission denied on Flink logs:
#   -> export FLINK_LOG_DIR=~/flink-logs && mkdir -p ~/flink-logs
#
# API not responding:
#   -> ps aux | grep etl-api
#   -> netstat -tuln | grep 8080
#
# No output in Kafka:
#   -> Check Flink job: cd /usr/local/flink && ./bin/flink list
#   -> Check input topic has data (see Step 2 verify commands above)
