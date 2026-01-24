################################################################################
# ETL FLINK PROJECT - COMPLETE BENCHMARK GUIDE
# This is a REFERENCE file - copy-paste commands as needed
################################################################################

################################################################################
# PHASE 1: BUILD PROJECT (Ubuntu VM - VS Code Terminal)
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

# Upload JARs
scp flink/target/etl-flink-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp services/etl-api/target/etl-api-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp services/etl-file-producer/target/etl-file-producer-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/

# Upload configs
scp config-*.json avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/

# Upload scripts
scp run-benchmark.sh avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/

################################################################################
# PHASE 3: CONNECT TO CLUSTER
################################################################################

ssh avoutas@clu04.softnet.tuc.gr
cd /home/avoutas/boutasThesis

# Make scripts executable (first time only)
chmod +x run-benchmark.sh

################################################################################
# PHASE 4: CREATE KAFKA TOPICS (FIRST TIME ONLY)
################################################################################

cd /usr/hdp/current/kafka-broker

bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 \
    --replication-factor 2 --partitions 4 --topic etl.input.v1

bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 \
    --replication-factor 2 --partitions 4 --topic etl.config.v1

bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 \
    --replication-factor 2 --partitions 4 --topic etl.output.v1

# Verify
bin/kafka-topics.sh --list --zookeeper clu01.softnet.tuc.gr:2182 | grep etl

################################################################################
# PHASE 5: RESET KAFKA TOPICS (FOR EACH NEW TEST)
################################################################################

cd /usr/hdp/current/kafka-broker

# Delete topics
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.input.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.config.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.output.v1 2>/dev/null || true
sleep 5

# Recreate topics
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.input.v1
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.config.v1
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.output.v1

# Verify empty (should show 0 records)
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

################################################################################
# PHASE 6: START ETL API & SUBMIT CONFIG
################################################################################

cd /home/avoutas/boutasThesis

# Start API in background (ONLY ONCE per testing session!)
# For subsequent tests, skip this and go directly to curl commands
java -jar etl-api-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic=etl.config.v1 \
    --server.port=8080 &

# Wait for startup
sleep 10

# Check if API is running: ps aux | grep etl-api

# Submit config (choose one or more)
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-test123.json
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-max-temp.json
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-high-temp.json
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-high-light.json
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-clean-data.json

################################################################################
# PHASE 7: PRODUCE TEST DATA
################################################################################

cd /home/avoutas/boutasThesis

# Test sizes:
#   1000     = Quick test
#   10000    = Medium test
#   100000   = Large test
#   1000000  = Performance test
#   2000000  = Full benchmark

java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.input.topic=etl.input.v1 \
    --producer.max.records=2000000

# Wait for: "Random data production COMPLETED. Sent exactly 2000000 records"

################################################################################
# PHASE 8: RUN BENCHMARK (with accurate timing)
################################################################################

cd /home/avoutas/boutasThesis

# Parallelism options:
#   4  = Minimal (testing)
#   11 = Baseline (1 task per worker) - RECOMMENDED
#   22 = High load (2 tasks per worker)
#   33 = Maximum (3 tasks per worker)

./run-benchmark.sh 11

# The script will:
# 1. Stop existing Flink jobs
# 2. Clean checkpoints
# 3. Reset consumer group
# 4. Verify Kafka topics
# 5. Submit Flink job
# 6. Monitor progress
# 7. Calculate PURE processing time (from output timestamps)

# OUTPUT EXAMPLE:
# ==========================================
# CONSUMPTION COMPLETED
# ==========================================
# Total records in Kafka:  2000000
# Total records processed: 2000000
#
# --- OLD MEASUREMENT (includes init time) ---
# Duration (submit->done): 45s
# Throughput:             44444 rec/sec
#
# --- NEW MEASUREMENT (pure processing time) ---
# Pure processing time:    30.00s       <-- USE THIS FOR THESIS
# Pure throughput:         66666 rec/sec <-- USE THIS FOR THESIS
#
# --- ANALYSIS ---
# Init overhead:           15.00s (33.3% of total)

