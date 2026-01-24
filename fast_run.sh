################################################################################
# ETL FLINK PROJECT - BENCHMARK GUIDE
# Copy-paste commands as needed
################################################################################

################################################################################
# PHASE 1: BUILD PROJECT (Ubuntu VM)
################################################################################

cd ~/Documents/etl-flink-project
git pull origin cluster
mvn clean package -DskipTests

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
# PHASE 4: RESET TOPICS (REQUIRED BEFORE EACH NEW TEST!)
################################################################################

cd /usr/hdp/current/kafka-broker

# Delete ALL topics
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.input.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.config.v1 2>/dev/null || true
bin/kafka-topics.sh --delete --zookeeper clu01.softnet.tuc.gr:2182 --topic etl.output.v1 2>/dev/null || true
sleep 5

# Recreate topics
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.input.v1
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.config.v1
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 --replication-factor 2 --partitions 4 --topic etl.output.v1

# Check records in topics
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list clu02.softnet.tuc.gr:6667 --topic etl.input.v1 --time -1 | awk -F: '{sum += $3} END {print sum}'
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list clu02.softnet.tuc.gr:6667 --topic etl.config.v1 --time -1 | awk -F: '{sum += $3} END {print sum}'
bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list clu02.softnet.tuc.gr:6667 --topic etl.output.v1 --time -1 | awk -F: '{sum += $3} END {print sum}'

################################################################################
# PHASE 5: START API & SUBMIT CONFIGS
################################################################################

cd /home/avoutas/boutasThesis

# Start API (only once per session)
java -jar etl-api-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic=etl.config.v1 \
    --server.port=8080 &
sleep 10

# Submit configs (element transformations only)
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-clean-data.json
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-elements.json
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-9.json
curl -X POST http://clu04.softnet.tuc.gr:8080/config -H "Content-Type: application/json" -d @config-high-light.json

################################################################################
# PHASE 6: PRODUCE DATA
################################################################################

cd /home/avoutas/boutasThesis

# Producer sends: 5000 warmup + START marker + data + END marker
# Warmup records allow Flink to load configs before timing starts

java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.input.topic=etl.input.v1 \
    --producer.max.records=500000

################################################################################
# PHASE 7: RUN BENCHMARK
################################################################################

cd /home/avoutas/boutasThesis
./run-benchmark.sh 4

# Output shows:
# - Total records
# - Measured records (excluding warmup)
# - Pure time (from START to END marker processedAt)
# - Throughput

################################################################################
# FOR SUBSEQUENT TESTS: GO TO PHASE 4 (RESET TOPICS) AND REPEAT
################################################################################
