# ETL Flink Project - SoftNet Cluster Deployment (v1.0.0)

A production-ready real-time ETL (Extract, Transform, Load) system built with Apache Flink that processes sensor data streams with dynamically configurable transformations and TRUE parallelism using CoFlatMap architecture with 3-second windowing.

**Deployed on:** SoftNet Cluster (TUC) with 23 servers, HDP 3.1.0, Flink 1.9.3
**Compiled with:** Java 8, Spring Boot 2.7.18, Kafka 2.4.1

**Status:** ✅ VERIFIED WORKING - Complete end-to-end data flow validated on cluster

---

## 🚀 Quick Start (SoftNet Cluster)

### Prerequisites
- Access to SoftNet cluster (clu04.softnet.tuc.gr)
- TUC VPN connection (if outside TUC network)
- Maven 3.x for building JARs

### Build & Deploy

```bash
# 1. Build all JARs (on your VM or local machine)
cd /path/to/etl-flink-project
git pull origin cluster
mvn clean package -DskipTests

# 2. Upload to cluster (replace 'avoutas' with your username)
scp flink/target/etl-flink-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp services/etl-api/target/etl-api-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp services/etl-file-producer/target/etl-file-producer-1.0.0.jar avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/
scp config-*.json avoutas@clu04.softnet.tuc.gr:/home/avoutas/boutasThesis/

# 3. SSH to cluster
ssh avoutas@clu04.softnet.tuc.gr

# 4. Submit Flink job (command-line arguments required!)
cd /usr/local/flink/
./bin/flink run -p 11 -d /home/avoutas/boutasThesis/etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1

# Verify job is RUNNING (not restarting)
./bin/flink list

# 5. Start ETL API
cd /home/avoutas/boutasThesis
java -jar etl-api-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.config.topic=etl.config.v1 \
    --server.port=8080 &

# Wait 10 seconds for API to start
sleep 10

# 6. Start File Producer (generates random sensor data)
java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --kafka.input.topic=etl.input.v1 \
    --producer.mode=random \
    --producer.rate.per.sec=10 &

# Wait 10 seconds for producer to start
sleep 10

# 7. Submit test config
curl -X POST http://clu04.softnet.tuc.gr:8080/config \
    -H "Content-Type: application/json" \
    -d @config-test123.json

# 8. Monitor output (wait 10-15 seconds for aggregation window)
cd /usr/hdp/current/kafka-broker
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.output.v1 \
    --from-beginning
```

**Expected Output:**
- Config confirmation (immediate)
- Aggregated results every 3 seconds

**Flink Dashboard:** http://clu01.softnet.tuc.gr:8081 (verify all operators show Records > 0)

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│           ETL FLINK PROJECT - SOFTNET CLUSTER DEPLOYMENT             │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│  ETL API         │    │ File Producer    │    │ SoftNet Flink       │
│  (clu04:8080)    │    │ (clu04)          │    │ Cluster             │
│  Config Submit   │    │ Sensor Data      │    │                     │
│                  │    │ Stream           │    │ Master: clu01:8081  │
└─────────┬────────┘    └─────────┬────────┘    │ Workers: 11 nodes   │
          │                       │             │ Slots: 33 total     │
          │                       │             └─────────┬───────────┘
          ▼                       ▼                       │
┌─────────────────────────────────────────────┐           │
│        SoftNet Kafka Cluster (HDP)          │           │
│  Brokers: clu02:6667, clu03:6667,           │           │
│           clu04:6667, clu06:6667            │           │
│                                             │           │
│  Topics:                                    │           │
│  • etl.config.v1  ◄─────────────────────────┼───────────┤
│  • etl.input.v1   ◄─────────────────────────┤           │
│  • etl.output.v1  ─────────────────────────►│           │
└─────────────────────────────────────────────┘           │
          ▲                                               │
          │                                               │
          └───────────────────────────────────────────────┘
                       Flink Job Processes
                    (CoFlatMap + Windowing)
```

---

## 📦 Project Components

### 1. Flink Job (`flink/`)
- **Main Class:** `com.etl.flink.EtlFlinkJob`
- **JAR:** `etl-flink-1.0.0.jar`
- **Purpose:** Stream processing with transformations and aggregations
- **Parallelism:** Configurable (default: 4, recommended: 11 for SoftNet)
- **Sink:** Kafka topic `etl.output.v1`

### 2. ETL API (`services/etl-api/`)
- **Port:** 8080
- **JAR:** `etl-api-1.0.0.jar`
- **Purpose:** REST API to submit ETL configurations
- **Endpoint:** `POST /config`

### 3. File Producer (`services/etl-file-producer/`)
- **JAR:** `etl-file-producer-1.0.0.jar`
- **Purpose:** Streams sensor data from JSON files to Kafka
- **Rate:** Configurable (default: 10 msg/sec)

---

## 🔧 Configuration

### Command-Line Arguments (Required for Cluster Deployment)

**IMPORTANT:** Command-line arguments are distributed to all Flink TaskManagers automatically. Environment variables DO NOT work in distributed Flink deployments.

**Flink Job:**
- `--kafka.bootstrap.servers` (default: `clu02.softnet.tuc.gr:6667,...`)
- `--kafka.config.topic` (default: `etl.config.v1`)
- `--kafka.input.topic` (default: `etl.input.v1`)
- `--kafka.output.topic` (default: `etl.output.v1`)

**ETL API:**
- `--kafka.bootstrap.servers` (required)
- `--kafka.config.topic` (default: `etl.config.v1`)
- `--server.port` (default: `8080`)

**File Producer:**
- `--kafka.bootstrap.servers` (required)
- `--kafka.input.topic` (default: `etl.input.v1`)
- `--producer.mode` (required: `random` or `file`)
- `--producer.rate.per.sec` (default: `10`)

### SoftNet Cluster Specifics

**Kafka Brokers:**
```
clu02.softnet.tuc.gr:6667
clu03.softnet.tuc.gr:6667
clu04.softnet.tuc.gr:6667
clu06.softnet.tuc.gr:6667
```

**Zookeeper (for topic management):**
```
clu01.softnet.tuc.gr:2182
clu02.softnet.tuc.gr:2182
clu03.softnet.tuc.gr:2182
```

**Flink Dashboard:**
```
http://clu01.softnet.tuc.gr:8081
```

---

## 📊 Kafka Topics

Create topics before deploying:

```bash
cd /usr/hdp/current/kafka-broker

# Input topic (sensor data)
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 \
    --replication-factor 2 --partitions 4 --topic etl.input.v1

# Config topic (ETL configurations)
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 \
    --replication-factor 2 --partitions 4 --topic etl.config.v1

# Output topic (processed results)
bin/kafka-topics.sh --create --zookeeper clu01.softnet.tuc.gr:2182 \
    --replication-factor 2 --partitions 4 --topic etl.output.v1

# List topics
bin/kafka-topics.sh --list --zookeeper clu01.softnet.tuc.gr:2182
```

---

## 🧪 Testing

### 1. Submit Configuration

```bash
curl -X POST http://clu04.softnet.tuc.gr:8080/config \
    -H "Content-Type: application/json" \
    -d '{
        "jobId": "test-job-001",
        "source": "sensor-stream",
        "keyBy": "universal",
        "transformations": [
            {
                "type": "MAP",
                "sourceField": "temperature",
                "targetField": "temp_celsius",
                "operation": "IDENTITY"
            }
        ],
        "aggregations": [
            {
                "type": "AVG",
                "field": "temperature",
                "windowType": "TUMBLING",
                "windowSize": 10
            }
        ]
    }'
```

### 2. Monitor Output

```bash
cd /usr/hdp/current/kafka-broker

# Watch output topic
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.output.v1 \
    --from-beginning
```

### 3. Check Flink Dashboard

- **URL:** http://clu01.softnet.tuc.gr:8081
- **Metrics:** Task slots, parallelism, backpressure
- **Logs:** Check worker nodes for detailed logs

---

## 🛠️ Troubleshooting

### Common Issues

**1. Flink Job Keeps Restarting / Data Source Shows 0 Records**
- **Cause:** Command-line arguments not passed when submitting job
- **Fix:** Use `./bin/flink run` with `--kafka.bootstrap.servers` and other arguments (see Quick Start)
- **Verify:** Check Flink Dashboard → Exceptions tab for Kafka connection errors

**2. Java Version Error (class file version 61.0)**
- **Cause:** JARs compiled with Java 17 but cluster has Java 8
- **Fix:** All components compiled with Java 8 (Spring Boot 2.7.18)
- **Verify:** Run `mvn clean package` to rebuild

**3. Data Source Receives 0 Records Despite Kafka Having Data**
- **Cause:** Datetime format mismatch - Jackson cannot parse ZonedDateTime with timezone ID
- **Fix:** File Producer now uses `.toInstant().toString()` for standard ISO-8601 format
- **Verify:** Check Kafka messages - datetime should be `"2025-10-12T18:44:21Z"`, NOT `"2025-10-12T21:44:21+03:00[Europe/Athens]"`

**4. NullPointerException When Starting with Empty Kafka Topics**
- **Cause:** Default deserializer can't handle null/empty messages
- **Fix:** Flink job now uses NullSafeStringSchema with null filters
- **Verify:** Job should start successfully even with empty topics

**5. Consumer Group Offsets Persist After Topic Recreation**
- **Cause:** Consumer group offsets stored separately in Kafka
- **Fix:** Delete consumer group: `bin/kafka-consumer-groups.sh --bootstrap-server clu02.softnet.tuc.gr:6667 --group etl-flink-consumer --delete`

**6. Topic Does Not Exist**
- **Cause:** Kafka topics not created
- **Fix:** Run topic creation commands (see Kafka Topics section)

### Debugging Commands

```bash
# Check Flink job status
cd /usr/local/flink
./bin/flink list

# Check Flink job metrics (Data Source receiving data?)
curl -s http://clu01.softnet.tuc.gr:8081/jobs/<JOB-ID>/vertices/<VERTEX-ID>/metrics | grep numRecordsOut

# Check running services
ps aux | grep etl

# Check Kafka consumer group
cd /usr/hdp/current/kafka-broker
bin/kafka-consumer-groups.sh --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --group etl-flink-consumer --describe

# Verify datetime format in Kafka (should be ISO-8601 with Z)
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.input.v1 \
    --offset latest \
    --partition 0 \
    --max-messages 2

# Monitor Kafka output topic
bin/kafka-console-consumer.sh \
    --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --topic etl.output.v1 \
    --from-beginning

# View Flink TaskManager logs
cd /usr/local/flink/log
ls -ltr  # Find latest log files
tail -f flink-*-taskexecutor-*.out
```

---

## 📁 Project Structure

```
etl-flink-project/
├── flink/                          # Flink streaming job
│   ├── src/main/java/com/etl/flink/
│   │   ├── EtlFlinkJob.java       # Main entry point
│   │   ├── model/                 # Data models
│   │   ├── process/               # CoFlatMap processor
│   │   └── udf/                   # User-defined functions
│   └── pom.xml
├── services/
│   ├── etl-api/                   # REST API for config submission
│   │   ├── src/main/java/com/etl/api/
│   │   └── pom.xml
│   └── etl-file-producer/         # Sensor data producer
│       ├── src/main/java/com/etl/producer/
│       └── pom.xml
├── data-samples/                  # Sample JSON files
├── pom.xml                        # Parent POM
├── DEPLOYMENT_GUIDE.txt           # Detailed deployment steps
└── README.md                      # This file
```

---

## 🔗 Resources

- **SoftNet Cluster:** Contact xenia@softnet.tuc.gr
- **Flink Documentation:** https://nightlies.apache.org/flink/
- **HDP Documentation:** https://docs.cloudera.com/HDPDocuments/
- **Deployment Guide:** See `DEPLOYMENT_GUIDE.txt` for step-by-step instructions

---

## 📝 Version History

- **v1.0.0** - Initial SoftNet cluster deployment (cluster branch)
  - Removed Docker Compose dependencies
  - Changed sink from MongoDB to Kafka
  - Configured for SoftNet cluster (23 nodes, HDP 3.1.0)
  - Parallelism optimized for 11 worker nodes
  - Downgraded to Java 8 compatibility (Flink 1.9.3, Spring Boot 2.7.18)
  - Fixed datetime serialization format (ISO-8601 with Z timezone)
  - Implemented NullSafeStringSchema for empty Kafka topics
  - Changed configuration from environment variables to command-line arguments
  - Complete end-to-end verification on SoftNet cluster

---

## 📄 License

This project is for academic/research purposes at Technical University of Crete (TUC).

---

## 👥 Contact

For deployment issues on SoftNet cluster, contact the cluster administrator at xenia@softnet.tuc.gr
