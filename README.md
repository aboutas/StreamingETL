# ETL Flink Project - SoftNet Cluster Deployment (v1.0.0)

A production-ready real-time ETL (Extract, Transform, Load) system built with Apache Flink that processes sensor data streams with dynamically configurable transformations and TRUE parallelism using CoFlatMap architecture with 3-second windowing.

**Deployed on:** SoftNet Cluster (TUC) with 23 servers, HDP 3.1.0, Flink 1.10.0

---

## 🚀 Quick Start (SoftNet Cluster)

### Prerequisites
- Access to SoftNet cluster (clu04.softnet.tuc.gr)
- TUC VPN connection (if outside TUC network)
- Maven 3.x for building JARs

### Build & Deploy

```bash
# 1. Build all JARs
mvn clean package -DskipTests

# 2. Upload to cluster (replace 'username' with your username)
scp flink/target/etl-flink-1.0.0.jar username@clu04.softnet.tuc.gr:/home/username/
scp services/etl-api/target/etl-api-1.0.0.jar username@clu04.softnet.tuc.gr:/home/username/
scp services/etl-file-producer/target/etl-file-producer-1.0.0.jar username@clu04.softnet.tuc.gr:/home/username/

# 3. SSH to cluster
ssh username@clu04.softnet.tuc.gr

# 4. Submit Flink job via Dashboard
# Open: http://clu01.softnet.tuc.gr:8081
# Upload JAR and set parallelism to 11

# 5. Start REST API
java -jar etl-api-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --server.port=8080 &

# 6. Start File Producer
java -jar etl-file-producer-1.0.0.jar \
    --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
    --producer.rate.per.sec=10 &

# 7. Submit config
curl -X POST http://clu04.softnet.tuc.gr:8080/config \
    -H "Content-Type: application/json" \
    -d @config-test.json
```

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

### Environment Variables (Override defaults)

**Flink Job:**
- `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka:9092`)
- `CONFIG_TOPIC` (default: `etl.config.v1`)
- `INPUT_TOPIC` (default: `etl.input.v1`)
- `OUTPUT_TOPIC` (default: `etl.output.v1`)

**ETL API:**
- `KAFKA_BOOTSTRAP_SERVERS`
- `CONFIG_TOPIC`

**File Producer:**
- `KAFKA_BOOTSTRAP_SERVERS`
- `INPUT_TOPIC`
- `DATA_FILE` (path to sensor data JSON)
- `RATE_PER_SEC` (messages per second)

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

**1. Connection refused to kafka:9092**
- **Cause:** Default Kafka servers not overridden
- **Fix:** Set `KAFKA_BOOTSTRAP_SERVERS` or use program arguments

**2. ClassNotFoundException / NoSuchMethodError**
- **Cause:** Flink version mismatch (1.18.1 vs 1.10.0)
- **Fix:** Rebuild with Flink 1.10.0 in `pom.xml`

**3. Topic does not exist**
- **Cause:** Kafka topics not created
- **Fix:** Run topic creation commands (see Kafka Topics section)

**4. No resources available**
- **Cause:** Flink cluster full
- **Fix:** Check http://clu01.softnet.tuc.gr:8081 for available slots

### Debugging Commands

```bash
# Check Flink job status
cd /usr/local/flink
./bin/flink list

# Check Kafka consumer group
cd /usr/hdp/current/kafka-broker
bin/kafka-consumer-groups.sh --bootstrap-server clu02.softnet.tuc.gr:6667 \
    --group etl-data-consumer-v2 --describe

# View service logs
tail -f /home/username/nohup.out
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

- **v1.0.0** - Initial SoftNet cluster deployment
  - Removed Docker Compose dependencies
  - Changed sink from MongoDB to Kafka
  - Configured for SoftNet cluster (23 nodes, HDP 3.1.0)
  - Parallelism optimized for 11 worker nodes

---

## 📄 License

This project is for academic/research purposes at Technical University of Crete (TUC).

---

## 👥 Contact

For deployment issues on SoftNet cluster, contact the cluster administrator at xenia@softnet.tuc.gr
