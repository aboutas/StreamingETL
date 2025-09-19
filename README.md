# ETL Flink Project - Universal Data Source Architecture

A real-time ETL (Extract, Transform, Load) system built with Apache Flink, Kafka, and MongoDB that supports multiple user configurations processing the same universal data source simultaneously.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ETL FLINK PROJECT ARCHITECTURE                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐    ┌──────────────────┐    ┌─────────────────────────────────┐
│   sensors.json   │    │   config-user1   │    │        config-user2.json        │
│  (Universal      │    │     .json        │    │    (threshold > 50.0)           │
│  Data Source)    │    │ (threshold >100) │    │                                 │
│   52 records     │    └─────────┬────────┘    └─────────────┬───────────────────┘
└─────────┬────────┘              │                           │
          │                       │                           │
          │                       ▼                           ▼
          │            ┌─────────────────────────────────────────────────────┐
          │            │              ETL API (Port 8080)                    │
          │            │        POST /config (Submit Configurations)         │
          │            └─────────────────┬───────────────────────────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────┐         ┌─────────────────────┐
│  File Producer      │         │     Kafka Topics    │
│  (Port: Internal)   │         │                     │
│  • Rate: 10 msg/sec │         │  etl.config.v1 ◄────┼── Config Stream
│  • Loops infinitely │         │  etl.input.v1  ◄────┼── Data Stream
│  • Adds jobId       │         │  etl.output.v1      │
└──────────┬──────────┘         └──────────┬──────────┘
           │                               │
           └───────────────────────────────┘
                                          │
                                          ▼
               ┌─────────────────────────────────────────────────────────────┐
               │                    APACHE FLINK CLUSTER                     │
               │  ┌─────────────────────────────────────────────────────────┐ │
               │  │              ETL Flink Job (Streaming)                  │ │
               │  │                                                         │ │
               │  │  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │ │
               │  │  │Config Source│    │Data Source  │    │  CoFlatMap  │  │ │
               │  │  │(Kafka)      │    │(Kafka)      │    │             │  │ │
               │  │  │             │    │             │    │┌───────────┐│  │ │
               │  │  │Records: 2   │    │Records:14K+ │    ││MapState   ││  │ │
               │  │  │             │    │(continuous) │    ││Multiple   ││  │ │
               │  │  │             │    │             │    ││Configs    ││  │ │
               │  │  └──────┬──────┘    └──────┬──────┘    │└───────────┘│  │ │
               │  │         │                  │           └─────┬───────┘  │ │
               │  │         └──────────────────┼─────────────────┘          │ │
               │  │                            │                            │ │
               │  │         ┌──────────────────▼─────────────────┐          │ │
               │  │         │     ETL Processing Engine          │          │ │
               │  │         │  • Filter (measurement > threshold)│          │ │
               │  │         │  • Aggregate (max by sensor type) │          │ │
               │  │         │  • Process EACH event against     │          │ │
               │  │         │    ALL stored configurations      │          │ │
               │  │         └──────────────┬─────────────────────┘          │ │
               │  └────────────────────────┼────────────────────────────────┘ │
               └───────────────────────────┼──────────────────────────────────┘
                                          │
                                          ▼
               ┌─────────────────────────────────────────────────────────────┐
               │                    OUTPUT SINKS                             │
               │                                                             │
               │  ┌─────────────────┐              ┌─────────────────────────┐ │
               │  │  Print Sink     │              │     MongoDB Sink        │ │
               │  │  (stdout logs)  │              │   (Dynamic Collections) │ │
               │  │                 │              │                         │ │
               │  │  ETL Results>   │              │  ┌─────────────────────┐ │ │
               │  │  user1-job: 107 │              │  │    user1_job        │ │ │
               │  │  user2-job: 61  │              │  │  (3 documents)      │ │ │
               │  │  ...            │              │  └─────────────────────┘ │ │
               │  └─────────────────┘              │  ┌─────────────────────┐ │ │
               │                                   │  │    user2_job        │ │ │
               │                                   │  │  (4 documents)      │ │ │
               │                                   │  └─────────────────────┘ │ │
               │                                   └─────────────────────────┘ │
               └─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              KEY FEATURES                                       │
│                                                                                 │
│ ✅ Universal Data Source: Single sensors.json feeds multiple configurations    │
│ ✅ CoFlatMap Processing: Multiple configs process same data simultaneously      │
│ ✅ Dynamic Collections: Each jobId gets separate MongoDB collection            │
│ ✅ Real-time Streaming: Continuous data flow at 10 messages/second            │
│ ✅ Fault Tolerance: Kafka persistence + Flink checkpointing                   │
│ ✅ Scalable Architecture: Horizontal scaling support                           │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 🔄 Data Flow Diagram - Detailed Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         DETAILED DATA FLOW & PARSING                           │
└─────────────────────────────────────────────────────────────────────────────────┘

📁 data/sensors.json (52 records) ──► FileProducerService (loops @ 10 msg/sec)
│
│ Raw JSON Records (as they come):
│ {"sensor": "temperature", "measurement": 23.5, "timestamp": "2024-01-15T10:30:00Z"}
│ {"sensor": "pressure", "measurement": 1013.2, "timestamp": "2024-01-15T10:30:01Z"}
│ {"sensor": "humidity", "measurement": 65.8, "timestamp": "2024-01-15T10:30:02Z"}
│
▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            KAFKA STREAMING LAYER                               │
└─────────────────────────────────────────────────────────────────────────────────┘

Producer Enrichment:                 ┌──────────────────────────────────────┐
Raw JSON + jobId field added ────►   │         etl.input.v1 Topic           │
                                     │  ┌────────────────────────────────────┤
Example:                             │  │ {"sensor": "temperature",          │
{                                    │  │  "measurement": 23.5,              │
  "sensor": "temperature",           │  │  "timestamp": "2024-01-15...",     │
  "measurement": 23.5,               │  │  "jobId": "universal-stream"}      │
  "timestamp": "2024-01-15...",      │  └────────────────────────────────────┤
  "jobId": "universal-stream"        │  │ Message Rate: ~10/sec              │
}                                    │  │ Total Messages: 14,000+ continuous │
                                     └──┴────────────────────────────────────┘
▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           FLINK PROCESSING ENGINE                              │
└─────────────────────────────────────────────────────────────────────────────────┘

Config Stream (etl.config.v1)    │    Data Stream (etl.input.v1)
                                 │
Config Example:                  │    Data Example:
{                               │    {
  "jobId": "user1-job",         │      "sensor": "temperature",
  "transformations": [          │      "measurement": 107.2,
    {                          │      "timestamp": "2024-01-15T...",
      "type": "filter_greater", │      "jobId": "universal-stream"
      "params": {              │    }
        "field": "measurement", │
        "threshold": 100.0      │
      }                        │
    }                          │
  ]                            │
}                              │
        │                     │           │
        ▼                     │           ▼
┌─────────────────────────────▼─────────────────────────────────┐
│                     CoFlatMapFunction                         │
│                                                              │
│  MapState<String, Config> configs  ◄─── Config Updates      │
│           │                                                  │
│           │  For EACH Data Event:                           │
│           │  ┌─────────────────────────────────────────────┐ │
│           └─►│  1. Parse JSON to SensorEvent object        │ │
│              │     ↓                                       │ │
│              │  2. Iterate through ALL stored configs      │ │
│              │     ↓                                       │ │
│              │  3. Apply transformations PER config:       │ │
│              │     • Filter: measurement > threshold        │ │
│              │     • KeyBy: group by sensor type           │ │
│              │     • Aggregate: max/sum/avg operations     │ │
│              │     ↓                                       │ │
│              │  4. Generate results per config:            │ │
│              │     EtlResult{                              │ │
│              │       jobId: "user1-job",                   │ │
│              │       result: 107.2,                        │ │
│              │       aggregationType: "max",               │ │
│              │       groupingField: "sensor",              │ │
│              │       groupingKey: "temperature"            │ │
│              │     }                                       │ │
│              └─────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              OUTPUT SINKS                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

Print Sink (Real-time Logs)          │     MongoDB Sink (Persistent Storage)
                                     │
ETL Results> EtlResult{              │     ┌──────────────────────────────────┐
  jobId='user1-job',                 │     │        etl_db Database           │
  result=107.2,                      │     │                                  │
  aggregationType='max',             │     │  ┌─────────────────────────────┐ │
  groupingField='sensor',            │     │  │      user1_job Collection   │ │
  groupingKey='temperature'          │     │  │  {                          │ │
}                                    │     │  │    "_id": "job:user1-job|   │ │
                                     │     │  │           g:sensor:temp|    │ │
ETL Results> EtlResult{              │     │  │           agg:max|...",     │ │
  jobId='user2-job',                 │     │  │    "jobId": "user1-job",    │ │
  result=78.9,                       │     │  │    "result": 107.2,         │ │
  aggregationType='max',             │     │  │    "aggregationType": "max", │ │
  groupingField='sensor',            │     │  │    "groupingField": "sensor"│ │
  groupingKey='humidity'             │     │  │  }                          │ │
}                                    │     │  └─────────────────────────────┘ │
                                     │     │                                  │
Rate: ~2-10 results/sec             │     │  ┌─────────────────────────────┐ │
(depends on data + config count)     │     │  │      user2_job Collection   │ │
                                     │     │  │  (Similar structure with    │ │
                                     │     │  │   different threshold)      │ │
                                     │     │  └─────────────────────────────┘ │
                                     │     └──────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                            PARSING & TRANSFORMATION DETAILS                     │
└─────────────────────────────────────────────────────────────────────────────────┘

🔍 Data Parsing Steps:
1. JSON Deserialization: Kafka String → SensorEvent POJO
2. Field Extraction: sensor, measurement, timestamp, jobId
3. Type Conversion: measurement (String→Double), timestamp (String→Instant)
4. Validation: Non-null checks, numeric validation

🔄 Transformation Pipeline (per config):
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Filter    │───►│   KeyBy     │───►│  Aggregate  │───►│   Output    │
│measurement  │    │   sensor    │    │    max()    │    │   Result    │
│   > 100.0   │    │   type      │    │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘

📊 Multi-Config Processing:
- SAME data event processed by ALL configurations simultaneously
- Each config applies its own threshold and transformations
- Results are tagged with respective jobId for separation
- MongoDB collections are dynamically created per jobId

🚀 Performance Characteristics:
- Input Rate: 10 messages/second (configurable)
- Processing Latency: <100ms per event
- Throughput: Scales with number of configs (2 configs = ~2x results)
- Memory Usage: MapState holds configs (typically <1MB per config)
```


## 📋 Prerequisites

- Docker & Docker Compose
- Java 17+ (for building Flink job)
- Maven 3.6+
- curl (for testing)

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 17+ (for building Flink job)
- Maven 3.6+
- curl (for testing)

### 1. Start the System
```bash
# Clone and navigate to project
git clone <repository-url>
cd etl-flink-project

# Build Flink job
cd flink && mvn clean package -DskipTests && cd ..

# Start all services
docker-compose up -d

# Wait for services to be healthy (30-60 seconds)
docker-compose ps
```

### 2. Submit Flink Job
```bash
# Upload Flink jar
curl -X POST -H "Expect:" -F "jarfile=@flink/target/etl-flink-1.0.0.jar" http://localhost:8081/jars/upload

# Run the job (replace JAR_ID with the ID from upload response)
curl -X POST http://localhost:8081/jars/{JAR_ID}/run -H "Content-Type: application/json" -d '{
  "entryClass": "com.etl.flink.EtlFlinkJob",
  "programArgs": "--bootstrap-servers kafka:29092 --input-topic etl.input.v1 --config-topic etl.config.v1 --output-topic etl.output.v1 --mongo-uri mongodb://mongo:27017/etl_db"
}'
```

### 3. Test the System
```bash
# Submit first configuration
curl -X POST -H "Content-Type: application/json" -d @config-user1.json http://localhost:8080/config

# Submit second configuration
curl -X POST -H "Content-Type: application/json" -d @config-user2.json http://localhost:8080/config

# Check results in MongoDB
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.getCollectionNames()"
```

## 🧪 Complete Testing Guide

### Step 1: Verify System Health
```bash
# Check all containers are running
docker-compose ps

# Verify Flink UI is accessible
curl http://localhost:8081

# Verify ETL API is accessible
curl http://localhost:8080/health
```

### Step 2: Monitor Data Flow
```bash
# Check Kafka topic offsets (should show increasing numbers)
docker exec etl-flink-project-kafka-1 kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic etl.input.v1

# Check file producer logs (should show cycling through data)
docker logs etl-flink-project-etl-file-producer-1 --tail 10
```

### Step 3: Test Configuration Submissions
```bash
# Submit user1 config (threshold > 100.0)
curl -X POST -H "Content-Type: application/json" -d @config-user1.json http://localhost:8080/config
# Expected: {"jobId":"user1-job","message":"Configuration submitted successfully","status":"success"}

# Check Flink job metrics (Config Source should show 1 record)
curl -s http://localhost:8081/jobs/{JOB_ID} | grep '"write-records"'
```

### Step 4: Verify Real-time Processing
```bash
# Watch Flink processing logs
docker logs etl-flink-project-flink-taskmanager-1 --tail 20 | grep "ETL Results"

# Expected output:
# ETL Results> EtlResult{id='null', jobId='user1-job', result=107.2}
# ETL Results> EtlResult{id='null', jobId='user1-job', result=1016.9}
```

### Step 5: Test Multi-Config Processing
```bash
# Submit second config (threshold > 50.0)
curl -X POST -H "Content-Type: application/json" -d @config-user2.json http://localhost:8080/config

# Verify Config Source now has 2 records
curl -s http://localhost:8081/jobs/{JOB_ID} | grep -A 5 "Config Source"

# Check logs show both configs processing
docker logs etl-flink-project-flink-taskmanager-1 --tail 10 | grep -E "(user1-job|user2-job)"

# Expected: Results for both jobIds with different threshold filtering
```

### Step 6: Verify MongoDB Collections
```bash
# List all collections (should see user1_job and user2_job)
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.getCollectionNames()"

# Check user1_job collection
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "
  print('user1_job count:', db.user1_job.countDocuments());
  db.user1_job.find().limit(2)
"

# Check user2_job collection
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "
  print('user2_job count:', db.user2_job.countDocuments());
  db.user2_job.find().limit(2)
"
```

### Step 7: Performance Validation
```bash
# Check data production rate (should be ~10 messages/second)
docker logs etl-flink-project-etl-file-producer-1 | grep "Sent.*messages"

# Verify Flink job is processing data continuously
curl -s http://localhost:8081/jobs/{JOB_ID} | grep '"write-records"'
# Data Source write-records should be increasing

# Check MongoDB growth over time
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "
  db.user1_job.countDocuments() + db.user2_job.countDocuments()
"
```

## 📋 Configuration Examples

### config-user1.json (High Threshold)
```json
{
  "jobId": "user1-job",
  "source": "data/sensors.json",
  "transformations": [
    {
      "type": "filter_greater",
      "params": {
        "field": "measurement",
        "threshold": 100.0
      }
    },
    {
      "type": "max",
      "keyBy": "sensor"
    }
  ],
  "outputTopic": "etl.output.v1"
}
```

### config-user2.json (Low Threshold)
```json
{
  "jobId": "user2-job",
  "source": "data/sensors.json",
  "transformations": [
    {
      "type": "filter_greater",
      "params": {
        "field": "measurement",
        "threshold": 50.0
      }
    },
    {
      "type": "max",
      "keyBy": "sensor"
    }
  ],
  "outputTopic": "etl.output.v1"
}
```

## 🔧 API Endpoints

### ETL API (Port 8080)
- `POST /config` - Submit ETL configuration
- `GET /health` - Health check endpoint

### Flink Web UI (Port 8081)
- `GET /` - Flink dashboard
- `GET /jobs` - List all jobs
- `GET /jobs/{jobId}` - Job details and metrics

### MongoDB (Port 27017)
- Database: `etl_db`
- Collections: `{jobId}` (e.g., `user1_job`, `user2_job`)

### Kafka (Port 9092)
- `etl.config.v1` - Configuration topic
- `etl.input.v1` - Data input topic
- `etl.output.v1` - Processing output topic


## 🔍 Troubleshooting

### Common Issues

**1. Flink Job Not Processing Data**
```bash
# Check if SensorEvent model matches JSON structure
docker logs etl-flink-project-flink-taskmanager-1 | grep -i error

# Verify Kafka connectivity
docker exec etl-flink-project-kafka-1 kafka-topics --bootstrap-server localhost:9092 --list
```

**2. No Results in MongoDB**
```bash
# Check if MongoDB sink is working
docker logs etl-flink-project-flink-taskmanager-1 | grep -i mongo

# Verify MongoDB connection
docker exec etl-flink-project-mongo-1 mongosh --eval "db.adminCommand('ping')"
```

**3. File Producer Not Sending Data**
```bash
# Check file producer logs
docker logs etl-flink-project-etl-file-producer-1

# Verify sensors.json exists and is readable
docker exec etl-flink-project-etl-file-producer-1 ls -la /data/
```

### Performance Tuning

**Increase Data Rate:**
```yaml
# In docker-compose.yml
environment:
  - RATE_PER_SEC=100  # Default: 10
```

**Disable Continuous Loop:**
```yaml
environment:
  - PRODUCER_LOOP_ENABLED=false  # Default: true
```

**Flink Parallelism:**
```yaml
# In docker-compose.yml flink services
environment:
  FLINK_PROPERTIES: |
    parallelism.default: 4  # Default: 2
```


## 📈 Expected Results

### Metrics After 5 Minutes
- **Kafka Messages**: ~3,000 in etl.input.v1
- **Config Records**: 2 (one per submitted config)
- **MongoDB Collections**: 2 (user1_job, user2_job)
- **Processing Rate**: ~10 messages/second sustained

### Sample MongoDB Documents
```javascript
// user1_job collection (threshold > 100.0)
{
  "_id": "job:user1-job|g:sensor:temperature|ws:none|we:none|agg:max|f:measurement|h:1447695077",
  "jobId": "user1-job",
  "result": 116.8,
  "aggregationType": "max",
  "groupingField": "sensor",
  "groupingKey": "temperature"
}

// user2_job collection (threshold > 50.0)
{
  "_id": "job:user2-job|g:sensor:humidity|ws:none|we:none|agg:max|f:measurement|h:274338533",
  "jobId": "user2-job",
  "result": 78.9,
  "aggregationType": "max",
  "groupingField": "sensor",
  "groupingKey": "humidity"
}
```

## 🏗️ System Components

### Services Overview
| Service | Port | Purpose | Technology |
|---------|------|---------|------------|
| **Zookeeper** | 2181 | Kafka coordination | Apache Zookeeper |
| **Kafka** | 9092 | Message streaming | Apache Kafka |
| **MongoDB** | 27017 | Results storage | MongoDB 7.0 |
| **Flink JobManager** | 8081 | Job coordination | Apache Flink 1.18 |
| **Flink TaskManager** | - | Job execution | Apache Flink 1.18 |
| **ETL API** | 8080 | Config management | Spring Boot |
| **File Producer** | - | Data generation | Spring Boot |

### Key Technologies
- **Apache Flink 1.18**: Stream processing engine
- **Apache Kafka 7.4**: Distributed streaming platform
- **MongoDB 7.0**: Document database for results
- **Spring Boot 3.x**: Microservices framework
- **Docker Compose**: Container orchestration

## 📚 Data Production Explained

### Why Many Records from 52-Line File?

The system produces thousands of records because:

1. **Streaming Simulation**: FileProducerService continuously loops through sensors.json
2. **Rate Control**: 10 messages/second (RATE_PER_SEC=10)
3. **Infinite Loop**: `PRODUCER_LOOP_ENABLED=true` restarts file after reaching end
4. **Real-time ETL**: Mimics continuous sensor data streams

**Example**: After 10 minutes running:
- File cycles: ~115 complete cycles (52 records × 115 = ~6,000 messages)
- Rate check: 6,000 ÷ (10 min × 60s) = 10 msg/sec ✅

### ETL Result Aggregation
- **Input**: Thousands of individual sensor measurements
- **Processing**: Filter + Max aggregation per sensor type
- **Output**: Few consolidated results (max temperature, max pressure, max humidity)
- **Storage**: Upserted to MongoDB (same _id updated, not duplicated)

**This continuous data flow enables realistic testing of:**
- Time-based windows
- Aggregation functions
- System performance under load
- Multi-user processing scenarios

## 📁 Project Structure

```
etl-flink-project/
├── config-user1.json          # User1 config (threshold > 100.0)
├── config-user2.json          # User2 config (threshold > 50.0)
├── config-user3.json          # User3 config (comprehensive transformations)
├── config-user4.json          # User4 config (date extraction + sum)
├── data/
│   └── sensors.json           # Universal sensor data source (52 records)
├── docker-compose.yml         # Service orchestration
├── flink/                     # Flink ETL job module
│   ├── pom.xml               # Flink project Maven configuration
│   ├── Dockerfile            # Flink job container
│   └── src/main/java/com/etl/flink/
│       ├── EtlFlinkJob.java          # Main Flink job
│       ├── model/                    # Data models
│       ├── process/                  # CoFlatMap processing logic
│       ├── sink/                     # MongoDB sink implementation
│       └── udf/                      # User-defined functions
├── services/
│   ├── etl-api/              # REST API service
│   └── etl-file-producer/    # Data streaming service
├── pom.xml                   # Parent Maven configuration
├── README.md                 # This documentation
└── .gitignore               # Git ignore rules
```

### Key Components
- **Configuration Files**: Ready-to-use configs testing different transformations
- **Universal Data Source**: Single sensors.json feeds all configurations
- **Flink Job**: Stream processing with CoFlatMap architecture
- **Services**: API for config management + file producer for data streaming
- **Docker Orchestration**: Complete containerized environment

## 📚 Additional Resources

- [Apache Flink Documentation](https://flink.apache.org/docs/)
- [Kafka Streams Guide](https://kafka.apache.org/documentation/streams/)
- [MongoDB Java Driver](https://docs.mongodb.com/drivers/java/)
- [Docker Compose Reference](https://docs.docker.com/compose/)

---

For questions or issues, please check the troubleshooting section or review the container logs using `docker logs <container-name>`.