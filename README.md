# ETL Flink Project - Real-Time Stream Processing with Random Data Generation

A real-time ETL (Extract, Transform, Load) system built with Apache Flink, Kafka, and MongoDB that processes live sensor data streams with configurable windowed aggregations and transformations.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ETL FLINK PROJECT ARCHITECTURE                        │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐    ┌──────────────────┐    ┌─────────────────────────────────┐
│  Random Sensor   │    │   config-user1   │    │        config-user2.json        │
│  Data Generator  │    │     .json        │    │    (threshold > 50.0)           │
│  (Real-time)     │    │ (threshold >100) │    │                                 │
│  5 msg/sec       │    └─────────┬────────┘    └─────────────┬───────────────────┘
└─────────┬────────┘              │                           │
          │                       │                           │
          │                       ▼                           ▼
          │            ┌─────────────────────────────────────────────────────┐
          │            │          Kafka Console Producer                    │
          │            │        (Config Submission via CLI)                 │
          │            └─────────────────┬───────────────────────────────────┘
          │                              │
          ▼                              ▼
┌─────────────────────┐         ┌─────────────────────┐
│  File Producer      │         │     Kafka Topics    │
│  (Port: Internal)   │         │                     │
│  • Rate: 5 msg/sec  │         │  etl.config.v1 ◄────┼── Config Stream
│  • Random data      │         │  etl.input.v1  ◄────┼── Data Stream
│  • Current timestamps│        │  etl.output.v1      │
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
               │  │  │Records: 1+  │    │Records:∞    │    ││MapState   ││  │ │
               │  │  │             │    │(continuous) │    ││Configs    ││  │ │
               │  │  │             │    │             │    ││Active     ││  │ │
               │  │  └──────┬──────┘    └──────┬──────┘    │└───────────┘│  │ │
               │  │         │                  │           └─────┬───────┘  │ │
               │  │         └──────────────────┼─────────────────┘          │ │
               │  │                            │                            │ │
               │  │         ┌──────────────────▼─────────────────┐          │ │
               │  │         │    ETL Processing Engine           │          │ │
               │  │         │  • Windowed Aggregations (10s)    │          │ │
               │  │         │  • Filter transformations         │          │ │
               │  │         │  • Duplicate config prevention    │          │ │
               │  │         │  • Real-time processing           │          │ │
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
               │  │  test-random:   │              │  │    test_random      │ │ │
               │  │  temp: 35.2     │              │  │  (69+ documents)    │ │ │
               │  │  humidity: 57.8 │              │  │  Growing in real-   │ │ │
               │  │  pressure:1038.1│              │  │  time with windows  │ │ │
               │  └─────────────────┘              │  └─────────────────────┘ │ │
               └─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              KEY FEATURES                                       │
│                                                                                 │
│ ✅ Random Data Generation: Real-time sensor data with current timestamps       │
│ ✅ Windowed Processing: 10-second windows for time-based aggregations          │
│ ✅ Duplicate Prevention: Prevents same config resubmission                     │
│ ✅ Growing Collections: MongoDB documents increase with each window             │
│ ✅ Real-time Streaming: Continuous data flow at 5 messages/second              │
│ ✅ Fault Tolerance: Kafka persistence + Flink checkpointing                    │
│ ✅ Single Collection Output: One collection per jobId (no config metadata)     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## 🔄 Data Flow - Real-Time Random Sensor Stream

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         RANDOM DATA GENERATION PIPELINE                        │
└─────────────────────────────────────────────────────────────────────────────────┘

🎲 Random Sensor Data Generator (FileProducerService):
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Mode: RANDOM                                                                   │
│  Rate: 5 messages/second                                                        │
│  Sensors: temperature, humidity, pressure                                       │
│  Locations: room-a, room-b, room-c, room-d                                     │
│  Timestamps: Current time (2025-09-19T14:50:XX)                                │
└─────────────────────────────────────────────────────────────────────────────────┘
                                    ▼
📨 Generated JSON Messages:
{
  "sensor": "humidity",
  "measurement": 68.2,
  "measurement_unit": "percent",
  "datetime": "2025-09-19T14:50:44Z",
  "location": "room-d"
}
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            KAFKA STREAMING LAYER                               │
└─────────────────────────────────────────────────────────────────────────────────┘

Producer Flow:                      ┌──────────────────────────────────────┐
Random JSON → Kafka Producer ────►  │         etl.input.v1 Topic           │
                                     │  ┌────────────────────────────────────┤
Real-time Example:                   │  │ {"sensor": "temperature",          │
{                                    │  │  "measurement": 23.7,              │
  "sensor": "temperature",           │  │  "datetime": "2025-09-19T...",     │
  "measurement": 23.7,               │  │  "location": "room-a"}             │
  "datetime": "2025-09-19T14:51:02Z",│  └────────────────────────────────────┤
  "location": "room-a"               │  │ Message Rate: 5/sec continuous     │
}                                    │  │ Growing indefinitely              │
                                     └──┴────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           FLINK PROCESSING ENGINE                              │
└─────────────────────────────────────────────────────────────────────────────────┘

Config Stream (etl.config.v1)    │    Data Stream (etl.input.v1)
                                 │
Config Example:                  │    Real-time Data Example:
{                               │    {
  "jobId": "test-random",       │      "sensor": "humidity",
  "source": "kafka://...",      │      "measurement": 68.2,
  "transformations": [          │      "datetime": "2025-09-19T14:50:44Z",
    {                          │      "location": "room-d"
      "type": "max",           │    }
      "keyBy": "sensor",       │
      "window": "10s",         │
      "params": {              │
        "field": "measurement" │
      }                        │
    }                          │
  ]                            │
}                              │
        │                     │           │
        ▼                     │           ▼
┌─────────────────────────────▼─────────────────────────────────┐
│                     CoFlatMapFunction                         │
│                                                              │
│  WindowedConfigProcessor + EtlCoFlatMapFunction              │
│                                                              │
│  For EACH 10-second window:                                  │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  1. Collect sensor data for window period               │ │
│  │     ↓                                                   │ │
│  │  2. Group by sensor type (keyBy: "sensor")             │ │
│  │     ↓                                                   │ │
│  │  3. Apply max aggregation per sensor:                  │ │
│  │     • Temperature: max(23.7, 25.1, 32.3) = 32.3       │ │
│  │     • Humidity: max(68.2, 55.5, 61.3) = 68.2          │ │
│  │     • Pressure: max(1013.2, 1038.1) = 1038.1          │ │
│  │     ↓                                                   │ │
│  │  4. Generate windowed results:                         │ │
│  │     EtlResult{                                          │ │
│  │       jobId: "test-random",                             │ │
│  │       result: 32.3,                                     │ │
│  │       windowStart: "2025-09-19T14:50:10Z",             │ │
│  │       windowEnd: "2025-09-19T14:50:20Z"                │ │
│  │     }                                                   │ │
│  └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              OUTPUT SINKS                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

Print Sink (Real-time Logs)          │     MongoDB Sink (Persistent Storage)
                                     │
ETL Results:1> EtlResult{            │     ┌──────────────────────────────────┐
  jobId='test-random',               │     │        etl_db Database           │
  result=32.3,                       │     │                                  │
  aggregationType='max',             │     │  ┌─────────────────────────────┐ │
  windowStart='2025-09-19T14:50:10Z',│     │  │   test_random Collection    │ │
  windowEnd='2025-09-19T14:50:20Z'   │     │  │  {                          │ │
}                                    │     │  │    "_id": "job:test-random| │ │
                                     │     │  │     g:sensor:temperature|   │ │
ETL Results:1> EtlResult{            │     │  │     ws:2025-09-19T14:50:10Z│ │
  jobId='test-random',               │     │  │     we:2025-09-19T14:50:20Z│ │
  result=68.2,                       │     │  │     agg:max|...",           │ │
  aggregationType='max',             │     │  │    "jobId": "test-random",  │ │
  groupingKey='humidity'             │     │  │    "result": 32.3,          │ │
}                                    │     │  │    "windowStart": "...",    │ │
                                     │     │  │    "windowEnd": "..."       │ │
Rate: Every 10 seconds              │     │  │  }                          │ │
(3 results per window)               │     │  └─────────────────────────────┘ │
                                     │     │                                  │
                                     │     │  Documents: 69+ and growing      │
                                     │     │  Growth: +3 docs every 10s       │
                                     │     └──────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                            WINDOWING & GROWTH PATTERN                          │
└─────────────────────────────────────────────────────────────────────────────────┘

🕐 Window Timeline (10-second windows):
14:50:10-14:50:20 → 3 documents (temp, humidity, pressure max values)
14:50:20-14:50:30 → 3 documents (temp, humidity, pressure max values)
14:50:30-14:50:40 → 3 documents (temp, humidity, pressure max values)
...continuously...

📈 Document Growth:
- Every 10 seconds: +3 new documents
- After 10 minutes: ~180 documents
- After 1 hour: ~1,080 documents
- Each document contains max value for one sensor type in one window

🔄 Real-time Processing:
- Data flows continuously at 5 msg/sec
- Windows process every 10 seconds
- Results stored immediately in MongoDB
- No duplicate configs (prevented by WindowedConfigProcessor)
```

## 📋 Prerequisites

- Docker & Docker Compose
- Java 17+ (for building Flink job)
- Maven 3.6+

## 🚀 Quick Start

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
# Submit configuration (using Kafka console producer)
echo '{"jobId":"test-random","source":"kafka://etl.input.v1","transformations":[{"type":"max","keyBy":"sensor","window":"10s","params":{"field":"measurement"}}]}' | docker exec -i etl-flink-project-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic etl.config.v1

# Check results in MongoDB
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.getCollectionNames()"
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.test_random.countDocuments()"
```

## 🧪 Complete Testing Guide

### Step 1: Verify Random Data Generation
```bash
# Check random data is being produced
timeout 10 docker exec etl-flink-project-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic etl.input.v1 --offset latest --partition 0 --max-messages 5

# Expected output: Real-time JSON with current timestamps
# {"sensor":"humidity","measurement":42.4,"datetime":"2025-09-19T14:48:44Z","location":"room-c"}
```

### Step 2: Submit Configuration
```bash
# Submit test config (as single-line JSON)
echo '{"jobId":"test-windowed","source":"kafka://etl.input.v1","transformations":[{"type":"max","keyBy":"sensor","window":"10s","params":{"field":"measurement"}}]}' | docker exec -i etl-flink-project-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 --topic etl.config.v1

# Verify config was received
timeout 5 docker exec etl-flink-project-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic etl.config.v1 --from-beginning --max-messages 1
```

### Step 3: Monitor Real-time Processing
```bash
# Watch Flink processing logs
docker logs etl-flink-project-flink-taskmanager-1 --tail 20 | grep "ETL Results"

# Expected output:
# ETL Results:1> EtlResult{jobId='test-windowed', result=57.8}
# ETL Results:1> EtlResult{jobId='test-windowed', result=1038.1}
```

### Step 4: Verify MongoDB Growth
```bash
# Check collection creation
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.getCollectionNames()"

# Monitor document growth
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.test_windowed.countDocuments()"

# Wait 10 seconds and check again (should increase by ~3)
sleep 10 && docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.test_windowed.countDocuments()"
```

### Step 5: Examine Document Structure
```bash
# View sample documents
docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.test_windowed.find().limit(3).forEach(printjson)"

# Expected structure with windowing:
# {
#   "_id": "job:test-windowed|g:sensor:humidity|ws:2025-09-19T14:50:10Z|we:2025-09-19T14:50:20Z|...",
#   "jobId": "test-windowed",
#   "result": 57.8,
#   "windowStart": "2025-09-19T14:50:10Z",
#   "windowEnd": "2025-09-19T14:50:20Z",
#   "groupingKey": "humidity"
# }
```

## 📋 Configuration Examples

### Basic Windowed Aggregation
```json
{
  "jobId": "test-windowed",
  "source": "kafka://etl.input.v1",
  "transformations": [
    {
      "type": "max",
      "keyBy": "sensor",
      "window": "10s",
      "params": {
        "field": "measurement"
      }
    }
  ]
}
```

### Filtered + Windowed Processing
```json
{
  "jobId": "user1-job",
  "source": "kafka://etl.input.v1",
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
      "keyBy": "sensor",
      "window": "10s"
    }
  ],
  "outputTopic": "etl.output.v1"
}
```

### Multi-transformation Pipeline
```json
{
  "jobId": "user3-job",
  "source": "kafka://etl.input.v1",
  "transformations": [
    {
      "type": "filter_less",
      "params": {
        "field": "measurement",
        "threshold": 200.0
      }
    },
    {
      "type": "lowercase"
    },
    {
      "type": "normalize_string",
      "params": {
        "fields": ["sensor", "measurement_unit"]
      }
    },
    {
      "type": "min",
      "keyBy": "sensor",
      "window": "10s"
    }
  ],
  "outputTopic": "etl.output.v1"
}
```

## 🔧 System Architecture Components

### Services Overview
| Service | Port | Purpose | Technology |
|---------|------|---------|------------|
| **Zookeeper** | 2181 | Kafka coordination | Apache Zookeeper |
| **Kafka** | 9092 | Message streaming | Apache Kafka |
| **MongoDB** | 27017 | Results storage | MongoDB 7.0 |
| **Flink JobManager** | 8081 | Job coordination | Apache Flink 1.18 |
| **Flink TaskManager** | - | Job execution | Apache Flink 1.18 |
| **ETL API** | 8080 | Health checks | Spring Boot |
| **File Producer** | - | Random data generation | Spring Boot |

### Key Features
- **Random Data Generation**: Real-time sensor data with current timestamps
- **Windowed Processing**: Time-based aggregations (10-second windows)
- **Duplicate Prevention**: Prevents resubmission of same configuration
- **Growing Collections**: MongoDB documents increase with each window
- **Single Output**: One collection per jobId (no config metadata duplicates)

## 🔍 Troubleshooting

### Random Data Not Generating
```bash
# Check file producer mode
docker logs etl-flink-project-etl-file-producer-1 | grep -i "mode\|random"

# Expected: "Producer mode: random"
# If showing "file mode", recreate containers:
docker-compose down && docker-compose up -d
```

### Config Submission Issues
```bash
# Check for JSON parsing errors
docker logs etl-flink-project-flink-taskmanager-1 | grep -i "json\|parse"

# Common issue: Multi-line JSON submission
# Solution: Use single-line JSON with echo command
```

### No MongoDB Growth
```bash
# Check Flink job is processing
curl -s http://localhost:8081/jobs | grep -i running

# Check for windowing issues
docker logs etl-flink-project-flink-taskmanager-1 | grep -i window

# Verify data is reaching Flink
curl -s "http://localhost:8081/jobs/{JOB_ID}" | grep '"write-records"'
```

## 📈 Expected Results

### After 5 Minutes Running
- **Random Data**: Continuous generation at 5 msg/sec
- **MongoDB Documents**: ~90 documents (3 per 10-second window)
- **Window Pattern**: Regular 10-second intervals with current timestamps
- **Growth Rate**: +3 documents every 10 seconds

### Sample MongoDB Documents
```javascript
// test_random collection (windowed max aggregation)
{
  "_id": "job:test-random|g:sensor:humidity|ws:2025-09-19T14:50:10Z|we:2025-09-19T14:50:20Z|agg:max|f:measurement|h:783379260",
  "jobId": "test-random",
  "result": 57.8,
  "aggregationType": "max",
  "groupingField": "sensor",
  "groupingKey": "humidity",
  "windowStart": "2025-09-19T14:50:10Z",
  "windowEnd": "2025-09-19T14:50:20Z",
  "processedAt": "2025-09-19T14:50:19.941Z",
  "diagnostics": [
    "Applied max aggregation with 10s window",
    "Window: 2025-09-19T14:50:10Z to 2025-09-19T14:50:20Z"
  ]
}
```

## 📁 Project Structure

```
etl-flink-project/
├── config-user1.json          # Sample config (threshold + max)
├── config-user2.json          # Sample config (different threshold)
├── config-user3.json          # Sample config (comprehensive transformations)
├── config-user4.json          # Sample config (date extraction + sum)
├── test_random_config.json     # Test config for windowed processing
├── docker-compose.yml          # Service orchestration
├── flink/                      # Flink ETL job module
│   ├── pom.xml                # Flink project Maven configuration
│   ├── Dockerfile             # Flink job container
│   └── src/main/java/com/etl/flink/
│       ├── EtlFlinkJob.java           # Main Flink job
│       ├── model/                     # Data models (EtlConfig, SensorEvent)
│       ├── process/                   # Processing logic
│       │   ├── EtlCoFlatMapFunction.java      # Main stream processor
│       │   ├── WindowedConfigProcessor.java   # Config management + windowing
│       │   ├── EtlWindowProcessor.java        # Window operations
│       │   ├── WindowedResultMapper.java      # Result mapping
│       ├── sink/                      # MongoDB sink implementation
│       └── udf/                       # User-defined functions
├── services/
│   ├── etl-api/               # REST API service
│   └── etl-file-producer/     # Random data generation service
├── pom.xml                    # Parent Maven configuration
├── README.md                  # This documentation
└── .gitignore                # Git ignore rules
```

## 🚀 Real-Time Monitoring Commands

```bash
# Monitor document growth in real-time
watch -n 2 'docker exec etl-flink-project-mongo-1 mongosh etl_db --eval "db.test_random.countDocuments()"'

# Watch live data generation
docker exec etl-flink-project-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic etl.input.v1 --offset latest

# Monitor Flink processing
docker logs etl-flink-project-flink-taskmanager-1 -f | grep "ETL Results"

# Check Flink job metrics
curl -s "http://localhost:8081/jobs/$(curl -s http://localhost:8081/jobs | grep -o '[a-f0-9]\{32\}')" | grep -E '"write-records"|read-records'
```

---

## 🌟 Key Improvements in This Version

1. **✅ Real-time Random Data**: Moved from static file replay to live random generation
2. **✅ Current Timestamps**: All data uses current timestamps, not 2025-01-15
3. **✅ Windowed Processing**: 10-second time windows for realistic stream aggregations
4. **✅ Growing MongoDB**: Documents increase continuously with each window
5. **✅ Duplicate Prevention**: Prevents resubmission of same configuration
6. **✅ Single Collections**: One collection per jobId (no config metadata duplication)
7. **✅ Simplified Submission**: Direct Kafka console producer for config submission

For questions or issues, check the troubleshooting section or review container logs using `docker logs <container-name>`.