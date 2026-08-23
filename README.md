# ETL Flink Project

A streaming ETL pipeline built with Apache Flink, Kafka, MongoDB, and Spring Boot. The project ingests sensor data, applies configurable transformations, aggregates results by job definition, and stores the output in MongoDB for downstream analysis.

## Overview

This repository demonstrates a small but realistic event-driven data processing system:

- A Spring Boot API accepts ETL job configurations.
- The API publishes configuration messages to Kafka.
- A file producer emits sensor events into Kafka.
- An Apache Flink job consumes both streams, applies transformations, windows data, and stores aggregated results in MongoDB.

The design follows a configurable ETL pattern: each job can define a set of transformations and aggregation rules without changing the Java code.

## Runtime Architecture

The application is designed to run as a containerized event-processing pipeline:

- The file producer emits sensor records into the Kafka input topic.
- The REST API accepts ETL job definitions and publishes them to the Kafka config topic.
- The Flink job consumes both Kafka streams, applies transformations and windowed aggregations, and writes results to MongoDB.
- The Flink dashboard is available locally for monitoring the running job.

### Components

- `flink/` — Flink streaming job and processing logic.
- `services/etl-api/` — Spring Boot REST API for submitting job configurations.
- `services/etl-file-producer/` — Simulated Kafka producer that emits sensor data.
- `configurations/` — JSON examples of ETL configurations.
- `docker-compose.yml` — Local orchestration for Kafka, Zookeeper, MongoDB, Flink, and services.

## Tech Stack

- Java 17
- Apache Flink 1.18.1
- Kafka + Zookeeper
- MongoDB 7
- Spring Boot 3.2
- Docker / Docker Compose
- Maven

## Supported Transformations

The ETL configuration supports these transformation types:

- `filter_greater`
- `filter_less`
- `normalize`
- `to_lowercase`
- `to_uppercase`
- `trim_whitespace`
- `sum`
- `max`
- `min`
- `avg`

These can be combined in a single job definition to create a pipeline for filtering, cleaning, grouping, and summarizing sensor data.

## Repository Structure

```text
.
├── README.md
├── docker-compose.yml
├── run.md
├── configurations/
│   ├── config-air-quality-hot-days.json
│   ├── config-cold-humid-rooms.json
│   ├── config-light-high-humidity.json
│   ├── ...
├── flink/
│   ├── Dockerfile
│   ├── pom.xml
│   └── src/
├── services/
│   ├── etl-api/
│   └── etl-file-producer/
└── target/
```

## Prerequisites

This project is built around Docker Compose, so the infrastructure services run in containers. Java and Maven are only needed if you want to rebuild the Flink JAR locally before starting the stack.

- Docker Desktop or Docker Engine with Compose enabled
- Java 17+ and Maven 3.8+ for local Flink builds
- curl or another HTTP client to test the API
- Free local ports: 2181, 27017, 8080, 8081, 9092

## Quick Start

### 1) Build the Flink job JAR

This is required because Docker mounts the locally built artifact into the Flink container.

```bash
cd flink
mvn package -DskipTests
cd ..
```

### 2) Start all services

```bash
docker-compose up --build -d
```

If your environment uses the newer Docker CLI syntax:

```bash
docker compose up --build -d
```

### 3) Check the containers

```bash
docker ps
```

You should see services for Kafka, Zookeeper, MongoDB, Flink jobmanager/taskmanager, the API, and the file producer.

### 4) Submit the Flink job manually

The Flink job is executed inside the running jobmanager container:

```bash
docker exec etl-flink-project-flink-jobmanager-1 flink run \
  -c com.etl.flink.EtlFlinkJob /opt/flink/usrlib/etl-flink-1.0.0.jar
```

> The exact container name may vary slightly depending on your Docker Compose version, but the pattern is usually `etl-flink-project-flink-jobmanager-1`.

### 5) Submit a configuration through the API

The project is designed to use the ready-made JSON configuration files from the `configurations/` folder.

Examples:

```bash
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d @configurations/config-air-quality-hot-days.json
```

```bash
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d @configurations/config-test-filter-greater.json
```

This sends a job definition to Kafka, and the Flink job begins processing matching sensor events.

## Configuration Files

The real ETL job definitions are stored in the `configurations/` directory. These are the files you should submit as payloads to the API.

Available examples include:

- `config-air-quality-hot-days.json`
- `config-cold-humid-rooms.json`
- `config-light-high-humidity.json`
- `config-noise-levels.json`
- `config-pressure-monitoring.json`
- `config-test-filter-greater.json`
- `config-test-filter-less.json`
- `config-test-normalize.json`
- `config-test-text-cleaning.json`

Each file contains a jobId and a list of transformations such as filters, normalization, and aggregations.

Example file structure:

```json
{
  "jobId": "air-quality-hot-days",
  "source": "kafka://etl.input.v1",
  "transformations": [
    {
      "type": "filter_greater",
      "params": {
        "sensor": "air_quality",
        "field": "measurement",
        "threshold": 100.0
      }
    },
    {
      "type": "max",
      "keyBy": "location",
      "params": {
        "field": "measurement",
        "sensor": "air_quality"
      }
    }
  ]
}
```

## API Endpoints

### Health

```bash
curl http://localhost:8080/health
```

Response example:

```json
{
  "status": "UP",
  "service": "etl-api",
  "timestamp": "2026-08-23T11:00:00Z"
}
```

### Submit config

Use one of the ready-made files from the `configurations/` folder instead of creating an ad-hoc inline JSON payload.

```bash
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d @configurations/config-air-quality-hot-days.json
```

Other valid examples:

```bash
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d @configurations/config-test-filter-greater.json

curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d @configurations/config-test-normalize.json
```

## Access Points

- ETL API: http://localhost:8080
- Flink UI: http://localhost:8081
- Kafka broker: localhost:9092
- MongoDB: localhost:27017
- Zookeeper: localhost:2181

## Data Flow

1. The file producer emits sensor events into Kafka.
2. The ETL API accepts and validates job configuration.
3. Kafka config messages are read by the Flink job.
4. The Flink job connects config and event streams.
5. Data is transformed, filtered, and aggregated.
6. Final results are written to MongoDB using the job ID as the collection identifier.

## Output Storage

Results are stored in MongoDB under a collection name derived from the `jobId`. For example, if `jobId` is `air-quality-hot-days`, the corresponding collection is created for that product stream.

## Stop the Stack

```bash
docker-compose down
```

Or:

```bash
docker compose down
```

## Notes

- The Flink job JAR is built locally and mounted into the container via `./flink/target:/opt/flink/usrlib`.
- The file producer starts generating sensor data automatically on startup.
- The project is intentionally modular and config-driven, which makes it easy to add new transformation rules and new ETL jobs.
- The included configuration examples are a good starting point for experimentation and demos.

## Troubleshooting

### Flink job not found

If the JAR is missing, rebuild it before starting Docker:

```bash
cd flink
mvn package -DskipTests
```

### API returns connection errors

Make sure the Kafka stack is healthy and the API service has started:

```bash
docker ps
curl http://localhost:8080/health
```

### MongoDB or Kafka not ready

Restart the stack after all dependencies have settled:

```bash
docker-compose down
docker-compose up --build -d
```


