# Streaming ETL

A configurable streaming ETL pipeline for sensor data. Configurations are published to Kafka, a Flink job applies the configured transformations and aggregations, and results are written back to Kafka.

This repository contains the cluster version used for benchmark experiments.

## Architecture

```text
etl-file-producer  ->  etl.input.v1  ->  Flink ETL job  ->  etl.output.v1
                                           ^
                                           |
                         etl-api  ->  etl.config.v1
```

- **Flink job** (`flink/`): consumes configuration and sensor-event streams, processes them, and produces ETL results.
- **ETL API** (`services/etl-api/`): validates and publishes transformation configurations.
- **File producer** (`services/etl-file-producer/`): reads sensor data from a file and publishes input events.
- **Configurations** (`configs/`): example one- and four-transformation JSON configurations.

## Compatibility

The project is built for the software versions available on the research cluster:

| Component | Version |
|---|---:|
| Java source/target | 8 |
| Apache Flink | 1.9.3 |
| Scala binary version | 2.11 |
| Apache Kafka clients | 2.4.1 |
| Spring Boot | 2.7.18 |
| Maven | 3.x |

The cluster commands assume:

- Flink installation: `/usr/local/flink`
- Kafka tools: `/usr/hdp/current/kafka-broker`
- Kafka brokers: `clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667`
- ZooKeeper: `clu01.softnet.tuc.gr:2182`
- Cluster working directory: `/home/MyDirectory`

## Build

On the Ubuntu VM or another machine with Java 8 and Maven installed:

```bash
mvn clean package -DskipTests
```

The build produces:

```text
flink/target/etl-flink-1.0.0.jar
services/etl-api/target/etl-api-1.0.0.jar
services/etl-file-producer/target/etl-file-producer-1.0.0.jar
```

## Cluster deployment

Copy the three JARs, the `configs/` directory, and the benchmark script to the cluster working directory:

```bash
scp flink/target/etl-flink-1.0.0.jar project/location
scp services/etl-api/target/etl-api-1.0.0.jar project/location
scp services/etl-file-producer/target/etl-file-producer-1.0.0.jar project/location
scp -r configs/ project/location
scp run-benchmark.sh project/location
```

Then connect to the cluster:

```bash
ssh clusterCredentials
chmod +x run-benchmark.sh
```

Create the Kafka topics before starting the services. The expected topics are:

- `etl.config.v1`: transformation configurations
- `etl.input.v1`: sensor events
- `etl.output.v1`: processed results

Start the API on the cluster with the cluster Kafka brokers:

```bash
java -jar etl-api-1.0.0.jar \
  --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
  --kafka.config.topic=etl.config.v1 \
  --server.port=8080 &

curl -s http://localhost:8080/health
```

Submit one or more configurations, for example:

```bash
curl -X POST http://localhost:8080/config \
  -H "Content-Type: application/json" \
  -d @configs/config-1t-filter-temp.json
```

Start the producer with the required cluster settings. Set `--producer.max.records` to the desired experiment size:

```bash
java -jar etl-file-producer-1.0.0.jar \
  --kafka.bootstrap.servers=clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667 \
  --kafka.input.topic=etl.input.v1 \
  --producer.max.records=5000000 \
  --producer.rate.per.sec=50000
```

After the input data and configurations are ready, run the benchmark with the desired Flink parallelism:

```bash
./run-benchmark.sh 4
./run-benchmark.sh 11
./run-benchmark.sh 22
```

The script cancels existing Flink jobs, resets the consumer group and output topic, submits the Flink JAR, and reports processing time and throughput.



## Configuration parameters

The Flink job accepts these parameters:

| Parameter | Default |
|---|---|
| `kafka.bootstrap.servers` | Cluster broker list |
| `kafka.config.topic` | `etl.config.v1` |
| `kafka.input.topic` | `etl.input.v1` |
| `kafka.output.topic` | `etl.output.v1` |

The producer also supports `--producer.max.records` and `--producer.rate.per.sec` for benchmark control.

## Stopping services

```bash
cd /usr/local/flink
./bin/flink list
./bin/flink cancel <JOB-ID>
pkill -f "etl-api-1.0.0.jar"
```

The file producer exits automatically when a finite `--producer.max.records` value is used.
