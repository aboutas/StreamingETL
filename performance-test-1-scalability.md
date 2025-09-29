# Performance Test 1: Scalability (Parallelism Impact)

## Test Objective
Measure how system performance scales with different parallelism levels while keeping input load and configuration constant.

## Fixed Parameters (Constants)
| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| **Input Rate** | 10 msg/sec | Controlled by RATE_PER_SEC in etl-file-producer |
| **Window Size** | 3 seconds | TumblingProcessingTimeWindows.of(Time.seconds(3)) |
| **Configurations** | 4 active configs | test123, light-high-humidity, air-quality-hot-days, cold-humid-rooms |
| **TaskManager Memory** | 1GB | -Xmx1024m per TaskManager |
| **Checkpointing** | 30s intervals | EXACTLY_ONCE mode |
| **Data Complexity** | Standard sensor events | 5 sensor types, 6 locations, JSON format |

## Variable Parameters (Test Matrix)
| Test Run | Parallelism | TaskManager Slots | Expected TaskManagers | Configuration Change |
|----------|-------------|-------------------|----------------------|---------------------|
| **Baseline** | 1 | 1 | 1 | `env.setParallelism(1)` + `taskmanager.numberOfTaskSlots: 1` |
| **Low Scale** | 2 | 2 | 1 | `env.setParallelism(2)` + `taskmanager.numberOfTaskSlots: 2` |
| **Current** | 4 | 4 | 1 | `env.setParallelism(4)` + `taskmanager.numberOfTaskSlots: 4` |
| **Medium Scale** | 8 | 4 | 2 | `env.setParallelism(8)` + Add 2nd TaskManager |
| **High Scale** | 16 | 4 | 4 | `env.setParallelism(16)` + Add 4 TaskManagers |

## Critical Metrics to Measure

| Metric | Measurement Method | Expected Range | Target/Acceptable |
|--------|-------------------|----------------|-------------------|
| **Throughput (records/sec)** | Count ETL Results in stdout logs over 60s | 40-160 records/min | Linear scaling with parallelism |
| **Processing Latency (ms)** | Time between Kafka input and ETL Result output | 100-5000ms | <3000ms for 99th percentile |
| **Memory Usage (MB)** | `docker stats etl-flink-project-flink-taskmanager-*` | 200-800MB | <70% of allocated memory |
| **CPU Utilization (%)** | `docker stats` CPU column | 20-90% | <80% average, spikes OK |
| **Backpressure** | Flink Web UI → Job → Operators → Backpressure tab | OK/LOW/HIGH | Should remain LOW/OK |
| **Checkpoint Duration (ms)** | Flink Web UI → Job → Checkpoints | 50-2000ms | <1000ms average |
| **Network Buffer Usage** | Flink Metrics → Network → Buffer pools | 0-100% | <80% utilization |
| **Config Distribution** | Log grep "Configuration registered.*subtask" | Even distribution | All subtasks should receive configs |

## Measurement Commands

```bash
# Start test with specific parallelism
# 1. Update EtlFlinkJob.java: env.setParallelism(X)
# 2. Update docker-compose.yml: taskmanager.numberOfTaskSlots: Y
mvn clean package -DskipTests
docker-compose up -d
docker exec etl-flink-project-flink-jobmanager-1 flink run -c com.etl.flink.EtlFlinkJob usrlib/etl-flink-1.0.0.jar

# Submit all test configurations
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-test123.json
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-light-high-humidity.json
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-air-quality-hot-days.json
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-cold-humid-rooms.json

# Performance measurement (run for 5 minutes each test)
timeout 300 docker logs etl-flink-project-flink-taskmanager-1 -f | grep "ETL Results" | wc -l
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
curl -s http://localhost:8081/jobs | jq '.jobs[] | .id' | head -1 | xargs -I {} curl -s http://localhost:8081/jobs/{}/metrics
```

## Expected Results & Conclusions

| Parallelism Level | Expected Throughput | Expected Latency | Expected Memory | Conclusion |
|-------------------|-------------------|------------------|-----------------|------------|
| **1** | 40 records/min | 1000-3000ms | 200-300MB | Baseline performance, single-threaded bottleneck |
| **2** | 60-80 records/min | 800-2000ms | 300-400MB | 1.5-2x improvement, good scaling |
| **4** | 120-160 records/min | 500-1500ms | 400-600MB | 3-4x improvement, optimal for current load |
| **8** | 160-200 records/min | 300-1000ms | 600-800MB | Diminishing returns, over-parallelized for load |
| **16** | 160-180 records/min | 300-800ms | 800-1200MB | No improvement, resource waste |

**Key Insights to Validate:**
- ✅ Linear scaling up to optimal parallelism (around 4-8 for 10 msg/sec)
- ✅ Memory usage increases linearly with parallelism
- ✅ Latency decreases with parallelism until over-parallelization
- ✅ Config distribution works correctly across all subtasks
- ✅ No backpressure at reasonable parallelism levels