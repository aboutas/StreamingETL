# Performance Test 2: Throughput (Input Load Impact)

## Test Objective
Determine maximum system capacity and identify bottlenecks by increasing input data rate while keeping system configuration constant.

## Fixed Parameters (Constants)
| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| **Parallelism** | 4 | `env.setParallelism(4)` - optimal from Test 1 |
| **TaskManager Slots** | 4 | `taskmanager.numberOfTaskSlots: 4` |
| **TaskManagers** | 1 | Single TaskManager instance |
| **Window Size** | 3 seconds | TumblingProcessingTimeWindows.of(Time.seconds(3)) |
| **Configurations** | 4 active configs | test123, light-high-humidity, air-quality-hot-days, cold-humid-rooms |
| **Memory Allocation** | 1GB per TaskManager | -Xmx1024m |
| **Checkpointing** | 30s intervals | EXACTLY_ONCE mode |
| **Data Format** | Standard sensor JSON | 5 sensor types, 6 locations per message |

## Variable Parameters (Test Matrix)
| Test Run | Input Rate (msg/sec) | Expected Events/Min | Producer Config Change | Load Level |
|----------|---------------------|-------------------|----------------------|------------|
| **Minimal** | 1 msg/sec | 240 events/min | `RATE_PER_SEC=1` | Underutilized |
| **Light** | 5 msg/sec | 1200 events/min | `RATE_PER_SEC=5` (current) | Normal load |
| **Medium** | 10 msg/sec | 2400 events/min | `RATE_PER_SEC=10` | Moderate stress |
| **Heavy** | 25 msg/sec | 6000 events/min | `RATE_PER_SEC=25` | High load |
| **Stress** | 50 msg/sec | 12000 events/min | `RATE_PER_SEC=50` | Stress test |
| **Max** | 100 msg/sec | 24000 events/min | `RATE_PER_SEC=100` | Maximum capacity |

## Critical Metrics to Measure

| Metric | Measurement Method | Normal Range | Alert Threshold | Failure Threshold |
|--------|-------------------|--------------|-----------------|-------------------|
| **Input Lag (records)** | Kafka consumer lag via `kafka-consumer-groups` | 0-100 | >500 records | >2000 records |
| **Processing Throughput (records/sec)** | ETL Results count per second | Match input rate ±10% | <80% of input | <50% of input |
| **End-to-End Latency (ms)** | Kafka timestamp → ETL Result timestamp | 100-3000ms | >5000ms | >10000ms |
| **Memory Usage (MB)** | `docker stats` memory column | 200-600MB | >800MB | >900MB |
| **CPU Utilization (%)** | `docker stats` CPU column | 20-70% | >85% | >95% sustained |
| **Kafka Topic Partition Lag** | Per-partition consumer lag | 0-50 | >200 | >1000 |
| **Flink Backpressure** | Web UI backpressure indicators | OK/LOW | HIGH | VERY_HIGH |
| **Network Throughput (MB/s)** | Docker network statistics | 0.1-5 MB/s | >10 MB/s | Network saturation |
| **Window Trigger Frequency** | Window close events per minute | Should match window size | Irregular | Missing windows |
| **Error Rate (%)** | Failed processing / Total events | 0-1% | >2% | >5% |

## Measurement Commands

```bash
# Update producer rate for each test
# Method 1: Environment variable
docker-compose stop etl-file-producer
docker-compose rm -f etl-file-producer
RATE_PER_SEC=25 docker-compose up -d etl-file-producer

# Method 2: Direct container restart
docker exec etl-flink-project-etl-file-producer-1 /bin/bash -c "export RATE_PER_SEC=25; java -jar app.jar"

# Measure input lag
docker exec etl-flink-project-kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group etl-data-consumer-v2

# Measure throughput (run for 2 minutes per test)
start_time=$(date +%s)
timeout 120 docker logs etl-flink-project-flink-taskmanager-1 -f | grep -c "ETL Results"
end_time=$(date +%s)
echo "Throughput: $((results_count / (end_time - start_time))) records/sec"

# Monitor system resources continuously
docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" --no-stream

# Check Flink metrics
JOB_ID=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id')
curl -s http://localhost:8081/jobs/$JOB_ID/vertices | jq '.vertices[] | {name: .name, parallelism: .parallelism, status: .status}'
```

## Expected Results & Performance Targets

| Input Rate | Expected Throughput | Expected Latency | Expected Memory | Expected CPU | System Status |
|------------|-------------------|------------------|-----------------|--------------|---------------|
| **1 msg/sec** | 240 events/min | 500-1500ms | 200-300MB | 10-20% | Underutilized, baseline |
| **5 msg/sec** | 1200 events/min | 800-2000ms | 300-400MB | 20-40% | Normal operation |
| **10 msg/sec** | 2400 events/min | 1000-3000ms | 400-500MB | 40-60% | Moderate load, good performance |
| **25 msg/sec** | 6000 events/min | 1500-4000ms | 500-700MB | 60-80% | High load, should handle well |
| **50 msg/sec** | ~10000 events/min | 2000-6000ms | 600-800MB | 70-90% | Stress level, may see lag |
| **100 msg/sec** | <15000 events/min | >3000ms | 700-900MB | 85-95% | At capacity, expect degradation |

## Critical Performance Insights to Validate

### **Throughput Scaling**
- ✅ Linear throughput scaling up to system capacity
- ⚠️  Identify the inflection point where throughput plateaus
- ❌ Detect when input rate exceeds processing capacity

### **Latency Behavior**
- ✅ Stable latency at low-medium loads (1-10 msg/sec)
- ⚠️  Gradual latency increase at high loads (25+ msg/sec)
- ❌ Exponential latency growth near capacity limits

### **Resource Utilization**
- ✅ Memory usage scales predictably with load
- ✅ CPU utilization increases linearly until bottleneck
- ⚠️  Network becomes bottleneck at high rates

### **System Limits**
- **Expected Capacity**: 25-50 msg/sec sustainable throughput
- **Breaking Point**: >50 msg/sec causes lag buildup
- **Recovery**: System should recover when load decreases

### **Bottleneck Identification**
1. **Network I/O**: At very high rates (>100 msg/sec)
2. **CPU**: At 80%+ utilization (typically 50+ msg/sec)
3. **Memory**: If windowing state grows too large
4. **Kafka Consumer**: If partition lag grows consistently
5. **Windowing**: If 3s windows can't close in time

## Test Success Criteria
- ✅ **Pass**: Handle 25 msg/sec with <5s latency and <10% loss
- ⚠️  **Warning**: Handle 50 msg/sec with degraded performance but recovery
- ❌ **Fail**: Cannot handle 10 msg/sec without significant lag buildup