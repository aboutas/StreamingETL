# Performance Test 3: Window Performance (Window Size Impact)

## Test Objective
Analyze how different window sizes affect memory usage, processing latency, result accuracy, and system throughput while keeping input load and parallelism constant.

## Fixed Parameters (Constants)
| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| **Input Rate** | 10 msg/sec | `RATE_PER_SEC=10` - moderate load from Test 2 |
| **Parallelism** | 4 | `env.setParallelism(4)` - optimal from Test 1 |
| **TaskManager Slots** | 4 | `taskmanager.numberOfTaskSlots: 4` |
| **TaskManagers** | 1 | Single TaskManager instance |
| **Memory Allocation** | 1GB | -Xmx1024m per TaskManager |
| **Configurations** | 4 active configs | test123, light-high-humidity, air-quality-hot-days, cold-humid-rooms |
| **Checkpointing** | 30s intervals | EXACTLY_ONCE mode |
| **Sensor Types** | 5 types | humidity, temperature, air_quality, light, pressure |
| **Location Count** | 6 locations | room-a, room-b, room-c, kitchen, lobby, conference-room |

## Variable Parameters (Test Matrix)
| Test Run | Window Size | Code Change Required | Window Type | Expected Behavior |
|----------|-------------|---------------------|-------------|-------------------|
| **Ultra-Fast** | 1 second | `Time.seconds(1)` | TumblingProcessingTimeWindows | High frequency results, minimal state |
| **Current** | 3 seconds | `Time.seconds(3)` | TumblingProcessingTimeWindows | Baseline performance |
| **Medium** | 10 seconds | `Time.seconds(10)` | TumblingProcessingTimeWindows | More accurate aggregations |
| **Long** | 30 seconds | `Time.seconds(30)` | TumblingProcessingTimeWindows | High accuracy, increased latency |
| **Extended** | 60 seconds | `Time.seconds(60)` | TumblingProcessingTimeWindows | Maximum accuracy, highest memory |

## Critical Metrics to Measure

| Metric | Measurement Method | 1s Window | 3s Window | 10s Window | 30s Window | 60s Window |
|--------|-------------------|-----------|-----------|------------|------------|------------|
| **Window State Memory (MB)** | Flink Web UI → Task Managers → Memory | 50-100MB | 100-200MB | 200-400MB | 400-600MB | 600-800MB |
| **Window Trigger Frequency (triggers/min)** | Count window close events | 3600/min | 1200/min | 360/min | 120/min | 60/min |
| **Result Output Rate (results/min)** | ETL Results with aggregationType≠null | ~600/min | ~200/min | ~60/min | ~20/min | ~10/min |
| **End-to-End Latency (ms)** | Input event → Aggregated result | 1000-2000ms | 3000-4000ms | 10000-12000ms | 30000-35000ms | 60000-65000ms |
| **Memory Growth Rate (MB/min)** | Monitor state backend growth | Stable | Stable | Slow growth | Moderate growth | Fast growth |
| **Aggregation Accuracy** | Events per window result | 10-30 events | 30-90 events | 100-300 events | 300-900 events | 600-1800 events |
| **CPU Usage During Window Close** | Peak CPU during aggregation | 20-40% | 30-50% | 40-70% | 50-80% | 60-90% |
| **Checkpoint Size (MB)** | State backend snapshot size | 10-50MB | 20-100MB | 50-200MB | 100-400MB | 200-600MB |
| **Network Overhead** | Inter-operator data transfer | Low | Low-Med | Medium | Med-High | High |
| **Backpressure Duration** | Window processing bottlenecks | None | None | Occasional | Frequent | Sustained |

## Code Changes Required for Each Test

```java
// EtlFlinkJob.java - Line 134
// TEST 1: Ultra-Fast Windows (1 second)
.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))

// TEST 2: Current Baseline (3 seconds) - No change needed
.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))

// TEST 3: Medium Windows (10 seconds)
.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

// TEST 4: Long Windows (30 seconds)
.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))

// TEST 5: Extended Windows (60 seconds)
.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
```

## Measurement Commands

```bash
# For each window size test:
# 1. Update EtlFlinkJob.java with new window size
# 2. Rebuild and restart
mvn clean package -DskipTests
docker exec etl-flink-project-flink-jobmanager-1 flink cancel $(docker exec etl-flink-project-flink-jobmanager-1 flink list | grep -o '[a-f0-9]\{32\}')
docker exec etl-flink-project-flink-jobmanager-1 flink run -c com.etl.flink.EtlFlinkJob usrlib/etl-flink-1.0.0.jar

# Submit test configurations
curl -X POST http://localhost:8080/config -H "Content-Type: application/json" -d @config-test123.json

# Measure window performance (run each test for 10 minutes)
timeout 600 docker logs etl-flink-project-flink-taskmanager-1 -f | grep -E "(ETL Results|Window)" | tee window-${WINDOW_SIZE}s-test.log

# Monitor memory growth
for i in {1..10}; do
  docker stats --no-stream --format "{{.MemUsage}}" etl-flink-project-flink-taskmanager-1
  sleep 60
done

# Check Flink Web UI metrics
JOB_ID=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id')
curl -s http://localhost:8081/jobs/$JOB_ID/vertices | jq '.vertices[] | select(.name | contains("Window")) | {name: .name, parallelism: .parallelism, metrics}'

# Measure checkpoint performance
curl -s http://localhost:8081/jobs/$JOB_ID/checkpoints | jq '.latest | {duration: .end_to_end_duration, size: .state_size}'
```

## Expected Results & Window Performance Matrix

| Window Size | Memory Usage | Result Frequency | Latency | Aggregation Quality | CPU Impact | Use Case |
|-------------|--------------|------------------|---------|-------------------|------------|----------|
| **1 second** | 50-100MB | Very High (3600/min) | Very Low (1-2s) | Low accuracy | Low-Medium | Real-time alerts |
| **3 seconds** | 100-200MB | High (1200/min) | Low (3-4s) | Good accuracy | Medium | Current baseline |
| **10 seconds** | 200-400MB | Medium (360/min) | Medium (10-12s) | High accuracy | Medium-High | Trend analysis |
| **30 seconds** | 400-600MB | Low (120/min) | High (30-35s) | Very high accuracy | High | Reporting |
| **60 seconds** | 600-800MB | Very Low (60/min) | Very High (60-65s) | Maximum accuracy | Very High | Batch-like processing |

## Critical Performance Insights to Validate

### **Memory vs. Window Size Relationship**
- ✅ **Linear Growth**: Memory usage should scale linearly with window size
- ✅ **State Cleanup**: Old window state should be garbage collected properly
- ⚠️  **Memory Leaks**: Watch for unexpected memory growth over time
- ❌ **OOM Risk**: 60s windows should not exceed 800MB memory usage

### **Latency vs. Accuracy Trade-off**
- ✅ **Predictable Latency**: Should match window size + processing time
- ✅ **Quality Improvement**: Longer windows should contain more events
- ⚠️  **Diminishing Returns**: 60s windows may not be worth the latency cost
- ❌ **Timeout Issues**: Windows should not fail to close due to size

### **Throughput Impact**
- ✅ **Stable Input Processing**: Window size shouldn't affect input consumption
- ✅ **Variable Output Rate**: Output frequency should be inversely proportional to window size
- ⚠️  **Processing Spikes**: Larger windows may cause CPU spikes during closes
- ❌ **Backpressure**: Large windows shouldn't cause upstream backpressure

### **System Stability**
- ✅ **Checkpoint Success**: All window sizes should checkpoint successfully
- ✅ **Recovery**: System should recover properly from failures
- ⚠️  **Resource Limits**: Monitor for resource exhaustion at large window sizes
- ❌ **System Failure**: No window size should crash the system

## Optimal Window Size Recommendations

| Business Requirement | Recommended Window Size | Trade-off Reasoning |
|----------------------|------------------------|-------------------|
| **Real-time Alerts** | 1-3 seconds | Priority: Low latency over accuracy |
| **Live Dashboards** | 3-10 seconds | Balance: Reasonable accuracy with responsiveness |
| **Trend Analysis** | 10-30 seconds | Priority: Statistical accuracy over speed |
| **Batch Reports** | 30-60 seconds | Priority: Maximum accuracy, latency acceptable |
| **Current System** | 3 seconds | Optimal for general-purpose real-time ETL |

## Test Success Criteria

- ✅ **Pass**: All window sizes (1s-60s) process without errors
- ✅ **Pass**: Memory usage stays within predicted ranges
- ✅ **Pass**: Latency matches window size expectations
- ⚠️  **Warning**: Memory growth >10% above predictions
- ⚠️  **Warning**: CPU spikes >90% during window processing
- ❌ **Fail**: Any window size causes system instability or OOM
- ❌ **Fail**: Windows fail to close or trigger properly