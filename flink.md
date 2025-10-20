# Flink ETL Processing Logic - Complete Documentation

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Models](#data-models)
4. [Main Execution Flow](#main-execution-flow)
5. [CoFlatMap Processing Logic](#coflatmap-processing-logic)
6. [Transformation Types](#transformation-types)
7. [Windowing and Aggregation](#windowing-and-aggregation)
8. [Time Semantics and Watermarks](#time-semantics-and-watermarks)
9. [Kafka Integration](#kafka-integration)
10. [State Management](#state-management)
11. [Key Distribution and Parallelism](#key-distribution-and-parallelism)
12. [Serialization and Deserialization](#serialization-and-deserialization)
13. [Error Handling](#error-handling)
14. [Configuration Examples](#configuration-examples)

---

## Overview

This is a real-time ETL (Extract, Transform, Load) system built on Apache Flink 1.9.3 that processes sensor data streams with dynamically configurable transformations. The system uses a CoFlatMap architecture to combine configuration and data streams, applies transformations, performs windowed aggregations, and outputs results to Kafka.

**Key Features:**
- Dynamic configuration via Kafka config stream
- TRUE CoFlatMap implementation with parallel processing
- Element transformations (filters) and aggregation transformations (sum, max, min, avg)
- 3-second tumbling windows for aggregations
- Null-safe deserialization for empty Kafka topics
- Support for multiple sensor types with independent processing paths
- Cross-field filtering capabilities

**Target Environment:**
- Deployed on SoftNet Cluster (TUC) with 23 servers
- Flink 1.9.3, Java 8, Kafka 2.4.1
- 11 worker nodes with configurable parallelism

---

## Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FLINK ETL JOB                               │
└─────────────────────────────────────────────────────────────────────┘

CONFIG STREAM                              DATA STREAM
─────────────                              ───────────
    │                                           │
    │ Kafka: etl.config.v1                     │ Kafka: etl.input.v1
    │ (EtlConfig JSON)                         │ (SensorEvent JSON)
    ▼                                           ▼
┌─────────────────┐                      ┌─────────────────┐
│ Config Consumer │                      │ Data Consumer   │
│ NullSafeSchema  │                      │ NullSafeSchema  │
└────────┬────────┘                      └────────┬────────┘
         │                                         │
         │ Filter nulls                            │ Filter nulls
         ▼                                         ▼
┌─────────────────┐                      ┌─────────────────┐
│ Deserialize to  │                      │ Deserialize to  │
│   EtlConfig     │                      │  SensorEvent    │
└────────┬────────┘                      └────────┬────────┘
         │                                         │
         │ KeyBy: "universal"                      │ Assign Timestamps
         │                                         │ & Watermarks
         │                                         │
         │                                         │ KeyBy: "universal"
         │                                         │
         └──────────┬──────────────────────────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │   CoFlatMapProcessor│
         │   (RichCoFlatMap)   │
         │                     │
         │  - Stores configs   │
         │    in MapState      │
         │  - Processes events │
         │    with configs     │
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │   EtlResult Stream  │
         └──────────┬──────────┘
                    │
         ┌──────────┴──────────┐
         │                     │
         ▼                     ▼
┌────────────────┐    ┌────────────────┐
│ Config Results │    │  Data Results  │
│ (no agg type)  │    │ (with agg type)│
└────────────────┘    └────────┬───────┘
                               │
                               │ Assign Timestamps
                               │ & Watermarks
                               ▼
                      ┌────────────────┐
                      │   KeyBy:       │
                      │ groupingKey +  │
                      │ jobId + aggType│
                      └────────┬───────┘
                               │
                               ▼
                      ┌────────────────┐
                      │ Tumbling Window│
                      │   3 seconds    │
                      └────────┬───────┘
                               │
                               ▼
                      ┌────────────────┐
                      │Window Aggregator│
                      │ (sum/max/min/  │
                      │      avg)      │
                      └────────┬───────┘
                               │
         ┌─────────────────────┘
         │
         ▼
┌────────────────────┐
│  Union All Results │
└─────────┬──────────┘
          │
          ▼
┌─────────────────────┐
│   Kafka Producer    │
│  etl.output.v1      │
└─────────────────────┘
```

### Component Breakdown

1. **EtlFlinkJob** (Main Entry Point) - `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:34`
   - Initializes Flink execution environment
   - Sets up Kafka consumers and producers
   - Orchestrates data flow pipeline

2. **CoFlatMapProcessor** - `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:27`
   - Processes configuration and data streams
   - Maintains configuration state per subtask
   - Applies transformations and filters

3. **ElementTransformations** - `flink/src/main/java/com/etl/flink/udf/ElementTransformations.java:12`
   - Filter transformations (filter_greater, filter_less, filter_cross_field)

4. **WindowedAggregations** - `flink/src/main/java/com/etl/flink/udf/WindowedAggregations.java:14`
   - Aggregation functions (sum, max, min, avg)

---

## Data Models

### SensorEvent
**File:** `flink/src/main/java/com/etl/flink/model/SensorEvent.java:9`

Represents a single sensor reading from the input stream.

```java
{
  "jobId": "optional-job-id",           // Job identifier
  "sensor": "temperature",              // Sensor type (temperature, humidity, etc.)
  "measurement": 25.5,                  // Numeric measurement value
  "measurement_unit": "celsius",        // Unit of measurement
  "datetime": "2025-10-12T18:44:21Z",   // ISO-8601 timestamp
  "location": "Room A",                 // Sensor location
  "data_quality": "good"                // Optional quality indicator
}
```

**Key Fields:**
- `sensor`: Used for keying, filtering, and grouping
- `measurement`: Primary numeric field for aggregations
- `datetime`: Converted to Instant for event time processing
- All timestamp fields use `java.time.Instant` for compatibility

### EtlConfig
**File:** `flink/src/main/java/com/etl/flink/model/EtlConfig.java:9`

Defines the transformation pipeline for processing events.

```java
{
  "jobId": "job-001",                    // Unique job identifier
  "source": "kafka://etl.input.v1",      // Data source
  "transformations": [                   // Array of transformations
    {
      "type": "filter_greater",          // Transformation type
      "params": {                        // Type-specific parameters
        "sensor": "temperature",
        "field": "measurement",
        "threshold": 25.0
      }
    },
    {
      "type": "avg",                     // Aggregation type
      "keyBy": "location",               // Grouping field
      "params": {
        "field": "measurement",
        "sensor": "humidity"
      }
    }
  ],
  "outputTopic": "etl.output.v1"        // Optional output override
}
```

### Transformation
**File:** `flink/src/main/java/com/etl/flink/model/Transformation.java:9`

Defines a single transformation operation.

```java
{
  "type": "sum|max|min|avg|filter_greater|filter_less|filter_cross_field",
  "params": {                            // Transformation-specific parameters
    "field": "measurement",              // Field to operate on
    "sensor": "temperature",             // Sensor filter (optional)
    "threshold": 30.0,                   // For filter operations
    // Cross-field filter params:
    "filter_sensor": "temperature",
    "filter_field": "measurement",
    "filter_operator": ">",
    "filter_threshold": 25.0,
    "target_sensor": "humidity"
  },
  "keyBy": "sensor|location|measurement_unit|...",  // Grouping field for aggregations
  "window": "TUMBLING"                   // Window type (currently hardcoded to 3s)
}
```

### EtlResult
**File:** `flink/src/main/java/com/etl/flink/model/EtlResult.java:9`

Output format containing processed results.

```java
{
  "_id": "optional-id",
  "jobId": "job-001",
  "source": "kafka://etl.input.v1",
  "transformations": [...],              // Copy of applied transformations
  "groupingField": "location",           // Field used for grouping
  "groupingKey": "Room A",               // Actual grouping value
  "windowStart": "2025-10-12T18:44:21Z", // Window start time (optional)
  "windowEnd": "2025-10-12T18:44:24Z",   // Window end time (optional)
  "aggregationType": "avg",              // Type of aggregation performed
  "field": "measurement",                // Field that was aggregated
  "result": 24.5,                        // Computed result value or original event
  "sensorType": "temperature",           // Contributing sensor type
  "measurementUnit": "celsius",          // Measurement unit
  "location": "Room A",                  // Sensor location
  "diagnostics": [                       // Processing diagnostics
    "Processed on subtask: 2",
    "Applied avg aggregation",
    "WINDOWED avg: 3 values aggregated = 24.50"
  ],
  "processedAt": "2025-10-12T18:44:24Z"  // Processing timestamp
}
```

---

## Main Execution Flow

### Initialization
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:38`

1. **Parse Command-Line Arguments** (Line 42)
   ```java
   ParameterTool params = ParameterTool.fromArgs(args);
   ```
   - Command-line args are distributed to all TaskManagers automatically
   - Environment variables do NOT work in distributed Flink deployments

2. **Configure Execution Environment** (Line 44-50)
   ```java
   StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
   env.getConfig().setGlobalJobParameters(params);
   env.enableCheckpointing(30000);  // 30 second checkpoints
   env.setParallelism(4);            // Default parallelism
   ```

3. **Setup Kafka Properties** (Line 63-69)
   ```java
   Properties kafkaProps = new Properties();
   kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers);
   kafkaProps.setProperty("group.id", "etl-flink-consumer");
   kafkaProps.setProperty("auto.offset.reset", "earliest");
   ```

### Stream Creation

**Config Stream** (Line 88-94)
```java
DataStream<EtlConfig> configStream = env
    .addSource(configConsumer)
    .name("Config Source")
    .filter(str -> str != null && !str.isEmpty())
    .map(new ConfigDeserializer())
    .filter(config -> config != null)
    .keyBy(new ConfigKeyExtractor());  // Keys by "universal"
```

**Event Stream** (Line 97-116)
```java
DataStream<SensorEvent> eventStream = env
    .addSource(dataConsumer)
    .name("Data Source")
    .filter(str -> str != null && !str.isEmpty())
    .map(new EventDeserializer())
    .filter(event -> event != null)
    .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor<SensorEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(SensorEvent event) {
                return event.getDatetime().toEpochMilli();
            }
        }
    )
    .keyBy(event -> "universal");  // All events use same key
```

### CoFlatMap Processing
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:119`

```java
DataStream<EtlResult> processedStream = configStream
    .connect(eventStream)
    .flatMap(new CoFlatMapProcessor());
```

The CoFlatMapProcessor is the heart of the system, explained in detail in the next section.

### Result Splitting
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:124`

Results are split into two streams based on whether they are configuration confirmations or data results:

```java
// Config confirmations (no aggregation type)
DataStream<EtlResult> configResults = processedStream
    .filter(result -> result.getAggregationType() == null);

// Data results (with aggregation type)
DataStream<EtlResult> dataResults = processedStream
    .filter(result -> result.getAggregationType() != null)
    .assignTimestampsAndWatermarks(...);
```

### Windowing
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:142`

Data results are grouped and windowed for aggregation:

```java
DataStream<EtlResult> windowedResults = dataResults
    .keyBy(result -> String.format("%s|%s|%s",
        result.getGroupingKey(),
        result.getJobId(),
        result.getAggregationType()
    ))
    .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
    .aggregate(new WindowAggregator());
```

**Key Components:**
- **KeyBy**: Groups by `groupingKey|jobId|aggregationType` to ensure independent aggregation streams
- **Window**: 3-second tumbling windows based on processing time
- **Aggregate**: Custom WindowAggregator that computes sum/max/min/avg

### Output
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:152`

```java
DataStream<EtlResult> allResults = configResults.union(windowedResults);
allResults.addSink(kafkaProducer).name("Kafka Sink");
```

---

## CoFlatMap Processing Logic

### Overview
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:27`

The `CoFlatMapProcessor` extends `RichCoFlatMapFunction<EtlConfig, SensorEvent, EtlResult>` and implements TRUE parallel processing by:
1. Storing configurations in keyed state (MapState)
2. Processing incoming events against all stored configurations
3. Supporting both element transformations and aggregation transformations
4. Providing detailed diagnostics for debugging

### State Initialization
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:34`

```java
@Override
public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    configState = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("configs-by-key", String.class, EtlConfig.class)
    );
}
```

Each parallel subtask maintains its own MapState with configurations keyed by jobId.

### Processing Configurations (flatMap1)
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:42`

When a configuration arrives:

```java
@Override
public void flatMap1(EtlConfig config, Collector<EtlResult> out) throws Exception {
    LOG.info("Received config for job: {} on subtask: {}",
        config.getJobId(), getRuntimeContext().getIndexOfThisSubtask());

    // Store config in state
    configState.put(config.getJobId(), config);

    // Emit configuration confirmation
    EtlResult configResult = new EtlResult();
    configResult.setJobId(config.getJobId());
    configResult.setResult("Configuration registered successfully on subtask " +
                          getRuntimeContext().getIndexOfThisSubtask());
    configResult.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());
    out.collect(configResult);
}
```

**Key Points:**
- Configuration stored by jobId in MapState
- Immediate confirmation result emitted
- Each subtask maintains its own configurations
- Since both streams use "universal" key, all configs go to all subtasks

### Processing Events (flatMap2)
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:68`

When a sensor event arrives:

```java
@Override
public void flatMap2(SensorEvent event, Collector<EtlResult> out) throws Exception {
    // Process event against ALL configs in this subtask's state
    Iterable<Map.Entry<String, EtlConfig>> configs = configState.entries();

    boolean hasConfigs = false;
    for (Map.Entry<String, EtlConfig> configEntry : configs) {
        hasConfigs = true;
        EtlConfig config = configEntry.getValue();

        try {
            processEventWithConfig(event, config, out);
        } catch (Exception e) {
            emitErrorResult(event, config, e, out);
        }
    }

    if (!hasConfigs) {
        LOG.debug("No configs available on subtask: {}, dropping event",
                 getRuntimeContext().getIndexOfThisSubtask());
    }
}
```

**Key Points:**
- Each event is processed against ALL configurations in state
- Multiple results can be emitted for a single event
- Events without configurations are silently dropped
- Errors are caught and emitted as error results

### Event Processing with Configuration
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:94`

The `processEventWithConfig` method handles the core transformation logic:

```java
private void processEventWithConfig(SensorEvent event, EtlConfig config,
                                   Collector<EtlResult> out) throws Exception {
    List<String> diagnostics = new ArrayList<>();

    // Separate element transformations from aggregation transformations
    List<Transformation> elementTransformations = new ArrayList<>();
    List<Transformation> aggregationTransformations = new ArrayList<>();

    for (Transformation transformation : config.getTransformations()) {
        if (isElementTransformation(transformation.getType())) {
            elementTransformations.add(transformation);
        } else if (isAggregationTransformation(transformation.getType())) {
            aggregationTransformations.add(transformation);
        }
    }

    // Process element transformations (filters)
    SensorEvent filteredEvent = event;
    for (Transformation transformation : elementTransformations) {
        filteredEvent = applyElementTransformation(filteredEvent, transformation, diagnostics);
        if (filteredEvent == null) break;
    }

    // Process aggregation transformations (each can have independent sensor filter)
    for (Transformation transformation : aggregationTransformations) {
        processWindowedAggregation(event, transformation, config, diagnostics, out);
    }

    // Emit filtered result if no aggregations
    if (aggregationTransformations.isEmpty() && filteredEvent != null) {
        createFinalResult(filteredEvent, config, diagnostics, out);
    }
}
```

**Processing Strategy:**
1. **Separate transformations** by type (element vs aggregation)
2. **Element transformations** are applied sequentially, potentially filtering events
3. **Aggregation transformations** each process the ORIGINAL event independently
4. This allows different sensor filters per aggregation

### Transformation Type Classification
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:194`

```java
private boolean isElementTransformation(String type) {
    return "filter_greater".equals(type) ||
           "filter_less".equals(type) ||
           "filter_cross_field".equals(type);
}

private boolean isAggregationTransformation(String type) {
    return "sum".equals(type) ||
           "max".equals(type) ||
           "min".equals(type) ||
           "avg".equals(type);
}
```

---

## Transformation Types

### Element Transformations (Filters)

Element transformations operate on individual events and can filter them out.

#### 1. Filter Greater
**File:** `flink/src/main/java/com/etl/flink/udf/ElementTransformations.java:14`

Keeps events where a field value exceeds a threshold.

```json
{
  "type": "filter_greater",
  "params": {
    "sensor": "temperature",      // Optional: filter by sensor type
    "field": "measurement",        // Field to check (default: "measurement")
    "threshold": 25.0              // Threshold value
  }
}
```

**Implementation:**
```java
public SensorEvent map(SensorEvent event) throws Exception {
    // Check sensor type if specified
    if (sensor != null && !sensor.equals(event.getSensor())) {
        return null;  // Filter out
    }

    // Check threshold
    Double fieldValue = getFieldValueAsDouble(event, field);
    if (fieldValue != null && fieldValue > threshold) {
        return event;  // Keep event
    }
    return null;  // Filter out
}
```

#### 2. Filter Less
**File:** `flink/src/main/java/com/etl/flink/udf/ElementTransformations.java:52`

Keeps events where a field value is below a threshold.

```json
{
  "type": "filter_less",
  "params": {
    "sensor": "humidity",
    "field": "measurement",
    "threshold": 30.0
  }
}
```

**Implementation:** Similar to filter_greater but uses `<` operator.

#### 3. Cross-Field Filter
**File:** `flink/src/main/java/com/etl/flink/udf/ElementTransformations.java:98`

Allows filtering based on one sensor type while processing another. Example: "Get max humidity when temperature > 25".

```json
{
  "type": "filter_cross_field",
  "params": {
    "filter_sensor": "temperature",     // Sensor to check condition
    "filter_field": "measurement",      // Field to check
    "filter_operator": ">",             // Operator: >, <, >=, <=, ==
    "filter_threshold": 25.0,           // Threshold value
    "target_sensor": "humidity"         // Sensor to process when condition met
  }
}
```

**Implementation:**
```java
public SensorEvent map(SensorEvent event) throws Exception {
    // If this is the target sensor, always pass through
    if (targetSensor != null && targetSensor.equals(event.getSensor())) {
        return event;
    }

    // If this is the filter sensor, check condition
    if (filterSensor != null && filterSensor.equals(event.getSensor())) {
        Double fieldValue = getFieldValueAsDouble(event, filterField);
        boolean conditionMet = evaluateCondition(fieldValue, filterOperator, filterThreshold);
        return conditionMet ? event : null;
    }

    return null;
}
```

### Aggregation Transformations

Aggregation transformations compute statistics over windows of events.

#### 1. Sum
**File:** `flink/src/main/java/com/etl/flink/udf/WindowedAggregations.java:16`

Sums values within a window.

```json
{
  "type": "sum",
  "keyBy": "location",           // Group by field
  "params": {
    "field": "measurement",       // Field to sum
    "sensor": "temperature"       // Optional: filter by sensor
  }
}
```

#### 2. Max
**File:** `flink/src/main/java/com/etl/flink/udf/WindowedAggregations.java:37`

Finds maximum value within a window.

```json
{
  "type": "max",
  "keyBy": "sensor",
  "params": {
    "field": "measurement",
    "sensor": "humidity"
  }
}
```

#### 3. Min
**File:** `flink/src/main/java/com/etl/flink/udf/WindowedAggregations.java:57`

Finds minimum value within a window.

```json
{
  "type": "min",
  "keyBy": "location",
  "params": {
    "field": "measurement"
  }
}
```

#### 4. Average
**File:** `flink/src/main/java/com/etl/flink/udf/WindowedAggregations.java:77`

Computes average value within a window.

```json
{
  "type": "avg",
  "keyBy": "measurement_unit",
  "params": {
    "field": "measurement",
    "sensor": "temperature"
  }
}
```

### Aggregation Processing in CoFlatMap
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:153`

```java
private boolean processWindowedAggregation(SensorEvent event, Transformation transformation,
                                          EtlConfig config, List<String> diagnostics,
                                          Collector<EtlResult> out) {
    String field = getFieldFromParams(transformation.getParams(), "measurement");
    String sensorFilter = getSensorFromParams(transformation.getParams());
    String keyBy = transformation.getKeyBy() != null ? transformation.getKeyBy() : "sensor";

    // Apply sensor filter if specified
    if (sensorFilter != null && !sensorFilter.equals(event.getSensor())) {
        return false;  // Skip this event - wrong sensor type
    }

    String groupingKey = getGroupingKeyFromEvent(event, keyBy);

    // Create result with aggregation metadata
    EtlResult result = new EtlResult();
    result.setJobId(config.getJobId());
    result.setGroupingField(keyBy);
    result.setGroupingKey(groupingKey);
    result.setAggregationType(transformation.getType());
    result.setField(field);
    result.setResult(getFieldValue(event, field));  // Raw value, will be aggregated downstream
    result.setSensorType(event.getSensor());
    result.setMeasurementUnit(event.getMeasurementUnit());
    result.setLocation(event.getLocation());

    out.collect(result);
    return true;
}
```

**Key Points:**
- Each aggregation emits individual results with raw values
- Results contain aggregation metadata (type, field, grouping)
- Actual aggregation happens downstream in the WindowAggregator
- Sensor filtering happens per aggregation, allowing multiple sensors in one config

---

## Windowing and Aggregation

### Window Configuration
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:148`

```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
```

**Window Type:** Tumbling (non-overlapping)
**Window Size:** 3 seconds
**Time Characteristic:** Processing time (not event time)

**Why Processing Time?**
- More predictable for demo/testing purposes
- Guarantees windows close every 3 seconds regardless of data arrival patterns
- Simpler watermark management

### Window Keying
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:143`

```java
.keyBy(result -> String.format("%s|%s|%s",
    result.getGroupingKey(),   // e.g., "Room A" (from location)
    result.getJobId(),          // e.g., "job-001"
    result.getAggregationType() // e.g., "avg"
))
```

This composite key ensures:
- Each unique combination gets its own aggregation window
- Different jobs don't interfere with each other
- Different aggregation types maintain separate state
- Same grouping key can have multiple aggregations

**Example:** If two events arrive:
- Event 1: location="Room A", jobId="job-001", type="avg" → Key: "Room A|job-001|avg"
- Event 2: location="Room A", jobId="job-001", type="max" → Key: "Room A|job-001|max"

These go to DIFFERENT windows and aggregate independently.

### WindowAggregator Implementation
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:220`

The WindowAggregator is an `AggregateFunction` with three type parameters:
- **IN**: EtlResult (incoming results with raw values)
- **ACC**: WindowAccumulator (accumulator state)
- **OUT**: EtlResult (final aggregated result)

#### WindowAccumulator
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:321`

```java
public static class WindowAccumulator {
    public String jobId;
    public String source;
    public List<Transformation> transformations;
    public String groupingField;
    public String groupingKey;
    public String aggregationType;
    public String field;
    public String sensorType;
    public String measurementUnit;
    public String location;
    public List<String> diagnostics;
    public int count;

    // Aggregation state
    public Double sum = 0.0;
    public Double max = Double.NEGATIVE_INFINITY;
    public Double min = Double.POSITIVE_INFINITY;

    public void accumulate(Double value) {
        if (value == null) return;
        sum += value;
        if (value > max) max = value;
        if (value < min) min = value;
    }

    public Double getResult() {
        switch (aggregationType) {
            case "sum": return sum;
            case "max": return max.equals(Double.NEGATIVE_INFINITY) ? 0.0 : max;
            case "min": return min.equals(Double.POSITIVE_INFINITY) ? 0.0 : min;
            case "avg": return count > 0 ? sum / count : 0.0;
            default: return sum;
        }
    }
}
```

#### createAccumulator
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:222`

```java
@Override
public WindowAccumulator createAccumulator() {
    return new WindowAccumulator();
}
```

Called once per window to create initial state.

#### add
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:227`

```java
@Override
public WindowAccumulator add(EtlResult result, WindowAccumulator accumulator) {
    // Initialize metadata from first result
    if (accumulator.jobId == null) {
        accumulator.jobId = result.getJobId();
        accumulator.source = result.getSource();
        accumulator.transformations = result.getTransformations();
        accumulator.groupingField = result.getGroupingField();
        accumulator.groupingKey = result.getGroupingKey();
        accumulator.aggregationType = result.getAggregationType();
        accumulator.field = result.getField();
        accumulator.sensorType = result.getSensorType();
        accumulator.measurementUnit = result.getMeasurementUnit();
        accumulator.location = result.getLocation();
        accumulator.diagnostics = new ArrayList<>();
    }

    // Accumulate value
    Double value = (Double) result.getResult();
    if (value != null) {
        accumulator.accumulate(value);
        accumulator.count++;
        accumulator.diagnostics.add(String.format("WINDOWED %s: %.2f contributing to %s",
                accumulator.aggregationType, value, accumulator.aggregationType));
    }

    return accumulator;
}
```

Called for each result entering the window.

#### getResult
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:254`

```java
@Override
public EtlResult getResult(WindowAccumulator accumulator) {
    EtlResult result = new EtlResult();
    result.setJobId(accumulator.jobId);
    result.setSource(accumulator.source);
    result.setTransformations(accumulator.transformations);
    result.setGroupingField(accumulator.groupingField);
    result.setGroupingKey(accumulator.groupingKey);
    result.setAggregationType(accumulator.aggregationType);
    result.setField(accumulator.field);
    result.setResult(accumulator.getResult());  // Computed aggregate value
    result.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());
    result.setSensorType(accumulator.sensorType);
    result.setMeasurementUnit(accumulator.measurementUnit);
    result.setLocation(accumulator.location);

    List<String> diagnostics = new ArrayList<>(accumulator.diagnostics);
    diagnostics.add(String.format("WINDOWED %s: %d values aggregated = %.2f",
            accumulator.aggregationType, accumulator.count, accumulator.getResult()));
    diagnostics.add("Window: 3s tumbling window");
    result.setDiagnostics(diagnostics);

    return result;
}
```

Called when window closes to produce final result.

#### merge
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:280`

```java
@Override
public WindowAccumulator merge(WindowAccumulator a, WindowAccumulator b) {
    a.sum += b.sum;
    a.count += b.count;
    if (b.max > a.max) a.max = b.max;
    if (b.min < a.min) a.min = b.min;
    a.diagnostics.addAll(b.diagnostics);
    return a;
}
```

Called when merging parallel sub-windows (for session windows or when parallelism > 1).

### Example Aggregation Flow

**Input Events (within 3-second window):**
```json
{"sensor": "temperature", "measurement": 20.0, "location": "Room A"}
{"sensor": "temperature", "measurement": 22.0, "location": "Room A"}
{"sensor": "temperature", "measurement": 24.0, "location": "Room A"}
```

**Config:**
```json
{
  "jobId": "job-001",
  "transformations": [{
    "type": "avg",
    "keyBy": "location",
    "params": {"field": "measurement", "sensor": "temperature"}
  }]
}
```

**Processing:**
1. CoFlatMapProcessor emits 3 EtlResults with raw values (20.0, 22.0, 24.0)
2. All keyed to "Room A|job-001|avg"
3. Enter same 3-second window
4. WindowAggregator:
   - `createAccumulator()` → new accumulator
   - `add(20.0)` → sum=20, count=1
   - `add(22.0)` → sum=42, count=2
   - `add(24.0)` → sum=66, count=3
   - Window closes
   - `getResult()` → 66/3 = 22.0

**Output:**
```json
{
  "jobId": "job-001",
  "groupingField": "location",
  "groupingKey": "Room A",
  "aggregationType": "avg",
  "field": "measurement",
  "result": 22.0,
  "sensorType": "temperature",
  "diagnostics": [
    "WINDOWED avg: 20.00 contributing to avg",
    "WINDOWED avg: 22.00 contributing to avg",
    "WINDOWED avg: 24.00 contributing to avg",
    "WINDOWED avg: 3 values aggregated = 22.00",
    "Window: 3s tumbling window"
  ]
}
```

---

## Time Semantics and Watermarks

This system uses **Processing Time** for windowing operations, though it also extracts event timestamps and generates watermarks. This section provides a deep dive into time semantics, watermark generation, and the design decisions behind using processing time.

### Time Semantics Overview

Flink supports two fundamental notions of time:

1. **Event Time**: The time when an event actually occurred (embedded in the data)
2. **Processing Time**: The time when an event is processed by the Flink operator (system clock)

**This system uses Processing Time windows, but extracts Event Time timestamps.**

### Why Processing Time Windows?

**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:148`

```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
```

#### Decision Rationale

**1. Real-Time Monitoring Use Case**

This system is designed for **live sensor monitoring** where the critical question is:
> "What's happening in my system RIGHT NOW?"

```
Processing Time Answer: "Average temperature of data that ARRIVED in the last 3 seconds"
✅ Perfect for real-time dashboards and immediate alerts

Event Time Answer: "Average temperature of data TIMESTAMPED in the last 3 seconds"
❓ Less useful when data might arrive with delays
```

**2. Predictable Behavior (Critical for Thesis/Demo)**

Processing time provides deterministic, clock-based behavior:

```
Timeline with Processing Time:
═══════════════════════════════════════════════════════════
System Clock:   10:00:00  10:00:03  10:00:06  10:00:09  10:00:12
                   ↓         ↓         ↓         ↓         ↓
Window Fires:   [✗]      [✓]       [✓]       [✓]       [✓]
                        (exactly every 3 seconds, guaranteed)

Result Latency: 3 seconds (window size)
```

With event time, window firing depends on watermark advancement, which depends on data arrival:

```
Timeline with Event Time:
═══════════════════════════════════════════════════════════
System Clock:   10:00:00  10:00:03  10:00:06  10:00:09  10:00:12
                   ↓         ↓         ↓         ↓         ↓
Data Arrives:   [Yes]     [Yes]     [No!]     [Yes]     [Yes]
Watermark:      10:00:00  10:00:03  10:00:03  10:00:09  10:00:12
                                      (stuck!)
Window Fires:   [✗]      [✗]       [✗]       [✓✓]      [✓]
                                              (delayed)

Result Latency: Variable (3s window + 5s watermark + data delays)
```

**3. Very Short Windows (3 seconds)**

The 3-second window size indicates **real-time** requirements:
- ✅ Immediate feedback for operators
- ✅ Fast detection of anomalies
- ✅ Low-latency dashboards
- ❌ NOT designed for historical replay
- ❌ NOT designed for batch reprocessing

If the system needed historical analysis, it would use:
- Longer windows (minutes/hours)
- Event time semantics
- Ability to replay old data

**4. Controlled Cluster Environment**

Deployed on SoftNet cluster with:
- ✅ Well-synchronized sensors (same network)
- ✅ Low network latency (local cluster)
- ✅ Predictable data flow
- ✅ Minimal clock skew

In this environment:
```
Event Time ≈ Processing Time (difference < 1 second typically)
```

Therefore, processing time is "good enough" without the complexity of event time.

**5. Operational Simplicity**

Processing time avoids:
- ❌ Late data handling logic
- ❌ Watermark tuning complexity
- ❌ Side outputs for late events
- ❌ Window trigger customization
- ❌ Clock skew management across sensors

### Watermark Generation (Present but Unused)

Despite using processing time windows, the system **does extract timestamps and generate watermarks**:

**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:103-115`

```java
DataStream<SensorEvent> eventStream = env
    .addSource(dataConsumer)
    .name("Data Source")
    .filter(str -> str != null && !str.isEmpty())
    .map(new EventDeserializer())
    .filter(event -> event != null)
    .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor<SensorEvent>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(SensorEvent event) {
                return event.getDatetime().toEpochMilli();
            }
        }
    )
    .keyBy(event -> "universal");
```

#### What This Does

**1. Timestamp Extraction**
```java
public long extractTimestamp(SensorEvent event) {
    return event.getDatetime().toEpochMilli();
}
```

Extracts the `datetime` field from each SensorEvent and converts it to milliseconds since epoch.

**2. Watermark Formula**

```
Watermark = Maximum Observed Event Timestamp - 5 seconds
```

The `BoundedOutOfOrdernessTimestampExtractor(Time.seconds(5))` means:
- Track the maximum timestamp seen so far
- Emit watermark = max_timestamp - 5000ms
- Tolerate events up to 5 seconds out of order

#### Watermark Behavior Example

```
Processing Timeline:
════════════════════════════════════════════════════════════════════
System Time:    10:00:00    10:00:01    10:00:02    10:00:03    10:00:04
                   │            │            │            │            │
Events Arrive:     A            B            C            D            E
                   │            │            │            │            │
Event Times:    10:00:00     10:00:02     10:00:01     10:00:08     10:00:00
                               │         (out of order!)    │        (very late!)
                               │                            │
Max Timestamp:  10:00:00     10:00:02     10:00:02     10:00:08     10:00:08
Watermark:      09:59:55     09:59:57     09:59:57     10:00:03     10:00:03
                ────────     ────────     ────────     ────────     ────────

Event C Status: ✅ OK (10:00:01 > 09:59:57, not late)
Event E Status: ❌ LATE (10:00:00 < 10:00:03, would be dropped if using event time)
```

**Key Points:**
- Event C arrives out of order (timestamped 10:00:01, arrives at 10:00:02) → ✅ Accepted
- Event E arrives very late (timestamped 10:00:00, arrives at 10:00:04) → ❌ Would be late
- Watermark advances only when new maximum timestamp is seen
- 5-second tolerance means events up to 5 seconds behind the maximum are accepted

#### Why Watermarks Are Present (But Unused)

**Theory 1: Future-Proofing**
```java
// Easy to switch to event time later:
// Just change one line from:
.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
// to:
.window(TumblingEventTimeWindows.of(Time.seconds(3)))
```

The infrastructure is ready to support event time if requirements change.

**Theory 2: Best Practice**
- Always extract timestamps as a best practice
- Enables time-based debugging and auditing
- Timestamp information preserved in results
- Can correlate events by event time even if windowing uses processing time

**Theory 3: Research/Educational Purpose**
- Thesis project demonstrating both approaches
- Can easily experiment with both time semantics
- Compare processing time vs event time behavior

**Theory 4: Originally Event Time, Switched for Demo**
- May have started with event time
- Switched to processing time for predictable demos
- Watermark code left in place (no harm, just unused)

### How Data Flows Through Time

#### Complete Temporal Flow

```
┌─────────────────────────────────────────────────────────────────┐
│  SENSOR GENERATES EVENT                                         │
│  Event Timestamp: 10:00:00.500                                  │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  KAFKA RECEIVES EVENT                                           │
│  Kafka Timestamp: 10:00:00.520 (20ms network delay)             │
│  Stored in partition 0                                          │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  FLINK CONSUMES EVENT                                           │
│  Processing Time: 10:00:00.550 (50ms total delay)               │
│  Event Time: 10:00:00.500 (extracted from event.datetime)       │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  TIMESTAMP EXTRACTION                                           │
│  extractTimestamp(event) → 1696939200500 (epoch millis)         │
│  Update max_timestamp = 1696939200500                           │
│  Generate watermark = max_timestamp - 5000 = 1696939195500      │
│                                                                 │
│  ⚠️  Watermark generated but NOT used for windowing!           │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  COFLATMAP PROCESSING                                           │
│  Processed At: ZonedDateTime.now(GREEK_TIMEZONE).toInstant()    │
│  = 10:00:00.555 (current processing time)                       │
│                                                                 │
│  Creates EtlResult with:                                        │
│  - result.setProcessedAt(10:00:00.555)                          │
│  - result.setResult(rawValue)                                   │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  WINDOW ASSIGNMENT                                              │
│  Processing Time Window: [10:00:00.000 - 10:00:03.000)          │
│                                                                 │
│  Assignment based on: System.currentTimeMillis()                │
│  NOT based on: event.datetime or watermark                      │
│                                                                 │
│  Event assigned to window based on when it ARRIVES,            │
│  not when it was GENERATED.                                     │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  WINDOW TRIGGERS                                                │
│  When: System clock reaches 10:00:03.000                        │
│  How: Flink's internal timer fires                              │
│  Independent of: Watermarks, event timestamps, data arrival     │
│                                                                 │
│  WindowAggregator.getResult() called                            │
│  Final result created with processedAt = 10:00:03.001           │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────────┐
│  OUTPUT TO KAFKA                                                │
│  Result written to etl.output.v1                                │
│  Total latency: ~2.5 seconds (event generated to result output) │
└─────────────────────────────────────────────────────────────────┘
```

#### Time Fields in Results

Every `EtlResult` contains:

```java
{
  "processedAt": "2025-10-20T10:00:03.001Z",  // When result was created (processing time)
  "windowStart": null,                         // Not used with processing time windows
  "windowEnd": null,                           // Not used with processing time windows
  "result": 24.5,                             // Aggregated value
  "diagnostics": [
    "WINDOWED avg: 20.00 contributing to avg",
    "WINDOWED avg: 22.00 contributing to avg",
    "WINDOWED avg: 24.00 contributing to avg",
    "WINDOWED avg: 3 values aggregated = 24.50",
    "Window: 3s tumbling window"
  ]
}
```

Note: `windowStart` and `windowEnd` could be populated from processing time windows, but currently aren't.

### Processing Time vs Event Time Comparison

#### Scenario: Out-of-Order Events

**Events Arriving:**
```
Time      Event    Event Time    Measurement    Location
────────  ──────   ──────────    ───────────    ────────
10:00:00   A       10:00:00      20.0           Room A
10:00:01   B       10:00:02      22.0           Room A  (future timestamp!)
10:00:02   C       10:00:01      24.0           Room A  (out of order!)
```

**With Processing Time Windows [10:00:00 - 10:00:03):**
```
Window receives:
  - Event A (arrived 10:00:00) → measurement 20.0
  - Event B (arrived 10:00:01) → measurement 22.0
  - Event C (arrived 10:00:02) → measurement 24.0

Average = (20.0 + 22.0 + 24.0) / 3 = 22.0 ✅

Window fires at: 10:00:03 (exactly)
Latency: 3 seconds
```

**With Event Time Windows [10:00:00 - 10:00:03) + 5s watermark:**
```
Events by event time:
  - Event A (time 10:00:00) → window [10:00:00 - 10:00:03)
  - Event C (time 10:00:01) → window [10:00:00 - 10:00:03)
  - Event B (time 10:00:02) → window [10:00:00 - 10:00:03)

All go to same window ✅

Watermark progression:
  10:00:00: watermark = 10:00:00 - 5s = 09:59:55
  10:00:01: watermark = 10:00:02 - 5s = 09:59:57
  10:00:02: watermark = 10:00:02 - 5s = 09:59:57

Window fires when: watermark >= 10:00:03
Requires: max_timestamp >= 10:00:08
Actual firing: ~10:00:08 or later

Latency: 8+ seconds (5s watermark delay + data delays)
```

#### Scenario: Late-Arriving Event

**Events Arriving:**
```
Time      Event    Event Time    Measurement    Location
────────  ──────   ──────────    ───────────    ────────
10:00:00   A       10:00:00      20.0           Room A
10:00:01   B       10:00:01      22.0           Room A
10:00:02   C       10:00:02      24.0           Room A
10:00:08   D       10:00:00      30.0           Room A  (8s late!)
```

**With Processing Time Windows:**
```
Window [10:00:00 - 10:00:03):
  - Event A → 20.0
  - Event B → 22.0
  - Event C → 24.0
  Average = 22.0
  Fires at: 10:00:03 ✅

Window [10:00:06 - 10:00:09):
  - Event D → 30.0 (goes to different window!)
  Average = 30.0
  Fires at: 10:00:09 ✅

Result: Event D processed in later window (might not be desired)
```

**With Event Time Windows:**
```
Window [10:00:00 - 10:00:03):
  - Event A (time 10:00:00) → 20.0
  - Event B (time 10:00:01) → 22.0
  - Event C (time 10:00:02) → 24.0

Watermark at 10:00:02: 09:59:57 (window doesn't fire yet)
Window fires when: watermark >= 10:00:03

At 10:00:08, Event D arrives with time 10:00:00:
  Current watermark = 10:00:08 - 5s = 10:00:03
  Event D time (10:00:00) < watermark (10:00:03)
  Event D is LATE! ❌

Result: Event D dropped (unless late data handling configured)
```

### When Event Time Would Be Better

Event time windows would be superior if:

**1. Sensor Clock Skew:**
```
Sensor A clock: +10 minutes fast  → timestamp 10:10:00, actual 10:00:00
Sensor B clock: correct            → timestamp 10:00:00, actual 10:00:00
Sensor C clock: -5 minutes slow    → timestamp 09:55:00, actual 10:00:00

Processing Time: Groups by arrival (mixed times) ❌
Event Time: Groups by reported time (correctable with clock sync) ✅
```

**2. Network Delays:**
```
Event generated:  10:00:00
Network delay:    +45 seconds
Event arrives:    10:00:45

Processing Time: Window [10:00:45-10:00:48) ❌ Wrong window
Event Time:      Window [10:00:00-10:00:03) ✅ Correct window
```

**3. Historical Replay:**
```
Replaying data from yesterday:

Processing Time: Windows based on current system time ❌
  Events from 2025-10-19 go into 2025-10-20 windows

Event Time: Windows based on event timestamps ✅
  Events from 2025-10-19 go into 2025-10-19 windows
```

**4. Multi-Source Data with Different Latencies:**
```
Source A: Real-time sensors (latency ~100ms)
Source B: Database CDC (latency ~5 seconds)
Source C: File batches (latency ~1 minute)

Processing Time: Mixed into windows based on arrival ❌
Event Time: Correctly ordered by event time ✅
```

**5. Exactly-Once End-to-End:**
```
With Kafka transactions and checkpointing:

Processing Time: Duplicates possible across windows during recovery
Event Time: Events assigned to same windows after recovery ✅
```

### Performance Impact of Time Semantics

**Processing Time Performance:**
```
CPU Overhead:    Low (no watermark tracking)
Memory Usage:    Low (no late data buffering)
Latency:         3 seconds (window size only)
Determinism:     High (clock-based)
Throughput:      High (minimal overhead)
```

**Event Time Performance (if switched):**
```
CPU Overhead:    Medium (watermark generation & propagation)
Memory Usage:    Medium (buffer late data, track max timestamps)
Latency:         8 seconds (3s window + 5s watermark)
Determinism:     Medium (depends on data arrival)
Throughput:      Medium (watermark overhead)
```

**Latency Breakdown:**

```
Processing Time:
─────────────────────────────────────────────────
Event → Flink → Window Assignment → Window Fires
10:00  10:00    (immediate)        10:00:03

Total: 3 seconds ✅


Event Time:
─────────────────────────────────────────────────────────────────
Event → Flink → Window Assignment → Wait for Watermark → Window Fires
10:00  10:00    (immediate)         (need watermark >= 10:00:03)
                                    Requires event with time >= 10:00:08
                                    (5s watermark delay)
                                    Actual: ~10:00:08+

Total: 8+ seconds ❌ (2.67x slower!)
```

### Switching to Event Time (If Needed)

To switch this system to event time windows, change **one line**:

**Current (Processing Time):**
```java
.window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
```

**Change to (Event Time):**
```java
.window(TumblingEventTimeWindows.of(Time.seconds(3)))
```

**Effect:**
- Windows now fire based on watermarks, not system clock
- Late events (> 5 seconds behind watermark) dropped
- Window firing becomes data-dependent
- Latency increases by ~5 seconds (watermark delay)
- Results correctly ordered by event time

**Additional Changes Recommended:**
```java
// 1. Add late data handling
.window(TumblingEventTimeWindows.of(Time.seconds(3)))
.allowedLateness(Time.seconds(10))  // Accept events up to 10s late
.sideOutputLateData(lateOutputTag)  // Capture dropped events

// 2. Configure watermark strategy
WatermarkStrategy
    .<SensorEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withIdleness(Duration.ofSeconds(30))  // Handle idle sources
    .withTimestampAssigner((event, timestamp) -> event.getDatetime().toEpochMilli())
```

### Summary of Time Semantics

**Current Implementation:**
- ⏰ **Window Type**: Tumbling Processing Time Windows (3 seconds)
- 📅 **Time Characteristic**: Processing Time (system clock)
- 🌊 **Watermarks**: Generated (5-second bounded out-of-orderness) but **NOT USED**
- 🎯 **Window Triggers**: Based on system clock, fires every 3 seconds exactly
- 📊 **Latency**: 3 seconds (window size)
- ✅ **Best For**: Real-time monitoring, predictable behavior, demo purposes

**Design Decisions:**
1. Processing time chosen for **predictable, low-latency** real-time monitoring
2. Event timestamps **extracted and preserved** for auditing and future-proofing
3. Watermarks **generated but unused** - infrastructure ready for event time if needed
4. **3-second windows** indicate real-time use case, not historical analysis
5. **Controlled cluster environment** makes processing time sufficient

**Trade-offs Accepted:**
- ✅ Gain: Predictable behavior, low latency (3s), operational simplicity
- ❌ Loss: Cannot handle severely late data correctly, clock skew affects results
- ⚖️  Acceptable: For real-time sensor monitoring in controlled environment

---

## Kafka Integration

### Kafka Configuration
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:63`

```java
Properties kafkaProps = new Properties();
kafkaProps.setProperty("bootstrap.servers", kafkaBootstrapServers);
kafkaProps.setProperty("group.id", "etl-flink-consumer");
kafkaProps.setProperty("auto.offset.reset", "earliest");
kafkaProps.setProperty("enable.auto.commit", "true");
kafkaProps.setProperty("session.timeout.ms", "30000");
kafkaProps.setProperty("request.timeout.ms", "40000");
```

**Default Kafka Brokers (SoftNet Cluster):**
```
clu02.softnet.tuc.gr:6667
clu03.softnet.tuc.gr:6667
clu04.softnet.tuc.gr:6667
clu06.softnet.tuc.gr:6667
```

### Kafka Topics

1. **etl.config.v1** - Configuration stream
   - Contains EtlConfig JSON objects
   - Read from earliest offset to ensure all configs are loaded
   - Partitions: 4, Replication: 2

2. **etl.input.v1** - Input data stream
   - Contains SensorEvent JSON objects
   - Read from earliest offset
   - Partitions: 4, Replication: 2

3. **etl.output.v1** - Output results stream
   - Contains EtlResult JSON objects
   - Written by Flink job
   - Partitions: 4, Replication: 2

### Kafka Consumers

#### Config Consumer
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:72`

```java
FlinkKafkaConsumer<String> configConsumer = new FlinkKafkaConsumer<>(
    configTopic,
    new NullSafeStringSchema(),
    kafkaProps
);
configConsumer.setStartFromEarliest();
```

#### Data Consumer
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:79`

```java
FlinkKafkaConsumer<String> dataConsumer = new FlinkKafkaConsumer<>(
    inputTopic,
    new NullSafeStringSchema(),
    kafkaProps
);
dataConsumer.setStartFromEarliest();
```

### NullSafeStringSchema
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:173`

Critical for handling empty Kafka topics without NullPointerException:

```java
public static class NullSafeStringSchema implements DeserializationSchema<String> {
    @Override
    public String deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            return null;  // Return null instead of throwing exception
        }
        return new String(message);
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
```

### Kafka Producer
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:155`

```java
FlinkKafkaProducer<EtlResult> kafkaProducer = new FlinkKafkaProducer<>(
    outputTopic,
    new EtlResultSerializationSchema(),
    kafkaProps,
    FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
);
```

**Semantic:** AT_LEAST_ONCE
- Results may be duplicated in case of failure
- No exactly-once guarantees (would require Kafka transactions and checkpointing)

### EtlResultSerializationSchema
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:293`

```java
public static class EtlResultSerializationSchema implements KeyedSerializationSchema<EtlResult> {
    private transient ObjectMapper objectMapper;

    @Override
    public byte[] serializeKey(EtlResult result) {
        return null;  // No key needed
    }

    @Override
    public byte[] serializeValue(EtlResult result) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }
        try {
            return objectMapper.writeValueAsBytes(result);
        } catch (Exception e) {
            LOG.error("Failed to serialize result", e);
            return new byte[0];
        }
    }

    @Override
    public String getTargetTopic(EtlResult result) {
        return null;  // Use default topic from producer config
    }
}
```

**Key Points:**
- No Kafka message key (null key)
- Uses Jackson ObjectMapper with JavaTimeModule for Instant serialization
- Lazy initialization of ObjectMapper (transient field)
- Errors return empty byte array instead of throwing

---

## State Management

### MapState for Configurations
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:31`

```java
private transient MapState<String, EtlConfig> configState;

@Override
public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    configState = getRuntimeContext().getMapState(
        new MapStateDescriptor<>("configs-by-key", String.class, EtlConfig.class)
    );
}
```

**State Characteristics:**
- **Type:** MapState (key-value store)
- **Key:** String (jobId)
- **Value:** EtlConfig (full configuration object)
- **Scope:** Per keyed subtask
- **Persistence:** Stored in Flink state backend (checkpointed)

### State Distribution

Since both config and data streams are keyed by "universal":

```
Subtask 0: configState = {
  "job-001" -> EtlConfig(...),
  "job-002" -> EtlConfig(...),
  ...
}

Subtask 1: configState = {
  "job-001" -> EtlConfig(...),
  "job-002" -> EtlConfig(...),
  ...
}

... (same for all subtasks)
```

**Why "universal" key?**
- Ensures all configs are replicated to all subtasks
- Any subtask can process any event
- Enables true parallel processing without data skew
- Trade-off: Higher state size (configs duplicated across subtasks)

### State Operations

**Put Configuration:**
```java
configState.put(config.getJobId(), config);
```

**Iterate Configurations:**
```java
Iterable<Map.Entry<String, EtlConfig>> configs = configState.entries();
for (Map.Entry<String, EtlConfig> configEntry : configs) {
    EtlConfig config = configEntry.getValue();
    processEventWithConfig(event, config, out);
}
```

**Get Single Configuration:**
```java
EtlConfig config = configState.get(jobId);
```

### Checkpointing
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:49`

```java
env.enableCheckpointing(30000);  // 30 seconds
```

- Checkpoints every 30 seconds
- Saves configState to persistent storage
- Enables recovery after failures
- State backend determined by Flink cluster configuration

---

## Key Distribution and Parallelism

### Keying Strategy

**Config Stream:**
```java
.keyBy(new ConfigKeyExtractor())  // Returns "universal"
```
**File:** `flink/src/main/java/com/etl/flink/process/ConfigKeyExtractor.java:14`

**Data Stream:**
```java
.keyBy(event -> "universal")  // All events use same key
```
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:116`

### Parallelism Configuration

**Default Parallelism:**
```java
env.setParallelism(4);  // Default if not overridden
```
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:50`

**Runtime Override:**
```bash
./bin/flink run -p 11 etl-flink-1.0.0.jar ...
```

With parallelism 11 on SoftNet cluster, data distribution:

```
┌────────────┬────────────┬────────────┬─────┬────────────┐
│ Subtask 0  │ Subtask 1  │ Subtask 2  │ ... │ Subtask 10 │
├────────────┼────────────┼────────────┼─────┼────────────┤
│ All configs│ All configs│ All configs│ ... │ All configs│
│ 1/11 events│ 1/11 events│ 1/11 events│ ... │ 1/11 events│
└────────────┴────────────┴────────────┴─────┴────────────┘
```

**Why this works:**
- Configs replicated to all subtasks via "universal" key
- Events distributed evenly via hash of "universal" key
- Each subtask processes ~1/11 of events with full config knowledge
- No cross-subtask communication needed

### Window Keying (Downstream)
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:143`

After CoFlatMap, results are re-keyed for aggregation:

```java
.keyBy(result -> String.format("%s|%s|%s",
    result.getGroupingKey(),
    result.getJobId(),
    result.getAggregationType()
))
```

This redistributes data by actual grouping needs, enabling parallel aggregation.

**Example Distribution:**

Input: 100 events, 2 locations, 1 job, 1 aggregation type
- Parallelism: 4
- Keys: "Room A|job-001|avg", "Room B|job-001|avg"
- Distribution:
  - Subtask 0: "Room A|job-001|avg" → 50 events
  - Subtask 1: "Room B|job-001|avg" → 50 events
  - Subtask 2: (idle)
  - Subtask 3: (idle)

### Parallelism Best Practices

1. **CoFlatMap Parallelism:** Should match number of worker nodes (e.g., 11)
2. **Window Parallelism:** Should be ≥ number of unique grouping keys
3. **Sink Parallelism:** Should match Kafka partition count (e.g., 4)

---

## Serialization and Deserialization

### JSON Serialization with Jackson

All data models use Jackson for JSON serialization/deserialization.

**Dependencies:**
```xml
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
    <version>2.15.2</version>
</dependency>
<dependency>
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
    <version>2.15.2</version>
</dependency>
```

### JavaTimeModule

Required for `java.time.Instant` serialization:

```java
ObjectMapper objectMapper = new ObjectMapper();
objectMapper.registerModule(new JavaTimeModule());
```

**Instant Format:**
- Serialized as ISO-8601 string: `"2025-10-12T18:44:21Z"`
- Timezone: Always UTC (Z suffix)
- Microsecond precision supported

**IMPORTANT:** Producer must use `.toInstant().toString()` format, NOT `ZonedDateTime.toString()` which includes timezone ID.

### ConfigDeserializer
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:193`

```java
public static class ConfigDeserializer implements MapFunction<String, EtlConfig> {
    @Override
    public EtlConfig map(String value) throws Exception {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(value, EtlConfig.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize config: {}", value, e);
            return null;  // Filter out via downstream filter
        }
    }
}
```

### EventDeserializer
**File:** `flink/src/main/java/com/etl/flink/EtlFlinkJob.java:206`

```java
public static class EventDeserializer implements MapFunction<String, SensorEvent> {
    @Override
    public SensorEvent map(String value) throws Exception {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());  // Required for Instant
            return objectMapper.readValue(value, SensorEvent.class);
        } catch (Exception e) {
            LOG.error("Failed to deserialize event: {}", value, e);
            return null;  // Filter out via downstream filter
        }
    }
}
```

### Null Filtering Pipeline

```
Kafka Message (byte[])
    ↓
NullSafeStringSchema → String (or null)
    ↓
.filter(str -> str != null && !str.isEmpty())
    ↓
Deserializer → Object (or null)
    ↓
.filter(obj -> obj != null)
    ↓
Valid object for processing
```

This multi-stage filtering ensures:
1. Empty Kafka messages don't crash the job
2. Malformed JSON doesn't crash the job
3. Only valid objects reach processing logic

---

## Error Handling

### Null Safety Layers

**Layer 1: Kafka Message Level**
```java
// NullSafeStringSchema returns null for empty messages
if (message == null || message.length == 0) {
    return null;
}
```

**Layer 2: JSON String Level**
```java
.filter(str -> str != null && !str.isEmpty())
```

**Layer 3: Deserialization Level**
```java
try {
    return objectMapper.readValue(value, EtlConfig.class);
} catch (Exception e) {
    LOG.error("Failed to deserialize config: {}", value, e);
    return null;
}
```

**Layer 4: Object Level**
```java
.filter(config -> config != null)
```

### Processing Error Handling
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:80`

```java
try {
    processEventWithConfig(event, config, out);
} catch (Exception e) {
    LOG.error("Error processing event with config for jobId: {} on subtask: {}",
             config.getJobId(), getRuntimeContext().getIndexOfThisSubtask(), e);
    emitErrorResult(event, config, e, out);
}
```

### Error Result Format
**File:** `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java:324`

```java
private void emitErrorResult(SensorEvent event, EtlConfig config, Exception e,
                            Collector<EtlResult> out) {
    EtlResult errorResult = new EtlResult();
    errorResult.setJobId(config.getJobId());
    errorResult.setSource(config.getSource());
    errorResult.setTransformations(config.getTransformations());
    errorResult.setGroupingField("sensor");
    errorResult.setGroupingKey(event.getSensor());
    errorResult.setResult(null);  // Null indicates error
    errorResult.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());
    errorResult.setSensorType(event.getSensor());
    errorResult.setMeasurementUnit(event.getMeasurementUnit());
    errorResult.setLocation(event.getLocation());

    List<String> diagnostics = new ArrayList<>();
    diagnostics.add("Processing error on subtask " +
                   getRuntimeContext().getIndexOfThisSubtask() + ": " + e.getMessage());
    errorResult.setDiagnostics(diagnostics);

    out.collect(errorResult);
}
```

### Common Error Scenarios

1. **Empty Kafka Topics on Job Start**
   - **Cause:** Kafka consumer receives null messages
   - **Solution:** NullSafeStringSchema returns null → filtered out
   - **Result:** Job starts successfully

2. **Malformed JSON in Kafka**
   - **Cause:** Invalid JSON syntax
   - **Solution:** Deserializer catches exception, logs error, returns null → filtered out
   - **Result:** Invalid messages skipped, job continues

3. **Datetime Format Mismatch**
   - **Cause:** Producer sends `ZonedDateTime.toString()` with timezone ID
   - **Solution:** Use `.toInstant().toString()` for ISO-8601 format
   - **Result:** Jackson can parse standard ISO-8601

4. **No Configuration Available**
   - **Cause:** Events arrive before configuration
   - **Solution:** Events silently dropped with debug log
   - **Result:** No errors, events wait for config

5. **Transformation Error**
   - **Cause:** Exception during transformation execution
   - **Solution:** Catch exception, emit error result with diagnostics
   - **Result:** Error tracked, job continues

### Logging

**Log Levels:**
- **INFO:** Job lifecycle events, config registration
- **DEBUG:** Event processing details, filtering decisions
- **ERROR:** Deserialization failures, processing errors

**Key Log Statements:**
```java
LOG.info("Starting ETL Flink Job - Flink 1.9.3 compatible");
LOG.info("Received config for job: {} on subtask: {}", jobId, subtask);
LOG.debug("Processing event with sensor: {} against config: {} on subtask: {}", ...);
LOG.error("Failed to deserialize config: {}", value, e);
LOG.error("Error processing event with config for jobId: {}", jobId, e);
```

---

## Configuration Examples

### Example 1: Simple Filter and Aggregation
**File:** `config-test123.json`

```json
{
  "jobId": "test123-job",
  "source": "kafka://etl.input.v1",
  "transformations": [
    {
      "type": "filter_greater",
      "params": {
        "sensor": "humidity",
        "field": "measurement",
        "threshold": 70
      }
    },
    {
      "type": "max",
      "keyBy": "location",
      "params": {
        "field": "measurement",
        "sensor": "temperature"
      }
    }
  ]
}
```

**Behavior:**
1. Filter: Keep only humidity events > 70
2. Aggregation: Find max temperature per location (independent of filter)
3. Output: Filtered humidity events + windowed max temperature per location

### Example 2: Multiple Sensor Types
**File:** `config-cold-humid-rooms.json` (hypothetical)

```json
{
  "jobId": "cold-humid-rooms",
  "source": "kafka://etl.input.v1",
  "transformations": [
    {
      "type": "avg",
      "keyBy": "location",
      "params": {
        "field": "measurement",
        "sensor": "temperature"
      }
    },
    {
      "type": "avg",
      "keyBy": "location",
      "params": {
        "field": "measurement",
        "sensor": "humidity"
      }
    }
  ]
}
```

**Behavior:**
- Computes average temperature per location (3-second windows)
- Computes average humidity per location (3-second windows)
- Each aggregation is independent with its own windows

### Example 3: Cross-Field Filtering
**File:** `config-light-high-humidity.json` (hypothetical)

```json
{
  "jobId": "light-high-humidity",
  "source": "kafka://etl.input.v1",
  "transformations": [
    {
      "type": "filter_cross_field",
      "params": {
        "filter_sensor": "humidity",
        "filter_field": "measurement",
        "filter_operator": ">",
        "filter_threshold": 70.0,
        "target_sensor": "light"
      }
    },
    {
      "type": "avg",
      "keyBy": "location",
      "params": {
        "field": "measurement",
        "sensor": "light"
      }
    }
  ]
}
```

**Behavior:**
- Filter: Pass through light events when humidity > 70
- Aggregation: Average light levels per location (only when humidity is high)

### Example 4: Multiple Filters
**File:** Custom configuration

```json
{
  "jobId": "extreme-temps",
  "source": "kafka://etl.input.v1",
  "transformations": [
    {
      "type": "filter_greater",
      "params": {
        "sensor": "temperature",
        "field": "measurement",
        "threshold": 30.0
      }
    },
    {
      "type": "filter_less",
      "params": {
        "sensor": "temperature",
        "field": "measurement",
        "threshold": 40.0
      }
    },
    {
      "type": "max",
      "keyBy": "location",
      "params": {
        "field": "measurement",
        "sensor": "temperature"
      }
    }
  ]
}
```

**Behavior:**
- Element filters applied sequentially: Keep temperature between 30-40
- Aggregation: Max temperature per location within that range

---

## Performance Considerations

### Parallelism Tuning

**Recommended Settings for SoftNet Cluster (11 workers):**
```bash
./bin/flink run -p 11 etl-flink-1.0.0.jar ...
```

**Reasoning:**
- 11 subtasks = 1 per worker node
- Balanced CPU utilization
- Minimal network shuffling for CoFlatMap (all configs local)

### State Size Management

**Factors Affecting State Size:**
1. Number of active configurations (stored in MapState)
2. Parallelism (configs replicated across all subtasks)
3. Configuration object size

**Example:**
- 10 active configs
- Average config size: 1 KB
- Parallelism: 11
- Total state: 10 × 1 KB × 11 = 110 KB (minimal)

**State Backend Options:**
- **MemoryStateBackend:** Fast, but limited by JVM heap
- **FsStateBackend:** Good for moderate state, uses filesystem
- **RocksDBStateBackend:** Best for large state, disk-based

### Windowing Performance

**3-Second Windows:**
- Low latency (results every 3 seconds)
- Low state overhead (small windows)
- Good for real-time monitoring

**Trade-offs:**
- Shorter windows = Lower latency, more frequent results, less smoothing
- Longer windows = Higher latency, fewer results, more smoothing

### Backpressure

**Potential Bottlenecks:**
1. **Kafka Consumer:** If input rate > processing rate
2. **CoFlatMap:** If transformation logic is complex
3. **Window Aggregation:** If many unique grouping keys
4. **Kafka Producer:** If output rate > Kafka capacity

**Monitoring:**
- Check Flink Dashboard (http://clu01.softnet.tuc.gr:8081)
- Look for backpressure indicators (red/yellow bars)
- Check Records Sent/Received metrics

### Optimization Tips

1. **Increase Parallelism:** More subtasks = more throughput
2. **Tune Kafka Partitions:** Match or exceed Flink parallelism
3. **Enable Checkpointing Compression:** Reduce checkpoint size
4. **Use Async Sink:** For better write throughput (requires Flink 1.14+)
5. **Filter Early:** Apply filters before expensive operations

---

## Troubleshooting Guide

### Issue: Flink Job Keeps Restarting

**Symptoms:**
- Job shows RESTARTING in Flink Dashboard
- Data Source shows 0 records received

**Cause:**
- Command-line arguments not passed → Can't connect to Kafka

**Solution:**
```bash
./bin/flink run -p 11 -d etl-flink-1.0.0.jar \
    --kafka.bootstrap.servers clu02.softnet.tuc.gr:6667,... \
    --kafka.config.topic etl.config.v1 \
    --kafka.input.topic etl.input.v1 \
    --kafka.output.topic etl.output.v1
```

### Issue: No Output Results

**Symptoms:**
- Job running successfully
- Input topic has data
- Output topic is empty

**Debugging Steps:**

1. **Check if configurations are registered:**
   ```bash
   bin/kafka-console-consumer.sh --bootstrap-server clu02.softnet.tuc.gr:6667 \
       --topic etl.output.v1 --from-beginning
   ```
   Look for "Configuration registered successfully" messages.

2. **Check Flink Dashboard metrics:**
   - Data Source → Records Received (should be > 0)
   - CoFlatMapProcessor → Records Out (should be > 0)
   - Window Aggregator → Records Out (should be > 0)

3. **Check for deserialization errors:**
   - View TaskManager logs
   - Look for "Failed to deserialize" errors

4. **Verify datetime format:**
   ```bash
   bin/kafka-console-consumer.sh --bootstrap-server clu02.softnet.tuc.gr:6667 \
       --topic etl.input.v1 --max-messages 1
   ```
   Datetime should be: `"2025-10-12T18:44:21Z"` (NOT with timezone ID)

### Issue: Aggregation Not Working

**Symptoms:**
- Individual events in output
- No aggregated results

**Possible Causes:**

1. **No aggregation transformations in config:**
   - Check config has `"type": "sum|max|min|avg"`

2. **Sensor filter mismatch:**
   - Config specifies `"sensor": "temperature"`
   - But events have `"sensor": "temp"` (different name)

3. **Window not closing:**
   - Wait at least 3 seconds + 5 seconds (watermark delay)
   - Check system clock synchronization

### Issue: Datetime Parsing Errors

**Symptoms:**
- Data Source receives 0 records despite Kafka having data
- Logs show Jackson parsing errors

**Cause:**
- Producer using `ZonedDateTime.toString()` → includes timezone ID
- Jackson can't parse timezone ID format

**Solution:**
- Use `.toInstant().toString()` in producer:
  ```java
  event.setDatetime(ZonedDateTime.now().toInstant());  // Stored as Instant
  ```

### Issue: High State Size

**Symptoms:**
- Slow checkpoints
- OutOfMemoryError

**Possible Causes:**
1. Too many configurations stored
2. High parallelism (configs replicated)
3. Large transformation arrays in configs

**Solutions:**
1. Clean up old/unused configurations
2. Reduce parallelism if not needed
3. Use RocksDBStateBackend for large state

---

## Summary

This Flink ETL system provides a flexible, scalable solution for real-time sensor data processing:

- **Dynamic Configuration:** Transformations can be changed at runtime via Kafka
- **Parallel Processing:** CoFlatMap architecture with "universal" keying enables true parallelism
- **Flexible Transformations:** Supports filtering, aggregation, and cross-field operations
- **Windowed Aggregation:** 3-second tumbling windows for real-time statistics
- **Processing Time Semantics:** Clock-based windowing for predictable, low-latency results
- **Watermark Infrastructure:** Event timestamps extracted and watermarks generated (ready for event time if needed)
- **Robust Error Handling:** Multi-layer null safety and error recovery
- **Production Ready:** Deployed on 23-node cluster with proven reliability

**Key Files:**
- Main Job: `flink/src/main/java/com/etl/flink/EtlFlinkJob.java`
- CoFlatMap Processor: `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java`
- Element Transformations: `flink/src/main/java/com/etl/flink/udf/ElementTransformations.java`
- Windowed Aggregations: `flink/src/main/java/com/etl/flink/udf/WindowedAggregations.java`

**Deployment:**
- Platform: SoftNet Cluster (TUC), Flink 1.9.3, Java 8
- Parallelism: 11 (matching worker nodes)
- Checkpointing: 30 seconds
- Window Size: 3 seconds tumbling (processing time)
- Watermark Delay: 5 seconds (for potential event time support)

**Time Semantics:**
- Window Type: Processing Time (system clock-based)
- Timestamp Extraction: Event timestamps extracted from SensorEvent.datetime
- Watermark Strategy: Bounded out-of-orderness (5 seconds tolerance)
- Window Firing: Every 3 seconds exactly (predictable, low-latency)
- Design Choice: Real-time monitoring over historical correctness

For deployment details, see `README.md` and `DEPLOYMENT_GUIDE.txt`.
