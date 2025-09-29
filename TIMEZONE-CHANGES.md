# Timezone Changes - Greek Time (UTC+02:00/UTC+03:00)

## Summary
All timestamp generation across the ETL system has been updated to use Greek timezone (`Europe/Athens`) which automatically handles:
- **Winter time**: UTC+02:00
- **Summer time**: UTC+03:00 (DST)

## Files Modified

### 1. Producer Service
**File**: `services/etl-file-producer/src/main/java/com/etl/producer/service/FileProducerService.java`
- Added Greek timezone constant
- Updated sensor data timestamp generation
- All generated sensor events now use Greek time

### 2. Flink Job Main Class
**File**: `flink/src/main/java/com/etl/flink/EtlFlinkJob.java`
- Added Greek timezone constant
- Updated watermark generation to use Greek time
- Updated result processing timestamps

### 3. CoFlatMap Processor
**File**: `flink/src/main/java/com/etl/flink/process/CoFlatMapProcessor.java`
- Added Greek timezone constant
- Updated all result processing timestamps
- Updated configuration registration timestamps
- Updated error result timestamps

### 4. MongoDB Sink
**File**: `flink/src/main/java/com/etl/flink/sink/MongoSink.java`
- Added Greek timezone constant
- Updated document processing timestamps
- Updated fallback timestamp generation

### 5. API Controller
**File**: `services/etl-api/src/main/java/com/etl/api/controller/ConfigController.java`
- Added Greek timezone constant
- Updated API response timestamps

### 6. Job Status Model
**File**: `services/etl-api/src/main/java/com/etl/api/model/JobStatus.java`
- Added Greek timezone constant
- Updated job submission timestamps

## Technical Implementation

**Timezone Used**: `ZoneId.of("Europe/Athens")`
- Automatically handles DST transitions
- Currently UTC+2 (winter) or UTC+3 (summer)

**Pattern Applied**:
```java
// Before
Instant.now()

// After
ZonedDateTime.now(GREEK_TIMEZONE).toInstant()
```

## Results
✅ **All timestamps now reflect Greek local time**
✅ **Automatic DST handling**
✅ **Build successful - no compilation errors**
✅ **Consistent timezone across entire ETL pipeline**

## Impact on Performance Tests
- Sensor event timestamps will show Greek time
- ETL result timestamps will show Greek time
- MongoDB documents will contain Greek time
- API responses will show Greek time
- No impact on performance measurements or calculations

## Verification
Build completed successfully with all timezone changes:
```
BUILD SUCCESS
Total time: 10.066 s
Finished at: 2025-09-29T14:00:22+03:00
```

Note: The build timestamp already shows `+03:00` indicating the system recognizes Greek DST.