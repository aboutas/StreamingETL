package com.etl.flink;

import com.etl.flink.deserializer.SafeStringDeserializer;
import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.process.CoFlatMapProcessor;
import com.etl.flink.process.ConfigKeyExtractor;
import com.etl.flink.sink.MongoSink;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public class EtlFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(EtlFlinkJob.class);
    private static final ZoneId GREEK_TIMEZONE = ZoneId.of("Europe/Athens"); // UTC+2 (UTC+3 in summer)

    public static void main(String[] args) throws Exception {
        LOG.info("Starting ETL Flink Job");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        env.setParallelism(4);

        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String configTopic = getEnvOrDefault("CONFIG_TOPIC", "etl.config.v1");
        String inputTopic = getEnvOrDefault("INPUT_TOPIC", "etl.input.v1");
        //String outputTopic = getEnvOrDefault("OUTPUT_TOPIC", "etl.output.v1");
        String mongoUri = getEnvOrDefault("MONGO_URI", "mongodb://mongo:27017/etl_db");

        LOG.info("Config: kafka={}, topics=[config={}, input={}], mongo={}",
                kafkaBootstrapServers, configTopic, inputTopic, mongoUri);

        KafkaSource<String> configSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(configTopic)
                .setGroupId("etl-config-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SafeStringDeserializer())
                .build();

        KafkaSource<String> dataSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("etl-data-consumer-v4")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SafeStringDeserializer())
                .build();

        // Create configuration stream - keyed by same field as data stream 
        DataStream<EtlConfig> configStream = env
                .fromSource(configSource, WatermarkStrategy.noWatermarks(), "Config Source")
                .map(new ConfigDeserializer())
                .filter(config -> config != null)
                .keyBy(new ConfigKeyExtractor()); // Key configs by their target keyBy field

        // Create keyed event stream 
        DataStream<SensorEvent> eventStream = env
                .fromSource(dataSource,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> {
                                    try {
                                        ObjectMapper mapper = new ObjectMapper();
                                        mapper.registerModule(new JavaTimeModule());
                                        SensorEvent sensorEvent = mapper.readValue(event, SensorEvent.class);
                                        if (sensorEvent != null && sensorEvent.getDatetime() != null) {
                                            return sensorEvent.getDatetime().toEpochMilli();
                                        }
                                        return ZonedDateTime.now(GREEK_TIMEZONE).toInstant().toEpochMilli();
                                    } catch (Exception e) {
                                        LOG.warn("Failed to extract timestamp from event: {}", event, e);
                                        return ZonedDateTime.now(GREEK_TIMEZONE).toInstant().toEpochMilli();
                                    }
                                }),
                        "Data Source")
                .map(new EventDeserializer())
                .filter(event -> event != null)
                .keyBy(event -> event.getSensor()); // Key by sensor type for parallelism

        // TRUE CoFlatMap: Both streams keyed by same field for optimal distribution
        DataStream<EtlResult> processedStream = configStream
                .connect(eventStream)
                .flatMap(new CoFlatMapProcessor());

        // Split streams: configs vs data with windowing
        DataStream<EtlResult> configResults = processedStream
                .filter(result -> result.getAggregationType() == null);

        DataStream<EtlResult> dataResults = processedStream
                .filter(result -> result.getAggregationType() != null)
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<EtlResult>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((result, timestamp) -> {
                            if (result.getProcessedAt() != null) {
                                return result.getProcessedAt().toEpochMilli();
                            }
                            return Instant.now().toEpochMilli();
                        })
                );

        // Windowing: 3-second tumbling windows
        DataStream<EtlResult> windowedResults = dataResults
                .keyBy(result -> {
                    return String.format("%s|%s|%s",
                        result.getGroupingKey() != null ? result.getGroupingKey() : "default",
                        result.getJobId() != null ? result.getJobId() : "unknown",
                        result.getAggregationType() != null ? result.getAggregationType() : "none"
                    );
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .aggregate(new WindowAggregator());

        // Union all results
        DataStream<EtlResult> allResults = configResults.union(windowedResults);

        // SINK: MongoDB
        allResults.addSink(new MongoSink(mongoUri));

        LOG.info("Executing ETL Flink Job");
        env.execute("ETL Flink Job");
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    public static class ConfigDeserializer implements MapFunction<String, EtlConfig> {
        @Override
        public EtlConfig map(String value) throws Exception {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                return objectMapper.readValue(value, EtlConfig.class);
            } catch (Exception e) {
                LOG.error("Failed to deserialize config: {}", value, e);
                return null;
            }
        }
    }

    public static class EventDeserializer implements MapFunction<String, SensorEvent> {

        @Override
        public SensorEvent map(String value) throws Exception {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.registerModule(new JavaTimeModule());
                return objectMapper.readValue(value, SensorEvent.class);
            } catch (Exception e) {
                LOG.error("Failed to deserialize event: {}", value, e);
                return null;
            }
        }
    }

    public static class WindowAggregator implements AggregateFunction<EtlResult, WindowAccumulator, EtlResult> {
        @Override
        public WindowAccumulator createAccumulator() {
            return new WindowAccumulator();
        }

        @Override
        public WindowAccumulator add(EtlResult result, WindowAccumulator accumulator) {
            if (accumulator.jobId == null) {
                accumulator.jobId = result.getJobId();
                accumulator.transformations = result.getTransformations();
                accumulator.groupingField = result.getGroupingField();
                accumulator.groupingKey = result.getGroupingKey();
                accumulator.aggregationType = result.getAggregationType();
                accumulator.field = result.getField();
                accumulator.sensorType = result.getSensorType();
                accumulator.measurementUnit = result.getMeasurementUnit();
                accumulator.location = result.getLocation();
            }

            Double value = (Double) result.getResult();
            if (value != null) {
                accumulator.accumulate(value);
                accumulator.count++;
            }

            return accumulator;
        }

        @Override
        public EtlResult getResult(WindowAccumulator accumulator) {
            EtlResult result = new EtlResult();
            result.setJobId(accumulator.jobId);
            result.setTransformations(accumulator.transformations);
            result.setGroupingField(accumulator.groupingField);
            result.setGroupingKey(accumulator.groupingKey);
            result.setAggregationType(accumulator.aggregationType);
            result.setField(accumulator.field);
            result.setResult(accumulator.getResult());
            result.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());
            result.setSensorType(accumulator.sensorType);
            result.setMeasurementUnit(accumulator.measurementUnit);
            result.setLocation(accumulator.location);

            return result;
        }

        @Override
        public WindowAccumulator merge(WindowAccumulator a, WindowAccumulator b) {
            a.sum += b.sum;
            a.count += b.count;
            if (b.max > a.max) a.max = b.max;
            if (b.min < a.min) a.min = b.min;
            return a;
        }
    }

    public static class WindowAccumulator {
        public String jobId;
        public List<com.etl.flink.model.Transformation> transformations;
        public String groupingField;
        public String groupingKey;
        public String aggregationType;
        public String field;
        public String sensorType;
        public String measurementUnit;
        public String location;
        public int count;

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
}