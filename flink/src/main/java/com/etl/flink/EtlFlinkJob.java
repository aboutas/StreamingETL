package com.etl.flink;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.process.CoFlatMapProcessor;
import com.etl.flink.process.ConfigKeyExtractor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

public class EtlFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(EtlFlinkJob.class);
    public static void main(String[] args) throws Exception {
        LOG.info("Starting ETL Flink Job - Flink 1.9.3 compatible");

        // Parse command-line parameters (distributed to all TaskManagers)
        ParameterTool params = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Make parameters globally available to all functions
        env.getConfig().setGlobalJobParameters(params);

        // comment in for testing
        // env.enableCheckpointing(30000);
        // Parallelism set via command-line: flink run -p <parallelism>

        // Read configuration from command-line arguments with defaults
        String kafkaBootstrapServers = params.get("kafka.bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        String configTopic = params.get("kafka.config.topic", "etl.config.v1");
        String inputTopic = params.get("kafka.input.topic", "etl.input.v1");
        String outputTopic = params.get("kafka.output.topic", "etl.output.v1");

        LOG.info("Kafka Bootstrap Servers: {}", kafkaBootstrapServers);
        LOG.info("Config Topic: {}", configTopic);
        LOG.info("Input Topic: {}", inputTopic);
        LOG.info("Output Topic: {}", outputTopic);

        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", kafkaBootstrapServers);
        consumerProps.setProperty("group.id", "etl-flink-consumer");
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("enable.auto.commit", "true");
        consumerProps.setProperty("session.timeout.ms", "30000");
        consumerProps.setProperty("request.timeout.ms", "40000");
        consumerProps.setProperty("max.poll.records", "5000");          // 500 → 5000: 10x fewer polls

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", kafkaBootstrapServers);
        producerProps.setProperty("batch.size", "131072");       // 128KB batches (default 16KB)
        producerProps.setProperty("linger.ms", "10");            // wait 10ms to fill batch (default 0)
        producerProps.setProperty("compression.type", "lz4");   // compress before sending
        producerProps.setProperty("buffer.memory", "67108864");  // 64MB buffer (default 32MB)
        producerProps.setProperty("request.timeout.ms", "40000");

        // Config Consumer (Flink 1.9.3 API) - null-safe for empty topics
        FlinkKafkaConsumer<String> configConsumer = new FlinkKafkaConsumer<>(
                configTopic,
                new NullSafeStringSchema(),
                consumerProps
        );
        configConsumer.setStartFromEarliest();

        // Data Consumer (Flink 1.9.3 API) - null-safe for empty topics
        FlinkKafkaConsumer<String> dataConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new NullSafeStringSchema(),
                consumerProps
        );
        dataConsumer.setStartFromEarliest();

        // Config stream
        DataStream<EtlConfig> configStream = env
                .addSource(configConsumer)
                .name("Config Source")
                .filter(str -> str != null && !str.isEmpty())  // Filter null/empty from deserializer
                .map(new ConfigDeserializer())
                .filter(config -> config != null)
                .keyBy(new ConfigKeyExtractor());

        // Event stream
        DataStream<SensorEvent> eventStream = env
                .addSource(dataConsumer)
                .name("Data Source")
                .filter(str -> str != null && !str.isEmpty())
                .map(new EventDeserializer())
                .filter(event -> event != null)
                .keyBy(event -> event.getSensor());

        // CoFlatMap processing
        DataStream<EtlResult> processedStream = configStream
                .connect(eventStream)
                .flatMap(new CoFlatMapProcessor());

        // Route: config registration messages (aggregationType == null) go directly to sink,
        // data results (aggregationType != null) go through windowed aggregation.
        DataStream<EtlResult> configResults = processedStream
                .filter(result -> result.getAggregationType() == null);

        DataStream<EtlResult> windowedResults = processedStream
                .filter(result -> result.getAggregationType() != null)
                .keyBy(result -> String.format("%s|%s|%s",
                        result.getGroupingKey() != null ? result.getGroupingKey() : "default",
                        result.getJobId() != null ? result.getJobId() : "unknown",
                        result.getAggregationType() != null ? result.getAggregationType() : "none"
                ))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .aggregate(new WindowAggregator());

        DataStream<EtlResult> allResults = configResults.union(windowedResults);

        // Kafka Producer (Flink 1.9.3 API)
        FlinkKafkaProducer<EtlResult> kafkaProducer = new FlinkKafkaProducer<>(
                outputTopic,
                new EtlResultSerializationSchema(),
                producerProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        allResults.filter(result -> Math.random() < 0.0001).addSink(kafkaProducer).name("Kafka Sink");

        LOG.info("Executing ETL Flink Job");
        env.execute("ETL Flink Job");
    }

    /**
     * Null-safe String deserializer for Kafka messages
     * Returns null for null/empty messages instead of throwing NullPointerException
     * This allows Flink job to start with empty Kafka topics
     */
    public static class NullSafeStringSchema implements DeserializationSchema<String> {
        @Override
        public String deserialize(byte[] message) {
            if (message == null || message.length == 0) {
                return null;
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

    public static class ConfigDeserializer implements MapFunction<String, EtlConfig> {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        @Override
        public EtlConfig map(String value) throws Exception {
            try {
                return MAPPER.readValue(value, EtlConfig.class);
            } catch (Exception e) {
                LOG.error("Failed to deserialize config: {}", value, e);
                return null;
            }
        }
    }

    public static class EventDeserializer implements MapFunction<String, SensorEvent> {
        static final ObjectMapper MAPPER = new ObjectMapper()
                .registerModule(new JavaTimeModule());

        @Override
        public SensorEvent map(String value) throws Exception {
            try {
                return MAPPER.readValue(value, SensorEvent.class);
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
            result.setProcessedAt(Instant.now());

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

    /**
     * Kafka Serialization Schema for Flink 1.9.3
     */
    public static class EtlResultSerializationSchema implements KeyedSerializationSchema<EtlResult> {
        private static final ObjectMapper MAPPER = new ObjectMapper()
                .registerModule(new JavaTimeModule());

        @Override
        public byte[] serializeKey(EtlResult result) {
            return null;
        }

        @Override
        public byte[] serializeValue(EtlResult result) {
            try {
                return MAPPER.writeValueAsBytes(result);
            } catch (Exception e) {
                LOG.error("Failed to serialize result", e);
                return new byte[0];
            }
        }

        @Override
        public String getTargetTopic(EtlResult result) {
            return null; // Use default topic from producer config
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
                case "sum":
                    return sum;
                case "max":
                    return max.equals(Double.NEGATIVE_INFINITY) ? 0.0 : max;
                case "min":
                    return min.equals(Double.POSITIVE_INFINITY) ? 0.0 : min;
                case "avg":
                    return count > 0 ? sum / count : 0.0;
                default:
                    return sum;
            }
        }
    }
}
