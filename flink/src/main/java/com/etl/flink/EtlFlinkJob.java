package com.etl.flink;

import com.etl.flink.deserializer.SafeStringDeserializer;
import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.process.CoFlatMapProcessor;
import com.etl.flink.process.ConfigKeyExtractor;
import com.etl.flink.sink.MongoSink;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.time.Instant;
import java.util.List;

public class EtlFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(EtlFlinkJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting ETL Flink Job");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String configTopic = getEnvOrDefault("CONFIG_TOPIC", "etl.config.v1");
        String inputTopic = getEnvOrDefault("INPUT_TOPIC", "etl.input.v1");
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
                .fromSource(dataSource, WatermarkStrategy.noWatermarks(), "Data Source")
                .map(new EventDeserializer())
                .filter(event -> event != null)
                .keyBy(event -> event.getSensor()); // Key by sensor type for parallelism

        // CoFlatMap: Both streams keyed by same fielδ
        DataStream<EtlResult> processedStream = configStream
                .connect(eventStream)
                .flatMap(new CoFlatMapProcessor());

        // Split streams: configs vs data with windowing
        DataStream<EtlResult> configResults = processedStream
                .filter(result -> result.getAggregationType() == null);

        DataStream<EtlResult> dataResults = processedStream
                .filter(result -> result.getAggregationType() != null);

       
        DataStream<EtlResult> windowedResults = dataResults
                .keyBy(result -> {
                    return String.format("%s|%s|%s",
                        result.getGroupingKey() != null ? result.getGroupingKey() : "default",
                        result.getJobId() != null ? result.getJobId() : "unknown",
                        result.getAggregationType() != null ? result.getAggregationType() : "none"
                    );
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new WindowAggregator());

        // Union all results
        DataStream<EtlResult> allResults = configResults.union(windowedResults);

        // SINK: MongoDB
        allResults.addSink(new MongoSink(mongoUri)).name("MongoDB Sink");

        LOG.info("Executing ETL Flink Job");
        env.execute("ETL Flink Job");
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
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
        private static final com.fasterxml.jackson.core.JsonFactory FACTORY = new com.fasterxml.jackson.core.JsonFactory();

        @Override
        public SensorEvent map(String value) throws Exception {
            try {
                SensorEvent event = new SensorEvent();
                try (com.fasterxml.jackson.core.JsonParser p = FACTORY.createParser(value)) {
                    p.nextToken(); // START_OBJECT
                    while (p.nextToken() == com.fasterxml.jackson.core.JsonToken.FIELD_NAME) {
                        String name = p.getCurrentName();
                        p.nextToken();
                        switch (name) {
                            case "sensor":           event.setSensor(p.getText()); break;
                            case "measurement":      event.setMeasurement(p.getDoubleValue()); break;
                            case "measurement_unit": event.setMeasurementUnit(p.getText()); break;
                            case "location":         event.setLocation(p.getText()); break;
                            case "data_quality":     event.setDataQuality(p.getText()); break;
                            case "jobId":            event.setJobId(p.getText()); break;
                            case "datetime":         event.setDatetime(p.getText()); break;
                            default: p.skipChildren(); break;
                        }
                    }
                }
                return event;
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
            throw new UnsupportedOperationException("Merge not supported for Tumbling Windows");
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