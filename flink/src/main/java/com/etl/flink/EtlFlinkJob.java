package com.etl.flink;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.process.EtlCoFlatMapFunction;
import com.etl.flink.process.EtlWindowProcessor;
import com.etl.flink.process.WindowedConfigProcessor;
import com.etl.flink.sink.MongoSink;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import java.time.Duration;
import java.time.Instant;

public class EtlFlinkJob {
    private static final Logger LOG = LoggerFactory.getLogger(EtlFlinkJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting ETL Flink Job");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30000);

        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String configTopic = getEnvOrDefault("CONFIG_TOPIC", "etl.config.v1");
        String inputTopic = getEnvOrDefault("INPUT_TOPIC", "etl.input.v1");
        String outputTopic = getEnvOrDefault("OUTPUT_TOPIC", "etl.output.v1");
        String mongoUri = getEnvOrDefault("MONGO_URI", "mongodb://mongo:27017/etl_db");

        LOG.info("Kafka Bootstrap Servers: {}", kafkaBootstrapServers);
        LOG.info("Config Topic: {}", configTopic);
        LOG.info("Input Topic: {}", inputTopic);
        LOG.info("Output Topic: {}", outputTopic);
        LOG.info("MongoDB URI: {}", mongoUri);

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        KafkaSource<String> configSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(configTopic)
                .setGroupId("etl-config-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSource<String> dataSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("etl-data-consumer-v2")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<EtlConfig> configStream = env
                .fromSource(configSource, WatermarkStrategy.noWatermarks(), "Config Source")
                .map(new ConfigDeserializer())
                .filter(config -> config != null)
                .keyBy(config -> "universal"); // Use universal key for all configs

        DataStream<SensorEvent> eventStream = env
                .fromSource(dataSource,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> {
                                    try {
                                        ObjectMapper mapper = new ObjectMapper();
                                        mapper.registerModule(new JavaTimeModule());
                                        SensorEvent sensorEvent = mapper.readValue(event, SensorEvent.class);
                                        return sensorEvent.getDatetime().toEpochMilli();
                                    } catch (Exception e) {
                                        LOG.warn("Failed to extract timestamp from event: {}", event, e);
                                        return Instant.now().toEpochMilli();
                                    }
                                }),
                        "Data Source")
                .map(new EventDeserializer())
                .filter(event -> event != null)
                .keyBy(event -> "universal"); // Use universal key for all data

        // New approach: Use WindowedConfigProcessor to dynamically create windowed streams
        DataStream<EtlResult> processedStream = configStream
                .connect(eventStream)
                .flatMap(new WindowedConfigProcessor());

        processedStream.addSink(new MongoSink(mongoUri));

        processedStream.print("ETL Results");

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
}