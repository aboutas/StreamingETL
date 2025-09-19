package com.etl.producer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class FileProducerService {
    private static final Logger LOG = LoggerFactory.getLogger(FileProducerService.class);

    @Value("${kafka.bootstrap.servers:kafka:9092}")
    private String kafkaBootstrapServers;

    @Value("${kafka.input.topic:etl.input.v1}")
    private String inputTopic;

    @Value("${producer.rate.per.sec:10}")
    private int ratePerSec;

    @Value("${producer.data.file:/data/sensors.json}")
    private String dataFilePath;

    @Value("${producer.loop.enabled:true}")
    private boolean loopEnabled;

    @Value("${producer.job.id:default-job}")
    private String defaultJobId;

    private KafkaProducer<String, String> kafkaProducer;
    private ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Bean
    public ApplicationRunner startProducer() {
        return args -> {
            initializeKafkaProducer();
            startFileProduction();
        };
    }

    private void initializeKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);

        kafkaProducer = new KafkaProducer<>(props);
        objectMapper = new ObjectMapper();

        LOG.info("Kafka producer initialized with bootstrap servers: {}", kafkaBootstrapServers);
        LOG.info("Input topic: {}", inputTopic);
        LOG.info("Rate per second: {}", ratePerSec);
        LOG.info("Data file path: {}", dataFilePath);
        LOG.info("Loop enabled: {}", loopEnabled);
        LOG.info("Default Job ID: {}", defaultJobId);
    }

    private void startFileProduction() {
        new Thread(() -> {
            try {
                Path filePath = Paths.get(dataFilePath);

                if (!Files.exists(filePath)) {
                    LOG.error("Data file not found: {}", dataFilePath);
                    return;
                }

                LOG.info("Starting file production from: {}", dataFilePath);

                long intervalMs = 1000L / ratePerSec;
                long messageCount = 0;

                do {
                    try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
                        String line;

                        while ((line = reader.readLine()) != null && running.get()) {
                            if (line.trim().isEmpty()) {
                                continue;
                            }

                            try {
                                if (isValidJson(line)) {
                                    final String enrichedLine = enrichWithJobId(line); // Add jobId to event
                                    String key = generateKey(enrichedLine);
                                    ProducerRecord<String, String> record = new ProducerRecord<>(inputTopic, key, enrichedLine);

                                    kafkaProducer.send(record, (metadata, exception) -> {
                                        if (exception != null) {
                                            LOG.error("Failed to send message to Kafka: {}", enrichedLine, exception);
                                        } else {
                                            LOG.debug("Message sent successfully to partition: {} offset: {}",
                                                    metadata.partition(), metadata.offset());
                                        }
                                    });

                                    messageCount++;
                                    if (messageCount % 100 == 0) {
                                        LOG.info("Sent {} messages to Kafka", messageCount);
                                    }

                                    Thread.sleep(intervalMs);
                                } else {
                                    LOG.warn("Invalid JSON line ignored: {}", line);
                                }
                            } catch (Exception e) {
                                LOG.error("Error processing line: {}", line, e);
                            }
                        }

                        if (loopEnabled && running.get()) {
                            LOG.info("Reached end of file, restarting from beginning. Total messages sent: {}", messageCount);
                            Thread.sleep(1000);
                        }

                    } catch (Exception e) {
                        LOG.error("Error reading file: {}", dataFilePath, e);
                        Thread.sleep(5000);
                    }

                } while (loopEnabled && running.get());

                LOG.info("File production completed. Total messages sent: {}", messageCount);

            } catch (Exception e) {
                LOG.error("Fatal error in file production", e);
            }
        }, "file-producer-thread").start();
    }

    private boolean isValidJson(String json) {
        try {
            objectMapper.readTree(json);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String enrichWithJobId(String json) {
        try {
            JsonNode jsonNode = objectMapper.readTree(json);

            // Check if jobId already exists
            if (jsonNode.has("jobId")) {
                return json; // Already has jobId, return as-is
            }

            // For universal data source, don't add jobId if not configured
            if ("default-job".equals(defaultJobId)) {
                return json; // Return universal data without jobId
            }

            // Add jobId to the JSON object only if explicitly configured
            var objectNode = jsonNode.deepCopy();
            ((com.fasterxml.jackson.databind.node.ObjectNode) objectNode).put("jobId", defaultJobId);

            return objectMapper.writeValueAsString(objectNode);
        } catch (Exception e) {
            LOG.warn("Failed to enrich JSON with jobId: {}", json, e);
            return json; // Return original if enrichment fails
        }
    }

    private String generateKey(String json) {
        try {
            JsonNode jsonNode = objectMapper.readTree(json);

            // Prefer jobId as key for proper partitioning
            JsonNode jobIdNode = jsonNode.get("jobId");
            if (jobIdNode != null) {
                return jobIdNode.asText();
            }

            // Fallback to sensor
            JsonNode sensorNode = jsonNode.get("sensor");
            return sensorNode != null ? sensorNode.asText() : "unknown";
        } catch (Exception e) {
            return "unknown";
        }
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutting down file producer service...");
        running.set(false);

        if (kafkaProducer != null) {
            kafkaProducer.close();
            LOG.info("Kafka producer closed");
        }
    }
}