package com.etl.api.service;

import com.etl.api.model.EtlConfig;
import com.etl.api.model.Transformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Service
public class ConfigService {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigService.class);

    private static final List<String> VALID_TRANSFORMATIONS = Arrays.asList(
            // Element transformations (filters and data cleaning)
            "filter_greater", "filter_less", "filter_cross_field",
            "normalize", "to_lowercase", "trim_whitespace",
            // Aggregation transformations
            "sum", "max", "min", "avg"
    );

    @Value("${kafka.bootstrap.servers:kafka:9092}")
    private String kafkaBootstrapServers;

    @Value("${kafka.config.topic:etl.config.v1}")
    private String configTopic;

    private KafkaProducer<String, String> kafkaProducer;
    private ObjectMapper objectMapper;
    private JobRegistryService jobRegistryService;

    public ConfigService(JobRegistryService jobRegistryService) {
        this.jobRegistryService = jobRegistryService;
        this.objectMapper = new ObjectMapper();
    }

    @PostConstruct
    public void init() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        kafkaProducer = new KafkaProducer<>(props);
        LOG.info("Kafka producer initialized with bootstrap servers: {}", kafkaBootstrapServers);
    }

    @PreDestroy
    public void cleanup() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
    }

    public void validateConfig(EtlConfig config) throws IllegalArgumentException {
        if (config.getJobId() == null || config.getJobId().trim().isEmpty()) {
            throw new IllegalArgumentException("jobId is required");
        }

        if (config.getTransformations() != null) {
            for (Transformation transformation : config.getTransformations()) {
                if (!VALID_TRANSFORMATIONS.contains(transformation.getType())) {
                    throw new IllegalArgumentException("Unknown transformation type: " + transformation.getType());
                }
            }
        }
    }

    public void publishConfig(EtlConfig config) throws Exception {
        try {
            String configJson = objectMapper.writeValueAsString(config);
            ProducerRecord<String, String> record = new ProducerRecord<>(configTopic, config.getJobId(), configJson);

            kafkaProducer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Failed to publish config for job: {}", config.getJobId(), exception);
                    jobRegistryService.updateJobStatus(config.getJobId(), "FAILED",
                                                     "Failed to publish config: " + exception.getMessage());
                } else {
                    LOG.info("Config published successfully for job: {} to partition: {}",
                            config.getJobId(), metadata.partition());
                    jobRegistryService.updateJobStatus(config.getJobId(), "PUBLISHED",
                                                     "Config published to Kafka");
                }
            });

            LOG.info("Config submitted for job: {}", config.getJobId());
            jobRegistryService.registerJob(config.getJobId(), "SUBMITTED", "Config validation passed, publishing to Kafka");

        } catch (Exception e) {
            LOG.error("Error publishing config", e);
            jobRegistryService.updateJobStatus(config.getJobId(), "FAILED",
                                             "Error publishing config: " + e.getMessage());
            throw e;
        }
    }
}