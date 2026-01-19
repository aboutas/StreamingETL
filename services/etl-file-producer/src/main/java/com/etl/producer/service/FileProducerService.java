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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class FileProducerService {
    private static final Logger LOG = LoggerFactory.getLogger(FileProducerService.class);
    private static final ZoneId GREEK_TIMEZONE = ZoneId.of("Europe/Athens"); // UTC+2 (UTC+3 in summer)

    @Value("${kafka.bootstrap.servers:kafka:9092}")
    private String kafkaBootstrapServers;

    @Value("${kafka.input.topic:etl.input.v1}")
    private String inputTopic;

    @Value("${producer.rate.per.sec:10}")
    private int ratePerSec;

    private KafkaProducer<String, String> kafkaProducer;
    private ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Random random = new Random();
    private final String[] sensorTypes = {"temperature", "pressure", "humidity", "air_quality", "light", "noise"};
    private final String[] locations = {"room-a", "room-b", "room-c", "room-d", "lobby", "conference-room", "kitchen", "server-room"};

    @Bean
    public ApplicationRunner startProducer() {
        return args -> {
            initializeKafkaProducer();
            startRandomDataProduction();
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
    }

    private void startRandomDataProduction() {
        new Thread(() -> {
            try {
                LOG.info("Starting comprehensive random data production mode");
                LOG.info("Each room will have all sensor types: temperature, pressure, humidity, air_quality, light, noise");
                LOG.info("Generating realistic sensor data with patterns and correlations");

                long intervalMs = 1000L / ratePerSec;
                long messageCount = 0;

                while (running.get()) {
                    try {
                        // Generate comprehensive sensor data (all sensor types for all rooms)
                        String[] allSensorData = generateComprehensiveSensorData();

                        for (String sensorData : allSensorData) {
                            String key = generateKey(sensorData);
                            ProducerRecord<String, String> record = new ProducerRecord<>(inputTopic, key, sensorData);

                            kafkaProducer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    LOG.error("Failed to send message to Kafka: {}", sensorData, exception);
                                } else {
                                    LOG.debug("Message sent successfully to partition: {} offset: {}",
                                            metadata.partition(), metadata.offset());
                                }
                            });

                            messageCount++;
                        }

                        if (messageCount % 100 == 0) {
                            LOG.info("Sent {} comprehensive messages to Kafka (all sensors for all rooms)", messageCount);
                            logDataGenerationStats(messageCount);
                        }

                        Thread.sleep(intervalMs);

                    } catch (Exception e) {
                        LOG.error("Error generating comprehensive sensor data", e);
                        Thread.sleep(1000);
                    }
                }

                LOG.info("Comprehensive data production completed. Total messages sent: {}", messageCount);

            } catch (Exception e) {
                LOG.error("Fatal error in comprehensive data production", e);
            }
        }, "comprehensive-producer-thread").start();
    }

    private String[] generateComprehensiveSensorData() {
        try {
            String timestamp = ZonedDateTime.now(GREEK_TIMEZONE).toInstant().toString();
            String[] allData = new String[locations.length * sensorTypes.length]; // 8 rooms * 6 sensors = 48 messages
            int index = 0;

            // Generate data for ALL sensor types in ALL rooms
            for (String location : locations) {
                for (String sensorType : sensorTypes) {
                    double measurement;
                    String unit;

                    switch (sensorType) {
                        case "temperature":
                            measurement = generateRealisticTemperature();
                            unit = "Celsius";
                            break;
                        case "pressure":
                            measurement = generateRealisticPressure();
                            unit = "hPa";
                            break;
                        case "humidity":
                            measurement = generateRealisticHumidity();
                            unit = "percent";
                            break;
                        case "air_quality":
                            measurement = generateRealisticAirQuality();
                            unit = "AQI";
                            break;
                        case "light":
                            measurement = generateRealisticLight();
                            unit = "lux";
                            break;
                        case "noise":
                            measurement = generateRealisticNoise();
                            unit = "dB";
                            break;
                        default:
                            measurement = random.nextDouble() * 100;
                            unit = "units";
                    }

                    // Round to 1 decimal place
                    measurement = Math.round(measurement * 10.0) / 10.0;

                    // Validate measurement before creating JSON
                    if (!isValidMeasurement(sensorType, measurement)) {
                        LOG.warn("Invalid measurement for sensor type {}: {}. Skipping.", sensorType, measurement);
                        continue;
                    }

                    // Create JSON object for this sensor in this room
                    var jsonObject = objectMapper.createObjectNode();
                    jsonObject.put("sensor", sensorType);
                    jsonObject.put("measurement", measurement);
                    jsonObject.put("measurement_unit", unit);
                    jsonObject.put("datetime", timestamp);
                    jsonObject.put("location", location);
                    jsonObject.put("data_quality", calculateDataQuality(sensorType, measurement));

                    allData[index++] = objectMapper.writeValueAsString(jsonObject);
                }
            }

            return allData;

        } catch (Exception e) {
            LOG.error("Error creating comprehensive sensor data", e);
            return new String[]{"{\"error\":\"Failed to generate comprehensive data\"}"};
        }
    }

    private double generateRealisticTemperature() {
        // Real-world temperature ranges: -30°C (extreme winter) to 70°C (extreme heat)
        // Gaussian distribution centered around typical indoor temperature
        double temp = 22.0 + (random.nextGaussian() * 8.0);
        // Clamp to realistic range
        return Math.max(-30.0, Math.min(70.0, temp));
    }

    private double generateRealisticPressure() {
        // Standard atmospheric pressure with realistic variation
        double pressure = 1013.25 + (random.nextGaussian() * 15.0);
        // Clamp to realistic atmospheric pressure range
        return Math.max(950.0, Math.min(1080.0, pressure));
    }

    private double generateRealisticHumidity() {
        // Realistic humidity range: 20-90%
        // Gaussian distribution centered around typical indoor humidity
        double humidity = 50.0 + (random.nextGaussian() * 15.0);
        return Math.max(20.0, Math.min(90.0, humidity));
    }

    private double generateRealisticAirQuality() {
        // Realistic AQI range: 0-200
        // Gaussian distribution centered around good indoor air quality
        double aqi = 50.0 + (random.nextGaussian() * 25.0);
        return Math.max(0.0, Math.min(200.0, aqi));
    }

    private double generateRealisticLight() {
        // Realistic light range: 50-1000 lux
        // Gaussian distribution centered around typical indoor lighting
        double lux = 350.0 + (random.nextGaussian() * 100.0);
        return Math.max(50.0, Math.min(1000.0, lux));
    }

    private double generateRealisticNoise() {
        // Realistic noise range: 30-80 dB
        // Gaussian distribution centered around typical indoor noise level
        double noise = 45.0 + (random.nextGaussian() * 10.0);
        return Math.max(30.0, Math.min(80.0, noise));
    }

    private boolean isValidMeasurement(String sensorType, double measurement) {
        switch (sensorType) {
            case "temperature":
                return measurement >= -30.0 && measurement <= 70.0; // Real-world range
            case "pressure":
                return measurement >= 950.0 && measurement <= 1080.0; // Atmospheric pressure range
            case "humidity":
                return measurement >= 0.0 && measurement <= 100.0; // Percentage range
            case "air_quality":
                return measurement >= 0.0 && measurement <= 500.0; // AQI scale
            case "light":
                return measurement >= 0.0 && measurement <= 2000.0; // Lux range
            case "noise":
                return measurement >= 20.0 && measurement <= 120.0; // Decibel range
            default:
                return measurement >= 0.0 && measurement <= 1000.0; // Generic range
        }
    }

    private String calculateDataQuality(String sensorType, double measurement) {
        // Calculate quality score based on expected ranges for sensor type
        double qualityScore = 1.0; // Start with perfect quality

        switch (sensorType) {
            case "temperature":
                // Typical indoor temperature is around 22°C
                double tempDeviation = Math.abs(measurement - 22.0);
                if (tempDeviation > 15.0) qualityScore *= 0.7;
                else if (tempDeviation > 8.0) qualityScore *= 0.9;
                break;
            case "humidity":
                if (measurement < 20.0 || measurement > 80.0) qualityScore *= 0.8;
                break;
            case "air_quality":
                if (measurement > 150.0) qualityScore *= 0.6; // Poor air quality
                else if (measurement > 100.0) qualityScore *= 0.8;
                break;
            case "noise":
                if (measurement > 70.0) qualityScore *= 0.7; // High noise
                break;
        }

        // Add small random variation to simulate real sensor quality variations
        qualityScore += (random.nextGaussian() * 0.05);
        qualityScore = Math.max(0.5, Math.min(1.0, qualityScore)); // Clamp between 0.5 and 1.0

        if (qualityScore >= 0.95) return "excellent";
        else if (qualityScore >= 0.85) return "good";
        else if (qualityScore >= 0.75) return "fair";
        else return "poor";
    }

    private void logDataGenerationStats(long messageCount) {
        try {
            LOG.info("=== Data Generation Statistics ===");
            LOG.info("Total messages sent: {}", messageCount);
            LOG.info("Locations generating data: {}", String.join(", ", locations));
            LOG.info("Sensor types generating data: {}", String.join(", ", sensorTypes));
            LOG.info("Messages per cycle: {} (locations: {} × sensors: {})",
                locations.length * sensorTypes.length, locations.length, sensorTypes.length);
            LOG.info("Rate: {} messages/second", ratePerSec);

            // Log expected data characteristics
            LOG.info("Expected sensor ranges:");
            LOG.info("  Temperature: -30 to 70°C (Gaussian, mean ~22°C)");
            LOG.info("  Pressure: 950-1080 hPa (Gaussian, mean ~1013 hPa)");
            LOG.info("  Humidity: 20-90% (Gaussian, mean ~50%)");
            LOG.info("  Air Quality: 0-200 AQI (Gaussian, mean ~50)");
            LOG.info("  Light: 50-1000 lux (Gaussian, mean ~350 lux)");
            LOG.info("  Noise: 30-80 dB (Gaussian, mean ~45 dB)");
            LOG.info("=====================================");
        } catch (Exception e) {
            LOG.warn("Error logging statistics", e);
        }
    }

    private String generateKey(String json) {
        try {
            JsonNode jsonNode = objectMapper.readTree(json);
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