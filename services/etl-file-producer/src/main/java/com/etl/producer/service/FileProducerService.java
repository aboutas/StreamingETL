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

import javax.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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

    @Value("${producer.data.file:/data/sensors.json}")
    private String dataFilePath;

    @Value("${producer.loop.enabled:true}")
    private boolean loopEnabled;

    @Value("${producer.job.id:default-job}")
    private String defaultJobId;

    @Value("${producer.mode:file}")
    private String producerMode; // "file" or "random"

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
            if ("random".equals(producerMode)) {
                startRandomDataProduction();
            } else {
                startFileProduction();
            }
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
        LOG.info("Producer mode: {}", producerMode);
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
            String timestamp = ZonedDateTime.now(GREEK_TIMEZONE).truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString();
            String[] allData = new String[locations.length * sensorTypes.length]; // 8 rooms * 6 sensors = 48 messages
            int index = 0;

            // Generate data for ALL sensor types in ALL rooms
            for (String location : locations) {
                for (String sensorType : sensorTypes) {
                    double measurement;
                    String unit;

                    switch (sensorType) {
                        case "temperature":
                            measurement = generateRealisticTemperature(location);
                            unit = "Celsius";
                            break;
                        case "pressure":
                            measurement = generateRealisticPressure();
                            unit = "hPa";
                            break;
                        case "humidity":
                            measurement = generateRealisticHumidity(location);
                            unit = "percent";
                            break;
                        case "air_quality":
                            measurement = generateRealisticAirQuality(location);
                            unit = "AQI";
                            break;
                        case "light":
                            measurement = generateRealisticLight(location);
                            unit = "lux";
                            break;
                        case "noise":
                            measurement = generateRealisticNoise(location);
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
                    com.fasterxml.jackson.databind.node.ObjectNode jsonObject = objectMapper.createObjectNode();
                    jsonObject.put("sensor", sensorType);
                    jsonObject.put("measurement", measurement);
                    jsonObject.put("measurement_unit", unit);
                    jsonObject.put("datetime", timestamp);
                    jsonObject.put("location", location);
                    jsonObject.put("data_quality", calculateDataQuality(sensorType, measurement, location));

                    allData[index++] = objectMapper.writeValueAsString(jsonObject);
                }
            }

            return allData;

        } catch (Exception e) {
            LOG.error("Error creating comprehensive sensor data", e);
            return new String[]{"{\"error\":\"Failed to generate comprehensive data\"}"};
        }
    }

    private double generateRealisticTemperature(String location) {
        // Base temperature ranges by location type
        double baseTemp;
        double variation;

        switch (location) {
            case "server-room":
                baseTemp = 22.0; // Cooler for servers
                variation = 3.0;
                break;
            case "kitchen":
                baseTemp = 26.0; // Warmer from cooking
                variation = 8.0;
                break;
            case "lobby":
                baseTemp = 21.0; // Standard office temperature
                variation = 4.0;
                break;
            case "conference-room":
                baseTemp = 23.0; // Slightly warmer with people
                variation = 5.0;
                break;
            default: // regular rooms
                baseTemp = 22.0;
                variation = 6.0;
        }

        // Add some realistic fluctuation
        return baseTemp + (random.nextGaussian() * variation);
    }

    private double generateRealisticPressure() {
        // Standard atmospheric pressure with realistic variation
        return 1013.25 + (random.nextGaussian() * 15.0); // 985-1040 hPa range
    }

    private double generateRealisticHumidity(String location) {
        double baseHumidity;
        double variation;

        switch (location) {
            case "kitchen":
                baseHumidity = 65.0; // Higher from cooking/washing
                variation = 15.0;
                break;
            case "server-room":
                baseHumidity = 40.0; // Lower, controlled environment
                variation = 8.0;
                break;
            default:
                baseHumidity = 50.0; // Standard office humidity
                variation = 12.0;
        }

        double humidity = baseHumidity + (random.nextGaussian() * variation);
        return Math.max(20.0, Math.min(90.0, humidity)); // Clamp to realistic range
    }

    private double generateRealisticAirQuality(String location) {
        double baseAQI;
        double variation;

        switch (location) {
            case "kitchen":
                baseAQI = 80.0; // Higher from cooking
                variation = 25.0;
                break;
            case "server-room":
                baseAQI = 35.0; // Lower, filtered air
                variation = 10.0;
                break;
            case "lobby":
                baseAQI = 60.0; // Moderate, external air influence
                variation = 20.0;
                break;
            default:
                baseAQI = 45.0; // Good indoor air quality
                variation = 15.0;
        }

        double aqi = baseAQI + (random.nextGaussian() * variation);
        return Math.max(0.0, Math.min(200.0, aqi)); // 0-200 AQI scale
    }

    private double generateRealisticLight(String location) {
        double baseLux;
        double variation;

        switch (location) {
            case "server-room":
                baseLux = 200.0; // Lower lighting
                variation = 50.0;
                break;
            case "conference-room":
                baseLux = 500.0; // Good meeting lighting
                variation = 100.0;
                break;
            case "lobby":
                baseLux = 300.0; // Ambient lighting
                variation = 80.0;
                break;
            case "kitchen":
                baseLux = 400.0; // Task lighting
                variation = 120.0;
                break;
            default:
                baseLux = 350.0; // Standard office lighting
                variation = 90.0;
        }

        double lux = baseLux + (random.nextGaussian() * variation);
        return Math.max(50.0, Math.min(1000.0, lux)); // Realistic indoor range
    }

    private double generateRealisticNoise(String location) {
        double baseDecibels;
        double variation;

        switch (location) {
            case "server-room":
                baseDecibels = 55.0; // Fan noise
                variation = 8.0;
                break;
            case "kitchen":
                baseDecibels = 60.0; // Equipment and activity
                variation = 12.0;
                break;
            case "lobby":
                baseDecibels = 50.0; // People talking, foot traffic
                variation = 10.0;
                break;
            case "conference-room":
                baseDecibels = 45.0; // Quieter for meetings
                variation = 15.0; // Can get loud during meetings
                break;
            default:
                baseDecibels = 40.0; // Quiet office environment
                variation = 8.0;
        }

        double noise = baseDecibels + (random.nextGaussian() * variation);
        return Math.max(30.0, Math.min(80.0, noise)); // Realistic indoor range
    }

    private boolean isValidMeasurement(String sensorType, double measurement) {
        switch (sensorType) {
            case "temperature":
                return measurement >= -10.0 && measurement <= 60.0; // Reasonable indoor range
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

    private String calculateDataQuality(String sensorType, double measurement, String location) {
        // Calculate quality score based on expected ranges for location/sensor combination
        double qualityScore = 1.0; // Start with perfect quality

        switch (sensorType) {
            case "temperature":
                // Adjust quality based on how far from expected range
                double expectedTemp = getExpectedTemperature(location);
                double tempDeviation = Math.abs(measurement - expectedTemp);
                if (tempDeviation > 10.0) qualityScore *= 0.7;
                else if (tempDeviation > 5.0) qualityScore *= 0.9;
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

    private double getExpectedTemperature(String location) {
        switch (location) {
            case "server-room": return 22.0;
            case "kitchen": return 26.0;
            case "lobby": return 21.0;
            case "conference-room": return 23.0;
            default: return 22.0;
        }
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
            LOG.info("  Temperature: 15-50°C (varies by location)");
            LOG.info("  Pressure: 985-1040 hPa");
            LOG.info("  Humidity: 20-90% (varies by location)");
            LOG.info("  Air Quality: 0-200 AQI (varies by location)");
            LOG.info("  Light: 50-1000 lux (varies by location)");
            LOG.info("  Noise: 30-80 dB (varies by location)");
            LOG.info("=====================================");
        } catch (Exception e) {
            LOG.warn("Error logging statistics", e);
        }
    }

    private String generateRandomSensorData() {
        try {
            String sensorType = sensorTypes[random.nextInt(sensorTypes.length)];
            String location = locations[random.nextInt(locations.length)];
            String timestamp = ZonedDateTime.now(GREEK_TIMEZONE).truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString();

            double measurement;
            String unit;

            switch (sensorType) {
                case "temperature":
                    measurement = 15.0 + (random.nextDouble() * 35.0); // 15-50°C
                    unit = "Celsius";
                    break;
                case "pressure":
                    measurement = 980.0 + (random.nextDouble() * 60.0); // 980-1040 hPa
                    unit = "hPa";
                    break;
                case "humidity":
                    measurement = 30.0 + (random.nextDouble() * 60.0); // 30-90%
                    unit = "percent";
                    break;
                default:
                    measurement = random.nextDouble() * 100;
                    unit = "units";
            }

            // Round to 1 decimal place
            measurement = Math.round(measurement * 10.0) / 10.0;

            // Create JSON object
            com.fasterxml.jackson.databind.node.ObjectNode jsonObject = objectMapper.createObjectNode();
            jsonObject.put("sensor", sensorType);
            jsonObject.put("measurement", measurement);
            jsonObject.put("measurement_unit", unit);
            jsonObject.put("datetime", timestamp);
            jsonObject.put("location", location);

            return objectMapper.writeValueAsString(jsonObject);

        } catch (Exception e) {
            LOG.error("Error creating random sensor data", e);
            return "{\"error\":\"Failed to generate data\"}";
        }
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
            com.fasterxml.jackson.databind.JsonNode objectNode = jsonNode.deepCopy();
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