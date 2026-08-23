package com.etl.flink.sink;

import com.etl.flink.model.EtlResult;
import com.etl.flink.model.Transformation;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MongoSink extends RichSinkFunction<EtlResult> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoSink.class);
    private static final ZoneId GREEK_TIMEZONE = ZoneId.of("Europe/Athens"); // UTC+2 (UTC+3 in summer)

    private final String mongoUri;
    private transient MongoClient mongoClient;
    private transient MongoDatabase database;
    private transient Set<String> indexedCollections;

    public MongoSink(String mongoUri) {
        this.mongoUri = mongoUri;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoClient = MongoClients.create(mongoUri);
        database = mongoClient.getDatabase("etl_db");
        indexedCollections = ConcurrentHashMap.newKeySet();
        LOG.info("MongoDB sink initialized with URI: {}", mongoUri);
    }

    private void ensureIndex(String collectionName) {
        if (indexedCollections.add(collectionName)) {
            LOG.info("Registered new collection: {}", collectionName);
        }
    }

    private String sanitizeCollectionName(String jobId) {
        if (jobId == null || jobId.trim().isEmpty()) {
            return "unknown_job";
        }
        return jobId.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public void invoke(EtlResult result, Context context) throws Exception {
        try {
            String id = generateId(result);
            result.setId(id);
            result.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());

            String collectionName = sanitizeCollectionName(result.getJobId());
            ensureIndex(collectionName);

            Document doc = convertToDocument(result);

            MongoCollection<Document> collection = database.getCollection(collectionName);
            collection.replaceOne(
                    new Document("_id", id),
                    doc,
                    new ReplaceOptions().upsert(true)
            );
        } catch (Exception e) {
            LOG.error("Error writing to MongoDB", e);
            throw e;
        }
    }

    private String generateId(EtlResult result) {
        String transformationsHash = Integer.toString(
                result.getTransformations() != null ? result.getTransformations().hashCode() : 0
        );

        return String.format("job:%s|g:%s:%s|agg:%s|f:%s|h:%s",
                result.getJobId() != null ? result.getJobId() : "unknown",
                result.getGroupingField() != null ? result.getGroupingField() : "default",
                result.getGroupingKey() != null ? result.getGroupingKey() : "default",
                result.getAggregationType() != null ? result.getAggregationType() : "none",
                result.getField() != null ? result.getField() : "default",
                transformationsHash
        );
    }

    private Document convertToDocument(EtlResult result) {
        Document doc = new Document()
                .append("_id", result.getId())
                .append("jobId", result.getJobId())
                .append("groupingField", result.getGroupingField())
                .append("groupingKey", result.getGroupingKey())
                .append("aggregationType", result.getAggregationType())
                .append("field", result.getField())
                .append("result", convertResultToDocument(result.getResult()))
                .append("processedAt", result.getProcessedAt() != null ?
                        result.getProcessedAt().toString() : ZonedDateTime.now(GREEK_TIMEZONE).toInstant().toString());

        if (result.getTransformations() != null) {
            List<Document> transformationDocs = new ArrayList<>();
            for (Transformation t : result.getTransformations()) {
                Document tDoc = new Document()
                        .append("type", t.getType())
                        .append("params", t.getParams())
                        .append("keyBy", t.getKeyBy());
                transformationDocs.add(tDoc);
            }
            doc.append("transformations", transformationDocs);
        }

        // Add sensor context information
        if (result.getSensorType() != null) {
            doc.append("sensorType", result.getSensorType());
        }
        if (result.getMeasurementUnit() != null) {
            doc.append("measurementUnit", result.getMeasurementUnit());
        }
        if (result.getLocation() != null) {
            doc.append("location", result.getLocation());
        }

        return doc;
    }

    private Object convertResultToDocument(Object resultObj) {
        if (resultObj == null) {
            return null;
        }

        // Handle primitive types and strings directly
        if (resultObj instanceof String || resultObj instanceof Number || resultObj instanceof Boolean) {
            return resultObj;
        }

        // Handle SensorEvent objects by converting to Document
        if (resultObj instanceof com.etl.flink.model.SensorEvent) {
            com.etl.flink.model.SensorEvent sensorEvent = (com.etl.flink.model.SensorEvent) resultObj;
            return new Document()
                    .append("sensor", sensorEvent.getSensor())
                    .append("measurement", sensorEvent.getMeasurement())
                    .append("measurementUnit", sensorEvent.getMeasurementUnit())
                    .append("datetime", sensorEvent.getDatetime() != null ? sensorEvent.getDatetime().toString() : null)
                    .append("location", sensorEvent.getLocation())
                    .append("dataQuality", sensorEvent.getDataQuality());
        }

        // Handle Instant objects
        if (resultObj instanceof Instant) {
            return ((Instant) resultObj).toString();
        }

        // For any other object type, convert to string as fallback
        return resultObj.toString();
    }

    @Override
    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
            LOG.info("MongoDB client closed");
        }
        super.close();
    }
}