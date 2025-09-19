package com.etl.flink.sink;

import com.etl.flink.model.EtlResult;
import com.etl.flink.model.Transformation;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class MongoSink extends RichSinkFunction<EtlResult> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoSink.class);

    private final String mongoUri;
    private transient MongoClient mongoClient;
    private transient MongoDatabase database;

    public MongoSink(String mongoUri) {
        this.mongoUri = mongoUri;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoClient = MongoClients.create(mongoUri);
        database = mongoClient.getDatabase("etl_db");
        LOG.info("MongoDB sink initialized with URI: {}", mongoUri);
    }

    private void createUniqueIndexForCollection(MongoCollection<Document> collection, String collectionName) {
        try {
            collection.createIndex(Indexes.ascending("_id"), new IndexOptions().unique(true));
            LOG.info("Created unique index on _id field for collection: {}", collectionName);
        } catch (Exception e) {
            LOG.warn("Index may already exist for collection {}: {}", collectionName, e.getMessage());
        }
    }

    private String sanitizeCollectionName(String jobId) {
        if (jobId == null || jobId.trim().isEmpty()) {
            return "unknown_job";
        }
        // Replace invalid characters with underscores
        return jobId.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public void invoke(EtlResult result, Context context) throws Exception {
        try {
            String id = generateId(result);
            result.setId(id);
            result.setProcessedAt(Instant.now());

            // Get collection name based on jobId
            String collectionName = sanitizeCollectionName(result.getJobId());
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Ensure index exists for this collection (idempotent operation)
            createUniqueIndexForCollection(collection, collectionName);

            Document doc = convertToDocument(result);

            collection.updateOne(
                    new Document("_id", id),
                    new Document("$set", doc),
                    new UpdateOptions().upsert(true)
            );

            LOG.debug("Upserted document with ID: {} to collection: {}", id, collectionName);
        } catch (Exception e) {
            LOG.error("Error writing to MongoDB", e);
            throw e;
        }
    }

    private String generateId(EtlResult result) {
        String transformationsHash = Integer.toString(
                result.getTransformations() != null ? result.getTransformations().hashCode() : 0
        );

        return String.format("job:%s|g:%s:%s|ws:%s|we:%s|agg:%s|f:%s|h:%s",
                result.getJobId() != null ? result.getJobId() : "unknown",
                result.getGroupingField() != null ? result.getGroupingField() : "default",
                result.getGroupingKey() != null ? result.getGroupingKey() : "default",
                result.getWindowStart() != null ? result.getWindowStart().toString() : "none",
                result.getWindowEnd() != null ? result.getWindowEnd().toString() : "none",
                result.getAggregationType() != null ? result.getAggregationType() : "none",
                result.getField() != null ? result.getField() : "default",
                transformationsHash
        );
    }

    private Document convertToDocument(EtlResult result) {
        Document doc = new Document()
                .append("_id", result.getId())
                .append("jobId", result.getJobId())
                .append("source", result.getSource())
                .append("groupingField", result.getGroupingField())
                .append("groupingKey", result.getGroupingKey())
                .append("aggregationType", result.getAggregationType())
                .append("field", result.getField())
                .append("result", result.getResult())
                .append("processedAt", result.getProcessedAt() != null ?
                        result.getProcessedAt().toString() : Instant.now().toString());

        if (result.getWindowStart() != null) {
            doc.append("windowStart", result.getWindowStart().toString());
        }
        if (result.getWindowEnd() != null) {
            doc.append("windowEnd", result.getWindowEnd().toString());
        }

        if (result.getTransformations() != null) {
            List<Document> transformationDocs = new ArrayList<>();
            for (Transformation t : result.getTransformations()) {
                Document tDoc = new Document()
                        .append("type", t.getType())
                        .append("params", t.getParams())
                        .append("keyBy", t.getKeyBy())
                        .append("window", t.getWindow());
                transformationDocs.add(tDoc);
            }
            doc.append("transformations", transformationDocs);
        }

        if (result.getDiagnostics() != null && !result.getDiagnostics().isEmpty()) {
            doc.append("diagnostics", result.getDiagnostics());
        }

        return doc;
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