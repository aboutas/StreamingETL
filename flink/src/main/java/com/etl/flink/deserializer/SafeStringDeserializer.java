package com.etl.flink.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Safe string deserializer that handles null/corrupt Kafka records gracefully.
 * Returns null for any record that cannot be deserialized.
 */
public class SafeStringDeserializer implements DeserializationSchema<String> {
    private static final Logger LOG = LoggerFactory.getLogger(SafeStringDeserializer.class);

    @Override
    public String deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            LOG.warn("Received null or empty message from Kafka, skipping");
            return null;
        }

        try {
            return new String(message, StandardCharsets.UTF_8);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize Kafka record, skipping: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
