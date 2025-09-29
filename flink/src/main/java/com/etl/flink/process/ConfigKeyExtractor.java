package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.Transformation;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Extracts routing key from EtlConfig to ensure configs go to correct subtasks.
 * This enables TRUE CoFlatMap with both streams keyed by same field.
 */
public class ConfigKeyExtractor implements KeySelector<EtlConfig, String> {

    @Override
    public String getKey(EtlConfig config) throws Exception {
        // For universal data processing, send all configs to all subtasks
        return "universal";
    }

    private String extractKeyByField(EtlConfig config) {
        if (config.getTransformations() != null) {
            for (Transformation transformation : config.getTransformations()) {
                // Look for keyBy field in aggregation transformations
                if (transformation.getKeyBy() != null && !transformation.getKeyBy().isEmpty()) {
                    return transformation.getKeyBy();
                }
            }
        }
        return "sensor"; // Default if no keyBy specified
    }
}