package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.Transformation;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Extracts routing key from EtlConfig to ensure configs go to correct subtasks.
 * This enables TRUE CoFlatMap with both streams keyed by same field.
 *
 * Strategy: Key by sensor type for parallelism
 * - Extracts sensor filter from transformation params
 * - Distributes configs across subtasks by sensor type
 * - Matches event stream keying by sensor field
 */
public class ConfigKeyExtractor implements KeySelector<EtlConfig, String> {

    @Override
    public String getKey(EtlConfig config) throws Exception {
        // Extract sensor from transformation params for distribution
        if (config.getTransformations() != null) {
            for (Transformation transformation : config.getTransformations()) {
                if (transformation.getParams() != null) {
                    Object sensor = transformation.getParams().get("sensor");
                    if (sensor != null) {
                        // Key by sensor type: temperature, humidity, pressure, etc.
                        return (String) sensor;
                    }
                }
            }
        }
        // FAIL FAST: No "universal" fallback - force explicit sensor specification
        throw new IllegalArgumentException(
            "Config '" + config.getJobId() + "' must have 'sensor' parameter in at least one transformation. " +
            "CoFlatMap requires matching keys between config and event streams. " +
            "Events are keyed by sensor field (temperature, humidity, light, etc.), " +
            "so configs must explicitly specify which sensor type they target."
        );
    }
}
