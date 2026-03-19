package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.model.Transformation;
import com.etl.flink.udf.ElementTransformations;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Both config and data streams keyed by same field for optimal distribution.
 */
public class CoFlatMapProcessor extends RichCoFlatMapFunction<EtlConfig, SensorEvent, EtlResult> {
    private static final Logger LOG = LoggerFactory.getLogger(CoFlatMapProcessor.class);
    private static final ZoneId GREEK_TIMEZONE = ZoneId.of("Europe/Athens"); // UTC+2 (UTC+3 in summer)

    private transient MapState<String, EtlConfig> configState;
    // Cache transformation functions per config jobId to avoid recreating per event
    private final Map<String, List<MapFunction<SensorEvent, SensorEvent>>> transformationCache = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("configs-by-key", String.class, EtlConfig.class)
        );
    }

    @Override
    public void flatMap1(EtlConfig config, Collector<EtlResult> out) throws Exception {
        LOG.info("Received config for job: {} on subtask: {}",
                config.getJobId(), getRuntimeContext().getIndexOfThisSubtask());

        // Store config by jobId - only configs for THIS key group
        configState.put(config.getJobId(), config);

        // Pre-build and cache transformation functions for this config
        if (config.getTransformations() != null) {
            List<MapFunction<SensorEvent, SensorEvent>> cached = new ArrayList<>();
            for (Transformation t : config.getTransformations()) {
                if (isElementTransformation(t.getType())) {
                    cached.add(ElementTransformations.createTransformation(t.getType(), t.getParams()));
                }
            }
            transformationCache.put(config.getJobId(), cached);
        }

        // Emit configuration status result
        EtlResult configResult = new EtlResult();
        configResult.setJobId(config.getJobId());
        configResult.setTransformations(config.getTransformations());
        configResult.setResult("Configuration registered successfully on subtask " +
                              getRuntimeContext().getIndexOfThisSubtask());
        configResult.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());

        out.collect(configResult);
    }

    @Override
    public void flatMap2(SensorEvent event, Collector<EtlResult> out) throws Exception {
        // Process this event against configs stored in THIS subtask's key group
        Iterable<Map.Entry<String, EtlConfig>> configs = configState.entries();

        boolean hasConfigs = false;
        for (Map.Entry<String, EtlConfig> configEntry : configs) {
            hasConfigs = true;
            EtlConfig config = configEntry.getValue();

            try {
                LOG.debug("Processing event with sensor: {} against config: {} on subtask: {}",
                         event.getSensor(), config.getJobId(), getRuntimeContext().getIndexOfThisSubtask());
                processEventWithConfig(event, config, out);
            } catch (Exception e) {
                LOG.error("Error processing event with config for jobId: {} on subtask: {}",
                         config.getJobId(), getRuntimeContext().getIndexOfThisSubtask(), e);
                emitErrorResult(event, config, e, out);
            }
        }

        if (!hasConfigs) {
            LOG.debug("No configs available on subtask: {}, dropping event with sensor: {}",
                     getRuntimeContext().getIndexOfThisSubtask(), event.getSensor());
        }
    }

    private void processEventWithConfig(SensorEvent event, EtlConfig config, Collector<EtlResult> out) throws Exception {
        if (config.getTransformations() == null || config.getTransformations().isEmpty()) {
            createFinalResult(event, config, out);
            return;
        }

        // Separate element transformations and aggregation transformations
        List<Transformation> elementTransformations = new ArrayList<>();
        List<Transformation> aggregationTransformations = new ArrayList<>();

        for (Transformation transformation : config.getTransformations()) {
            if (isElementTransformation(transformation.getType())) {
                elementTransformations.add(transformation);
            } else if (isAggregationTransformation(transformation.getType())) {
                aggregationTransformations.add(transformation);
            }
        }

        // Strategy: Process each transformation path independently
        // This allows different sensor filters to work in parallel

        // Process element transformations using cached functions
        SensorEvent filteredEvent = event;
        List<MapFunction<SensorEvent, SensorEvent>> cachedFunctions = transformationCache.get(config.getJobId());
        if (cachedFunctions != null) {
            for (MapFunction<SensorEvent, SensorEvent> fn : cachedFunctions) {
                filteredEvent = applyElementTransformation(filteredEvent, fn);
                if (filteredEvent == null) {
                    break;
                }
            }
        }

        // Process aggregation transformations - only if event passed element filters
        boolean hasValidAggregations = false;
        if (filteredEvent != null) {
            for (Transformation transformation : aggregationTransformations) {
                if (processWindowedAggregation(filteredEvent, transformation, config, out)) {
                    hasValidAggregations = true;
                }
            }
        }

        // If we have element transformations that passed and no aggregations, emit the filtered result
        if (!aggregationTransformations.isEmpty()) {
            // Aggregations were processed, no need to emit additional results
            if (!hasValidAggregations && filteredEvent != null) {
                // Element transformations passed but no aggregations matched - emit filtered result
                createFinalResult(filteredEvent, config, out);
            }
        } else if (filteredEvent != null) {
            // Only element transformations, emit the result
            createFinalResult(filteredEvent, config, out);
        }
        // If filteredEvent is null and no aggregations matched, the event is completely filtered out
    }

    private boolean processWindowedAggregation(SensorEvent event, Transformation transformation,
                                          EtlConfig config, Collector<EtlResult> out) {

        String field = getFieldFromParams(transformation.getParams(), "measurement");
        String sensorFilter = getSensorFromParams(transformation.getParams());
        String keyBy = transformation.getKeyBy() != null ? transformation.getKeyBy() : "sensor";

        // Apply sensor filter if specified
        if (sensorFilter != null && !sensorFilter.equals(event.getSensor())) {
            return false; // Skip this event - wrong sensor type
        }

        String groupingKey = getGroupingKeyFromEvent(event, keyBy);

        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setTransformations(config.getTransformations());
        result.setGroupingField(keyBy);
        result.setGroupingKey(groupingKey);
        result.setAggregationType(transformation.getType());
        result.setField(field);
        result.setResult(getFieldValue(event, field));
        result.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());

        // Add sensor context information
        result.setSensorType(event.getSensor());
        result.setMeasurementUnit(event.getMeasurementUnit());
        result.setLocation(event.getLocation());

        out.collect(result);
        return true; // Successfully processed
    }

    private boolean isElementTransformation(String type) {
        return "filter_greater".equals(type) ||
               "filter_less".equals(type) ||
               "normalize".equals(type) ||
               "to_lowercase".equals(type) ||
               "to_uppercase".equals(type) ||
               "trim_whitespace".equals(type);
    }

    private boolean isAggregationTransformation(String type) {
        return "sum".equals(type) || "max".equals(type) || "min".equals(type) || "avg".equals(type);
    }

    private SensorEvent applyElementTransformation(SensorEvent event, MapFunction<SensorEvent, SensorEvent> fn) throws Exception {
        try {
            return fn.map(event);
        } catch (Exception e) {
            LOG.warn("Element transformation error: {}", e.getMessage());
            return event;
        }
    }

    private String getFieldFromParams(Map<String, Object> params, String defaultField) {
        if (params != null && params.containsKey("field")) {
            return (String) params.get("field");
        }
        return defaultField;
    }

    private String getSensorFromParams(Map<String, Object> params) {
        if (params != null && params.containsKey("sensor")) {
            return (String) params.get("sensor");
        }
        return null; // No sensor filter specified
    }

    private String getGroupingKeyFromEvent(SensorEvent event, String keyBy) {
        switch (keyBy) {
            case "sensor":
                return event.getSensor() != null ? event.getSensor() : "unknown";
            case "measurement_unit":
                return event.getMeasurementUnit() != null ? event.getMeasurementUnit() : "unknown";
            case "location":
                return event.getLocation() != null ? event.getLocation() : "unknown";
            case "data_quality":
                return event.getDataQuality() != null ? event.getDataQuality() : "unknown";
            case "measurement":
                return event.getMeasurement() != null ? String.valueOf(event.getMeasurement()) : "unknown";
            case "datetime":
                return event.getDatetime() != null ? event.getDatetime().toString() : "unknown";
            default:
                return "unknown";
        }
    }

    private Object getFieldValue(SensorEvent event, String field) {
        switch (field) {
            case "measurement":
                return event.getMeasurement();
            case "sensor":
                return event.getSensor();
            case "measurement_unit":
                return event.getMeasurementUnit();
            case "location":
                return event.getLocation();
            case "data_quality":
                return event.getDataQuality();
            case "datetime":
                return event.getDatetime();
            default:
                return null;
        }
    }

    private void createFinalResult(SensorEvent event, EtlConfig config, Collector<EtlResult> out) {
        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setTransformations(config.getTransformations());
        result.setGroupingField("sensor");
        result.setGroupingKey(event.getSensor());
        result.setResult(event);
        result.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());

        // Add sensor context information
        result.setSensorType(event.getSensor());
        result.setMeasurementUnit(event.getMeasurementUnit());
        result.setLocation(event.getLocation());

        out.collect(result);
    }

    private void emitErrorResult(SensorEvent event, EtlConfig config, Exception e, Collector<EtlResult> out) {
        EtlResult errorResult = new EtlResult();
        errorResult.setJobId(config.getJobId());
        errorResult.setTransformations(config.getTransformations());
        errorResult.setGroupingField("sensor");
        errorResult.setGroupingKey(event.getSensor());
        errorResult.setResult(null);
        errorResult.setProcessedAt(ZonedDateTime.now(GREEK_TIMEZONE).toInstant());

        // Add sensor context information for error tracking
        errorResult.setSensorType(event.getSensor());
        errorResult.setMeasurementUnit(event.getMeasurementUnit());
        errorResult.setLocation(event.getLocation());

        LOG.error("Processing error on subtask {}: {}", getRuntimeContext().getIndexOfThisSubtask(), e.getMessage());

        out.collect(errorResult);
    }
}