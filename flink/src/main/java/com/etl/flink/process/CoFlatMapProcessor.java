package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.model.Transformation;
import com.etl.flink.udf.ElementTransformations;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TRUE CoFlatMap implementation with true parallelism.
 * Both config and data streams keyed by same field for optimal distribution.
 */
public class CoFlatMapProcessor extends RichCoFlatMapFunction<EtlConfig, SensorEvent, EtlResult> {
    private static final Logger LOG = LoggerFactory.getLogger(CoFlatMapProcessor.class);

    private transient MapState<String, EtlConfig> configState;

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

        // Emit configuration status result
        EtlResult configResult = new EtlResult();
        configResult.setJobId(config.getJobId());
        configResult.setSource(config.getSource());
        configResult.setTransformations(config.getTransformations());
        configResult.setResult("Configuration registered successfully on subtask " +
                              getRuntimeContext().getIndexOfThisSubtask());
        configResult.setProcessedAt(Instant.now());

        List<String> diagnostics = new ArrayList<>();
        diagnostics.add("Config registered for jobId: " + config.getJobId());
        diagnostics.add("Processed on subtask: " + getRuntimeContext().getIndexOfThisSubtask());
        diagnostics.add("Key group: " + getRuntimeContext().getIndexOfThisSubtask());
        configResult.setDiagnostics(diagnostics);

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
        List<String> diagnostics = new ArrayList<>();
        diagnostics.add("Processed on subtask: " + getRuntimeContext().getIndexOfThisSubtask());
        diagnostics.add("Key group processing: " + getRuntimeContext().getIndexOfThisSubtask());

        SensorEvent currentEvent = event;

        if (config.getTransformations() == null || config.getTransformations().isEmpty()) {
            createSimpleResult(currentEvent, config, diagnostics, out);
            return;
        }

        // Process element transformations first
        for (Transformation transformation : config.getTransformations()) {
            if (isElementTransformation(transformation.getType())) {
                currentEvent = applyElementTransformation(currentEvent, transformation, diagnostics);
                if (currentEvent == null) {
                    return; // Event was filtered out
                }
            }
        }

        // Process aggregation transformations
        for (Transformation transformation : config.getTransformations()) {
            if (isAggregationTransformation(transformation.getType())) {
                processWindowedAggregation(currentEvent, transformation, config, diagnostics, out);
            }
        }

        // If no aggregations, emit final result
        boolean hasAggregations = config.getTransformations().stream()
                .anyMatch(t -> isAggregationTransformation(t.getType()));

        if (!hasAggregations) {
            createFinalResult(currentEvent, config, diagnostics, out);
        }
    }

    private void processWindowedAggregation(SensorEvent event, Transformation transformation,
                                          EtlConfig config, List<String> diagnostics,
                                          Collector<EtlResult> out) {

        String field = getFieldFromParams(transformation.getParams(), "measurement");
        String keyBy = transformation.getKeyBy() != null ? transformation.getKeyBy() : "sensor";
        String windowStr = transformation.getWindow() != null ? transformation.getWindow() : "1s";

        // Calculate window boundaries
        long windowSizeMs = parseWindowDurationToMs(windowStr);
        long eventTimeMs = event.getDatetime().toEpochMilli();
        long windowStart = (eventTimeMs / windowSizeMs) * windowSizeMs;
        long windowEnd = windowStart + windowSizeMs;

        String groupingKey = getGroupingKeyFromEvent(event, keyBy);

        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setSource(config.getSource());
        result.setTransformations(config.getTransformations());
        result.setGroupingField(keyBy);
        result.setGroupingKey(groupingKey);
        result.setAggregationType(transformation.getType());
        result.setField(field);
        result.setResult(getFieldValue(event, field));
        result.setProcessedAt(Instant.now());

        // Add sensor context information
        result.setSensorType(event.getSensor());
        result.setMeasurementUnit(event.getMeasurementUnit());
        result.setLocation(event.getLocation());

        // Set window boundaries
        result.setWindowStart(Instant.ofEpochMilli(windowStart));
        result.setWindowEnd(Instant.ofEpochMilli(windowEnd));

        diagnostics.add("Applied " + transformation.getType() + " aggregation with " + windowStr + " window");
        diagnostics.add("Window: " + result.getWindowStart() + " to " + result.getWindowEnd());
        diagnostics.add("Contributing sensor: " + event.getSensor() + " (" + event.getMeasurementUnit() + ")");
        diagnostics.add("From location: " + event.getLocation());
        result.setDiagnostics(new ArrayList<>(diagnostics));

        out.collect(result);
    }

    private boolean isElementTransformation(String type) {
        return "filter_greater".equals(type) || "filter_less".equals(type);
    }

    private boolean isAggregationTransformation(String type) {
        return "sum".equals(type) || "max".equals(type) || "min".equals(type) || "avg".equals(type);
    }

    private SensorEvent applyElementTransformation(SensorEvent event, Transformation transformation,
                                                 List<String> diagnostics) throws Exception {
        try {
            var mapFunction = ElementTransformations.createTransformation(transformation.getType(), transformation.getParams());
            SensorEvent result = mapFunction.map(event);

            if (result == null && ("filter_greater".equals(transformation.getType()) || "filter_less".equals(transformation.getType()))) {
                diagnostics.add("Event filtered out by " + transformation.getType());
            } else if (result != null) {
                result.setJobId(event.getJobId());
                diagnostics.add("Applied " + transformation.getType() + " transformation");
            }

            return result;
        } catch (Exception e) {
            diagnostics.add("Element transformation error: " + e.getMessage());
            return event;
        }
    }

    private String getFieldFromParams(Map<String, Object> params, String defaultField) {
        if (params != null && params.containsKey("field")) {
            return (String) params.get("field");
        }
        return defaultField;
    }

    private String getGroupingKeyFromEvent(SensorEvent event, String keyBy) {
        switch (keyBy) {
            case "sensor":
                return event.getSensor() != null ? event.getSensor() : "unknown";
            case "measurement_unit":
                return event.getMeasurementUnit() != null ? event.getMeasurementUnit() : "unknown";
            case "jobId":
                return event.getJobId() != null ? event.getJobId() : "unknown";
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
            case "jobId":
                return event.getJobId();
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

    private long parseWindowDurationToMs(String windowStr) {
        if (windowStr == null || windowStr.isEmpty()) {
            return 1000; // Default to 1 second
        }

        String numStr = windowStr.substring(0, windowStr.length() - 1);
        char unit = windowStr.charAt(windowStr.length() - 1);

        try {
            int duration = Integer.parseInt(numStr);
            switch (unit) {
                case 's': return duration * 1000L;
                case 'm': return duration * 60 * 1000L;
                case 'h': return duration * 60 * 60 * 1000L;
                default: return 1000L;
            }
        } catch (NumberFormatException e) {
            return 1000L; // Default fallback
        }
    }

    private void createSimpleResult(SensorEvent event, EtlConfig config,
                                  List<String> diagnostics, Collector<EtlResult> out) {
        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setSource(config.getSource());
        result.setTransformations(config.getTransformations());
        result.setGroupingField("sensor");
        result.setGroupingKey(event.getSensor());
        result.setResult(event);
        result.setProcessedAt(Instant.now());

        // Add sensor context information
        result.setSensorType(event.getSensor());
        result.setMeasurementUnit(event.getMeasurementUnit());
        result.setLocation(event.getLocation());

        diagnostics.add("No transformations applied, returning original event");
        result.setDiagnostics(diagnostics);

        out.collect(result);
    }

    private void createFinalResult(SensorEvent event, EtlConfig config,
                                 List<String> diagnostics, Collector<EtlResult> out) {
        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setSource(config.getSource());
        result.setTransformations(config.getTransformations());
        result.setGroupingField("sensor");
        result.setGroupingKey(event.getSensor());
        result.setResult(event);
        result.setProcessedAt(Instant.now());

        // Add sensor context information
        result.setSensorType(event.getSensor());
        result.setMeasurementUnit(event.getMeasurementUnit());
        result.setLocation(event.getLocation());

        if (!diagnostics.isEmpty()) {
            result.setDiagnostics(new ArrayList<>(diagnostics));
        }

        out.collect(result);
    }

    private void emitErrorResult(SensorEvent event, EtlConfig config, Exception e, Collector<EtlResult> out) {
        EtlResult errorResult = new EtlResult();
        errorResult.setJobId(config.getJobId());
        errorResult.setSource(config.getSource());
        errorResult.setTransformations(config.getTransformations());
        errorResult.setGroupingField("sensor");
        errorResult.setGroupingKey(event.getSensor());
        errorResult.setResult(null);
        errorResult.setProcessedAt(Instant.now());

        // Add sensor context information for error tracking
        errorResult.setSensorType(event.getSensor());
        errorResult.setMeasurementUnit(event.getMeasurementUnit());
        errorResult.setLocation(event.getLocation());

        List<String> diagnostics = new ArrayList<>();
        diagnostics.add("Processing error on subtask " + getRuntimeContext().getIndexOfThisSubtask() + ": " + e.getMessage());
        errorResult.setDiagnostics(diagnostics);

        out.collect(errorResult);
    }
}