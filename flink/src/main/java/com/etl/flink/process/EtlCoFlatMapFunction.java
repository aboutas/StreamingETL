package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.model.Transformation;
import com.etl.flink.udf.ElementTransformations;
import com.etl.flink.udf.WindowedAggregations;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EtlCoFlatMapFunction extends RichCoFlatMapFunction<EtlConfig, SensorEvent, EtlResult> {
    private static final Logger LOG = LoggerFactory.getLogger(EtlCoFlatMapFunction.class);

    private transient MapState<String, EtlConfig> configState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("configs", String.class, EtlConfig.class)
        );
    }

    @Override
    public void flatMap1(EtlConfig config, Collector<EtlResult> out) throws Exception {
        LOG.info("Received config for job: {}", config.getJobId());
        configState.put(config.getJobId(), config);

        // Optionally emit a configuration status result
        EtlResult configResult = new EtlResult();
        configResult.setJobId(config.getJobId());
        configResult.setSource(config.getSource());
        configResult.setTransformations(config.getTransformations());
        configResult.setResult("Configuration registered successfully");
        configResult.setProcessedAt(Instant.now());

        List<String> diagnostics = new ArrayList<>();
        diagnostics.add("Config registered for jobId: " + config.getJobId());
        configResult.setDiagnostics(diagnostics);

        out.collect(configResult);
    }

    @Override
    public void flatMap2(SensorEvent event, Collector<EtlResult> out) throws Exception {
        // Process this universal data event against ALL stored configs
        Iterable<Map.Entry<String, EtlConfig>> configs = configState.entries();

        boolean hasConfigs = false;
        for (Map.Entry<String, EtlConfig> configEntry : configs) {
            hasConfigs = true;
            EtlConfig config = configEntry.getValue();

            try {
                LOG.debug("Processing event with sensor: {} against config: {}", event.getSensor(), config.getJobId());
                processEventWithConfig(event, config, out);
            } catch (Exception e) {
                LOG.error("Error processing event with config for jobId: {}", config.getJobId(), e);

                EtlResult errorResult = new EtlResult();
                errorResult.setJobId(config.getJobId());
                errorResult.setSource(config.getSource());
                errorResult.setTransformations(config.getTransformations());
                errorResult.setGroupingField("sensor");
                errorResult.setGroupingKey(event.getSensor());
                errorResult.setResult(null);
                errorResult.setProcessedAt(Instant.now());

                List<String> diagnostics = new ArrayList<>();
                diagnostics.add("Processing error: " + e.getMessage());
                errorResult.setDiagnostics(diagnostics);

                out.collect(errorResult);
            }
        }

        if (!hasConfigs) {
            LOG.debug("No configs available, dropping event with sensor: {}", event.getSensor());
        }
    }

    private void processEventWithConfig(SensorEvent event, EtlConfig config, Collector<EtlResult> out) throws Exception {
        List<String> diagnostics = new ArrayList<>();
        SensorEvent currentEvent = event;

        if (config.getTransformations() == null || config.getTransformations().isEmpty()) {
            createSimpleResult(currentEvent, config, diagnostics, out);
            return;
        }

        for (Transformation transformation : config.getTransformations()) {
            currentEvent = applyTransformation(currentEvent, transformation, config, diagnostics, out);
            if (currentEvent == null) {
                break; // Event was filtered out or processed by aggregation
            }
        }

        // If we still have an event after all transformations, emit final result
        if (currentEvent != null) {
            createFinalResult(currentEvent, config, diagnostics, out);
        }
    }

    private SensorEvent applyTransformation(SensorEvent event, Transformation transformation,
                                         EtlConfig config, List<String> diagnostics,
                                         Collector<EtlResult> out) throws Exception {

        String transformationType = transformation.getType();

        if (isElementTransformation(transformationType)) {
            return applyElementTransformation(event, transformation, diagnostics);
        } else if (isAggregationTransformation(transformationType)) {
            // For aggregations, emit result immediately and return null to stop pipeline
            applyAggregationTransformation(event, transformation, config, diagnostics, out);
            return null;
        } else {
            diagnostics.add("Unknown transformation type: " + transformationType);
            return event;
        }
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
                // Preserve jobId in transformed event
                result.setJobId(event.getJobId());
                diagnostics.add("Applied " + transformation.getType() + " transformation");
            }

            return result;
        } catch (Exception e) {
            diagnostics.add("Element transformation error: " + e.getMessage());
            return event;
        }
    }

    private void applyAggregationTransformation(SensorEvent event, Transformation transformation,
                                              EtlConfig config, List<String> diagnostics,
                                              Collector<EtlResult> out) {

        String field = getFieldFromParams(transformation.getParams(), "measurement");
        String keyBy = transformation.getKeyBy() != null ? transformation.getKeyBy() : "sensor";

        if (keyBy == null || keyBy.isEmpty()) {
            keyBy = "sensor";
            diagnostics.add("Missing keyBy field, defaulting to 'sensor'");
        }

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

        // Note: This function processes single events.
        // For proper windowing, use createWindowedAggregationStream() method instead
        if (transformation.getWindow() != null) {
            diagnostics.add("Note: Single-event processing. Use createWindowedAggregationStream() for proper windowing");
        } else {
            diagnostics.add("Applied " + transformation.getType() + " aggregation (single event)");
        }

        if (!diagnostics.isEmpty()) {
            result.setDiagnostics(new ArrayList<>(diagnostics));
        }

        out.collect(result);
    }

    /**
     * Creates a windowed aggregation stream for a given transformation.
     * This method should be called from the main job to set up proper windowing.
     */
    public static DataStream<EtlResult> createWindowedAggregationStream(
            DataStream<SensorEvent> eventStream,
            Transformation transformation,
            EtlConfig config) {

        String field = getFieldFromParamsStatic(transformation.getParams(), "measurement");
        String keyBy = transformation.getKeyBy() != null ? transformation.getKeyBy() : "sensor";
        String windowStr = transformation.getWindow() != null ? transformation.getWindow() : "1s";

        // Parse window duration
        Time windowSize = parseWindowDuration(windowStr);

        return eventStream
            .filter(event -> event != null)
            .keyBy(event -> getGroupingKeyFromEventStatic(event, keyBy))
            .window(TumblingEventTimeWindows.of(windowSize))
            .reduce(
                WindowedAggregations.createAggregation(transformation.getType(), field),
                new WindowedAggregations.WindowEventFunction(transformation.getType(), field)
            )
            .map(new WindowedResultMapper(config, transformation, keyBy, field));
    }

    private static String getFieldFromParamsStatic(Map<String, Object> params, String defaultField) {
        if (params != null && params.containsKey("field")) {
            return (String) params.get("field");
        }
        return defaultField;
    }

    private static String getGroupingKeyFromEventStatic(SensorEvent event, String keyBy) {
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

    private static Time parseWindowDuration(String windowStr) {
        if (windowStr == null || windowStr.isEmpty()) {
            return Time.seconds(1); // Default to 1 second
        }

        String numStr = windowStr.substring(0, windowStr.length() - 1);
        char unit = windowStr.charAt(windowStr.length() - 1);

        try {
            int duration = Integer.parseInt(numStr);
            switch (unit) {
                case 's': return Time.seconds(duration);
                case 'm': return Time.minutes(duration);
                case 'h': return Time.hours(duration);
                default: return Time.seconds(1);
            }
        } catch (NumberFormatException e) {
            return Time.seconds(1); // Default fallback
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

        diagnostics.add("No transformations applied, returning original event");

        if (!diagnostics.isEmpty()) {
            result.setDiagnostics(diagnostics);
        }

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

        if (!diagnostics.isEmpty()) {
            result.setDiagnostics(new ArrayList<>(diagnostics));
        }

        out.collect(result);
    }
}