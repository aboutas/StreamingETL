package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.model.Transformation;
import com.etl.flink.udf.ElementTransformations;
import com.etl.flink.udf.WindowedAggregations;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EtlCoProcessFunction extends KeyedCoProcessFunction<String, EtlConfig, SensorEvent, EtlResult> {
    private static final Logger LOG = LoggerFactory.getLogger(EtlCoProcessFunction.class);

    private transient ValueState<EtlConfig> configState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("config", EtlConfig.class)
        );
    }

    @Override
    public void processElement1(EtlConfig config, Context context, Collector<EtlResult> out) throws Exception {
        LOG.info("Received config for job: {}", config.getJobId());
        configState.update(config);
    }

    @Override
    public void processElement2(SensorEvent event, Context context, Collector<EtlResult> out) throws Exception {
        EtlConfig config = configState.value();
        if (config == null) {
            LOG.debug("No config available for key: {}, dropping event", context.getCurrentKey());
            return;
        }

        try {
            processEventWithConfig(event, config, out, context.getCurrentKey());
        } catch (Exception e) {
            LOG.error("Error processing event with config", e);

            EtlResult errorResult = new EtlResult();
            errorResult.setJobId(config.getJobId());
            errorResult.setSource(config.getSource());
            errorResult.setTransformations(config.getTransformations());
            errorResult.setGroupingField("sensor");
            errorResult.setGroupingKey(context.getCurrentKey());
            errorResult.setResult(null);

            List<String> diagnostics = new ArrayList<>();
            diagnostics.add("Processing error: " + e.getMessage());
            errorResult.setDiagnostics(diagnostics);

            out.collect(errorResult);
        }
    }

    private void processEventWithConfig(SensorEvent event, EtlConfig config, Collector<EtlResult> out, String currentKey) throws Exception {
        List<String> diagnostics = new ArrayList<>();
        SensorEvent currentEvent = event;

        if (config.getTransformations() == null || config.getTransformations().isEmpty()) {
            createSimpleResult(currentEvent, config, currentKey, diagnostics, out);
            return;
        }

        for (Transformation transformation : config.getTransformations()) {
            currentEvent = applyTransformation(currentEvent, transformation, config, currentKey, diagnostics, out);
            if (currentEvent == null) {
                break;
            }
        }
    }

    private SensorEvent applyTransformation(SensorEvent event, Transformation transformation,
                                         EtlConfig config, String currentKey,
                                         List<String> diagnostics, Collector<EtlResult> out) throws Exception {

        String transformationType = transformation.getType();

        if (isElementTransformation(transformationType)) {
            return applyElementTransformation(event, transformation, diagnostics);
        } else if (isAggregationTransformation(transformationType)) {
            applyAggregationTransformation(event, transformation, config, currentKey, diagnostics, out);
            return null;
        } else {
            diagnostics.add("Unknown transformation type: " + transformationType);
            return event;
        }
    }

    private boolean isElementTransformation(String type) {
        return "normalize_string".equals(type) || "lowercase".equals(type) || "uppercase".equals(type) ||
               "filter_greater".equals(type) || "filter_less".equals(type) ||
               "extract_year".equals(type) || "extract_month".equals(type) || "extract_day".equals(type);
    }

    private boolean isAggregationTransformation(String type) {
        return "sum".equals(type) || "max".equals(type) || "min".equals(type);
    }

    private SensorEvent applyElementTransformation(SensorEvent event, Transformation transformation,
                                                 List<String> diagnostics) throws Exception {
        try {
            var mapFunction = ElementTransformations.createTransformation(transformation.getType(), transformation.getParams());
            SensorEvent result = mapFunction.map(event);

            if (result == null && ("filter_greater".equals(transformation.getType()) || "filter_less".equals(transformation.getType()))) {
                diagnostics.add("Event filtered out by " + transformation.getType());
            }

            return result;
        } catch (Exception e) {
            diagnostics.add("Element transformation error: " + e.getMessage());
            return event;
        }
    }

    private void applyAggregationTransformation(SensorEvent event, Transformation transformation,
                                              EtlConfig config, String currentKey,
                                              List<String> diagnostics, Collector<EtlResult> out) {

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

        if (!diagnostics.isEmpty()) {
            result.setDiagnostics(new ArrayList<>(diagnostics));
        }

        out.collect(result);
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
            default:
                return null;
        }
    }

    private void createSimpleResult(SensorEvent event, EtlConfig config, String currentKey,
                                  List<String> diagnostics, Collector<EtlResult> out) {
        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setSource(config.getSource());
        result.setTransformations(config.getTransformations());
        result.setGroupingField("sensor");
        result.setGroupingKey(currentKey);
        result.setResult(event);

        if (!diagnostics.isEmpty()) {
            result.setDiagnostics(diagnostics);
        }

        out.collect(result);
    }
}