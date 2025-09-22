package com.etl.flink.process;

import com.etl.flink.model.EtlConfig;
import com.etl.flink.model.EtlResult;
import com.etl.flink.model.SensorEvent;
import com.etl.flink.model.Transformation;
import com.etl.flink.udf.WindowedAggregations.WindowedSensorEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class WindowedResultMapper implements MapFunction<WindowedSensorEvent, EtlResult> {

    private final EtlConfig config;
    private final Transformation transformation;
    private final String keyBy;
    private final String field;

    public WindowedResultMapper(EtlConfig config, Transformation transformation, String keyBy, String field) {
        this.config = config;
        this.transformation = transformation;
        this.keyBy = keyBy;
        this.field = field;
    }

    @Override
    public EtlResult map(WindowedSensorEvent event) throws Exception {
        EtlResult result = new EtlResult();
        result.setJobId(config.getJobId());
        result.setSource(config.getSource());
        result.setTransformations(config.getTransformations());
        result.setGroupingField(keyBy);
        result.setGroupingKey(getGroupingKeyFromEvent(event, keyBy));
        result.setAggregationType(transformation.getType());
        result.setField(field);
        result.setResult(getFieldValue(event, field));
        result.setProcessedAt(Instant.now());

        // Set window information from WindowedSensorEvent
        result.setWindowStart(event.getWindowStart());
        result.setWindowEnd(event.getWindowEnd());

        List<String> diagnostics = new ArrayList<>();
        String windowStr = transformation.getWindow() != null ? transformation.getWindow() : "1s";
        diagnostics.add("Applied " + transformation.getType() + " aggregation with " + windowStr + " window");
        diagnostics.add("Window: " + event.getWindowStart() + " to " + event.getWindowEnd());
        result.setDiagnostics(diagnostics);

        return result;
    }

    private String getGroupingKeyFromEvent(WindowedSensorEvent event, String keyBy) {
        switch (keyBy) {
            case "sensor":
                return event.getSensor() != null ? event.getSensor() : "unknown";
            case "measurement_unit":
                return event.getMeasurementUnit() != null ? event.getMeasurementUnit() : "unknown";
            case "jobId":
                return event.getJobId() != null ? event.getJobId() : "unknown";
            case "location":
                return event.getLocation() != null ? event.getLocation() : "unknown";
            default:
                return "unknown";
        }
    }

    private Object getFieldValue(WindowedSensorEvent event, String field) {
        switch (field) {
            case "measurement":
                return event.getMeasurement();
            case "sensor":
                return event.getSensor();
            case "measurement_unit":
                return event.getMeasurementUnit();
            case "jobId":
                return event.getJobId();
            default:
                return null;
        }
    }

}