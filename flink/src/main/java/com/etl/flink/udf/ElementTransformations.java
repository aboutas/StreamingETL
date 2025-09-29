package com.etl.flink.udf;

import com.etl.flink.model.SensorEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class ElementTransformations {

    public static class FilterGreaterFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;
        private final String sensor;
        private final double threshold;

        public FilterGreaterFunction(String field, double threshold, String sensor) {
            this.field = field != null ? field : "measurement";
            this.threshold = threshold;
            this.sensor = sensor;
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            // First check sensor type if specified
            if (sensor != null && !sensor.equals(event.getSensor())) {
                return null;
            }

            // Get field value and check threshold
            Double fieldValue = getFieldValueAsDouble(event, field);
            if (fieldValue != null && fieldValue > threshold) {
                return event;
            }
            return null;
        }

        private Double getFieldValueAsDouble(SensorEvent event, String field) {
            switch (field) {
                case "measurement":
                    return event.getMeasurement();
                case "datetime":
                    return event.getDatetime() != null ? (double) event.getDatetime().toEpochMilli() : null;
                default:
                    return null;
            }
        }
    }

    public static class FilterLessFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;
        private final String sensor;
        private final double threshold;

        public FilterLessFunction(String field, double threshold, String sensor) {
            this.field = field != null ? field : "measurement";
            this.threshold = threshold;
            this.sensor = sensor;
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            // First check sensor type if specified
            if (sensor != null && !sensor.equals(event.getSensor())) {
                return null;
            }

            // Get field value and check threshold
            Double fieldValue = getFieldValueAsDouble(event, field);
            if (fieldValue != null && fieldValue < threshold) {
                return event;
            }
            return null;
        }

        private Double getFieldValueAsDouble(SensorEvent event, String field) {
            switch (field) {
                case "measurement":
                    return event.getMeasurement();
                case "datetime":
                    return event.getDatetime() != null ? (double) event.getDatetime().toEpochMilli() : null;
                default:
                    return null;
            }
        }
    }




    /**
     * Cross-field filtering function that allows filtering based on one sensor type's measurement
     * while processing events from another sensor type.
     * Example: "max humidity with temp > 59" - get max humidity only when temperature > 59
     */
    public static class CrossFieldFilterFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String filterSensor;      // Sensor to filter by (e.g., "temperature")
        private final String filterField;       // Field to filter by (e.g., "measurement")
        private final String filterOperator;    // ">" or "<"
        private final double filterThreshold;   // Threshold value
        private final String targetSensor;      // Target sensor to process (e.g., "humidity")

        public CrossFieldFilterFunction(String filterSensor, String filterField, String filterOperator,
                                       double filterThreshold, String targetSensor) {
            this.filterSensor = filterSensor;
            this.filterField = filterField != null ? filterField : "measurement";
            this.filterOperator = filterOperator != null ? filterOperator : ">";
            this.filterThreshold = filterThreshold;
            this.targetSensor = targetSensor;
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            // If this is the target sensor we want to process, always pass it through
            if (targetSensor != null && targetSensor.equals(event.getSensor())) {
                return event;
            }

            // If this is the filter sensor, check if it meets the filter condition
            if (filterSensor != null && filterSensor.equals(event.getSensor())) {
                Double fieldValue = getFieldValueAsDouble(event, filterField);
                if (fieldValue != null) {
                    boolean conditionMet = false;
                    switch (filterOperator) {
                        case ">":
                            conditionMet = fieldValue > filterThreshold;
                            break;
                        case "<":
                            conditionMet = fieldValue < filterThreshold;
                            break;
                        case ">=":
                            conditionMet = fieldValue >= filterThreshold;
                            break;
                        case "<=":
                            conditionMet = fieldValue <= filterThreshold;
                            break;
                        case "=":
                        case "==":
                            conditionMet = Math.abs(fieldValue - filterThreshold) < 0.001;
                            break;
                    }

                    if (conditionMet) {
                        return event; // Pass through filter sensor events that meet condition
                    }
                }
            }

            return null; // Filter out events that don't match conditions
        }

        private Double getFieldValueAsDouble(SensorEvent event, String field) {
            switch (field) {
                case "measurement":
                    return event.getMeasurement();
                case "datetime":
                    return event.getDatetime() != null ? (double) event.getDatetime().toEpochMilli() : null;
                default:
                    return null;
            }
        }
    }

    public static MapFunction<SensorEvent, SensorEvent> createTransformation(String type, Map<String, Object> params) {
        switch (type) {
            case "filter_greater":
                String field = (String) params.get("field");
                String sensor = (String) params.get("sensor");
                Object thresholdObj = params.get("threshold");
                double threshold = thresholdObj instanceof Number ?
                    ((Number) thresholdObj).doubleValue() :
                    Double.parseDouble(String.valueOf(thresholdObj));
                return new FilterGreaterFunction(field, threshold, sensor);
            case "filter_less":
                field = (String) params.get("field");
                sensor = (String) params.get("sensor");
                thresholdObj = params.get("threshold");
                threshold = thresholdObj instanceof Number ?
                    ((Number) thresholdObj).doubleValue() :
                    Double.parseDouble(String.valueOf(thresholdObj));
                return new FilterLessFunction(field, threshold, sensor);
            case "filter_cross_field":
                String filterSensor = (String) params.get("filter_sensor");
                String filterField = (String) params.get("filter_field");
                String filterOperator = (String) params.get("filter_operator");
                Object filterThresholdObj = params.get("filter_threshold");
                double filterThreshold = filterThresholdObj instanceof Number ?
                    ((Number) filterThresholdObj).doubleValue() :
                    Double.parseDouble(String.valueOf(filterThresholdObj));
                String targetSensor = (String) params.get("target_sensor");
                return new CrossFieldFilterFunction(filterSensor, filterField, filterOperator, filterThreshold, targetSensor);
            default:
                throw new IllegalArgumentException("Unknown transformation type: " + type);
        }
    }
}