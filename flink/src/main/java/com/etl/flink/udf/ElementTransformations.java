package com.etl.flink.udf;

import com.etl.flink.model.SensorEvent;
import org.apache.flink.api.common.functions.MapFunction;

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
     * Normalize transformation - Min-Max scaling to [0, 1] range
     * Use case: ML feature preparation, data normalization
     */
    public static class NormalizeFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;
        private final double min;
        private final double max;

        public NormalizeFunction(String field, double min, double max) {
            this.field = field != null ? field : "measurement";
            this.min = min;
            this.max = max;
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            if ("measurement".equals(field)) {
                Double value = event.getMeasurement();
                if (value != null) {
                    // Normalize: (value - min) / (max - min)
                    double normalized = (value - min) / (max - min);
                    // Clamp to [0, 1] in case value is outside range
                    normalized = Math.max(0.0, Math.min(1.0, normalized));
                    event.setMeasurement(normalized);
                }
            }
            return event;
        }
    }

    /**
     * To lowercase transformation - Normalize text fields to lowercase
     * Use case: Standardize location names, sensor types for consistency
     */
    public static class ToLowercaseFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;

        public ToLowercaseFunction(String field) {
            this.field = field != null ? field : "location";
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            switch (field) {
                case "location":
                    if (event.getLocation() != null) {
                        event.setLocation(event.getLocation().toLowerCase());
                    }
                    break;
                case "sensor":
                    if (event.getSensor() != null) {
                        event.setSensor(event.getSensor().toLowerCase());
                    }
                    break;
                case "measurement_unit":
                    if (event.getMeasurementUnit() != null) {
                        event.setMeasurementUnit(event.getMeasurementUnit().toLowerCase());
                    }
                    break;
                case "data_quality":
                    if (event.getDataQuality() != null) {
                        event.setDataQuality(event.getDataQuality().toLowerCase());
                    }
                    break;
            }
            return event;
        }
    }

    /**
     * To uppercase transformation - Normalize text fields to uppercase
     * Use case: Standardize location names, sensor types for consistency
     */
    public static class ToUppercaseFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;

        public ToUppercaseFunction(String field) {
            this.field = field != null ? field : "location";
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            switch (field) {
                case "location":
                    if (event.getLocation() != null) {
                        event.setLocation(event.getLocation().toUpperCase());
                    }
                    break;
                case "sensor":
                    if (event.getSensor() != null) {
                        event.setSensor(event.getSensor().toUpperCase());
                    }
                    break;
                case "measurement_unit":
                    if (event.getMeasurementUnit() != null) {
                        event.setMeasurementUnit(event.getMeasurementUnit().toUpperCase());
                    }
                    break;
                case "data_quality":
                    if (event.getDataQuality() != null) {
                        event.setDataQuality(event.getDataQuality().toUpperCase());
                    }
                    break;
            }
            return event;
        }
    }

    /**
     * Trim whitespace transformation - Remove leading/trailing spaces
     * Use case: Clean string fields from data entry errors
     */
    public static class TrimWhitespaceFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;

        public TrimWhitespaceFunction(String field) {
            this.field = field != null ? field : "location";
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            switch (field) {
                case "location":
                    if (event.getLocation() != null) {
                        event.setLocation(event.getLocation().trim());
                    }
                    break;
                case "sensor":
                    if (event.getSensor() != null) {
                        event.setSensor(event.getSensor().trim());
                    }
                    break;
                case "measurement_unit":
                    if (event.getMeasurementUnit() != null) {
                        event.setMeasurementUnit(event.getMeasurementUnit().trim());
                    }
                    break;
                case "data_quality":
                    if (event.getDataQuality() != null) {
                        event.setDataQuality(event.getDataQuality().trim());
                    }
                    break;
            }
            return event;
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
            case "normalize":
                field = (String) params.get("field");
                Object minObj = params.get("min");
                Object maxObj = params.get("max");
                double min = minObj instanceof Number ?
                    ((Number) minObj).doubleValue() :
                    Double.parseDouble(String.valueOf(minObj));
                double max = maxObj instanceof Number ?
                    ((Number) maxObj).doubleValue() :
                    Double.parseDouble(String.valueOf(maxObj));
                return new NormalizeFunction(field, min, max);
            case "to_lowercase":
                field = (String) params.get("field");
                return new ToLowercaseFunction(field);
            case "to_uppercase":
                field = (String) params.get("field");
                return new ToUppercaseFunction(field);
            case "trim_whitespace":
                field = (String) params.get("field");
                return new TrimWhitespaceFunction(field);
            default:
                throw new IllegalArgumentException("Unknown transformation type: " + type);
        }
    }
}