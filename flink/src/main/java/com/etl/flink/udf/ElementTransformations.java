package com.etl.flink.udf;

import com.etl.flink.model.SensorEvent;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

public class ElementTransformations {

    public static class NormalizeStringFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final List<String> fields;

        public NormalizeStringFunction(List<String> fields) {
            this.fields = fields;
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            SensorEvent result = new SensorEvent(event.getSensor(), event.getMeasurement(),
                    event.getMeasurementUnit(), event.getDatetime());

            if (fields.contains("sensor") && result.getSensor() != null) {
                result.setSensor(result.getSensor().trim().toLowerCase().replaceAll("\\s+", "_"));
            }
            if (fields.contains("measurement_unit") && result.getMeasurementUnit() != null) {
                result.setMeasurementUnit(result.getMeasurementUnit().trim().toLowerCase().replaceAll("\\s+", "_"));
            }

            return result;
        }
    }

    public static class LowercaseFunction implements MapFunction<SensorEvent, SensorEvent> {
        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            SensorEvent result = new SensorEvent(event.getSensor(), event.getMeasurement(),
                    event.getMeasurementUnit(), event.getDatetime());

            if (result.getSensor() != null) {
                result.setSensor(result.getSensor().toLowerCase());
            }
            if (result.getMeasurementUnit() != null) {
                result.setMeasurementUnit(result.getMeasurementUnit().toLowerCase());
            }

            return result;
        }
    }

    public static class UppercaseFunction implements MapFunction<SensorEvent, SensorEvent> {
        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            SensorEvent result = new SensorEvent(event.getSensor(), event.getMeasurement(),
                    event.getMeasurementUnit(), event.getDatetime());

            if (result.getSensor() != null) {
                result.setSensor(result.getSensor().toUpperCase());
            }
            if (result.getMeasurementUnit() != null) {
                result.setMeasurementUnit(result.getMeasurementUnit().toUpperCase());
            }

            return result;
        }
    }

    public static class FilterGreaterFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;
        private final double threshold;

        public FilterGreaterFunction(String field, double threshold) {
            this.field = field != null ? field : "measurement";
            this.threshold = threshold;
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            if ("measurement".equals(field) && event.getMeasurement() != null && event.getMeasurement() > threshold) {
                return event;
            }
            return null;
        }
    }

    public static class FilterLessFunction implements MapFunction<SensorEvent, SensorEvent> {
        private final String field;
        private final double threshold;

        public FilterLessFunction(String field, double threshold) {
            this.field = field != null ? field : "measurement";
            this.threshold = threshold;
        }

        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            if ("measurement".equals(field) && event.getMeasurement() != null && event.getMeasurement() < threshold) {
                return event;
            }
            return null;
        }
    }

    public static class ExtractYearFunction implements MapFunction<SensorEvent, SensorEvent> {
        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            if (event.getDatetime() != null) {
                LocalDateTime dateTime = LocalDateTime.ofInstant(event.getDatetime(), ZoneOffset.UTC);
                int year = dateTime.getYear();

                SensorEvent result = new SensorEvent(event.getSensor(), (double) year,
                        "year", event.getDatetime());
                return result;
            }
            return event;
        }
    }

    public static class ExtractMonthFunction implements MapFunction<SensorEvent, SensorEvent> {
        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            if (event.getDatetime() != null) {
                LocalDateTime dateTime = LocalDateTime.ofInstant(event.getDatetime(), ZoneOffset.UTC);
                int month = dateTime.getMonthValue();

                SensorEvent result = new SensorEvent(event.getSensor(), (double) month,
                        "month", event.getDatetime());
                return result;
            }
            return event;
        }
    }

    public static class ExtractDayFunction implements MapFunction<SensorEvent, SensorEvent> {
        @Override
        public SensorEvent map(SensorEvent event) throws Exception {
            if (event.getDatetime() != null) {
                LocalDateTime dateTime = LocalDateTime.ofInstant(event.getDatetime(), ZoneOffset.UTC);
                int day = dateTime.getDayOfMonth();

                SensorEvent result = new SensorEvent(event.getSensor(), (double) day,
                        "day", event.getDatetime());
                return result;
            }
            return event;
        }
    }

    public static MapFunction<SensorEvent, SensorEvent> createTransformation(String type, Map<String, Object> params) {
        switch (type) {
            case "normalize_string":
                @SuppressWarnings("unchecked")
                List<String> fields = (List<String>) params.get("fields");
                return new NormalizeStringFunction(fields);
            case "lowercase":
                return new LowercaseFunction();
            case "uppercase":
                return new UppercaseFunction();
            case "filter_greater":
                String field = (String) params.get("field");
                Object thresholdObj = params.get("threshold");
                double threshold = thresholdObj instanceof Number ?
                    ((Number) thresholdObj).doubleValue() :
                    Double.parseDouble(String.valueOf(thresholdObj));
                return new FilterGreaterFunction(field, threshold);
            case "filter_less":
                field = (String) params.get("field");
                thresholdObj = params.get("threshold");
                threshold = thresholdObj instanceof Number ?
                    ((Number) thresholdObj).doubleValue() :
                    Double.parseDouble(String.valueOf(thresholdObj));
                return new FilterLessFunction(field, threshold);
            case "extract_year":
                return new ExtractYearFunction();
            case "extract_month":
                return new ExtractMonthFunction();
            case "extract_day":
                return new ExtractDayFunction();
            default:
                throw new IllegalArgumentException("Unknown transformation type: " + type);
        }
    }
}