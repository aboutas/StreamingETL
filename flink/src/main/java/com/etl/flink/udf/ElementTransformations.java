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




    public static MapFunction<SensorEvent, SensorEvent> createTransformation(String type, Map<String, Object> params) {
        switch (type) {
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
            default:
                throw new IllegalArgumentException("Unknown transformation type: " + type);
        }
    }
}