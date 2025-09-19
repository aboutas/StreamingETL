package com.etl.flink.udf;

import com.etl.flink.model.SensorEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Iterator;

public class WindowedAggregations {

    public static class SumReduceFunction implements ReduceFunction<SensorEvent> {
        private final String field;

        public SumReduceFunction(String field) {
            this.field = field != null ? field : "measurement";
        }

        @Override
        public SensorEvent reduce(SensorEvent event1, SensorEvent event2) throws Exception {
            if ("measurement".equals(field) && event1.getMeasurement() != null && event2.getMeasurement() != null) {
                return new SensorEvent(
                        event1.getSensor(),
                        event1.getMeasurement() + event2.getMeasurement(),
                        event1.getMeasurementUnit(),
                        event2.getDatetime()
                );
            }
            return event2;
        }
    }

    public static class MaxReduceFunction implements ReduceFunction<SensorEvent> {
        private final String field;

        public MaxReduceFunction(String field) {
            this.field = field != null ? field : "measurement";
        }

        @Override
        public SensorEvent reduce(SensorEvent event1, SensorEvent event2) throws Exception {
            if ("measurement".equals(field) && event1.getMeasurement() != null && event2.getMeasurement() != null) {
                if (event1.getMeasurement() >= event2.getMeasurement()) {
                    return event1;
                } else {
                    return event2;
                }
            }
            return event2;
        }
    }

    public static class MinReduceFunction implements ReduceFunction<SensorEvent> {
        private final String field;

        public MinReduceFunction(String field) {
            this.field = field != null ? field : "measurement";
        }

        @Override
        public SensorEvent reduce(SensorEvent event1, SensorEvent event2) throws Exception {
            if ("measurement".equals(field) && event1.getMeasurement() != null && event2.getMeasurement() != null) {
                if (event1.getMeasurement() <= event2.getMeasurement()) {
                    return event1;
                } else {
                    return event2;
                }
            }
            return event2;
        }
    }

    public static class WindowEventFunction implements WindowFunction<SensorEvent, WindowedSensorEvent, String, TimeWindow> {
        private final String aggregationType;
        private final String field;

        public WindowEventFunction(String aggregationType, String field) {
            this.aggregationType = aggregationType;
            this.field = field;
        }

        @Override
        public void apply(String key, TimeWindow window, Iterable<SensorEvent> input, Collector<WindowedSensorEvent> out) throws Exception {
            Iterator<SensorEvent> iterator = input.iterator();
            if (iterator.hasNext()) {
                SensorEvent event = iterator.next();
                WindowedSensorEvent result = new WindowedSensorEvent(
                        key,
                        event.getMeasurement(),
                        event.getMeasurementUnit(),
                        event.getDatetime(),
                        Instant.ofEpochMilli(window.getStart()),
                        Instant.ofEpochMilli(window.getEnd())
                );
                // Copy jobId if it exists
                if (event.getJobId() != null) {
                    result.setJobId(event.getJobId());
                }
                out.collect(result);
            }
        }
    }

    public static ReduceFunction<SensorEvent> createAggregation(String type, String field) {
        switch (type) {
            case "sum":
                return new SumReduceFunction(field);
            case "max":
                return new MaxReduceFunction(field);
            case "min":
                return new MinReduceFunction(field);
            default:
                throw new IllegalArgumentException("Unknown aggregation type: " + type);
        }
    }

    /**
     * Windowed sensor event that includes window boundaries
     */
    public static class WindowedSensorEvent extends SensorEvent {
        private final Instant windowStart;
        private final Instant windowEnd;

        public WindowedSensorEvent(String sensor, Double measurement, String measurementUnit,
                                 Instant datetime, Instant windowStart, Instant windowEnd) {
            super(sensor, measurement, measurementUnit, datetime);
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        public Instant getWindowStart() {
            return windowStart;
        }

        public Instant getWindowEnd() {
            return windowEnd;
        }
    }
}