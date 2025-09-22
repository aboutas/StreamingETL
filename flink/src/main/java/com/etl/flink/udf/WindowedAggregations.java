package com.etl.flink.udf;

import com.etl.flink.model.SensorEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;

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

    public static class AvgReduceFunction implements ReduceFunction<SensorEvent> {
        private final String field;

        public AvgReduceFunction(String field) {
            this.field = field != null ? field : "measurement";
        }

        @Override
        public SensorEvent reduce(SensorEvent event1, SensorEvent event2) throws Exception {
            if ("measurement".equals(field) && event1.getMeasurement() != null && event2.getMeasurement() != null) {
                // For average, we'll need to track count and sum
                // This is a simplified average that just takes the mean of two values
                Double avgValue = (event1.getMeasurement() + event2.getMeasurement()) / 2.0;
                return new SensorEvent(
                        event1.getJobId(),
                        event1.getSensor(),
                        avgValue,
                        event1.getMeasurementUnit(),
                        event2.getDatetime(),
                        event1.getLocation()
                );
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
                        event.getSensor(),
                        event.getMeasurement(),
                        event.getMeasurementUnit(),
                        event.getDatetime(),
                        Instant.ofEpochMilli(window.getStart()),
                        Instant.ofEpochMilli(window.getEnd()),
                        event.getLocation()
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
            case "avg":
                return new AvgReduceFunction(field);
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
            super(null, sensor, measurement, measurementUnit, datetime, null);
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        public WindowedSensorEvent(String sensor, Double measurement, String measurementUnit,
                                 Instant datetime, Instant windowStart, Instant windowEnd, String location) {
            super(null, sensor, measurement, measurementUnit, datetime, location);
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

    /**
     * Enhanced sensor event that tracks contributing sensors in aggregations
     */
    public static class AggregatedSensorEvent extends SensorEvent {
        private final Set<String> contributingSensors;
        private final Set<String> contributingUnits;
        private final int eventCount;

        public AggregatedSensorEvent(String sensor, Double measurement, String measurementUnit,
                                   Instant datetime, String location, Set<String> contributingSensors,
                                   Set<String> contributingUnits, int eventCount) {
            super(null, sensor, measurement, measurementUnit, datetime, location);
            this.contributingSensors = new HashSet<>(contributingSensors);
            this.contributingUnits = new HashSet<>(contributingUnits);
            this.eventCount = eventCount;
        }

        public Set<String> getContributingSensors() {
            return contributingSensors;
        }

        public Set<String> getContributingUnits() {
            return contributingUnits;
        }

        public int getEventCount() {
            return eventCount;
        }
    }
}