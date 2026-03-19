package com.etl.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

public class SensorEvent {
    @JsonProperty("sensor")
    private String sensor;

    @JsonProperty("measurement")
    private Double measurement;

    @JsonProperty("measurement_unit")
    private String measurementUnit;

    @JsonProperty("datetime")
    // Flexible datetime parsing - accepts any ISO-8601 format (ZonedDateTime, Instant, etc.)
    // JavaTimeModule handles conversion automatically
    private Instant datetime;

    @JsonProperty("location")
    private String location;

    @JsonProperty("data_quality")
    private String dataQuality;

    public SensorEvent() {}

    public String getSensor() {
        return sensor;
    }

    public void setSensor(String sensor) {
        this.sensor = sensor;
    }

    public Double getMeasurement() {
        return measurement;
    }

    public void setMeasurement(Double measurement) {
        this.measurement = measurement;
    }

    public String getMeasurementUnit() {
        return measurementUnit;
    }

    public void setMeasurementUnit(String measurementUnit) {
        this.measurementUnit = measurementUnit;
    }

    public Instant getDatetime() {
        return datetime;
    }

    public void setDatetime(Instant datetime) {
        this.datetime = datetime;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDataQuality() {
        return dataQuality;
    }

    public void setDataQuality(String dataQuality) {
        this.dataQuality = dataQuality;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorEvent that = (SensorEvent) o;
        return Objects.equals(sensor, that.sensor) &&
                Objects.equals(measurement, that.measurement) &&
                Objects.equals(measurementUnit, that.measurementUnit) &&
                Objects.equals(datetime, that.datetime) &&
                Objects.equals(location, that.location) &&
                Objects.equals(dataQuality, that.dataQuality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensor, measurement, measurementUnit, datetime, location, dataQuality);
    }

    @Override
    public String toString() {
        return "SensorEvent{" +
                "sensor='" + sensor + '\'' +
                ", measurement=" + measurement +
                ", measurementUnit='" + measurementUnit + '\'' +
                ", datetime=" + datetime +
                ", location='" + location + '\'' +
                ", dataQuality='" + dataQuality + '\'' +
                '}';
    }
}