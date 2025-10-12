package com.etl.flink.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

public class SensorEvent {
    @JsonProperty("jobId")
    private String jobId;

    @JsonProperty("sensor")
    private String sensor;

    @JsonProperty("measurement")
    private Double measurement;

    @JsonProperty("measurement_unit")
    private String measurementUnit;

    @JsonProperty("datetime")
    // Accept any ISO-8601 datetime format (ZonedDateTime, Instant, etc.)
    // JavaTimeModule handles conversion automatically
    private Instant datetime;

    @JsonProperty("location")
    private String location;

    @JsonProperty("data_quality")
    private String dataQuality;

    public SensorEvent() {}

    public SensorEvent(String jobId, String sensor, Double measurement, String measurementUnit, Instant datetime, String location) {
        this(jobId, sensor, measurement, measurementUnit, datetime, location, null);
    }

    public SensorEvent(String jobId, String sensor, Double measurement, String measurementUnit, Instant datetime, String location, String dataQuality) {
        this.jobId = jobId;
        this.sensor = sensor;
        this.measurement = measurement;
        this.measurementUnit = measurementUnit;
        this.datetime = datetime;
        this.location = location;
        this.dataQuality = dataQuality;
    }

    // Legacy constructor for backward compatibility
    public SensorEvent(String sensor, Double measurement, String measurementUnit, Instant datetime) {
        this(null, sensor, measurement, measurementUnit, datetime, null);
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

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
        return Objects.equals(jobId, that.jobId) &&
                Objects.equals(sensor, that.sensor) &&
                Objects.equals(measurement, that.measurement) &&
                Objects.equals(measurementUnit, that.measurementUnit) &&
                Objects.equals(datetime, that.datetime) &&
                Objects.equals(location, that.location) &&
                Objects.equals(dataQuality, that.dataQuality);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, sensor, measurement, measurementUnit, datetime, location, dataQuality);
    }

    @Override
    public String toString() {
        return "SensorEvent{" +
                "jobId='" + jobId + '\'' +
                ", sensor='" + sensor + '\'' +
                ", measurement=" + measurement +
                ", measurementUnit='" + measurementUnit + '\'' +
                ", datetime=" + datetime +
                ", location='" + location + '\'' +
                ", dataQuality='" + dataQuality + '\'' +
                '}';
    }
}