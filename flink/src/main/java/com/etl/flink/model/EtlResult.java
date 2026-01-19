package com.etl.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class EtlResult {
    @JsonProperty("_id")
    private String id;

    @JsonProperty("jobId")
    private String jobId;

    @JsonProperty("transformations")
    private List<Transformation> transformations;

    @JsonProperty("groupingField")
    private String groupingField;

    @JsonProperty("groupingKey")
    private String groupingKey;

    @JsonProperty("windowStart")
    private Instant windowStart;

    @JsonProperty("windowEnd")
    private Instant windowEnd;

    @JsonProperty("aggregationType")
    private String aggregationType;

    @JsonProperty("field")
    private String field;

    @JsonProperty("result")
    private Object result;

    @JsonProperty("processedAt")
    private Instant processedAt;

    @JsonProperty("sensorType")
    private String sensorType;

    @JsonProperty("measurementUnit")
    private String measurementUnit;

    @JsonProperty("location")
    private String location;

    public EtlResult() {}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<Transformation> transformations) {
        this.transformations = transformations;
    }

    public String getGroupingField() {
        return groupingField;
    }

    public void setGroupingField(String groupingField) {
        this.groupingField = groupingField;
    }

    public String getGroupingKey() {
        return groupingKey;
    }

    public void setGroupingKey(String groupingKey) {
        this.groupingKey = groupingKey;
    }

    public Instant getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Instant windowStart) {
        this.windowStart = windowStart;
    }

    public Instant getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Instant windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(String aggregationType) {
        this.aggregationType = aggregationType;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    public String getSensorType() {
        return sensorType;
    }

    public void setSensorType(String sensorType) {
        this.sensorType = sensorType;
    }

    public String getMeasurementUnit() {
        return measurementUnit;
    }

    public void setMeasurementUnit(String measurementUnit) {
        this.measurementUnit = measurementUnit;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EtlResult etlResult = (EtlResult) o;
        return Objects.equals(id, etlResult.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "EtlResult{" +
                "id='" + id + '\'' +
                ", jobId='" + jobId + '\'' +
                ", result=" + result +
                '}';
    }
}