package com.etl.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class Transformation {
    @JsonProperty("type")
    private String type;

    @JsonProperty("params")
    private Map<String, Object> params;

    @JsonProperty("keyBy")
    private String keyBy;

    public Transformation() {}

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public String getKeyBy() {
        return keyBy;
    }

    public void setKeyBy(String keyBy) {
        this.keyBy = keyBy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transformation that = (Transformation) o;
        return Objects.equals(type, that.type) &&
                Objects.equals(params, that.params) &&
                Objects.equals(keyBy, that.keyBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, params, keyBy);
    }

    @Override
    public String toString() {
        return "Transformation{" +
                "type='" + type + '\'' +
                ", params=" + params +
                ", keyBy='" + keyBy + '\'' +
                '}';
    }
}