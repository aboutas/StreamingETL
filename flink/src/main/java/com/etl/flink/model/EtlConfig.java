package com.etl.flink.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class EtlConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    @JsonProperty("jobId")
    private String jobId;

    @JsonProperty("source")
    private String source;

    @JsonProperty("transformations")
    private List<Transformation> transformations;

    @JsonProperty("outputTopic")
    private String outputTopic;

    public EtlConfig() {}

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<Transformation> transformations) {
        this.transformations = transformations;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EtlConfig etlConfig = (EtlConfig) o;
        return Objects.equals(jobId, etlConfig.jobId) &&
                Objects.equals(source, etlConfig.source) &&
                Objects.equals(transformations, etlConfig.transformations) &&
                Objects.equals(outputTopic, etlConfig.outputTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, source, transformations, outputTopic);
    }

    @Override
    public String toString() {
        return "EtlConfig{" +
                "jobId='" + jobId + '\'' +
                ", source='" + source + '\'' +
                ", transformations=" + transformations +
                ", outputTopic='" + outputTopic + '\'' +
                '}';
    }
}