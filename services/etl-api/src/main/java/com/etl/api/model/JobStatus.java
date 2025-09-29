package com.etl.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class JobStatus {
    private static final ZoneId GREEK_TIMEZONE = ZoneId.of("Europe/Athens"); // UTC+2 (UTC+3 in summer)

    @JsonProperty("jobId")
    private String jobId;

    @JsonProperty("status")
    private String status;

    @JsonProperty("submittedAt")
    private Instant submittedAt;

    @JsonProperty("message")
    private String message;

    public JobStatus() {}

    public JobStatus(String jobId, String status, String message) {
        this.jobId = jobId;
        this.status = status;
        this.message = message;
        this.submittedAt = ZonedDateTime.now(GREEK_TIMEZONE).toInstant();
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Instant getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(Instant submittedAt) {
        this.submittedAt = submittedAt;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}