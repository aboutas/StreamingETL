package com.etl.api.service;

import com.etl.api.model.JobStatus;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Service
public class JobRegistryService {
    private final Map<String, JobStatus> jobRegistry = new ConcurrentHashMap<>();

    public void registerJob(String jobId, String status, String message) {
        JobStatus jobStatus = new JobStatus(jobId, status, message);
        jobRegistry.put(jobId, jobStatus);
    }

    public JobStatus getJobStatus(String jobId) {
        return jobRegistry.get(jobId);
    }

    public void updateJobStatus(String jobId, String status, String message) {
        JobStatus existingStatus = jobRegistry.get(jobId);
        if (existingStatus != null) {
            existingStatus.setStatus(status);
            existingStatus.setMessage(message);
        } else {
            registerJob(jobId, status, message);
        }
    }

    public Map<String, JobStatus> getAllJobs() {
        return new ConcurrentHashMap<>(jobRegistry);
    }
}