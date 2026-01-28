package com.etl.api.controller;

import com.etl.api.model.EtlConfig;
import com.etl.api.service.ConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class ConfigController {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigController.class);
    private static final ZoneId GREEK_TIMEZONE = ZoneId.of("Europe/Athens");

    private final ConfigService configService;

    public ConfigController(ConfigService configService) {
        this.configService = configService;
    }

    @PostMapping("/config")
    public ResponseEntity<Map<String, Object>> submitConfig(@RequestBody EtlConfig config) {
        Map<String, Object> response = new HashMap<>();

        try {
            LOG.info("Received config submission for job: {}", config.getJobId());

            configService.validateConfig(config);
            configService.publishConfig(config);

            response.put("status", "success");
            response.put("jobId", config.getJobId());
            response.put("message", "Configuration submitted successfully");

            return ResponseEntity.ok(response);

        } catch (IllegalArgumentException e) {
            LOG.warn("Config validation failed for job: {}", config.getJobId(), e);
            response.put("status", "error");
            response.put("message", "Validation error: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);

        } catch (Exception e) {
            LOG.error("Error processing config for job: {}", config.getJobId(), e);
            response.put("status", "error");
            response.put("message", "Internal error: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "etl-api");
        response.put("timestamp", ZonedDateTime.now(GREEK_TIMEZONE).toInstant());
        return ResponseEntity.ok(response);
    }
}
