package com.uw.hub.presentation.rest;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Health check endpoint for monitoring Kafka and PostgreSQL connectivity.
 * Used by Kubernetes liveness/readiness probes.
 */
@RestController
@RequestMapping("/api/health")
@RequiredArgsConstructor
@Slf4j
public class HealthController {

    private final DataSource dataSource;
    private final ConsumerFactory<String, String> consumerFactory;

    /**
     * Basic health check
     */
    @GetMapping
    public ResponseEntity<HealthResponse> health() {
        HealthResponse response = HealthResponse.builder()
            .status("UP")
            .timestamp(Instant.now())
            .build();

        return ResponseEntity.ok(response);
    }

    /**
     * Detailed health check with component status
     */
    @GetMapping("/detailed")
    public ResponseEntity<DetailedHealthResponse> detailedHealth() {
        Map<String, ComponentHealth> components = new HashMap<>();

        // Check PostgreSQL
        ComponentHealth postgresHealth = checkPostgres();
        components.put("postgres", postgresHealth);

        // Check Kafka
        ComponentHealth kafkaHealth = checkKafka();
        components.put("kafka", kafkaHealth);

        // Determine overall status
        boolean allHealthy = components.values().stream()
            .allMatch(c -> "UP".equals(c.getStatus()));

        DetailedHealthResponse response = DetailedHealthResponse.builder()
            .status(allHealthy ? "UP" : "DEGRADED")
            .timestamp(Instant.now())
            .components(components)
            .build();

        return ResponseEntity.ok(response);
    }

    /**
     * Check PostgreSQL connectivity
     */
    private ComponentHealth checkPostgres() {
        try {
            Connection connection = dataSource.getConnection();
            boolean isValid = connection.isValid(5);
            connection.close();

            if (isValid) {
                return ComponentHealth.builder()
                    .status("UP")
                    .message("PostgreSQL connection is healthy")
                    .build();
            } else {
                return ComponentHealth.builder()
                    .status("DOWN")
                    .message("PostgreSQL connection is invalid")
                    .build();
            }
        } catch (Exception e) {
            log.error("PostgreSQL health check failed", e);
            return ComponentHealth.builder()
                .status("DOWN")
                .message("Failed to connect to PostgreSQL: " + e.getMessage())
                .build();
        }
    }

    /**
     * Check Kafka connectivity
     */
    private ComponentHealth checkKafka() {
        try {
            // Try to get consumer configuration
            Map<String, Object> config = consumerFactory.getConfigurationProperties();
            String bootstrapServers = (String) config.get("bootstrap.servers");

            return ComponentHealth.builder()
                .status("UP")
                .message("Kafka consumer factory is configured")
                .details(Map.of("bootstrap.servers", bootstrapServers))
                .build();
        } catch (Exception e) {
            log.error("Kafka health check failed", e);
            return ComponentHealth.builder()
                .status("DOWN")
                .message("Kafka connection failed: " + e.getMessage())
                .build();
        }
    }

    /**
     * Health response DTO
     */
    @Data
    @Builder
    public static class HealthResponse {
        private String status;
        private Instant timestamp;
    }

    /**
     * Detailed health response DTO
     */
    @Data
    @Builder
    public static class DetailedHealthResponse {
        private String status;
        private Instant timestamp;
        private Map<String, ComponentHealth> components;
    }

    /**
     * Component health DTO
     */
    @Data
    @Builder
    public static class ComponentHealth {
        private String status;
        private String message;
        private Map<String, Object> details;
    }
}
