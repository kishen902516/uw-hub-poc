package com.uw.hub.infrastructure.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for Prometheus metrics.
 * Defines custom metrics for CDC message processing.
 */
@Configuration
@Slf4j
public class MetricsConfig {

    /**
     * Counter for CDC messages processed by operation type
     */
    @Bean
    public Counter cdcMessageInsertCounter(MeterRegistry registry) {
        return Counter.builder("cdc.messages.processed")
            .tag("operation", "INSERT")
            .description("Total number of INSERT CDC messages processed")
            .register(registry);
    }

    @Bean
    public Counter cdcMessageUpdateCounter(MeterRegistry registry) {
        return Counter.builder("cdc.messages.processed")
            .tag("operation", "UPDATE")
            .description("Total number of UPDATE CDC messages processed")
            .register(registry);
    }

    @Bean
    public Counter cdcMessageDeleteCounter(MeterRegistry registry) {
        return Counter.builder("cdc.messages.processed")
            .tag("operation", "DELETE")
            .description("Total number of DELETE CDC messages processed")
            .register(registry);
    }

    /**
     * Counter for CDC message processing errors
     */
    @Bean
    public Counter cdcMessageErrorCounter(MeterRegistry registry) {
        return Counter.builder("cdc.messages.errors")
            .description("Total number of CDC message processing errors")
            .register(registry);
    }

    /**
     * Timer for CDC message processing duration
     */
    @Bean
    public Timer cdcMessageProcessingTimer(MeterRegistry registry) {
        return Timer.builder("cdc.messages.processing.duration")
            .description("Time taken to process CDC messages")
            .register(registry);
    }

    /**
     * Timer for SSE message broadcast duration
     */
    @Bean
    public Timer sseBroadcastTimer(MeterRegistry registry) {
        return Timer.builder("sse.broadcast.duration")
            .description("Time taken to broadcast messages via SSE")
            .register(registry);
    }

    /**
     * Counter for SSE connections
     */
    @Bean
    public Counter sseConnectionCounter(MeterRegistry registry) {
        return Counter.builder("sse.connections")
            .tag("status", "opened")
            .description("Total number of SSE connections opened")
            .register(registry);
    }

    @Bean
    public Counter sseDisconnectionCounter(MeterRegistry registry) {
        return Counter.builder("sse.connections")
            .tag("status", "closed")
            .description("Total number of SSE connections closed")
            .register(registry);
    }

    /**
     * Counter for database queries
     */
    @Bean
    public Counter databaseQueryCounter(MeterRegistry registry) {
        return Counter.builder("database.queries")
            .description("Total number of database queries executed")
            .register(registry);
    }

    /**
     * Timer for database query duration
     */
    @Bean
    public Timer databaseQueryTimer(MeterRegistry registry) {
        return Timer.builder("database.query.duration")
            .description("Time taken for database queries")
            .register(registry);
    }

    /**
     * Utility class for easier metric access
     */
    @org.springframework.stereotype.Component
    @lombok.RequiredArgsConstructor
    public static class MetricsService {

        private final MeterRegistry meterRegistry;

        public void incrementCdcMessageCounter(String operation) {
            Counter.builder("cdc.messages.processed")
                .tag("operation", operation)
                .register(meterRegistry)
                .increment();
        }

        public void incrementErrorCounter() {
            Counter.builder("cdc.messages.errors")
                .register(meterRegistry)
                .increment();
        }

        public Timer.Sample startTimer() {
            return Timer.start(meterRegistry);
        }

        public void recordProcessingTime(Timer.Sample sample, String metricName) {
            sample.stop(Timer.builder(metricName)
                .register(meterRegistry));
        }

        public void recordSseConnection(boolean opened) {
            Counter.builder("sse.connections")
                .tag("status", opened ? "opened" : "closed")
                .register(meterRegistry)
                .increment();
        }
    }
}
