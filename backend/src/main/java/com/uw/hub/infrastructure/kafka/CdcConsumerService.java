package com.uw.hub.infrastructure.kafka;

import com.uw.hub.application.usecase.ConsumeMessageUseCase;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.Collection;

/**
 * Service to wire CdcMessageConsumer with ConsumeMessageUseCase and manage consumer lifecycle.
 *
 * Responsibilities:
 * 1. Coordinate consumer startup and shutdown
 * 2. Monitor consumer health and lag
 * 3. Provide consumer management operations (pause, resume, restart)
 * 4. Integration point for consumer metrics and monitoring
 *
 * This service acts as a facade over Spring Kafka's listener infrastructure,
 * providing a clean interface for consumer management and monitoring.
 *
 * @see CdcMessageConsumer
 * @see ConsumeMessageUseCase
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class CdcConsumerService {

    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    private final ConsumeMessageUseCase consumeMessageUseCase;
    private final CdcMessageConsumer cdcMessageConsumer;

    private static final String CONSUMER_ID = "cdcKafkaListenerContainerFactory";

    /**
     * Initialize consumer service on application startup.
     * Verify Kafka connection and consumer group registration.
     */
    @PostConstruct
    public void initialize() {
        log.info("Initializing CDC Consumer Service");

        try {
            // Verify consumer endpoints are registered
            Collection<String> listenerContainerIds = kafkaListenerEndpointRegistry.getListenerContainerIds();
            log.info("Registered Kafka listener containers: {}", listenerContainerIds);

            // Check if CDC message consumer is registered
            boolean cdcConsumerRegistered = listenerContainerIds.stream()
                    .anyMatch(id -> id.contains("consumeMessageUseCase") || id.contains("cdcMessageConsumer"));

            if (cdcConsumerRegistered) {
                log.info("CDC message consumer successfully registered and ready to consume messages");
            } else {
                log.warn("CDC message consumer not found in registered listeners - check @KafkaListener configuration");
            }

            // Verify use case dependencies
            if (consumeMessageUseCase.isHealthy()) {
                log.info("ConsumeMessageUseCase dependencies are healthy");
            } else {
                log.error("ConsumeMessageUseCase dependencies are NOT healthy - check repository and broadcaster");
            }

        } catch (Exception e) {
            log.error("Failed to initialize CDC Consumer Service: {}", e.getMessage(), e);
            throw new RuntimeException("CDC Consumer Service initialization failed", e);
        }
    }

    /**
     * Shutdown consumer gracefully on application shutdown.
     * Ensures all in-flight messages are processed before stopping.
     */
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down CDC Consumer Service");

        try {
            // Stop all listeners gracefully (allows in-flight messages to complete)
            kafkaListenerEndpointRegistry.stop();
            log.info("CDC Consumer Service shut down successfully");

        } catch (Exception e) {
            log.error("Error during CDC Consumer Service shutdown: {}", e.getMessage(), e);
        }
    }

    /**
     * Pause CDC message consumption.
     * Useful for maintenance or when downstream systems are unavailable.
     */
    public void pauseConsumer() {
        log.info("Pausing CDC message consumer");

        try {
            Collection<MessageListenerContainer> listenerContainers = kafkaListenerEndpointRegistry.getAllListenerContainers();
            listenerContainers.forEach(MessageListenerContainer::pause);

            log.info("CDC message consumer paused successfully");

        } catch (Exception e) {
            log.error("Failed to pause CDC message consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to pause consumer", e);
        }
    }

    /**
     * Resume CDC message consumption after pause.
     */
    public void resumeConsumer() {
        log.info("Resuming CDC message consumer");

        try {
            Collection<MessageListenerContainer> listenerContainers = kafkaListenerEndpointRegistry.getAllListenerContainers();
            listenerContainers.forEach(MessageListenerContainer::resume);

            log.info("CDC message consumer resumed successfully");

        } catch (Exception e) {
            log.error("Failed to resume CDC message consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to resume consumer", e);
        }
    }

    /**
     * Restart CDC message consumer.
     * Useful for recovering from errors or applying configuration changes.
     */
    public void restartConsumer() {
        log.info("Restarting CDC message consumer");

        try {
            pauseConsumer();
            Thread.sleep(1000); // Allow in-flight messages to complete
            resumeConsumer();

            log.info("CDC message consumer restarted successfully");

        } catch (Exception e) {
            log.error("Failed to restart CDC message consumer: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to restart consumer", e);
        }
    }

    /**
     * Check if CDC message consumer is running.
     *
     * @return true if consumer is running and processing messages
     */
    public boolean isConsumerRunning() {
        try {
            return kafkaListenerEndpointRegistry.getAllListenerContainers().stream()
                    .anyMatch(MessageListenerContainer::isRunning);
        } catch (Exception e) {
            log.error("Failed to check consumer running status: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Check if CDC message consumer is paused.
     *
     * @return true if consumer is paused
     */
    public boolean isConsumerPaused() {
        try {
            return kafkaListenerEndpointRegistry.getAllListenerContainers().stream()
                    .anyMatch(MessageListenerContainer::isPauseRequested);
        } catch (Exception e) {
            log.error("Failed to check consumer paused status: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get consumer status for health check and monitoring.
     *
     * @return Consumer status object
     */
    public ConsumerStatus getConsumerStatus() {
        boolean running = isConsumerRunning();
        boolean paused = isConsumerPaused();
        boolean healthy = cdcMessageConsumer.isHealthy() && consumeMessageUseCase.isHealthy();

        int activeContainers = kafkaListenerEndpointRegistry.getAllListenerContainers().size();

        return new ConsumerStatus(running, paused, healthy, activeContainers);
    }

    /**
     * Get consumer metrics for monitoring dashboard.
     *
     * @return Consumer metrics
     */
    public CdcMessageConsumer.ConsumerMetrics getConsumerMetrics() {
        return cdcMessageConsumer.getMetrics();
    }

    /**
     * Consumer status record for health checks and monitoring.
     */
    public record ConsumerStatus(
        boolean running,
        boolean paused,
        boolean healthy,
        int activeContainers
    ) {
        public String getStatusText() {
            if (!running) {
                return "STOPPED";
            }
            if (paused) {
                return "PAUSED";
            }
            if (healthy) {
                return "RUNNING";
            }
            return "DEGRADED";
        }
    }
}
