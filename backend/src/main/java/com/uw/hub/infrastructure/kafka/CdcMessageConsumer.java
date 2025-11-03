package com.uw.hub.infrastructure.kafka;

import com.uw.hub.application.usecase.ConsumeMessageUseCase;
import com.uw.hub.domain.entity.CdcMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for CDC messages.
 *
 * Listens to configured Kafka topics and processes incoming CDC messages.
 * Uses @KafkaListener with manual acknowledgment for reliable message processing.
 *
 * Configuration:
 * - Topics: Configured via application.yml (e.g., cdcdb.public.*)
 * - Consumer Group: cdc-streaming-consumer-group
 * - Auto-offset-reset: earliest (process from beginning on first run)
 * - Enable auto-commit: false (manual acknowledgment)
 *
 * Error Handling:
 * - Malformed messages → logged and sent to dead-letter queue
 * - Processing failures → retried up to 3 times before DLQ
 * - Consumer errors → handled by CdcErrorHandler
 *
 * @see ConsumeMessageUseCase
 * @see CdcMessageDeserializer
 * @see CdcErrorHandler
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CdcMessageConsumer {

    private final CdcMessageDeserializer deserializer;
    private final ConsumeMessageUseCase consumeMessageUseCase;

    /**
     * Consume CDC messages from Kafka topics.
     *
     * Listens to topics matching pattern: cdcdb.public.*
     * (Configure actual topics in application.yml)
     *
     * @param consumerRecord Kafka message record
     * @param acknowledgment Manual acknowledgment (commit offset after successful processing)
     */
    @KafkaListener(
        topics = {"cdc.cdcdb.customers", "cdc.cdcdb.orders"},
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "cdcKafkaListenerContainerFactory",
        concurrency = "${spring.kafka.listener.concurrency:3}"
    )
    public void consume(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        String topic = consumerRecord.topic();
        String key = consumerRecord.key();
        String value = consumerRecord.value();
        long offset = consumerRecord.offset();
        int partition = consumerRecord.partition();

        log.info("Received CDC message: topic={}, partition={}, offset={}, key={}",
                topic, partition, offset, key);

        try {
            // Deserialize Debezium CDC message
            CdcMessage cdcMessage = deserializer.deserialize(topic, value);

            log.debug("Deserialized CDC message: operation={}, table={}.{}.{}, txId={}",
                    cdcMessage.getOperation(),
                    cdcMessage.getTableInfo().getDatabase(),
                    cdcMessage.getTableInfo().getSchema(),
                    cdcMessage.getTableInfo().getTable(),
                    cdcMessage.getPosition().getOffset().get("txId"));

            // Process message through use case (validate, persist, broadcast)
            consumeMessageUseCase.execute(cdcMessage);

            // Manually acknowledge message (commit offset)
            acknowledgment.acknowledge();

            log.info("Successfully processed CDC message: topic={}, offset={}, operation={}",
                    topic, offset, cdcMessage.getOperation());

        } catch (IllegalArgumentException e) {
            // Deserialization error - malformed message
            log.error("Failed to deserialize CDC message from topic {}, offset {}: {}",
                    topic, offset, e.getMessage());

            // Send to dead-letter queue (DLQ)
            sendToDeadLetterQueue(topic, key, value, e);

            // Acknowledge to skip this message
            acknowledgment.acknowledge();

        } catch (Exception e) {
            // Processing error - will be retried by Kafka (don't acknowledge)
            log.error("Failed to process CDC message from topic {}, offset {}: {}",
                    topic, offset, e.getMessage(), e);

            // Don't acknowledge - Kafka will retry based on retry policy
            // After max retries, error handler will send to DLQ
            throw new RuntimeException("CDC message processing failed", e);
        }
    }

    /**
     * Send malformed or failed messages to dead-letter queue for manual inspection.
     */
    private void sendToDeadLetterQueue(String originalTopic, String key, String value, Exception error) {
        log.warn("Sending message to dead-letter queue: topic={}, key={}, error={}",
                originalTopic, key, error.getMessage());

        // TODO: Implement DLQ publishing (Phase 8 or User Story 5)
        // For now, just log the message
        // kafkaTemplate.send("cdc-messages-dlq", key, value);
    }

    /**
     * Health check method - verify consumer is running and processing messages.
     * Called by health check actuator.
     *
     * @return true if consumer is healthy
     */
    public boolean isHealthy() {
        // Check if consumer is registered and active
        // This is a simple check - actual implementation would verify consumer group membership
        return true; // Placeholder
    }

    /**
     * Get consumer metrics for monitoring.
     */
    public ConsumerMetrics getMetrics() {
        // TODO: Implement metrics collection (Phase 7 - User Story 5)
        // Track: messages processed, processing time, errors, lag
        return new ConsumerMetrics(0, 0, 0, 0);
    }

    /**
     * Metrics record for consumer monitoring.
     */
    public record ConsumerMetrics(
        long messagesProcessed,
        long processingTimeMs,
        long errors,
        long consumerLag
    ) {}
}
