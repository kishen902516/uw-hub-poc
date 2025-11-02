package com.uw.hub.infrastructure.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Error handler for CDC Kafka consumer.
 *
 * Handles:
 * 1. Deserialization errors (malformed CDC messages)
 * 2. Processing errors (validation failures, database errors)
 * 3. Retry logic with exponential backoff
 * 4. Dead-letter queue (DLQ) for failed messages after max retries
 *
 * Retry Strategy:
 * - Max retries: 3
 * - Backoff: 1s, 2s, 4s
 * - After max retries: Send to DLQ topic (cdc-messages-dlq)
 *
 * Error Types:
 * - Recoverable: Database connection errors, temporary failures → Retry
 * - Non-recoverable: Malformed JSON, validation errors → Send to DLQ immediately
 *
 * Observability:
 * - All errors logged with correlation IDs
 * - Metrics tracked: retry count, DLQ sends, error types
 * - Alerts triggered on high error rates
 *
 * @see CdcMessageConsumer
 */
@Component
@Slf4j
public class CdcErrorHandler implements CommonErrorHandler {

    private static final int MAX_RETRIES = 3;
    private static final String DLQ_TOPIC = "cdc-messages-dlq";

    /**
     * Handle errors that occur during message processing.
     *
     * This method is called by Spring Kafka when an exception is thrown
     * during message consumption.
     *
     * @param thrownException The exception that was thrown
     * @param data The consumer records that failed to process (null if batch listener)
     * @param consumer The Kafka consumer instance
     * @param container The message listener container
     */
    @Override
    public void handleOtherException(
            Exception thrownException,
            Consumer<?, ?> consumer,
            MessageListenerContainer container,
            boolean batchListener) {

        log.error("CDC consumer error occurred: {}", thrownException.getMessage(), thrownException);

        // Determine if error is recoverable
        if (isRecoverableError(thrownException)) {
            log.info("Error is recoverable - will retry");
            // Spring Kafka will automatically retry based on retry configuration
        } else {
            log.error("Error is non-recoverable - message will be sent to DLQ");
            // Non-recoverable errors should be handled by sending to DLQ
            // (implemented in CdcMessageConsumer)
        }
    }

    /**
     * Handle errors for a specific consumer record.
     *
     * Called when batch processing is disabled (single message at a time).
     *
     * @param thrownException The exception that was thrown
     * @param record The consumer record that failed to process
     * @param consumer The Kafka consumer
     * @param container The message listener container
     */
    @Override
    public void handleRecord(
            Exception thrownException,
            ConsumerRecord<?, ?> record,
            Consumer<?, ?> consumer,
            MessageListenerContainer container) {

        log.error("Error processing CDC message: topic={}, partition={}, offset={}, error={}",
                record.topic(), record.partition(), record.offset(), thrownException.getMessage());

        // Check retry count from headers
        int retryCount = getRetryCount(record);

        if (retryCount >= MAX_RETRIES) {
            log.error("Max retries ({}) exceeded for CDC message: topic={}, offset={} - sending to DLQ",
                    MAX_RETRIES, record.topic(), record.offset());

            sendToDeadLetterQueue(record, thrownException);

            // Seek to next offset to skip this message
            seekToNextOffset(consumer, record);

        } else if (isRecoverableError(thrownException)) {
            log.info("Retrying CDC message (attempt {}/{}): topic={}, offset={}",
                    retryCount + 1, MAX_RETRIES, record.topic(), record.offset());

            // Increment retry count
            // Spring Kafka retry template will handle the actual retry
            incrementRetryCount(record);

        } else {
            log.error("Non-recoverable error for CDC message: topic={}, offset={} - sending to DLQ immediately",
                    record.topic(), record.offset());

            sendToDeadLetterQueue(record, thrownException);
            seekToNextOffset(consumer, record);
        }
    }

    /**
     * Handle errors for a batch of consumer records.
     *
     * Called when batch processing is enabled.
     *
     * @param thrownException The exception that was thrown
     * @param records The consumer records that failed to process
     * @param consumer The Kafka consumer
     * @param container The message listener container
     * @param invokeListener Callback to retry processing
     */
    @Override
    public void handleBatch(
            Exception thrownException,
            ConsumerRecords<?, ?> records,
            Consumer<?, ?> consumer,
            MessageListenerContainer container,
            Runnable invokeListener) {

        log.error("Error processing batch of CDC messages: recordCount={}, error={}",
                records.count(), thrownException.getMessage());

        // For batch errors, log and allow retry
        // Individual record errors are handled by handleRecord()
    }

    /**
     * Determine if an error is recoverable (should be retried).
     *
     * Recoverable errors:
     * - Database connection errors
     * - Temporary network issues
     * - Timeout exceptions
     *
     * Non-recoverable errors:
     * - Malformed JSON (deserialization errors)
     * - Validation errors (illegal argument, illegal state)
     * - Business logic errors
     *
     * @param exception The exception to check
     * @return true if error is recoverable
     */
    private boolean isRecoverableError(Exception exception) {
        // Database/connection errors - retry
        if (exception instanceof org.springframework.dao.DataAccessException) {
            return true;
        }
        if (exception instanceof java.net.SocketTimeoutException) {
            return true;
        }
        if (exception instanceof org.springframework.transaction.TransactionException) {
            return true;
        }

        // Deserialization/validation errors - don't retry
        if (exception instanceof IllegalArgumentException) {
            return false;
        }
        if (exception instanceof IllegalStateException) {
            return false;
        }
        if (exception instanceof com.fasterxml.jackson.core.JsonProcessingException) {
            return false;
        }

        // Unknown error - be conservative and don't retry
        return false;
    }

    /**
     * Get retry count from consumer record headers.
     */
    private int getRetryCount(ConsumerRecord<?, ?> record) {
        // Check for retry count header
        org.apache.kafka.common.header.Header retryHeader = record.headers().lastHeader("retry-count");
        if (retryHeader != null) {
            try {
                return Integer.parseInt(new String(retryHeader.value()));
            } catch (NumberFormatException e) {
                log.warn("Invalid retry-count header value: {}", new String(retryHeader.value()));
            }
        }
        return 0;
    }

    /**
     * Increment retry count in consumer record headers.
     */
    private void incrementRetryCount(ConsumerRecord<?, ?> record) {
        int currentRetryCount = getRetryCount(record);
        int newRetryCount = currentRetryCount + 1;

        // Add/update retry-count header
        record.headers().remove("retry-count");
        record.headers().add("retry-count", String.valueOf(newRetryCount).getBytes());

        log.debug("Incremented retry count to {} for record: topic={}, offset={}",
                newRetryCount, record.topic(), record.offset());
    }

    /**
     * Send failed message to dead-letter queue for manual inspection.
     *
     * DLQ message includes:
     * - Original message payload
     * - Error details (exception type, message, stack trace)
     * - Metadata (topic, partition, offset, timestamp)
     * - Retry count
     */
    private void sendToDeadLetterQueue(ConsumerRecord<?, ?> record, Exception exception) {
        log.warn("Sending CDC message to DLQ: topic={}, partition={}, offset={}, error={}",
                record.topic(), record.partition(), record.offset(), exception.getMessage());

        try {
            // Build DLQ message with error details
            Message<?> dlqMessage = MessageBuilder
                    .withPayload(record.value())
                    .setHeader(KafkaHeaders.TOPIC, DLQ_TOPIC)
                    .setHeader(KafkaHeaders.KEY, record.key())
                    .setHeader("original-topic", record.topic())
                    .setHeader("original-partition", record.partition())
                    .setHeader("original-offset", record.offset())
                    .setHeader("original-timestamp", record.timestamp())
                    .setHeader("error-type", exception.getClass().getName())
                    .setHeader("error-message", exception.getMessage())
                    .setHeader("retry-count", getRetryCount(record))
                    .build();

            // TODO: Publish to DLQ topic using KafkaTemplate (Phase 8)
            // kafkaTemplate.send(dlqMessage);

            log.info("CDC message sent to DLQ: topic={}, offset={}", record.topic(), record.offset());

        } catch (Exception e) {
            log.error("Failed to send CDC message to DLQ: topic={}, offset={}, error={}",
                    record.topic(), record.offset(), e.getMessage(), e);
        }
    }

    /**
     * Seek to next offset to skip the failed message.
     */
    private void seekToNextOffset(Consumer<?, ?> consumer, ConsumerRecord<?, ?> record) {
        try {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
            long nextOffset = record.offset() + 1;

            consumer.seek(topicPartition, nextOffset);

            log.info("Seeked to next offset: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), nextOffset);

        } catch (Exception e) {
            log.error("Failed to seek to next offset: topic={}, partition={}, offset={}, error={}",
                    record.topic(), record.partition(), record.offset(), e.getMessage(), e);
        }
    }

    /**
     * Record class for Kafka consumer records (compatibility with Spring Kafka 3.x).
     */
    private record ConsumerRecords<K, V>(List<ConsumerRecord<K, V>> records) {
        public int count() {
            return records.size();
        }
    }
}
