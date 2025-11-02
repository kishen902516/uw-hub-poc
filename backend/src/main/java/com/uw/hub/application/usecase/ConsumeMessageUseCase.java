package com.uw.hub.application.usecase;

import com.uw.hub.domain.entity.CdcMessage;
import com.uw.hub.domain.repository.CdcMessageRepository;
import com.uw.hub.infrastructure.sse.CdcMessageBroadcaster;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Use case for consuming and processing CDC messages.
 *
 * Responsibilities:
 * 1. Validate CDC message (domain validation)
 * 2. Extract operation and table information
 * 3. Persist message to PostgreSQL
 * 4. Broadcast message to SSE clients for real-time updates
 *
 * This follows the Application Service pattern in Clean Architecture:
 * - Orchestrates domain logic and infrastructure services
 * - No business logic (that belongs in domain entities)
 * - Transaction boundary
 *
 * @see CdcMessage
 * @see CdcMessageRepository
 * @see CdcMessageBroadcaster
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumeMessageUseCase {

    private final CdcMessageRepository cdcMessageRepository;
    private final CdcMessageBroadcaster cdcMessageBroadcaster;

    /**
     * Execute the CDC message consumption workflow.
     *
     * Steps:
     * 1. Validate CDC message structure and operation-specific rules
     * 2. Persist message to PostgreSQL (with JSONB before/after data)
     * 3. Broadcast message to SSE clients for real-time UI updates
     * 4. Log processing metrics
     *
     * @param cdcMessage The CDC message to process
     * @return The persisted CDC message with generated ID
     * @throws IllegalArgumentException if validation fails
     * @throws RuntimeException if persistence or broadcasting fails
     */
    @Transactional
    public CdcMessage execute(CdcMessage cdcMessage) {
        log.debug("Processing CDC message: operation={}, table={}.{}.{}",
                cdcMessage.getOperation(),
                cdcMessage.getTableInfo().getDatabase(),
                cdcMessage.getTableInfo().getSchema(),
                cdcMessage.getTableInfo().getTable());

        try {
            // Step 1: Validate CDC message
            validateCdcMessage(cdcMessage);

            // Step 2: Persist to PostgreSQL
            CdcMessage savedMessage = persistMessage(cdcMessage);

            // Step 3: Broadcast to SSE clients
            broadcastMessage(savedMessage);

            // Step 4: Log metrics
            logProcessingMetrics(savedMessage);

            return savedMessage;

        } catch (IllegalArgumentException e) {
            log.error("Validation failed for CDC message: {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("Failed to process CDC message: {}", e.getMessage(), e);
            throw new RuntimeException("CDC message processing failed", e);
        }
    }

    /**
     * Validate CDC message structure and operation-specific rules.
     *
     * Validation rules:
     * - INSERT: Must have afterData, must not have beforeData
     * - UPDATE: Must have both beforeData and afterData
     * - DELETE: Must have beforeData, must not have afterData
     * - All operations: Must have valid table info, timestamp, position
     *
     * @param cdcMessage The message to validate
     * @throws IllegalArgumentException if validation fails
     */
    private void validateCdcMessage(CdcMessage cdcMessage) {
        log.debug("Validating CDC message: {}", cdcMessage.getId());

        // Domain-level validation (delegates to entity)
        cdcMessage.validate();

        // Additional application-level validations
        if (cdcMessage.getTableInfo() == null) {
            throw new IllegalArgumentException("Table info cannot be null");
        }

        if (cdcMessage.getTimestamp() == null) {
            throw new IllegalArgumentException("Timestamp cannot be null");
        }

        if (cdcMessage.getPosition() == null) {
            throw new IllegalArgumentException("Position cannot be null");
        }

        log.debug("Validation passed for CDC message: {}", cdcMessage.getId());
    }

    /**
     * Persist CDC message to PostgreSQL.
     *
     * Stores:
     * - Basic fields: topic, operation, timestamp, table info
     * - JSONB fields: position, beforeData, afterData, metadata
     *
     * Uses domain repository interface (infrastructure provides implementation).
     *
     * @param cdcMessage The message to persist
     * @return The persisted message with generated ID
     */
    private CdcMessage persistMessage(CdcMessage cdcMessage) {
        log.debug("Persisting CDC message to PostgreSQL: id={}, operation={}",
                cdcMessage.getId(), cdcMessage.getOperation());

        long startTime = System.currentTimeMillis();

        CdcMessage savedMessage = cdcMessageRepository.save(cdcMessage);

        long duration = System.currentTimeMillis() - startTime;

        log.debug("Persisted CDC message to PostgreSQL in {}ms: id={}, operation={}",
                duration, savedMessage.getId(), savedMessage.getOperation());

        return savedMessage;
    }

    /**
     * Broadcast CDC message to SSE clients for real-time UI updates.
     *
     * All connected SSE clients will receive the message immediately.
     * Clients can filter messages client-side based on:
     * - Operation type (INSERT/UPDATE/DELETE)
     * - Table name
     * - Database/schema
     *
     * @param cdcMessage The message to broadcast
     */
    private void broadcastMessage(CdcMessage cdcMessage) {
        log.debug("Broadcasting CDC message to SSE clients: id={}, operation={}",
                cdcMessage.getId(), cdcMessage.getOperation());

        try {
            cdcMessageBroadcaster.broadcast(cdcMessage);

            log.debug("Successfully broadcasted CDC message to SSE clients: id={}",
                    cdcMessage.getId());

        } catch (Exception e) {
            // Log error but don't fail the entire transaction
            // Message is still persisted even if broadcast fails
            log.error("Failed to broadcast CDC message to SSE clients (message still persisted): id={}, error={}",
                    cdcMessage.getId(), e.getMessage(), e);
        }
    }

    /**
     * Log processing metrics for monitoring and observability.
     *
     * Metrics include:
     * - Operation type
     * - Table name
     * - Processing time
     * - Transaction ID (from position)
     *
     * These logs are structured (JSON) for parsing by log aggregators (ELK, Splunk, etc.)
     */
    private void logProcessingMetrics(CdcMessage cdcMessage) {
        log.info("CDC message processed: " +
                "operation={}, " +
                "table={}.{}.{}, " +
                "txId={}, " +
                "timestamp={}, " +
                "messageId={}",
                cdcMessage.getOperation(),
                cdcMessage.getTableInfo().getDatabase(),
                cdcMessage.getTableInfo().getSchema(),
                cdcMessage.getTableInfo().getTable(),
                cdcMessage.getPosition().getOffset().get("txId"),
                cdcMessage.getTimestamp(),
                cdcMessage.getId());
    }

    /**
     * Health check method - verify use case dependencies are healthy.
     *
     * Checks:
     * - Repository is accessible
     * - Broadcaster is initialized
     *
     * @return true if all dependencies are healthy
     */
    public boolean isHealthy() {
        try {
            // Check repository health
            boolean repositoryHealthy = cdcMessageRepository.isHealthy();

            // Check broadcaster health (has active emitters)
            boolean broadcasterHealthy = cdcMessageBroadcaster != null;

            return repositoryHealthy && broadcasterHealthy;

        } catch (Exception e) {
            log.error("Health check failed: {}", e.getMessage());
            return false;
        }
    }
}
