package com.uw.hub.domain.repository;

import com.uw.hub.domain.entity.CdcMessage;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository interface for CDC messages following DDD principles.
 * This is a domain interface - infrastructure layer provides the implementation.
 */
public interface CdcMessageRepository {

    /**
     * Save a CDC message
     */
    CdcMessage save(CdcMessage message);

    /**
     * Save multiple CDC messages in batch
     */
    List<CdcMessage> saveAll(List<CdcMessage> messages);

    /**
     * Find a CDC message by ID
     */
    Optional<CdcMessage> findById(UUID id);

    /**
     * Find all CDC messages with pagination
     */
    Page<CdcMessage> findAll(Pageable pageable);

    /**
     * Find CDC messages by topic with pagination
     */
    Page<CdcMessage> findByTopic(String topic, Pageable pageable);

    /**
     * Find CDC messages by operation type with pagination
     */
    Page<CdcMessage> findByOperation(CdcMessage.Operation operation, Pageable pageable);

    /**
     * Find CDC messages by table (database.schema.table) with pagination
     */
    Page<CdcMessage> findByTable(String database, String schema, String table, Pageable pageable);

    /**
     * Find CDC messages by multiple filters
     */
    Page<CdcMessage> findByFilters(
        String topic,
        CdcMessage.Operation operation,
        String database,
        String schema,
        String table,
        Instant fromDate,
        Instant toDate,
        Pageable pageable
    );

    /**
     * Find CDC messages within a time range
     */
    Page<CdcMessage> findByTimestampBetween(Instant start, Instant end, Pageable pageable);

    /**
     * Get unique list of topics
     */
    List<String> findDistinctTopics();

    /**
     * Get unique list of tables (in format database.schema.table)
     */
    List<String> findDistinctTables();

    /**
     * Count total CDC messages
     */
    long count();

    /**
     * Count CDC messages by operation
     */
    long countByOperation(CdcMessage.Operation operation);

    /**
     * Delete CDC messages older than a specific date (for cleanup)
     */
    long deleteByTimestampBefore(Instant cutoffDate);

    /**
     * Check if repository is healthy (connection test)
     */
    boolean isHealthy();
}
