package com.uw.hub.infrastructure.persistence;

import com.uw.hub.infrastructure.persistence.entity.CdcMessageEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Spring Data JPA repository for CdcMessageEntity.
 * Provides database access methods with automatic query generation.
 */
@Repository
public interface CdcMessageJpaRepository extends JpaRepository<CdcMessageEntity, UUID> {

    /**
     * Find CDC messages by topic with pagination
     */
    Page<CdcMessageEntity> findByTopicOrderByTimestampDesc(String topic, Pageable pageable);

    /**
     * Find CDC messages by operation with pagination
     */
    Page<CdcMessageEntity> findByOperationOrderByTimestampDesc(
        CdcMessageEntity.OperationType operation,
        Pageable pageable
    );

    /**
     * Find CDC messages by table info with pagination
     */
    Page<CdcMessageEntity> findByDatabaseNameAndSchemaNameAndTableNameOrderByTimestampDesc(
        String databaseName,
        String schemaName,
        String tableName,
        Pageable pageable
    );

    /**
     * Find CDC messages within timestamp range
     */
    Page<CdcMessageEntity> findByTimestampBetweenOrderByTimestampDesc(
        Instant start,
        Instant end,
        Pageable pageable
    );

    /**
     * Complex query with multiple optional filters
     */
    @Query("SELECT c FROM CdcMessageEntity c WHERE " +
           "(:topic IS NULL OR c.topic = :topic) AND " +
           "(:operation IS NULL OR c.operation = :operation) AND " +
           "(:databaseName IS NULL OR c.databaseName = :databaseName) AND " +
           "(:schemaName IS NULL OR c.schemaName = :schemaName) AND " +
           "(:tableName IS NULL OR c.tableName = :tableName) AND " +
           "(:fromDate IS NULL OR c.timestamp >= :fromDate) AND " +
           "(:toDate IS NULL OR c.timestamp <= :toDate) " +
           "ORDER BY c.timestamp DESC")
    Page<CdcMessageEntity> findByFilters(
        @Param("topic") String topic,
        @Param("operation") CdcMessageEntity.OperationType operation,
        @Param("databaseName") String databaseName,
        @Param("schemaName") String schemaName,
        @Param("tableName") String tableName,
        @Param("fromDate") Instant fromDate,
        @Param("toDate") Instant toDate,
        Pageable pageable
    );

    /**
     * Find distinct topics
     */
    @Query("SELECT DISTINCT c.topic FROM CdcMessageEntity c ORDER BY c.topic")
    List<String> findDistinctTopics();

    /**
     * Find distinct tables (concatenated as database.schema.table)
     */
    @Query("SELECT DISTINCT CONCAT(c.databaseName, '.', c.schemaName, '.', c.tableName) " +
           "FROM CdcMessageEntity c ORDER BY CONCAT(c.databaseName, '.', c.schemaName, '.', c.tableName)")
    List<String> findDistinctTables();

    /**
     * Count messages by operation
     */
    long countByOperation(CdcMessageEntity.OperationType operation);

    /**
     * Delete messages older than cutoff date
     */
    @Modifying
    @Query("DELETE FROM CdcMessageEntity c WHERE c.timestamp < :cutoffDate")
    long deleteByTimestampBefore(@Param("cutoffDate") Instant cutoffDate);

    /**
     * Check if any records exist (health check)
     */
    @Query("SELECT CASE WHEN COUNT(c) > 0 THEN true ELSE false END FROM CdcMessageEntity c")
    boolean existsAny();
}
