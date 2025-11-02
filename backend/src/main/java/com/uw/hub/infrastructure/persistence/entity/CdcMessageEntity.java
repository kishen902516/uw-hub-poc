package com.uw.hub.infrastructure.persistence.entity;

import com.uw.hub.infrastructure.persistence.converter.JsonbConverter;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * JPA entity for storing CDC messages in PostgreSQL.
 * Maps to the 'cdc_messages' table with JSONB columns.
 */
@Entity
@Table(name = "cdc_messages", indexes = {
    @Index(name = "idx_cdc_messages_topic", columnList = "topic"),
    @Index(name = "idx_cdc_messages_operation", columnList = "operation"),
    @Index(name = "idx_cdc_messages_timestamp", columnList = "timestamp"),
    @Index(name = "idx_cdc_messages_table", columnList = "database_name, schema_name, table_name"),
    @Index(name = "idx_cdc_messages_composite", columnList = "operation, table_name, timestamp")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcMessageEntity {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "topic", nullable = false, length = 255)
    private String topic;

    @Column(name = "database_name", nullable = false, length = 255)
    private String databaseName;

    @Column(name = "schema_name", nullable = false, length = 255)
    private String schemaName;

    @Column(name = "table_name", nullable = false, length = 255)
    private String tableName;

    @Column(name = "operation", nullable = false, length = 10)
    @Enumerated(EnumType.STRING)
    private OperationType operation;

    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;

    @Column(name = "position", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    @Convert(converter = JsonbConverter.class)
    private Map<String, Object> position;

    @Column(name = "before_data", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    @Convert(converter = JsonbConverter.class)
    private Map<String, Object> beforeData;

    @Column(name = "after_data", columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    @Convert(converter = JsonbConverter.class)
    private Map<String, Object> afterData;

    @Column(name = "metadata", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    @Convert(converter = JsonbConverter.class)
    private Map<String, Object> metadata;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
        if (id == null) {
            id = UUID.randomUUID();
        }
    }

    /**
     * Enum for CDC operation types
     */
    public enum OperationType {
        INSERT, UPDATE, DELETE
    }

    /**
     * Helper method to get fully qualified table name
     */
    public String getFullyQualifiedTableName() {
        return String.format("%s.%s.%s", databaseName, schemaName, tableName);
    }
}
