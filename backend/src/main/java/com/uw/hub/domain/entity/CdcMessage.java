package com.uw.hub.domain.entity;

import com.uw.hub.domain.valueobject.Metadata;
import com.uw.hub.domain.valueobject.Position;
import com.uw.hub.domain.valueobject.TableInfo;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Domain entity representing a Change Data Capture (CDC) message from Kafka.
 * This is the core aggregate root for the CDC streaming domain.
 *
 * Supports Debezium CDC format with:
 * - INSERT: Only afterData is present
 * - UPDATE: Both beforeData and afterData are present
 * - DELETE: Only beforeData is present
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcMessage {

    /**
     * Unique identifier for this CDC message
     */
    @NotNull
    private UUID id;

    /**
     * Kafka topic this message was consumed from
     */
    @NotBlank(message = "Topic cannot be blank")
    private String topic;

    /**
     * Table information (database.schema.table)
     */
    @NotNull(message = "Table info cannot be null")
    private TableInfo tableInfo;

    /**
     * CDC operation type: INSERT, UPDATE, DELETE
     */
    @NotNull(message = "Operation cannot be null")
    private Operation operation;

    /**
     * Timestamp when the CDC event occurred
     */
    @NotNull(message = "Timestamp cannot be null")
    private Instant timestamp;

    /**
     * Position metadata (LSN, txId, offset)
     */
    @NotNull(message = "Position cannot be null")
    private Position position;

    /**
     * Row data BEFORE the change (for UPDATE and DELETE operations)
     * Null for INSERT operations
     */
    private Map<String, Object> beforeData;

    /**
     * Row data AFTER the change (for INSERT and UPDATE operations)
     * Null for DELETE operations
     */
    private Map<String, Object> afterData;

    /**
     * CDC metadata (connector info, schema version)
     */
    @NotNull(message = "Metadata cannot be null")
    private Metadata metadata;

    /**
     * Enum representing CDC operation types
     */
    public enum Operation {
        INSERT,
        UPDATE,
        DELETE;

        /**
         * Parse operation from Debezium format (case-insensitive)
         */
        public static Operation fromString(String value) {
            if (value == null) {
                throw new IllegalArgumentException("Operation cannot be null");
            }
            try {
                return Operation.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    String.format("Invalid operation: %s. Must be INSERT, UPDATE, or DELETE", value)
                );
            }
        }
    }

    /**
     * Factory method to create a new CDC message with auto-generated ID
     */
    public static CdcMessage create(
        String topic,
        TableInfo tableInfo,
        Operation operation,
        Instant timestamp,
        Position position,
        Map<String, Object> beforeData,
        Map<String, Object> afterData,
        Metadata metadata
    ) {
        return CdcMessage.builder()
            .id(UUID.randomUUID())
            .topic(topic)
            .tableInfo(tableInfo)
            .operation(operation)
            .timestamp(timestamp)
            .position(position)
            .beforeData(beforeData)
            .afterData(afterData)
            .metadata(metadata)
            .build();
    }

    /**
     * Validates the CDC message based on operation type
     */
    public void validate() {
        switch (operation) {
            case INSERT -> {
                if (afterData == null || afterData.isEmpty()) {
                    throw new IllegalStateException("INSERT operation must have afterData");
                }
                if (beforeData != null) {
                    throw new IllegalStateException("INSERT operation should not have beforeData");
                }
            }
            case UPDATE -> {
                if (beforeData == null || beforeData.isEmpty()) {
                    throw new IllegalStateException("UPDATE operation must have beforeData");
                }
                if (afterData == null || afterData.isEmpty()) {
                    throw new IllegalStateException("UPDATE operation must have afterData");
                }
            }
            case DELETE -> {
                if (beforeData == null || beforeData.isEmpty()) {
                    throw new IllegalStateException("DELETE operation must have beforeData");
                }
                if (afterData != null) {
                    throw new IllegalStateException("DELETE operation should not have afterData");
                }
            }
        }
    }

    /**
     * Returns a summary string for logging
     */
    public String getSummary() {
        return String.format("CdcMessage[id=%s, operation=%s, table=%s, txId=%s]",
            id, operation, tableInfo.getFullyQualifiedName(), position.getOffset().getTxId());
    }

    @Override
    public String toString() {
        return getSummary();
    }
}
