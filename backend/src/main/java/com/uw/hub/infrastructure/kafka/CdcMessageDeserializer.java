package com.uw.hub.infrastructure.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.uw.hub.domain.entity.CdcMessage;
import com.uw.hub.domain.valueobject.Metadata;
import com.uw.hub.domain.valueobject.Position;
import com.uw.hub.domain.valueobject.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Deserializer for CDC messages from Kafka.
 * Parses the JSON format and converts to domain CdcMessage entity.
 *
 * Supports two formats:
 *
 * 1. Standard Debezium envelope format:
 * {
 *   "payload": {
 *     "before": {...},
 *     "after": {...},
 *     "source": {
 *       "db": "database",
 *       "schema": "public",
 *       "table": "customers",
 *       "lsn": 123456,
 *       "txId": 789
 *     },
 *     "op": "c|u|d", // create, update, delete
 *     "ts_ms": 1234567890
 *   }
 * }
 *
 * 2. Custom flat format:
 * {
 *   "table": {"database": "...", "schema": "...", "table": "..."},
 *   "operation": "INSERT|UPDATE|DELETE",
 *   "timestamp": "2025-11-03T03:42:32.864Z",
 *   "position": {"sourcePartition": "...", "offset": {...}},
 *   "before": {...},
 *   "after": {...},
 *   "metadata": {...}
 * }
 */
@Component
@Slf4j
public class CdcMessageDeserializer {

    private final ObjectMapper objectMapper;

    public CdcMessageDeserializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    /**
     * Deserialize CDC JSON message to domain CdcMessage
     * Supports both standard Debezium format and custom flat format
     */
    public CdcMessage deserialize(String topic, String jsonMessage) {
        try {
            log.debug("Deserializing CDC message from topic: {}", topic);
            JsonNode root = objectMapper.readTree(jsonMessage);

            // Check if this is standard Debezium format (has 'payload') or custom flat format
            JsonNode payload = root.get("payload");

            if (payload != null) {
                // Standard Debezium format
                return deserializeDebeziumFormat(topic, root, payload);
            } else {
                // Custom flat format
                return deserializeCustomFormat(topic, root);
            }

        } catch (Exception e) {
            log.error("Failed to deserialize CDC message from topic {}: {}", topic, jsonMessage, e);
            throw new IllegalArgumentException("Failed to parse CDC message", e);
        }
    }

    /**
     * Deserialize standard Debezium envelope format
     */
    private CdcMessage deserializeDebeziumFormat(String topic, JsonNode root, JsonNode payload) {
        // Extract table info from source
        JsonNode source = payload.get("source");
        TableInfo tableInfo = extractTableInfo(source);

        // Extract operation
        CdcMessage.Operation operation = extractOperation(payload);

        // Extract timestamp
        Instant timestamp = extractTimestamp(payload);

        // Extract position
        Position position = extractPosition(source);

        // Extract before/after data
        Map<String, Object> beforeData = extractJsonData(payload.get("before"));
        Map<String, Object> afterData = extractJsonData(payload.get("after"));

        // Extract metadata
        Metadata metadata = extractMetadata(source);

        // Create and validate CDC message
        CdcMessage message = CdcMessage.create(
            topic,
            tableInfo,
            operation,
            timestamp,
            position,
            beforeData,
            afterData,
            metadata
        );

        message.validate();
        log.debug("Successfully deserialized Debezium CDC message: {}", message.getSummary());
        return message;
    }

    /**
     * Deserialize custom flat format
     * Format: { "table": {...}, "operation": "...", "position": {...}, "before": {...}, "after": {...}, "metadata": {...} }
     */
    private CdcMessage deserializeCustomFormat(String topic, JsonNode root) {
        // Extract table info
        JsonNode tableNode = root.get("table");
        if (tableNode == null) {
            throw new IllegalArgumentException("Custom format missing 'table' field");
        }
        TableInfo tableInfo = new TableInfo(
            tableNode.get("database").asText(),
            tableNode.get("schema").asText(),
            tableNode.get("table").asText()
        );

        // Extract operation
        String operationStr = root.get("operation").asText();
        CdcMessage.Operation operation = CdcMessage.Operation.valueOf(operationStr);

        // Extract timestamp
        String timestampStr = root.get("timestamp").asText();
        Instant timestamp = Instant.parse(timestampStr);

        // Extract position
        Position position = extractCustomPosition(root.get("position"));

        // Extract before/after data
        Map<String, Object> beforeData = extractJsonData(root.get("before"));
        Map<String, Object> afterData = extractJsonData(root.get("after"));

        // Extract metadata
        Metadata metadata = extractCustomMetadata(root.get("metadata"));

        // Create and validate CDC message
        CdcMessage message = CdcMessage.create(
            topic,
            tableInfo,
            operation,
            timestamp,
            position,
            beforeData,
            afterData,
            metadata
        );

        message.validate();
        log.debug("Successfully deserialized custom CDC message: {}", message.getSummary());
        return message;
    }

    /**
     * Extract table information from source field
     */
    private TableInfo extractTableInfo(JsonNode source) {
        String database = source.get("db").asText();
        String schema = source.get("schema").asText();
        String table = source.get("table").asText();
        return new TableInfo(database, schema, table);
    }

    /**
     * Extract operation from Debezium op field
     * Debezium ops: c=create(INSERT), u=update(UPDATE), d=delete(DELETE)
     */
    private CdcMessage.Operation extractOperation(JsonNode payload) {
        String op = payload.get("op").asText();
        return switch (op) {
            case "c", "r" -> CdcMessage.Operation.INSERT; // c=create, r=read(snapshot)
            case "u" -> CdcMessage.Operation.UPDATE;
            case "d" -> CdcMessage.Operation.DELETE;
            default -> throw new IllegalArgumentException("Unknown Debezium operation: " + op);
        };
    }

    /**
     * Extract timestamp from Debezium ts_ms field
     */
    private Instant extractTimestamp(JsonNode payload) {
        long tsMs = payload.get("ts_ms").asLong();
        return Instant.ofEpochMilli(tsMs);
    }

    /**
     * Extract position metadata from source
     */
    private Position extractPosition(JsonNode source) {
        // Source partition (Debezium format)
        String sourcePartition = String.format("{server=%s}", source.get("name").asText("unknown"));

        // Offset details
        Long lsn = source.has("lsn") ? source.get("lsn").asLong() : null;
        Long lsnCommit = source.has("lsn_commit") ? source.get("lsn_commit").asLong() : null;
        Long txId = source.has("txId") ? source.get("txId").asLong() : null;
        Long timestamp = source.has("ts_ms") ? source.get("ts_ms").asLong() : null;
        Boolean snapshot = source.has("snapshot") ? source.get("snapshot").asBoolean() : null;

        Position.Offset offset = new Position.Offset(lsn, lsnCommit, txId, timestamp, snapshot, new HashMap<>());

        return new Position(sourcePartition, offset);
    }

    /**
     * Extract metadata from source
     */
    private Metadata extractMetadata(JsonNode source) {
        String connector = source.get("connector").asText("postgresql");
        String version = source.get("version").asText("unknown");
        String sourceName = source.get("name").asText("debezium");

        // Constructor: Metadata(schemaVersion, connector, source, version)
        return new Metadata("1.0.0", connector, sourceName, version);
    }

    /**
     * Convert JsonNode to Map<String, Object>
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractJsonData(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        try {
            return objectMapper.convertValue(node, Map.class);
        } catch (Exception e) {
            log.warn("Failed to convert JSON node to Map: {}", node, e);
            return new HashMap<>();
        }
    }

    /**
     * Extract position from custom format
     * Format: { "sourcePartition": "...", "offset": { "lsn": ..., "txId": ..., ... } }
     */
    private Position extractCustomPosition(JsonNode positionNode) {
        if (positionNode == null) {
            throw new IllegalArgumentException("Custom format missing 'position' field");
        }

        String sourcePartition = positionNode.get("sourcePartition").asText();
        JsonNode offsetNode = positionNode.get("offset");

        Long lsn = offsetNode.has("lsn") ? offsetNode.get("lsn").asLong() : null;
        Long lsnCommit = offsetNode.has("lsn_commit") ? offsetNode.get("lsn_commit").asLong() : null;
        Long txId = offsetNode.has("txId") ? offsetNode.get("txId").asLong() : null;
        Long timestamp = offsetNode.has("ts_usec") ? offsetNode.get("ts_usec").asLong() : null;
        Boolean snapshot = offsetNode.has("snapshot") ? offsetNode.get("snapshot").asBoolean() : null;

        // Extract additional offset fields
        Map<String, Object> additionalFields = new HashMap<>();
        if (offsetNode.has("lsn_proc")) {
            additionalFields.put("lsn_proc", offsetNode.get("lsn_proc").asLong());
        }
        if (offsetNode.has("messageType")) {
            additionalFields.put("messageType", offsetNode.get("messageType").asText());
        }

        Position.Offset offset = new Position.Offset(lsn, lsnCommit, txId, timestamp, snapshot, additionalFields);
        return new Position(sourcePartition, offset);
    }

    /**
     * Extract metadata from custom format
     * Format: { "version": "...", "connector": "...", "source": "...", "schemaVersion": ... }
     */
    private Metadata extractCustomMetadata(JsonNode metadataNode) {
        if (metadataNode == null) {
            // Return default metadata if not present
            // Constructor: Metadata(schemaVersion, connector, source, version)
            return new Metadata("1.0.0", "postgresql", "debezium-cdc-app", "unknown");
        }

        String schemaVersion = metadataNode.has("schemaVersion") ?
            String.valueOf(metadataNode.get("schemaVersion").asInt()) : "1.0.0";
        String connector = metadataNode.has("connector") ? metadataNode.get("connector").asText() : "postgresql";
        String source = metadataNode.has("source") ? metadataNode.get("source").asText() : "debezium-cdc-app";
        String version = metadataNode.has("version") ? metadataNode.get("version").asText() : "unknown";

        // Constructor: Metadata(schemaVersion, connector, source, version)
        return new Metadata(schemaVersion, connector, source, version);
    }
}
