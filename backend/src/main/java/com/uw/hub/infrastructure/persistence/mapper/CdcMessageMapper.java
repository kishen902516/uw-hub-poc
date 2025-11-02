package com.uw.hub.infrastructure.persistence.mapper;

import com.uw.hub.domain.entity.CdcMessage;
import com.uw.hub.domain.valueobject.Metadata;
import com.uw.hub.domain.valueobject.Position;
import com.uw.hub.domain.valueobject.TableInfo;
import com.uw.hub.infrastructure.persistence.entity.CdcMessageEntity;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapper for converting between domain CdcMessage and persistence CdcMessageEntity.
 * Follows the adapter pattern for infrastructure layer.
 */
@Component
public class CdcMessageMapper {

    /**
     * Convert domain entity to JPA entity
     */
    public CdcMessageEntity toEntity(CdcMessage domain) {
        if (domain == null) {
            return null;
        }

        return CdcMessageEntity.builder()
            .id(domain.getId())
            .topic(domain.getTopic())
            .databaseName(domain.getTableInfo().getDatabase())
            .schemaName(domain.getTableInfo().getSchema())
            .tableName(domain.getTableInfo().getTable())
            .operation(mapOperation(domain.getOperation()))
            .timestamp(domain.getTimestamp())
            .position(mapPositionToMap(domain.getPosition()))
            .beforeData(domain.getBeforeData())
            .afterData(domain.getAfterData())
            .metadata(mapMetadataToMap(domain.getMetadata()))
            .createdAt(domain.getTimestamp())
            .build();
    }

    /**
     * Convert JPA entity to domain entity
     */
    public CdcMessage toDomain(CdcMessageEntity entity) {
        if (entity == null) {
            return null;
        }

        return CdcMessage.builder()
            .id(entity.getId())
            .topic(entity.getTopic())
            .tableInfo(new TableInfo(
                entity.getDatabaseName(),
                entity.getSchemaName(),
                entity.getTableName()
            ))
            .operation(mapOperationType(entity.getOperation()))
            .timestamp(entity.getTimestamp())
            .position(mapMapToPosition(entity.getPosition()))
            .beforeData(entity.getBeforeData())
            .afterData(entity.getAfterData())
            .metadata(mapMapToMetadata(entity.getMetadata()))
            .build();
    }

    /**
     * Map domain Operation to entity OperationType
     */
    private CdcMessageEntity.OperationType mapOperation(CdcMessage.Operation operation) {
        return CdcMessageEntity.OperationType.valueOf(operation.name());
    }

    /**
     * Map entity OperationType to domain Operation
     */
    private CdcMessage.Operation mapOperationType(CdcMessageEntity.OperationType operationType) {
        return CdcMessage.Operation.valueOf(operationType.name());
    }

    /**
     * Map Position value object to Map for JSONB storage
     */
    private Map<String, Object> mapPositionToMap(Position position) {
        Map<String, Object> map = new HashMap<>();
        map.put("sourcePartition", position.getSourcePartition());

        Map<String, Object> offsetMap = new HashMap<>();
        Position.Offset offset = position.getOffset();
        if (offset.getLsn() != null) offsetMap.put("lsn", offset.getLsn());
        if (offset.getLsnCommit() != null) offsetMap.put("lsn_commit", offset.getLsnCommit());
        if (offset.getTxId() != null) offsetMap.put("txId", offset.getTxId());
        if (offset.getTimestamp() != null) offsetMap.put("timestamp", offset.getTimestamp());
        if (offset.getSnapshot() != null) offsetMap.put("snapshot", offset.getSnapshot());
        if (offset.getAdditionalProperties() != null) {
            offsetMap.putAll(offset.getAdditionalProperties());
        }

        map.put("offset", offsetMap);
        return map;
    }

    /**
     * Map Map to Position value object
     */
    @SuppressWarnings("unchecked")
    private Position mapMapToPosition(Map<String, Object> map) {
        String sourcePartition = (String) map.get("sourcePartition");
        Map<String, Object> offsetMap = (Map<String, Object>) map.get("offset");

        Position.Offset offset = new Position.Offset(
            getLongValue(offsetMap, "lsn"),
            getLongValue(offsetMap, "lsn_commit"),
            getLongValue(offsetMap, "txId"),
            getLongValue(offsetMap, "timestamp"),
            getBooleanValue(offsetMap, "snapshot"),
            offsetMap // Store all properties for flexibility
        );

        return new Position(sourcePartition, offset);
    }

    /**
     * Map Metadata value object to Map for JSONB storage
     */
    private Map<String, Object> mapMetadataToMap(Metadata metadata) {
        Map<String, Object> map = new HashMap<>();
        map.put("schemaVersion", metadata.getSchemaVersion());
        map.put("connector", metadata.getConnector());
        map.put("source", metadata.getSource());
        map.put("version", metadata.getVersion());
        return map;
    }

    /**
     * Map Map to Metadata value object
     */
    private Metadata mapMapToMetadata(Map<String, Object> map) {
        return new Metadata(
            (String) map.get("schemaVersion"),
            (String) map.get("connector"),
            (String) map.get("source"),
            (String) map.get("version")
        );
    }

    /**
     * Helper to safely extract Long value from Map
     */
    private Long getLongValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Long) return (Long) value;
        if (value instanceof Integer) return ((Integer) value).longValue();
        if (value instanceof String) return Long.parseLong((String) value);
        return null;
    }

    /**
     * Helper to safely extract Boolean value from Map
     */
    private Boolean getBooleanValue(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) return null;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof String) return Boolean.parseBoolean((String) value);
        return null;
    }
}
