package com.uw.hub.infrastructure.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uw.hub.domain.entity.CdcMessage;
import com.uw.hub.domain.valueobject.Metadata;
import com.uw.hub.domain.valueobject.Position;
import com.uw.hub.domain.valueobject.TableInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Contract test for Debezium CDC message deserialization.
 * Tests INSERT, UPDATE, and DELETE message formats to ensure compliance with Debezium CDC format.
 *
 * @see CdcMessageDeserializer
 */
@DisplayName("CDC Message Deserialization Contract Tests")
class CdcMessageDeserializationTest {

    private CdcMessageDeserializer deserializer;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        deserializer = new CdcMessageDeserializer();
    }

    @Test
    @DisplayName("Should deserialize INSERT operation with after data only")
    void shouldDeserializeInsertOperation() throws Exception {
        // Given: Debezium CDC INSERT message (from tasks.md example)
        String insertJson = """
            {
              "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
              "operation": "INSERT",
              "timestamp": "2025-11-02T04:45:54.287Z",
              "position": {
                "sourcePartition": "{server=postgres-localhost-cdcdb}",
                "offset": { "txId": 751, "lsn": 27696384, "snapshot": true }
              },
              "after": {
                "customer_id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com"
              },
              "metadata": {
                "version": "1.0.0",
                "connector": "postgresql",
                "schemaVersion": "1",
                "source": "postgres-localhost-cdcdb"
              }
            }
            """;

        // When: Deserialize the message
        CdcMessage message = deserializer.deserialize("cdcdb.public.customers", insertJson.getBytes());

        // Then: Verify all fields are correctly deserialized
        assertThat(message).isNotNull();

        // Table info
        assertThat(message.getTableInfo().getDatabase()).isEqualTo("cdcdb");
        assertThat(message.getTableInfo().getSchema()).isEqualTo("public");
        assertThat(message.getTableInfo().getTable()).isEqualTo("customers");

        // Operation
        assertThat(message.getOperation()).isEqualTo("INSERT");

        // Timestamp
        assertThat(message.getTimestamp()).isNotNull();
        assertThat(message.getTimestamp()).isEqualTo(Instant.parse("2025-11-02T04:45:54.287Z"));

        // Position
        assertThat(message.getPosition()).isNotNull();
        assertThat(message.getPosition().getSourcePartition()).isEqualTo("{server=postgres-localhost-cdcdb}");
        assertThat(message.getPosition().getOffset()).containsEntry("txId", 751);
        assertThat(message.getPosition().getOffset()).containsEntry("lsn", 27696384);
        assertThat(message.getPosition().getOffset()).containsEntry("snapshot", true);

        // After data (should exist for INSERT)
        assertThat(message.getAfterData()).isNotNull();
        assertThat(message.getAfterData()).containsEntry("customer_id", 1);
        assertThat(message.getAfterData()).containsEntry("first_name", "John");
        assertThat(message.getAfterData()).containsEntry("last_name", "Doe");
        assertThat(message.getAfterData()).containsEntry("email", "john.doe@example.com");

        // Before data (should be null for INSERT)
        assertThat(message.getBeforeData()).isNull();

        // Metadata
        assertThat(message.getMetadata()).isNotNull();
        assertThat(message.getMetadata().getVersion()).isEqualTo("1.0.0");
        assertThat(message.getMetadata().getConnector()).isEqualTo("postgresql");
    }

    @Test
    @DisplayName("Should deserialize UPDATE operation with before and after data")
    void shouldDeserializeUpdateOperation() throws Exception {
        // Given: Debezium CDC UPDATE message (from tasks.md example)
        String updateJson = """
            {
              "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
              "operation": "UPDATE",
              "timestamp": "2025-11-02T08:54:07.298Z",
              "position": {
                "sourcePartition": "{server=postgres-localhost-cdcdb}",
                "offset": { "lsn_commit": 27709280, "txId": 759 }
              },
              "before": {
                "customer_id": 6,
                "first_name": "Kumar",
                "last_name": "Sivalingam 2",
                "email": "kumar@example.com"
              },
              "after": {
                "customer_id": 6,
                "first_name": "Kumar",
                "last_name": "Sivalingam Name change",
                "email": "kumar@example.com"
              },
              "metadata": {
                "version": "1.0.0",
                "connector": "postgresql",
                "schemaVersion": "1",
                "source": "postgres-localhost-cdcdb"
              }
            }
            """;

        // When: Deserialize the message
        CdcMessage message = deserializer.deserialize("cdcdb.public.customers", updateJson.getBytes());

        // Then: Verify UPDATE-specific fields
        assertThat(message).isNotNull();
        assertThat(message.getOperation()).isEqualTo("UPDATE");

        // Both before and after should exist for UPDATE
        assertThat(message.getBeforeData()).isNotNull();
        assertThat(message.getAfterData()).isNotNull();

        // Verify before state
        assertThat(message.getBeforeData()).containsEntry("customer_id", 6);
        assertThat(message.getBeforeData()).containsEntry("last_name", "Sivalingam 2");

        // Verify after state
        assertThat(message.getAfterData()).containsEntry("customer_id", 6);
        assertThat(message.getAfterData()).containsEntry("last_name", "Sivalingam Name change");

        // Verify changed field detection (last_name changed, others stayed same)
        assertThat(message.getBeforeData().get("first_name")).isEqualTo(message.getAfterData().get("first_name"));
        assertThat(message.getBeforeData().get("last_name")).isNotEqualTo(message.getAfterData().get("last_name"));
    }

    @Test
    @DisplayName("Should deserialize DELETE operation with before data only")
    void shouldDeserializeDeleteOperation() throws Exception {
        // Given: Debezium CDC DELETE message
        String deleteJson = """
            {
              "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
              "operation": "DELETE",
              "timestamp": "2025-11-02T10:15:30.123Z",
              "position": {
                "sourcePartition": "{server=postgres-localhost-cdcdb}",
                "offset": { "lsn_commit": 27715000, "txId": 765 }
              },
              "before": {
                "customer_id": 99,
                "first_name": "Test",
                "last_name": "User",
                "email": "test@example.com"
              },
              "metadata": {
                "version": "1.0.0",
                "connector": "postgresql",
                "schemaVersion": "1",
                "source": "postgres-localhost-cdcdb"
              }
            }
            """;

        // When: Deserialize the message
        CdcMessage message = deserializer.deserialize("cdcdb.public.customers", deleteJson.getBytes());

        // Then: Verify DELETE-specific fields
        assertThat(message).isNotNull();
        assertThat(message.getOperation()).isEqualTo("DELETE");

        // Before data should exist for DELETE
        assertThat(message.getBeforeData()).isNotNull();
        assertThat(message.getBeforeData()).containsEntry("customer_id", 99);
        assertThat(message.getBeforeData()).containsEntry("first_name", "Test");

        // After data should be null for DELETE
        assertThat(message.getAfterData()).isNull();
    }

    @Test
    @DisplayName("Should throw exception for malformed JSON")
    void shouldThrowExceptionForMalformedJson() {
        // Given: Invalid JSON
        String malformedJson = "{ invalid json }";

        // When/Then: Should throw exception
        assertThatThrownBy(() ->
            deserializer.deserialize("test.topic", malformedJson.getBytes())
        ).isInstanceOf(RuntimeException.class)
         .hasMessageContaining("Failed to deserialize CDC message");
    }

    @Test
    @DisplayName("Should throw exception for missing required fields")
    void shouldThrowExceptionForMissingRequiredFields() {
        // Given: JSON missing 'operation' field
        String incompleteJson = """
            {
              "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
              "timestamp": "2025-11-02T04:45:54.287Z"
            }
            """;

        // When/Then: Should throw exception
        assertThatThrownBy(() ->
            deserializer.deserialize("test.topic", incompleteJson.getBytes())
        ).isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("Should handle null topic gracefully")
    void shouldHandleNullTopic() throws Exception {
        // Given: Valid CDC message with null topic
        String validJson = """
            {
              "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
              "operation": "INSERT",
              "timestamp": "2025-11-02T04:45:54.287Z",
              "position": {
                "sourcePartition": "{server=postgres-localhost-cdcdb}",
                "offset": { "txId": 751 }
              },
              "after": { "customer_id": 1 },
              "metadata": { "version": "1.0.0", "connector": "postgresql" }
            }
            """;

        // When: Deserialize with null topic
        CdcMessage message = deserializer.deserialize(null, validJson.getBytes());

        // Then: Should still deserialize successfully
        assertThat(message).isNotNull();
        assertThat(message.getTopic()).isNull();
    }

    @Test
    @DisplayName("Should preserve topic from parameter")
    void shouldPreserveTopicFromParameter() throws Exception {
        // Given: Valid CDC message
        String validJson = """
            {
              "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
              "operation": "INSERT",
              "timestamp": "2025-11-02T04:45:54.287Z",
              "position": {
                "sourcePartition": "{server=postgres-localhost-cdcdb}",
                "offset": { "txId": 751 }
              },
              "after": { "customer_id": 1 },
              "metadata": { "version": "1.0.0", "connector": "postgresql" }
            }
            """;

        String topicName = "cdcdb.public.customers";

        // When: Deserialize
        CdcMessage message = deserializer.deserialize(topicName, validJson.getBytes());

        // Then: Topic should be set from parameter
        assertThat(message.getTopic()).isEqualTo(topicName);
    }
}
