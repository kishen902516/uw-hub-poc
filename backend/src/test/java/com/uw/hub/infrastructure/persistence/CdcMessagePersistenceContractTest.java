package com.uw.hub.infrastructure.persistence;

import com.uw.hub.domain.entity.CdcMessage;
import com.uw.hub.domain.valueobject.Metadata;
import com.uw.hub.domain.valueobject.Position;
import com.uw.hub.domain.valueobject.TableInfo;
import com.uw.hub.infrastructure.persistence.entity.CdcMessageEntity;
import com.uw.hub.infrastructure.persistence.mapper.CdcMessageMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract test for PostgreSQL JSONB storage of CDC messages.
 * Verifies that position, before_data, after_data, and metadata are correctly stored and retrieved as JSONB.
 *
 * Uses Testcontainers to spin up a real PostgreSQL instance for integration testing.
 *
 * @see CdcMessageEntity
 * @see CdcMessageJpaRepository
 */
@DataJpaTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@DisplayName("CDC Message PostgreSQL JSONB Storage Contract Tests")
class CdcMessagePersistenceContractTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("cdctest")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private CdcMessageJpaRepository repository;

    private CdcMessageMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new CdcMessageMapper();
    }

    @Test
    @DisplayName("Should store and retrieve INSERT message with JSONB after data")
    void shouldStoreAndRetrieveInsertMessage() {
        // Given: INSERT CDC message with after data
        Map<String, Object> afterData = new HashMap<>();
        afterData.put("customer_id", 1);
        afterData.put("first_name", "John");
        afterData.put("last_name", "Doe");
        afterData.put("email", "john.doe@example.com");

        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("txId", 751);
        offsetMap.put("lsn", 27696384);
        offsetMap.put("snapshot", true);

        CdcMessage message = CdcMessage.builder()
                .topic("cdcdb.public.customers")
                .tableInfo(new TableInfo("cdcdb", "public", "customers"))
                .operation("INSERT")
                .timestamp(Instant.parse("2025-11-02T04:45:54.287Z"))
                .position(new Position("{server=postgres-localhost-cdcdb}", offsetMap))
                .beforeData(null)
                .afterData(afterData)
                .metadata(new Metadata("1", "postgresql", "postgres-localhost-cdcdb", "1.0.0"))
                .build();

        CdcMessageEntity entity = mapper.toEntity(message);

        // When: Save to PostgreSQL
        CdcMessageEntity savedEntity = repository.save(entity);
        entityManager.flush();
        entityManager.clear();

        // Then: Retrieve and verify JSONB fields
        CdcMessageEntity retrievedEntity = repository.findById(savedEntity.getId()).orElseThrow();

        assertThat(retrievedEntity).isNotNull();
        assertThat(retrievedEntity.getOperation()).isEqualTo("INSERT");

        // Verify JSONB after_data
        assertThat(retrievedEntity.getAfterData()).isNotNull();
        assertThat(retrievedEntity.getAfterData()).containsEntry("customer_id", 1);
        assertThat(retrievedEntity.getAfterData()).containsEntry("first_name", "John");
        assertThat(retrievedEntity.getAfterData()).containsEntry("last_name", "Doe");

        // Verify JSONB position
        assertThat(retrievedEntity.getPosition()).isNotNull();
        assertThat(retrievedEntity.getPosition()).containsEntry("sourcePartition", "{server=postgres-localhost-cdcdb}");
        assertThat(retrievedEntity.getPosition()).containsKey("offset");

        // Verify before_data is null for INSERT
        assertThat(retrievedEntity.getBeforeData()).isNull();
    }

    @Test
    @DisplayName("Should store and retrieve UPDATE message with JSONB before and after data")
    void shouldStoreAndRetrieveUpdateMessage() {
        // Given: UPDATE CDC message with both before and after data
        Map<String, Object> beforeData = new HashMap<>();
        beforeData.put("customer_id", 6);
        beforeData.put("last_name", "Sivalingam 2");

        Map<String, Object> afterData = new HashMap<>();
        afterData.put("customer_id", 6);
        afterData.put("last_name", "Sivalingam Name change");

        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("txId", 759);
        offsetMap.put("lsn_commit", 27709280);

        CdcMessage message = CdcMessage.builder()
                .topic("cdcdb.public.customers")
                .tableInfo(new TableInfo("cdcdb", "public", "customers"))
                .operation("UPDATE")
                .timestamp(Instant.parse("2025-11-02T08:54:07.298Z"))
                .position(new Position("{server=postgres-localhost-cdcdb}", offsetMap))
                .beforeData(beforeData)
                .afterData(afterData)
                .metadata(new Metadata("1", "postgresql", "postgres-localhost-cdcdb", "1.0.0"))
                .build();

        CdcMessageEntity entity = mapper.toEntity(message);

        // When: Save to PostgreSQL
        CdcMessageEntity savedEntity = repository.save(entity);
        entityManager.flush();
        entityManager.clear();

        // Then: Retrieve and verify both before and after JSONB fields
        CdcMessageEntity retrievedEntity = repository.findById(savedEntity.getId()).orElseThrow();

        assertThat(retrievedEntity.getOperation()).isEqualTo("UPDATE");

        // Verify before_data JSONB
        assertThat(retrievedEntity.getBeforeData()).isNotNull();
        assertThat(retrievedEntity.getBeforeData()).containsEntry("customer_id", 6);
        assertThat(retrievedEntity.getBeforeData()).containsEntry("last_name", "Sivalingam 2");

        // Verify after_data JSONB
        assertThat(retrievedEntity.getAfterData()).isNotNull();
        assertThat(retrievedEntity.getAfterData()).containsEntry("customer_id", 6);
        assertThat(retrievedEntity.getAfterData()).containsEntry("last_name", "Sivalingam Name change");

        // Verify field-level change detection
        assertThat(retrievedEntity.getBeforeData().get("last_name"))
                .isNotEqualTo(retrievedEntity.getAfterData().get("last_name"));
    }

    @Test
    @DisplayName("Should store and retrieve DELETE message with JSONB before data")
    void shouldStoreAndRetrieveDeleteMessage() {
        // Given: DELETE CDC message with before data only
        Map<String, Object> beforeData = new HashMap<>();
        beforeData.put("customer_id", 99);
        beforeData.put("first_name", "Test");
        beforeData.put("last_name", "User");

        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("txId", 765);
        offsetMap.put("lsn_commit", 27715000);

        CdcMessage message = CdcMessage.builder()
                .topic("cdcdb.public.customers")
                .tableInfo(new TableInfo("cdcdb", "public", "customers"))
                .operation("DELETE")
                .timestamp(Instant.parse("2025-11-02T10:15:30.123Z"))
                .position(new Position("{server=postgres-localhost-cdcdb}", offsetMap))
                .beforeData(beforeData)
                .afterData(null)
                .metadata(new Metadata("1", "postgresql", "postgres-localhost-cdcdb", "1.0.0"))
                .build();

        CdcMessageEntity entity = mapper.toEntity(message);

        // When: Save to PostgreSQL
        CdcMessageEntity savedEntity = repository.save(entity);
        entityManager.flush();
        entityManager.clear();

        // Then: Retrieve and verify before_data JSONB
        CdcMessageEntity retrievedEntity = repository.findById(savedEntity.getId()).orElseThrow();

        assertThat(retrievedEntity.getOperation()).isEqualTo("DELETE");

        // Verify before_data JSONB
        assertThat(retrievedEntity.getBeforeData()).isNotNull();
        assertThat(retrievedEntity.getBeforeData()).containsEntry("customer_id", 99);
        assertThat(retrievedEntity.getBeforeData()).containsEntry("first_name", "Test");

        // Verify after_data is null for DELETE
        assertThat(retrievedEntity.getAfterData()).isNull();
    }

    @Test
    @DisplayName("Should handle complex nested JSONB structures")
    void shouldHandleComplexNestedJsonb() {
        // Given: CDC message with nested JSON in after_data
        Map<String, Object> afterData = new HashMap<>();
        afterData.put("customer_id", 1);

        Map<String, Object> address = new HashMap<>();
        address.put("street", "123 Main St");
        address.put("city", "Seattle");
        address.put("state", "WA");
        address.put("zip", "98101");
        afterData.put("address", address);

        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("txId", 800);

        CdcMessage message = CdcMessage.builder()
                .topic("cdcdb.public.customers")
                .tableInfo(new TableInfo("cdcdb", "public", "customers"))
                .operation("INSERT")
                .timestamp(Instant.now())
                .position(new Position("{server=postgres-localhost-cdcdb}", offsetMap))
                .afterData(afterData)
                .metadata(new Metadata("1", "postgresql", "postgres-localhost-cdcdb", "1.0.0"))
                .build();

        CdcMessageEntity entity = mapper.toEntity(message);

        // When: Save to PostgreSQL
        CdcMessageEntity savedEntity = repository.save(entity);
        entityManager.flush();
        entityManager.clear();

        // Then: Retrieve and verify nested JSONB structure
        CdcMessageEntity retrievedEntity = repository.findById(savedEntity.getId()).orElseThrow();

        assertThat(retrievedEntity.getAfterData()).isNotNull();
        assertThat(retrievedEntity.getAfterData()).containsKey("address");

        @SuppressWarnings("unchecked")
        Map<String, Object> retrievedAddress = (Map<String, Object>) retrievedEntity.getAfterData().get("address");
        assertThat(retrievedAddress).isNotNull();
        assertThat(retrievedAddress).containsEntry("street", "123 Main St");
        assertThat(retrievedAddress).containsEntry("city", "Seattle");
        assertThat(retrievedAddress).containsEntry("state", "WA");
        assertThat(retrievedAddress).containsEntry("zip", "98101");
    }

    @Test
    @DisplayName("Should query by operation using index")
    void shouldQueryByOperation() {
        // Given: Multiple CDC messages with different operations
        saveSampleMessage("INSERT", null, Map.of("id", 1));
        saveSampleMessage("UPDATE", Map.of("id", 2), Map.of("id", 2, "name", "Updated"));
        saveSampleMessage("DELETE", Map.of("id", 3), null);
        saveSampleMessage("INSERT", null, Map.of("id", 4));

        entityManager.flush();
        entityManager.clear();

        // When: Query by operation
        List<CdcMessageEntity> insertMessages = repository.findByOperation("INSERT");
        List<CdcMessageEntity> updateMessages = repository.findByOperation("UPDATE");
        List<CdcMessageEntity> deleteMessages = repository.findByOperation("DELETE");

        // Then: Verify correct filtering
        assertThat(insertMessages).hasSize(2);
        assertThat(updateMessages).hasSize(1);
        assertThat(deleteMessages).hasSize(1);
    }

    @Test
    @DisplayName("Should query by table name using index")
    void shouldQueryByTableName() {
        // Given: Messages for different tables
        saveSampleMessageForTable("cdcdb", "public", "customers", "INSERT");
        saveSampleMessageForTable("cdcdb", "public", "orders", "INSERT");
        saveSampleMessageForTable("cdcdb", "public", "customers", "UPDATE");

        entityManager.flush();
        entityManager.clear();

        // When: Query by table name
        List<CdcMessageEntity> customerMessages = repository.findByTableName("customers");
        List<CdcMessageEntity> orderMessages = repository.findByTableName("orders");

        // Then: Verify correct filtering
        assertThat(customerMessages).hasSize(2);
        assertThat(orderMessages).hasSize(1);
    }

    // Helper methods

    private void saveSampleMessage(String operation, Map<String, Object> beforeData, Map<String, Object> afterData) {
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("txId", (int) (Math.random() * 1000));

        CdcMessage message = CdcMessage.builder()
                .topic("cdcdb.public.test")
                .tableInfo(new TableInfo("cdcdb", "public", "test"))
                .operation(operation)
                .timestamp(Instant.now())
                .position(new Position("{server=test}", offsetMap))
                .beforeData(beforeData)
                .afterData(afterData)
                .metadata(new Metadata("1", "postgresql", "test", "1.0.0"))
                .build();

        CdcMessageEntity entity = mapper.toEntity(message);
        repository.save(entity);
    }

    private void saveSampleMessageForTable(String database, String schema, String table, String operation) {
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("txId", (int) (Math.random() * 1000));

        CdcMessage message = CdcMessage.builder()
                .topic(database + "." + schema + "." + table)
                .tableInfo(new TableInfo(database, schema, table))
                .operation(operation)
                .timestamp(Instant.now())
                .position(new Position("{server=test}", offsetMap))
                .afterData(Map.of("id", 1))
                .metadata(new Metadata("1", "postgresql", "test", "1.0.0"))
                .build();

        CdcMessageEntity entity = mapper.toEntity(message);
        repository.save(entity);
    }
}
