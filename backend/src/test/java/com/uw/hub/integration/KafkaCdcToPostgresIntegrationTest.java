package com.uw.hub.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uw.hub.domain.entity.CdcMessage;
import com.uw.hub.infrastructure.persistence.CdcMessageJpaRepository;
import com.uw.hub.infrastructure.persistence.entity.CdcMessageEntity;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for Kafka CDC → PostgreSQL flow.
 *
 * Tests the complete pipeline:
 * 1. Produce CDC message to Kafka topic
 * 2. Consumer picks up message and deserializes it
 * 3. Message is persisted to PostgreSQL with JSONB fields
 * 4. Verify data integrity end-to-end
 *
 * Uses Testcontainers for PostgreSQL and EmbeddedKafka for Kafka.
 */
@SpringBootTest
@Testcontainers
@EmbeddedKafka(
    partitions = 1,
    topics = {"cdcdb.public.customers", "cdcdb.public.orders"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:9092",
        "port=9092"
    }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@DisplayName("Kafka CDC to PostgreSQL Integration Tests")
class KafkaCdcToPostgresIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("cdctest")
            .withUsername("test")
            .withPassword("test");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // PostgreSQL
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");

        // Kafka
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:9092");
        registry.add("spring.kafka.consumer.group-id", () -> "test-consumer-group");
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    @Autowired
    private CdcMessageJpaRepository repository;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaProducer<String, String> kafkaProducer;

    @BeforeEach
    void setUp() {
        // Clear database
        repository.deleteAll();

        // Configure Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        kafkaProducer = new KafkaProducer<>(props);
    }

    @Test
    @DisplayName("Should consume INSERT message from Kafka and persist to PostgreSQL")
    void shouldConsumeInsertMessageAndPersistToPostgres() throws Exception {
        // Given: Debezium CDC INSERT message
        String insertMessage = """
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

        String topic = "cdcdb.public.customers";
        String key = "customer:1";

        // When: Produce message to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, insertMessage);
        kafkaProducer.send(record).get(10, TimeUnit.SECONDS);
        kafkaProducer.flush();

        // Then: Wait for consumer to process and verify PostgreSQL storage
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(repository.count()).isEqualTo(1);
            });

        CdcMessageEntity savedMessage = repository.findAll().get(0);
        assertThat(savedMessage).isNotNull();
        assertThat(savedMessage.getTopic()).isEqualTo(topic);
        assertThat(savedMessage.getOperation()).isEqualTo("INSERT");
        assertThat(savedMessage.getTableName()).isEqualTo("customers");
        assertThat(savedMessage.getDatabase()).isEqualTo("cdcdb");
        assertThat(savedMessage.getSchema()).isEqualTo("public");

        // Verify JSONB after_data
        assertThat(savedMessage.getAfterData()).isNotNull();
        assertThat(savedMessage.getAfterData()).containsEntry("customer_id", 1);
        assertThat(savedMessage.getAfterData()).containsEntry("first_name", "John");
        assertThat(savedMessage.getAfterData()).containsEntry("last_name", "Doe");

        // Verify JSONB position
        assertThat(savedMessage.getPosition()).isNotNull();
        assertThat(savedMessage.getPosition()).containsKey("offset");

        // Verify before_data is null for INSERT
        assertThat(savedMessage.getBeforeData()).isNull();
    }

    @Test
    @DisplayName("Should consume UPDATE message from Kafka and persist both before/after to PostgreSQL")
    void shouldConsumeUpdateMessageAndPersistToPostgres() throws Exception {
        // Given: Debezium CDC UPDATE message
        String updateMessage = """
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

        String topic = "cdcdb.public.customers";
        String key = "customer:6";

        // When: Produce message to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, updateMessage);
        kafkaProducer.send(record).get(10, TimeUnit.SECONDS);
        kafkaProducer.flush();

        // Then: Wait for consumer to process and verify PostgreSQL storage
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(repository.count()).isEqualTo(1);
            });

        CdcMessageEntity savedMessage = repository.findAll().get(0);
        assertThat(savedMessage.getOperation()).isEqualTo("UPDATE");

        // Verify both before and after data exist
        assertThat(savedMessage.getBeforeData()).isNotNull();
        assertThat(savedMessage.getAfterData()).isNotNull();

        // Verify before state
        assertThat(savedMessage.getBeforeData()).containsEntry("customer_id", 6);
        assertThat(savedMessage.getBeforeData()).containsEntry("last_name", "Sivalingam 2");

        // Verify after state
        assertThat(savedMessage.getAfterData()).containsEntry("customer_id", 6);
        assertThat(savedMessage.getAfterData()).containsEntry("last_name", "Sivalingam Name change");

        // Verify changed field
        assertThat(savedMessage.getBeforeData().get("last_name"))
            .isNotEqualTo(savedMessage.getAfterData().get("last_name"));
    }

    @Test
    @DisplayName("Should consume DELETE message from Kafka and persist before data to PostgreSQL")
    void shouldConsumeDeleteMessageAndPersistToPostgres() throws Exception {
        // Given: Debezium CDC DELETE message
        String deleteMessage = """
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

        String topic = "cdcdb.public.customers";
        String key = "customer:99";

        // When: Produce message to Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, deleteMessage);
        kafkaProducer.send(record).get(10, TimeUnit.SECONDS);
        kafkaProducer.flush();

        // Then: Wait for consumer to process and verify PostgreSQL storage
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(repository.count()).isEqualTo(1);
            });

        CdcMessageEntity savedMessage = repository.findAll().get(0);
        assertThat(savedMessage.getOperation()).isEqualTo("DELETE");

        // Verify before_data exists
        assertThat(savedMessage.getBeforeData()).isNotNull();
        assertThat(savedMessage.getBeforeData()).containsEntry("customer_id", 99);
        assertThat(savedMessage.getBeforeData()).containsEntry("first_name", "Test");

        // Verify after_data is null for DELETE
        assertThat(savedMessage.getAfterData()).isNull();
    }

    @Test
    @DisplayName("Should handle multiple CDC messages in sequence")
    void shouldHandleMultipleCdcMessagesInSequence() throws Exception {
        // Given: Multiple CDC messages (INSERT → UPDATE → DELETE)
        String insertMessage = createCdcMessage("INSERT", null, Map.of("id", 1, "name", "Test"));
        String updateMessage = createCdcMessage("UPDATE",
                Map.of("id", 1, "name", "Test"),
                Map.of("id", 1, "name", "Updated"));
        String deleteMessage = createCdcMessage("DELETE", Map.of("id", 1, "name", "Updated"), null);

        String topic = "cdcdb.public.customers";

        // When: Produce messages in sequence
        kafkaProducer.send(new ProducerRecord<>(topic, "1", insertMessage)).get(10, TimeUnit.SECONDS);
        kafkaProducer.send(new ProducerRecord<>(topic, "1", updateMessage)).get(10, TimeUnit.SECONDS);
        kafkaProducer.send(new ProducerRecord<>(topic, "1", deleteMessage)).get(10, TimeUnit.SECONDS);
        kafkaProducer.flush();

        // Then: Wait for all messages to be processed
        await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(repository.count()).isEqualTo(3);
            });

        // Verify operations are stored correctly
        assertThat(repository.findByOperation("INSERT")).hasSize(1);
        assertThat(repository.findByOperation("UPDATE")).hasSize(1);
        assertThat(repository.findByOperation("DELETE")).hasSize(1);
    }

    @Test
    @DisplayName("Should handle messages from different topics")
    void shouldHandleMessagesFromDifferentTopics() throws Exception {
        // Given: CDC messages for different tables/topics
        String customersMessage = createCdcMessageForTable("cdcdb", "public", "customers", "INSERT");
        String ordersMessage = createCdcMessageForTable("cdcdb", "public", "orders", "INSERT");

        // When: Produce messages to different topics
        kafkaProducer.send(new ProducerRecord<>("cdcdb.public.customers", "1", customersMessage))
                .get(10, TimeUnit.SECONDS);
        kafkaProducer.send(new ProducerRecord<>("cdcdb.public.orders", "1", ordersMessage))
                .get(10, TimeUnit.SECONDS);
        kafkaProducer.flush();

        // Then: Wait for both messages to be processed
        await()
            .atMost(Duration.ofSeconds(15))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                assertThat(repository.count()).isEqualTo(2);
            });

        // Verify messages are stored for different tables
        assertThat(repository.findByTableName("customers")).hasSize(1);
        assertThat(repository.findByTableName("orders")).hasSize(1);
    }

    // Helper methods

    private String createCdcMessage(String operation, Map<String, Object> before, Map<String, Object> after) throws Exception {
        Map<String, Object> message = new HashMap<>();
        message.put("table", Map.of("database", "cdcdb", "schema", "public", "table", "customers"));
        message.put("operation", operation);
        message.put("timestamp", "2025-11-02T12:00:00.000Z");
        message.put("position", Map.of(
                "sourcePartition", "{server=test}",
                "offset", Map.of("txId", (int) (Math.random() * 1000))
        ));
        if (before != null) {
            message.put("before", before);
        }
        if (after != null) {
            message.put("after", after);
        }
        message.put("metadata", Map.of("version", "1.0.0", "connector", "postgresql"));

        return objectMapper.writeValueAsString(message);
    }

    private String createCdcMessageForTable(String database, String schema, String table, String operation) throws Exception {
        Map<String, Object> message = new HashMap<>();
        message.put("table", Map.of("database", database, "schema", schema, "table", table));
        message.put("operation", operation);
        message.put("timestamp", "2025-11-02T12:00:00.000Z");
        message.put("position", Map.of(
                "sourcePartition", "{server=test}",
                "offset", Map.of("txId", (int) (Math.random() * 1000))
        ));
        message.put("after", Map.of("id", 1));
        message.put("metadata", Map.of("version", "1.0.0", "connector", "postgresql"));

        return objectMapper.writeValueAsString(message);
    }
}
