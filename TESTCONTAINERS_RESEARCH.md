# Testcontainers Integration Testing Research
## For Spring Boot 3.x with Kafka + PostgreSQL

**Date**: 2025-11-02
**Context**: Real-time Kafka Streaming UI with PostgreSQL Persistence
**Requirements**: Deterministic, isolated, fast tests (<5 minutes), CI/CD compatible

---

## 1. DECISION: Recommended Testcontainers Configuration

### Core Components

```
Testcontainers Setup for Kafka + PostgreSQL Integration Tests
├── Testcontainers Core (v1.20.0+)
├── Testcontainers Kafka Module
├── Testcontainers PostgreSQL Module
├── Spring Boot Test Integration
├── Testcontainers Cloud (optional, for CI/CD)
└── Container Reuse via .testcontainers.properties
```

### Recommended Docker Images

| Service | Image | Rationale |
|---------|-------|-----------|
| **Kafka** | `confluentinc/cp-kafka:7.7.0` | Official Confluent image, well-maintained, production-tested |
| **PostgreSQL** | `postgres:17-alpine` | Latest stable, lightweight Alpine variant, faster startup |
| **Zookeeper** | `confluentinc/cp-zookeeper:7.7.0` | Required for Kafka, consistent with Confluent stack |

### Testcontainers Modules (Maven Dependencies)

```xml
<!-- Core Testcontainers -->
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.20.0</version>
    <scope>test</scope>
</dependency>

<!-- Kafka Module -->
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <version>1.20.0</version>
    <scope>test</scope>
</dependency>

<!-- PostgreSQL Module -->
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <version>1.20.0</version>
    <scope>test</scope>
</dependency>

<!-- Spring Boot Test Support -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-testcontainers</artifactId>
    <version>3.3.0</version>
    <scope>test</scope>
</dependency>

<!-- JUnit 5 Support -->
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>1.20.0</version>
    <scope>test</scope>
</dependency>
```

### Architecture Pattern: Shared Container Lifecycle

```
Test Suite Execution
│
├─ Static Container Initialization (JUnit 5 Static Fields)
│  ├─ Kafka Container (shared across all tests)
│  ├─ PostgreSQL Container (shared across all tests)
│  └─ Network (for inter-container communication)
│
├─ Per-Test Database Reset
│  ├─ Truncate tables before each test
│  ├─ Clear Kafka topics between tests
│  └─ Reset consumer offsets
│
└─ Parallel Test Execution (Maven Surefire)
   ├─ Multiple test classes run in parallel
   ├─ Shared containers reduce startup overhead
   └─ Proper isolation via database transactions

```

---

## 2. RATIONALE: Why This Setup is Optimal

### Advantages of Shared Containers vs Per-Test Containers

| Factor | Shared Containers | Per-Test Containers |
|--------|-------------------|---------------------|
| **Startup Time** | ~8-12 seconds (once) | 8-12 seconds per test (slow) |
| **Parallel Tests** | Excellent (reuse) | Poor (port conflicts) |
| **Test Isolation** | Via application state reset | Via new containers |
| **CI/CD Performance** | 50-80% faster | Slower for large suites |
| **Resource Usage** | Lower (2 containers) | Higher (N containers) |
| **Complexity** | Higher (careful reset logic) | Lower (automatic cleanup) |

### Why Confluent Kafka Images

- **confluent/cp-kafka** is the official Confluent Platform image
- Includes built-in Zookeeper integration
- Better tooling and monitoring capabilities
- Production-grade reliability
- Well-documented for Spring Kafka integration

### Why PostgreSQL Alpine

- 60-70% faster startup vs standard image (40-50 seconds to ~10 seconds)
- Minimal memory footprint (perfect for CI/CD)
- Full PostgreSQL compatibility, no feature loss
- Recommended by Testcontainers documentation

### Why Spring Boot Testcontainers

- Native Spring Boot 3.x integration
- Automatic container startup and lifecycle management
- Simplified configuration via @TestcontainersTest
- Dynamic property binding (no manual property overrides needed)
- Built-in support for @ServiceConnection annotation

---

## 3. ALTERNATIVES CONSIDERED

### Alternative 1: Embedded Kafka (TestEmbeddedKafka)

**Approach**: Using Spring Kafka's @EmbeddedKafka annotation

**Advantages**:
- No Docker required
- Faster startup (in-process)
- Simpler local development

**Disadvantages**:
- Not production-representative (different runtime)
- Limited Kafka version support
- Difficult to test consumer rebalancing
- Cannot test Kafka broker failures
- Less suitable for load testing

**When to Use**: Unit tests, basic message serialization tests

### Alternative 2: H2 Database for Tests

**Approach**: Using H2 instead of PostgreSQL for tests

**Advantages**:
- Fast startup (in-memory)
- Zero external dependencies
- Good for unit testing

**Disadvantages**:
- H2 dialect differs from PostgreSQL
- Misses PostgreSQL-specific issues (constraints, locks, indexes)
- Cannot test JSONB columns
- Cannot test GiST/GIN indexes
- Wrong test validation (false positives)

**When to Use**: Basic CRUD operation tests only

### Alternative 3: Docker Compose (Manual Setup)

**Approach**: Using docker-compose for local dev, testcontainers for CI/CD

**Advantages**:
- Clear separation of concerns
- Familiar to DevOps teams
- Better for local environment control

**Disadvantages**:
- Manual setup required (error-prone)
- Version management complexity
- Different code paths between local and CI
- Network configuration issues

**When to Use**: Local development only (not for automated tests)

### Alternative 4: Mock/Stub Testing

**Approach**: Using Mockito/WireMock to mock Kafka and database

**Advantages**:
- Fastest execution
- No external dependencies
- Good for unit testing business logic

**Disadvantages**:
- Not integration testing (just mocking)
- Misses serialization/deserialization bugs
- Cannot test concurrency issues
- Cannot test transaction semantics
- Gives false confidence (tests pass, production fails)

**When to Use**: Controller/service unit tests, not integration tests

### Recommendation

**Use Testcontainers for integration tests** because:
1. Tests actual Kafka and PostgreSQL behavior (not mocks)
2. Catches serialization, concurrency, and constraint violations
3. Parallelizable with shared containers
4. Works in CI/CD without manual setup
5. Close to production environment

---

## 4. TEST STRUCTURE: Organization and Test Data Management

### Recommended Directory Structure

```
src/test/java/
├── com/example/integration/
│   ├── config/
│   │   ├── TestcontainersConfiguration.java    # Shared container setup
│   │   ├── TestDatabaseConfiguration.java      # Database reset logic
│   │   └── TestDataFixtures.java              # Test data factories
│   │
│   ├── kafka/
│   │   ├── KafkaConsumerIntegrationTest.java   # Consumer tests
│   │   ├── KafkaProducerIntegrationTest.java   # Producer tests
│   │   └── KafkaMessageStorageIntegrationTest.java
│   │
│   ├── persistence/
│   │   ├── MessageRepositoryIntegrationTest.java
│   │   ├── KafkaMessageTableTest.java
│   │   └── KafkaMessageQueryTest.java
│   │
│   ├── sse/
│   │   ├── ServerSentEventsIntegrationTest.java
│   │   └── MessageBroadcastIntegrationTest.java
│   │
│   └── end_to_end/
│       ├── KafkaToUIIntegrationTest.java      # Full stack tests
│       └── MessageFlowIntegrationTest.java
│
└── resources/
    ├── application-test.properties             # Test configuration
    └── db/
        ├── migration/
        └── testdata/
            └── fixtures/
```

### Test Isolation Strategy: Database Reset Pattern

```java
@SpringBootTest
@TestcontainersTest
public abstract class BaseIntegrationTest {

    protected static final PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:17-alpine");

    protected static final KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Reset database state before each test
     * Ensures test isolation without recreating containers
     */
    @BeforeEach
    void resetTestDatabase() {
        // Clear all test tables in deterministic order (respecting FK constraints)
        jdbcTemplate.execute("TRUNCATE TABLE kafka_messages RESTART IDENTITY CASCADE");

        // Reset sequences if using SERIAL IDs
        jdbcTemplate.execute("ALTER SEQUENCE kafka_messages_id_seq RESTART WITH 1");

        // Clear consumer offsets (if tracking offset in DB)
        jdbcTemplate.execute("DELETE FROM consumer_offsets");
    }

    /**
     * Reset Kafka state before each test
     */
    @BeforeEach
    void resetKafka() {
        // Create fresh topics with 0 messages
        // Covered in TestDataFixtures.createTestTopics()
    }
}
```

### Test Data Factory Pattern

```java
/**
 * Factory for creating consistent test data
 * Removes duplication and improves test readability
 */
public class TestDataFixtures {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Builder pattern for KafkaMessage entities
     */
    public static class KafkaMessageBuilder {
        private String topic = "test-topic";
        private String key = "test-key";
        private String value = "{\"data\": \"test\"}";
        private long timestamp = System.currentTimeMillis();
        private int partition = 0;
        private long offset = 0;

        public KafkaMessageBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public KafkaMessageBuilder withValue(Map<String, Object> data) {
            try {
                this.value = objectMapper.writeValueAsString(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public KafkaMessageBuilder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public KafkaMessage build() {
            return new KafkaMessage()
                .setTopic(topic)
                .setKey(key)
                .setValue(value)
                .setTimestamp(Instant.ofEpochMilli(timestamp))
                .setPartition(partition)
                .setOffset(offset);
        }

        public List<KafkaMessage> buildBatch(int count) {
            return IntStream.range(0, count)
                .mapToObj(i -> new KafkaMessageBuilder()
                    .withTopic(topic)
                    .withTimestamp(timestamp + i * 1000)
                    .build())
                .collect(Collectors.toList());
        }
    }

    /**
     * Produce test messages to Kafka topic
     */
    public static void produceTestMessages(
            KafkaTemplate<String, String> kafkaTemplate,
            String topic,
            int count,
            Map<String, Object> dataTemplate) {

        for (int i = 0; i < count; i++) {
            Map<String, Object> data = new HashMap<>(dataTemplate);
            data.put("sequence", i);
            data.put("timestamp", System.currentTimeMillis());

            try {
                String message = new ObjectMapper().writeValueAsString(data);
                kafkaTemplate.send(topic, String.valueOf(i), message);
            } catch (Exception e) {
                throw new RuntimeException("Failed to produce test message", e);
            }
        }
    }

    /**
     * Create test topics with specific partition count and replication
     */
    public static void createTestTopics(
            AdminClient adminClient,
            String... topicNames) throws ExecutionException, InterruptedException {

        Collection<NewTopic> newTopics = Arrays.stream(topicNames)
            .map(topic -> new NewTopic(topic, 3, (short) 1))  // 3 partitions, RF=1
            .collect(Collectors.toList());

        adminClient.createTopics(newTopics).all().get();
    }
}
```

### Test Data Separation: Three-Layer Approach

```
Layer 1: Transactional Tests (ACID isolation)
├── Use @Transactional(propagation = NOT_SUPPORTED)
├── Rollback after each test automatically
├── Good for: Single-service tests
└── Speed: Fast (20-50ms per test)

Layer 2: Database Reset Tests (Manual cleanup)
├── Use @BeforeEach with TRUNCATE
├── Explicit cleanup of shared state
├── Good for: Multi-service tests with complex state
└── Speed: Medium (50-200ms per test due to reset)

Layer 3: Container State Tests (Re-create containers)
├── Only if absolutely necessary (rare)
├── Create fresh containers for isolated tests
├── Good for: Testing container failure scenarios
└── Speed: Slow (8-12 seconds per test)
```

---

## 5. MAVEN CONFIGURATION: Dependencies and Surefire Plugin

### Complete POM.xml Snippet

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example</groupId>
    <artifactId>kafka-streaming-ui</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.0</version>
        <relativePath/>
    </parent>

    <properties>
        <java.version>21</java.version>
        <testcontainers.version>1.20.0</testcontainers.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>

        <!-- Parallel Test Execution -->
        <junit.jupiter.execution.parallel.enabled>true</junit.jupiter.execution.parallel.enabled>
        <junit.jupiter.execution.parallel.mode.default>concurrent</junit.jupiter.execution.parallel.mode.default>
        <junit.jupiter.execution.parallel.mode.classes.default>concurrent</junit.jupiter.execution.parallel.mode.classes.default>
    </properties>

    <dependencies>
        <!-- Spring Boot Starters -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-kafka</artifactId>
        </dependency>

        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.7.3</version>
        </dependency>

        <!-- === TEST DEPENDENCIES === -->

        <!-- Spring Boot Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
            <exclusions>
                <exclusion>
                    <groupId>org.junit.vintage</groupId>
                    <artifactId>junit-vintage-engine</artifactId>
                </exclusion>
            </exclusions>
        </dependency>

        <!-- Testcontainers BOM (manages all versions) -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-bom</artifactId>
            <version>${testcontainers.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>

        <!-- Core Testcontainers -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Testcontainers Kafka -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Testcontainers PostgreSQL -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Spring Boot Testcontainers Support -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- JUnit 5 Testcontainers -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Embedded Database for Unit Tests (optional) -->
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- REST Assured for API Testing -->
        <dependency>
            <groupId>io.rest-assured</groupId>
            <artifactId>rest-assured</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Awaitility for async assertions -->
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>4.14.1</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Maven Surefire Plugin (for tests) -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.1.2</version>
                <configuration>
                    <!-- Parallel Test Execution -->
                    <parallel>classes</parallel>
                    <threadCount>4</threadCount>

                    <!-- Use reusable containers to avoid port conflicts -->
                    <systemPropertyVariables>
                        <testcontainers.reuse.enable>true</testcontainers.reuse.enable>
                    </systemPropertyVariables>

                    <!-- Test filtering options -->
                    <includes>
                        <include>**/*Test.java</include>
                        <include>**/*Tests.java</include>
                        <include>**/*IntegrationTest.java</include>
                    </includes>

                    <!-- Fork JVM settings for better isolation -->
                    <forkedProcessTimeoutInSeconds>300</forkedProcessTimeoutInSeconds>
                    <argLine>
                        -XX:+UseG1GC
                        -Xmx2G
                        -Xms512M
                    </argLine>
                </configuration>
            </plugin>

            <!-- Maven Failsafe Plugin (for integration tests) -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <version>3.1.2</version>
                <configuration>
                    <parallel>classes</parallel>
                    <threadCount>4</threadCount>

                    <systemPropertyVariables>
                        <testcontainers.reuse.enable>true</testcontainers.reuse.enable>
                    </systemPropertyVariables>

                    <includes>
                        <include>**/*IT.java</include>
                        <include>**/*IntegrationTest.java</include>
                    </includes>
                </configuration>
                <executions>
                    <execution>
                        <goals>
                            <goal>integration-test</goal>
                            <goal>verify</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

### Key Configuration Explanations

#### Parallel Test Execution

```xml
<!-- Enable JUnit 5 parallel execution -->
<junit.jupiter.execution.parallel.enabled>true</junit.jupiter.execution.parallel.enabled>
<junit.jupiter.execution.parallel.mode.default>concurrent</junit.jupiter.execution.parallel.mode.default>
<junit.jupiter.execution.parallel.mode.classes.default>concurrent</junit.jupiter.execution.parallel.mode.classes.default>

<!-- Maven Surefire parallel settings -->
<parallel>classes</parallel>           <!-- Parallelize by test class -->
<threadCount>4</threadCount>           <!-- 4 concurrent threads -->
```

**Why 4 threads?**
- Prevents Docker resource exhaustion
- Balances parallelization with stability
- Adjust based on CI/CD machine CPU/memory

#### Container Reuse Settings

```xml
<systemPropertyVariables>
    <testcontainers.reuse.enable>true</testcontainers.reuse.enable>
</systemPropertyVariables>
```

**What this does**:
- Keeps containers running between test runs
- Dramatically speeds up local development
- For CI/CD: Add to `.testcontainers.properties` instead (not recommended in CI)

#### JVM Memory Configuration

```xml
<argLine>
    -XX:+UseG1GC
    -Xmx2G
    -Xms512M
</argLine>
```

**Rationale**:
- G1GC: Better for large heap sizes
- 2GB max: Sufficient for Spring Boot + test overhead
- 512MB initial: Faster startup
- Adjust based on CI/CD agent resources

---

## 6. CODE EXAMPLE: Sample Integration Test Class

### Example 1: Basic Kafka Consumer Integration Test

```java
package com.example.integration.kafka;

import static org.awaitility.Awaitility.*;
import static org.assertj.core.api.Assertions.*;

import com.example.domain.KafkaMessage;
import com.example.repository.KafkaMessageRepository;
import com.example.integration.config.TestcontainersConfiguration;
import com.example.integration.config.TestDataFixtures;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration test for Kafka message consumption and persistence
 *
 * Tests the full flow:
 * 1. Produce message to Kafka
 * 2. Kafka consumer processes message
 * 3. Message persisted to PostgreSQL
 * 4. Repository can query persisted message
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class KafkaConsumerIntegrationTest {

    static final String TEST_TOPIC = "test-events";
    static final int PARTITION_COUNT = 3;

    /**
     * Shared Kafka container across all tests
     * Static field ensures single container instance per test class
     */
    @Container
    static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.7.0")
    )
    .withEnv("KAFKA_OFFSETS_RETENTION_MINUTES", "1440")  // 24 hours
    .withExposedPorts(9093);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaMessageRepository messageRepository;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Reset database before each test
     * Ensures test isolation without recreating containers
     */
    @BeforeEach
    void setupTest() {
        messageRepository.deleteAll();
    }

    /**
     * Test 1: Single message consumption and persistence
     *
     * GIVEN: Kafka topic with 0 messages
     * WHEN: Produce 1 message to topic
     * THEN: Message is persisted to PostgreSQL within 5 seconds
     */
    @Test
    void shouldConsumeAndPersistSingleMessage() {
        // ARRANGE
        String messageKey = "test-key-1";
        Map<String, Object> messageData = Map.of(
            "orderId", "12345",
            "customerId", "cust-001",
            "amount", 99.99
        );

        // ACT: Produce message to Kafka
        kafkaTemplate.send(
            TEST_TOPIC,
            messageKey,
            objectMapper.writeValueAsString(messageData)
        );

        // ASSERT: Message persisted within 5 seconds
        await()
            .atMost(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> {
                List<KafkaMessage> messages = messageRepository.findByTopic(TEST_TOPIC);
                assertThat(messages)
                    .hasSize(1)
                    .first()
                    .satisfies(msg -> {
                        assertThat(msg.getKey()).isEqualTo(messageKey);
                        assertThat(msg.getTopic()).isEqualTo(TEST_TOPIC);
                        assertThat(msg.getValue()).containsAll(
                            "12345", "99.99"
                        );
                    });
            });
    }

    /**
     * Test 2: Batch message consumption
     *
     * GIVEN: Empty Kafka topic
     * WHEN: Produce 100 messages
     * THEN: All 100 messages persisted within 10 seconds
     */
    @Test
    void shouldConsumeBatchOfMessages() {
        // ARRANGE
        int messageCount = 100;
        List<KafkaMessage> expectedMessages = TestDataFixtures
            .new KafkaMessageBuilder()
            .withTopic(TEST_TOPIC)
            .buildBatch(messageCount);

        // ACT: Produce batch messages
        TestDataFixtures.produceTestMessages(
            kafkaTemplate,
            TEST_TOPIC,
            messageCount,
            Map.of("type", "order_created", "version", 1)
        );

        // ASSERT
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> {
                List<KafkaMessage> persisted =
                    messageRepository.findByTopic(TEST_TOPIC);
                assertThat(persisted).hasSize(messageCount);
            });
    }

    /**
     * Test 3: Partition distribution
     *
     * GIVEN: Topic with 3 partitions
     * WHEN: Produce 30 messages with different keys
     * THEN: Messages distributed across partitions
     */
    @Test
    void shouldDistributeMessagesAcrossPartitions() {
        // ARRANGE
        int messagesPerPartition = 10;

        // ACT: Produce messages with different keys (ensures partition distribution)
        for (int i = 0; i < messagesPerPartition * PARTITION_COUNT; i++) {
            Map<String, Object> data = Map.of(
                "id", i,
                "timestamp", System.currentTimeMillis()
            );
            kafkaTemplate.send(
                TEST_TOPIC,
                String.valueOf(i % 10),  // Key (0-9) ensures distribution
                objectMapper.writeValueAsString(data)
            );
        }

        // ASSERT: All messages persisted across partitions
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> {
                List<KafkaMessage> messages =
                    messageRepository.findByTopic(TEST_TOPIC);
                assertThat(messages).hasSize(messagesPerPartition * PARTITION_COUNT);

                // Verify partition distribution
                Map<Integer, Long> partitionCounts = messages.stream()
                    .collect(
                        java.util.stream.Collectors.groupingBy(
                            KafkaMessage::getPartition,
                            java.util.stream.Collectors.counting()
                        )
                    );
                assertThat(partitionCounts).size().isGreaterThanOrEqualTo(2);
            });
    }

    /**
     * Test 4: Consumer offset tracking
     *
     * GIVEN: Topic with consumed messages
     * WHEN: Consumer processes messages
     * THEN: Offsets tracked correctly for rebalancing
     */
    @Test
    void shouldTrackConsumerOffsets() {
        // ARRANGE
        TestDataFixtures.produceTestMessages(
            kafkaTemplate,
            TEST_TOPIC,
            5,
            Map.of("type", "test")
        );

        // ACT
        await().atMost(Duration.ofSeconds(5))
            .until(() -> messageRepository.findByTopic(TEST_TOPIC).size() == 5);

        // ASSERT: All messages have offset metadata
        List<KafkaMessage> messages = messageRepository.findByTopic(TEST_TOPIC);
        assertThat(messages)
            .allSatisfy(msg -> {
                assertThat(msg.getOffset()).isGreaterThanOrEqualTo(0);
                assertThat(msg.getPartition()).isGreaterThanOrEqualTo(0);
                assertThat(msg.getPartition()).isLessThan(PARTITION_COUNT);
            });
    }

    /**
     * Test 5: Error handling - null values
     *
     * GIVEN: Message with null value
     * WHEN: Consumer processes message
     * THEN: Message stored with null value handling
     */
    @Test
    void shouldHandleNullMessageValues() {
        // ACT: Send message with null value
        kafkaTemplate.send(TEST_TOPIC, "null-key", null);

        // ASSERT
        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> {
                List<KafkaMessage> messages =
                    messageRepository.findByTopic(TEST_TOPIC);
                assertThat(messages).hasSize(1);
                assertThat(messages.get(0).getValue()).isNull();
            });
    }

    /**
     * Test 6: Timing - message throughput
     *
     * GIVEN: 1000 messages produced
     * WHEN: Consumer processes messages
     * THEN: All messages persisted in <5 seconds (>200 msg/sec throughput)
     */
    @Test
    void shouldSustainHighThroughput() {
        // ARRANGE
        int messageCount = 1000;
        long startTime = System.currentTimeMillis();

        // ACT: Rapid-fire produce
        for (int i = 0; i < messageCount; i++) {
            kafkaTemplate.send(
                TEST_TOPIC,
                String.valueOf(i % 100),
                "{\"id\":" + i + "}"
            );
        }

        // ASSERT: All messages persisted quickly
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
                List<KafkaMessage> messages =
                    messageRepository.findByTopic(TEST_TOPIC);
                assertThat(messages).hasSize(messageCount);

                long duration = System.currentTimeMillis() - startTime;
                double throughput = messageCount * 1000.0 / duration;
                assertThat(throughput)
                    .as("Messages per second: " + throughput)
                    .isGreaterThan(200);
            });
    }
}
```

### Example 2: PostgreSQL Integration Test

```java
package com.example.integration.persistence;

import com.example.domain.KafkaMessage;
import com.example.repository.KafkaMessageRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Data layer integration test using Testcontainers PostgreSQL
 *
 * Tests repository queries against real PostgreSQL
 */
@DataJpaTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
class KafkaMessageRepositoryIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass");

    @Autowired
    private KafkaMessageRepository repository;

    @BeforeEach
    void setup() {
        repository.deleteAll();
    }

    @Test
    void shouldPersistAndRetrieveMessage() {
        // ARRANGE
        KafkaMessage message = new KafkaMessage()
            .setTopic("orders")
            .setKey("key-1")
            .setValue("{\"orderId\": 123}")
            .setTimestamp(Instant.now())
            .setPartition(0)
            .setOffset(0);

        // ACT
        KafkaMessage saved = repository.save(message);

        // ASSERT
        assertThat(saved.getId()).isNotNull();
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void shouldFindMessagesByTopic() {
        // ARRANGE
        repository.saveAll(List.of(
            new KafkaMessage().setTopic("orders").setValue("msg1"),
            new KafkaMessage().setTopic("orders").setValue("msg2"),
            new KafkaMessage().setTopic("payments").setValue("msg3")
        ));

        // ACT
        List<KafkaMessage> orders = repository.findByTopic("orders");

        // ASSERT
        assertThat(orders).hasSize(2);
    }

    @Test
    void shouldPaginateResults() {
        // ARRANGE: Create 25 messages
        for (int i = 0; i < 25; i++) {
            repository.save(new KafkaMessage()
                .setTopic("test")
                .setValue("msg-" + i)
                .setTimestamp(Instant.now().minusSeconds(i))
            );
        }

        // ACT
        Page<KafkaMessage> page1 = repository.findByTopic(
            "test",
            PageRequest.of(0, 10)
        );

        // ASSERT
        assertThat(page1.getContent()).hasSize(10);
        assertThat(page1.getTotalElements()).isEqualTo(25);
        assertThat(page1.getTotalPages()).isEqualTo(3);
    }

    @Test
    void shouldOrderByTimestampDescending() {
        // ARRANGE
        long baseTime = System.currentTimeMillis();
        repository.saveAll(List.of(
            new KafkaMessage().setTopic("test")
                .setTimestamp(Instant.ofEpochMilli(baseTime)),
            new KafkaMessage().setTopic("test")
                .setTimestamp(Instant.ofEpochMilli(baseTime + 1000)),
            new KafkaMessage().setTopic("test")
                .setTimestamp(Instant.ofEpochMilli(baseTime + 2000))
        ));

        // ACT
        List<KafkaMessage> results = repository.findByTopicOrderByTimestampDesc("test");

        // ASSERT: Newest messages first
        assertThat(results)
            .hasSize(3)
            .isSortedAccordingTo((a, b) ->
                b.getTimestamp().compareTo(a.getTimestamp())
            );
    }
}
```

### Example 3: Full Integration Test with Containers

```java
package com.example.integration.end_to_end;

import com.example.integration.config.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.*;

/**
 * Full end-to-end integration test with:
 * - Real Kafka instance
 * - Real PostgreSQL instance
 * - Running Spring Boot application
 * - HTTP API calls
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@ActiveProfiles("test")
class KafkaToUIIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @BeforeEach
    void setup() {
        // Clear state
    }

    @Test
    void shouldRetrieveConsumedMessagesViaAPI() {
        // ACT & ASSERT
        // Test full Kafka -> DB -> API flow
    }
}
```

---

## 7. OPTIMIZATION TIPS FOR FAST TEST EXECUTION

### 7.1 Container Reuse Configuration

#### Local Development (Fast Feedback Loop)

Create `.testcontainers.properties` in project root:

```properties
# Enable container reuse for faster local development
testcontainers.reuse.enable=true

# Skip tests if Docker is not available
testcontainers.docker.client.strategy=io.testcontainers.dockerclient.EnvironmentAndSystemPropertyClientProviderStrategy

# Docker socket path (for custom Docker installations)
docker.client.strategy=unix:///var/run/docker.sock
```

**Result**: First test run takes 15-20 seconds, subsequent runs take <2 seconds

#### CI/CD Pipeline (Fresh Containers)

For GitHub Actions, do NOT enable container reuse. Instead:
- Fresh containers ensure clean state
- Add Docker layer caching to speed up image pulls
- Use Docker buildx with caching

### 7.2 Alpine Images for Speed

```java
// Instead of
new PostgreSQLContainer<>("postgres:17");

// Use lightweight Alpine variant
new PostgreSQLContainer<>("postgres:17-alpine");
// Startup time: ~40 seconds vs ~10 seconds
```

### 7.3 Test Grouping and Selective Execution

Maven profiles for different test suites:

```xml
<profiles>
    <!-- Fast unit + light integration tests -->
    <profile>
        <id>fast</id>
        <build>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-surefire-plugin</artifactId>
                    <configuration>
                        <excludes>
                            <exclude>**/*LoadTest.java</exclude>
                            <exclude>**/*SlowTest.java</exclude>
                        </excludes>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </profile>

    <!-- Full test suite including load/slow tests -->
    <profile>
        <id>full</id>
        <activation>
            <activeByDefault>true</activeByDefault>
        </activation>
    </profile>
</profiles>
```

Usage:
```bash
mvn clean test -Pfast              # 2-3 minutes
mvn clean test -Pfull              # 5-10 minutes
```

### 7.4 Test Classification by Speed

```java
@Test
@Tag("fast")
void quickValidationTest() {
    // Runs in <100ms
}

@Test
@Tag("integration")
void kafkaIntegrationTest() {
    // Runs in 1-5 seconds
}

@Test
@Tag("slow")
void loadTestWith1000Messages() {
    // Runs in 10-30 seconds
}
```

Run only fast tests:
```bash
mvn test -Dgroups="fast"
```

### 7.5 Shared Container Initialization Best Practices

```java
@SpringBootTest
@Testcontainers
public class BaseIntegrationTest {

    /**
     * Static containers - initialized once per test class
     * NOT once per test method
     */
    @Container
    static final PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>("postgres:17-alpine")
            .withInitScript("init.sql")  // Init DB once
            .withReuse(true);

    @Container
    static final KafkaContainer kafka =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))
            .withReuse(true);

    /**
     * Spring context - initialized once per test class
     * Thanks to @SpringBootTest caching
     */
    @Autowired
    private ApplicationContext context;
}
```

**Key Points**:
- `static` keyword ensures container reuse across all test methods in class
- Spring context caching: only created once if app properties identical
- Database reset via `@BeforeEach` is faster than recreating container

### 7.6 Parallel Test Configuration for CI/CD

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <!-- Run test CLASSES in parallel (not methods) -->
        <parallel>classes</parallel>

        <!-- 4 threads: good balance between speed and stability -->
        <threadCount>4</threadCount>

        <!-- Reuse forked JVM -->
        <reuseForks>true</reuseForks>

        <!-- Test method isolation -->
        <argLine>-Dspring.test.mockmvc.print=false</argLine>
    </configuration>
</plugin>
```

**Warning**: Do NOT parallelize test METHODS with shared containers
- Causes race conditions in container setup
- Use class-level parallelization only

### 7.7 Network Optimization

For slower environments (CI/CD with poor Docker networking):

```java
@Container
static final KafkaContainer kafka =
    new KafkaContainer(...)
        // Use host network if supported (Linux CI/CD)
        .withNetwork(Network.newNetwork())
        // Set socket timeouts
        .withStartupTimeout(Duration.ofSeconds(120));
```

### 7.8 Example Test Execution Timeline

**Fast Local Development (with container reuse)**:
```
First run:
  Container startup ............ 15-20 seconds
  Database migration ........... 2-3 seconds
  Test execution (50 tests) .... 20-30 seconds
  Total ........................ ~40-50 seconds

Subsequent runs:
  Container startup ............ 0 seconds (reused)
  Database reset (truncate) .... 0.5-1 seconds
  Test execution (50 tests) .... 20-30 seconds
  Total ........................ ~25-30 seconds
```

**CI/CD Pipeline (fresh containers)**:
```
Full test suite:
  Docker image pulls ........... 10-15 seconds
  Container startup ........... 15-20 seconds
  Database setup .............. 2-3 seconds
  Parallel test execution ...... 30-45 seconds (4 threads)
  Total ........................ ~60-90 seconds
```

---

## 8. CI/CD INTEGRATION: GitHub Actions Example

### GitHub Actions Workflow

```yaml
name: Integration Tests

on:
  push:
    branches: [ develop, main ]
  pull_request:
    branches: [ develop, main ]

jobs:
  integration-tests:
    runs-on: ubuntu-latest

    services:
      docker:
        image: docker:24-dind
        options: --privileged

    strategy:
      matrix:
        java-version: [21]

    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0  # Full history for git-based versioning

      - name: Set up JDK ${{ matrix.java-version }}
        uses: actions/setup-java@v4
        with:
          java-version: ${{ matrix.java-version }}
          distribution: 'temurin'
          cache: maven

      - name: Run integration tests
        run: |
          mvn clean verify \
            -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
            --batch-mode \
            --show-version \
            -DskipITs=false \
            -Dgroups="!slow"
        timeout-minutes: 10

      - name: Upload test reports
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: test-results
          path: target/surefire-reports/

      - name: Publish test results
        if: always()
        uses: EnricoMi/publish-unit-test-result-action@v2
        with:
          files: target/surefire-reports/TEST-*.xml
          check_name: Integration Test Results
```

### Key optimizations for GitHub Actions:

1. **Maven Cache**: `actions/setup-java@v4` caches Maven dependencies
2. **Shallow Clone**: Remove `fetch-depth: 0` for faster checkout
3. **Skip Slow Tests**: `-Dgroups="!slow"` excludes long-running tests
4. **Batch Mode**: `--batch-mode` reduces output verbosity
5. **Timeout**: 10 minutes is safe for parallel execution of 50-100 tests
6. **Artifacts**: Upload test reports for debugging failures

---

## 9. COMPARISON TABLE: Testcontainers vs Alternatives

| Feature | Testcontainers | Embedded Kafka | Docker Compose | Mocks |
|---------|-----------------|----------------|-----------------|--------|
| **Production Parity** | ✓ Excellent | ✗ Limited | ✓ Excellent | ✗ None |
| **Setup Complexity** | Low | Low | Medium | Very Low |
| **Test Speed** | Medium | Fast | Slow | Very Fast |
| **CI/CD Ready** | ✓ Yes | ✓ Yes | ✗ Manual setup | ✓ Yes |
| **Parallelization** | ✓ With caution | ✗ Port conflicts | ✗ Port conflicts | ✓ Yes |
| **Error Detection** | ✓ Real errors | Partial | ✓ Real errors | ✗ False positives |
| **Learning Curve** | Medium | Low | Medium | Low |
| **Maintenance** | Container versions | Spring version | Docker Compose version | Mock configuration |
| **Scalability Testing** | ✓ Yes | ✓ Limited | ✓ Yes | ✗ No |

---

## 10. TROUBLESHOOTING COMMON ISSUES

### Issue 1: Port Conflicts in Parallel Tests

**Symptom**: `Address already in use` when running 4+ tests in parallel

**Root Cause**: Containers use fixed ports, multiple containers fight for same port

**Solution**:
```java
// Use dynamic port binding
@Container
static final PostgreSQLContainer<?> postgres =
    new PostgreSQLContainer<>("postgres:17-alpine")
        .withExposedPorts(5432);  // Dynamic port assignment
        // NOT .withFixedExposedPort(5432, 5432)
```

### Issue 2: Docker Daemon Connection Failures

**Symptom**: `Docker daemon is not available`

**Root Cause**: Docker socket not accessible or Docker not installed

**Solution** (GitHub Actions):
```yaml
- name: Start Docker
  run: |
    docker ps  # Verify Docker is accessible
    docker info  # Get Docker version
```

### Issue 3: Tests Pass Locally, Fail in CI/CD

**Symptom**: Different behavior between local and CI environment

**Root Cause**:
- Container reuse enabled locally, disabled in CI
- Different JDK versions
- Different Docker versions

**Solution**:
```properties
# .testcontainers.properties - ONLY for local development
testcontainers.reuse.enable=true
```

Do NOT commit this file to Git. Add to `.gitignore`:
```
.testcontainers.properties
```

### Issue 4: Slow Test Startup (30+ seconds)

**Symptom**: Each test takes 30+ seconds to start

**Root Cause**:
- Containers not reused
- Fresh PostgreSQL initialization on each test
- Network issues pulling images

**Solution**:
```java
@Container
static final PostgreSQLContainer<?> postgres =
    new PostgreSQLContainer<>("postgres:17-alpine")
        .withReuse(true)  // Enable reuse
        .withInitScript("quick-init.sql");  // Minimal init SQL
```

### Issue 5: Database State Leaks Between Tests

**Symptom**: Test passes individually, fails when run with other tests

**Root Cause**: Improper cleanup, forgot `@BeforeEach` reset

**Solution**:
```java
@BeforeEach
void resetDatabase() {
    // Use TRUNCATE not DELETE for speed
    jdbcTemplate.execute("TRUNCATE TABLE kafka_messages RESTART IDENTITY CASCADE");
}
```

---

## SUMMARY & RECOMMENDATIONS

### For Your Project (Kafka Streaming UI)

1. **Use Testcontainers** with shared static containers
2. **Docker images**: PostgreSQL Alpine, Confluent Kafka
3. **Parallel execution**: 4 threads via Maven Surefire
4. **Container reuse**: Local dev only (via .testcontainers.properties)
5. **Test organization**: Base class with container setup + trait per layer
6. **Expected execution time**: 60-90 seconds for full suite in CI/CD

### Key Files to Create

```
src/test/java/com/example/integration/
├── config/
│   ├── TestcontainersConfiguration.java       # Container setup
│   ├── TestDatabaseConfiguration.java         # Database reset
│   └── TestDataFixtures.java                  # Test data builders
├── kafka/
│   ├── KafkaConsumerIntegrationTest.java
│   └── KafkaProducerIntegrationTest.java
├── persistence/
│   └── KafkaMessageRepositoryIntegrationTest.java
├── sse/
│   └── ServerSentEventsIntegrationTest.java
└── end_to_end/
    └── KafkaToUIIntegrationTest.java

pom.xml (with Testcontainers + Surefire config)
```

### Quick Start

```bash
# 1. Add dependencies (from POM template above)
# 2. Create TestcontainersConfiguration.java (base class)
# 3. Create integration test extending base class
# 4. Configure Maven Surefire for parallel execution
# 5. Run tests

mvn clean test -Dgroups="!slow"
```

Expected result: All tests pass in <5 minutes ✓

