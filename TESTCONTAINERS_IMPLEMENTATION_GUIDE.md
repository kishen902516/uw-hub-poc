# Testcontainers Implementation Guide
## Step-by-Step Setup for Kafka Streaming UI Project

---

## PHASE 1: Maven POM Configuration

### Step 1: Add Testcontainers Dependencies

Replace the `<properties>` and `<dependencies>` sections in your `pom.xml`:

```xml
<properties>
    <java.version>21</java.version>
    <maven.compiler.source>21</maven.compiler.source>
    <maven.compiler.target>21</maven.compiler.target>

    <!-- Testcontainers Version -->
    <testcontainers.version>1.20.0</testcontainers.version>

    <!-- Parallel Test Execution Settings -->
    <junit.jupiter.execution.parallel.enabled>true</junit.jupiter.execution.parallel.enabled>
    <junit.jupiter.execution.parallel.mode.default>concurrent</junit.jupiter.execution.parallel.mode.default>
    <junit.jupiter.execution.parallel.mode.classes.default>concurrent</junit.jupiter.execution.parallel.mode.classes.default>
</properties>

<dependencyManagement>
    <dependencies>
        <!-- Testcontainers BOM -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers-bom</artifactId>
            <version>${testcontainers.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <!-- Core Testcontainers -->
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers</artifactId>
        <scope>test</scope>
    </dependency>

    <!-- Kafka Module -->
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>kafka</artifactId>
        <scope>test</scope>
    </dependency>

    <!-- PostgreSQL Module -->
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

    <!-- JUnit 5 Support -->
    <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>junit-jupiter</artifactId>
        <scope>test</scope>
    </dependency>

    <!-- Async/Await Assertions -->
    <dependency>
        <groupId>org.awaitility</groupId>
        <artifactId>awaitility</artifactId>
        <version>4.14.1</version>
        <scope>test</scope>
    </dependency>

    <!-- REST Testing -->
    <dependency>
        <groupId>io.rest-assured</groupId>
        <artifactId>rest-assured</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

### Step 2: Configure Maven Surefire Plugin

Add this to `<build><plugins>`:

```xml
<!-- Maven Surefire Plugin for Unit Tests -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>3.1.2</version>
    <configuration>
        <!-- Parallel Execution: Run test classes in parallel -->
        <parallel>classes</parallel>
        <threadCount>4</threadCount>

        <!-- Reuse forked JVM for faster startup -->
        <reuseForks>true</reuseForks>

        <!-- JVM Settings -->
        <forkedProcessTimeoutInSeconds>300</forkedProcessTimeoutInSeconds>
        <argLine>
            -XX:+UseG1GC
            -Xmx2G
            -Xms512M
        </argLine>

        <!-- Only run unit tests (not integration tests) -->
        <includes>
            <include>**/*Test.java</include>
            <include>**/*Tests.java</include>
        </includes>

        <!-- Exclude integration tests for 'test' phase -->
        <excludes>
            <exclude>**/*IntegrationTest.java</exclude>
            <exclude>**/*IT.java</exclude>
        </excludes>

        <!-- Environment Variables -->
        <systemPropertyVariables>
            <testcontainers.reuse.enable>false</testcontainers.reuse.enable>
            <org.slf4j.simpleLogger.defaultLogLevel>warn</org.slf4j.simpleLogger.defaultLogLevel>
        </systemPropertyVariables>
    </configuration>
</plugin>

<!-- Maven Failsafe Plugin for Integration Tests -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-failsafe-plugin</artifactId>
    <version>3.1.2</version>
    <configuration>
        <parallel>classes</parallel>
        <threadCount>4</threadCount>
        <reuseForks>true</reuseForks>

        <systemPropertyVariables>
            <testcontainers.reuse.enable>false</testcontainers.reuse.enable>
        </systemPropertyVariables>

        <includes>
            <include>**/*IntegrationTest.java</include>
            <include>**/*IT.java</include>
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
```

### Step 3: Create Local Development Config

Create `.testcontainers.properties` in project root (NOT committed to Git):

```properties
# Enable container reuse for faster local development
testcontainers.reuse.enable=true

# Docker client strategy
docker.client.strategy=unix:///var/run/docker.sock
```

Add to `.gitignore`:
```
.testcontainers.properties
```

---

## PHASE 2: Create Test Infrastructure Classes

### Step 1: Base Test Configuration Class

Create `src/test/java/com/example/integration/config/TestcontainersConfiguration.java`:

```java
package com.example.integration.config;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Testcontainers configuration for integration tests
 *
 * This class initializes shared Kafka and PostgreSQL containers
 * that are reused across all integration tests for speed.
 *
 * Container lifecycle:
 * - Static initialization: Containers start once per test class
 * - Shared across test methods: No per-test container creation
 * - Cleanup: Containers stopped after all tests in class complete
 */
@Testcontainers
@TestConfiguration
public class TestcontainersConfiguration {

    /**
     * PostgreSQL Container (shared across all tests)
     *
     * Configuration:
     * - Image: postgres:17-alpine (lightweight, fast startup ~10 seconds)
     * - Database: testdb
     * - Port: Dynamic (assigned automatically, prevents conflicts)
     * - Reuse: Enabled in development (.testcontainers.properties)
     */
    @Container
    public static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("kafka_ui_db")
            .withUsername("testuser")
            .withPassword("testpass")
            // Script runs on container start (only once if reused)
            .withInitScript("db/init-test-schema.sql")
            // Expose port dynamically
            .withExposedPorts(5432);

    /**
     * Kafka Container (shared across all tests)
     *
     * Configuration:
     * - Image: confluentinc/cp-kafka:7.7.0 (official Confluent image)
     * - Includes Zookeeper automatically
     * - Broker port: Dynamic assignment
     * - Reuse: Enabled in development
     *
     * Note: Confluent image handles Zookeeper setup internally
     */
    @Container
    public static final KafkaContainer KAFKA =
        new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.0")
        )
            // Expose ports dynamically
            .withExposedPorts(9093)
            // Kafka broker configuration
            .withEnv("KAFKA_OFFSETS_RETENTION_MINUTES", "1440")  // 24 hours
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

    /**
     * Dynamic property source for Spring Boot test context
     *
     * This method is called by Spring to inject container properties
     * into the application context at runtime.
     *
     * Properties override application.properties and application-test.properties
     */
    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // PostgreSQL Configuration
        registry.add("spring.datasource.url",
            () -> "jdbc:postgresql://" +
                  POSTGRES.getHost() + ":" +
                  POSTGRES.getFirstMappedPort() +
                  "/kafka_ui_db");

        registry.add("spring.datasource.username",
            POSTGRES::getUsername);

        registry.add("spring.datasource.password",
            POSTGRES::getPassword);

        registry.add("spring.datasource.driver-class-name",
            () -> "org.postgresql.Driver");

        // Kafka Configuration
        registry.add("spring.kafka.bootstrap-servers",
            KAFKA::getBootstrapServers);

        registry.add("spring.kafka.properties.auto.offset.reset",
            () -> "earliest");

        registry.add("spring.kafka.consumer.group-id",
            () -> "test-consumer-group");

        registry.add("spring.kafka.consumer.auto-offset-reset",
            () -> "earliest");

        registry.add("spring.kafka.consumer.enable-auto-commit",
            () -> "true");

        registry.add("spring.kafka.consumer.auto-commit-interval-ms",
            () -> "100");
    }

    /**
     * Verify containers are running
     */
    public static boolean isRunning() {
        return POSTGRES.isRunning() && KAFKA.isRunning();
    }
}
```

### Step 2: Database Reset Utility

Create `src/test/java/com/example/integration/config/TestDatabaseReset.java`:

```java
package com.example.integration.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Utility for resetting database state between tests
 *
 * Provides clean isolation without recreating containers
 */
@Component
public class TestDatabaseReset {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Reset all tables to empty state
     *
     * Uses TRUNCATE for speed (faster than DELETE)
     * CASCADE handles foreign key constraints automatically
     * RESTART IDENTITY resets auto-increment sequences
     */
    public void resetAllTables() {
        // Order matters: truncate in dependency order (reverse of FK relationships)
        truncateTable("kafka_messages");
        truncateTable("consumer_offsets");
        truncateTable("message_cache");
    }

    /**
     * Clear specific table
     */
    public void truncateTable(String tableName) {
        try {
            jdbcTemplate.execute(
                String.format("TRUNCATE TABLE %s RESTART IDENTITY CASCADE", tableName)
            );
        } catch (Exception e) {
            // Table might not exist, ignore
            if (!e.getMessage().contains("does not exist")) {
                throw new RuntimeException("Failed to truncate table: " + tableName, e);
            }
        }
    }

    /**
     * Reset Kafka consumer offsets
     */
    public void resetConsumerOffsets() {
        try {
            jdbcTemplate.execute("DELETE FROM consumer_offsets");
        } catch (Exception e) {
            // Ignore if table doesn't exist
        }
    }

    /**
     * Get table row count
     */
    public long getTableRowCount(String tableName) {
        try {
            Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + tableName,
                Integer.class
            );
            return count != null ? count : 0;
        } catch (Exception e) {
            return 0;
        }
    }
}
```

### Step 3: Test Data Fixtures Factory

Create `src/test/java/com/example/integration/config/TestDataFixtures.java`:

```java
package com.example.integration.config;

import com.example.domain.KafkaMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Factory for creating test data consistently
 *
 * Removes duplication and improves test readability
 */
public class TestDataFixtures {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Builder pattern for KafkaMessage entities
     */
    public static class KafkaMessageBuilder {
        private String topic = "test-topic";
        private String key = UUID.randomUUID().toString();
        private String value = "{\"data\": \"test\"}";
        private long timestamp = System.currentTimeMillis();
        private int partition = 0;
        private long offset = 0;

        public KafkaMessageBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public KafkaMessageBuilder withKey(String key) {
            this.key = key;
            return this;
        }

        public KafkaMessageBuilder withValue(String value) {
            this.value = value;
            return this;
        }

        public KafkaMessageBuilder withJsonValue(Map<String, Object> data) {
            try {
                this.value = objectMapper.writeValueAsString(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize JSON", e);
            }
            return this;
        }

        public KafkaMessageBuilder withTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public KafkaMessageBuilder withTimestamp(Instant instant) {
            this.timestamp = instant.toEpochMilli();
            return this;
        }

        public KafkaMessageBuilder withPartition(int partition) {
            this.partition = partition;
            return this;
        }

        public KafkaMessageBuilder withOffset(long offset) {
            this.offset = offset;
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

        /**
         * Create batch of messages
         *
         * @param count Number of messages to create
         * @return List of KafkaMessage objects
         */
        public List<KafkaMessage> buildBatch(int count) {
            return IntStream.range(0, count)
                .mapToObj(i -> {
                    KafkaMessageBuilder copy = new KafkaMessageBuilder()
                        .withTopic(topic)
                        .withKey(key + "-" + i)
                        .withPartition(i % 3)  // Distribute across 3 partitions
                        .withOffset(i)
                        .withTimestamp(timestamp + (i * 1000));  // 1 second apart

                    try {
                        // Add sequence number to JSON
                        Map<String, Object> jsonData = objectMapper.readValue(value, Map.class);
                        jsonData.put("sequence", i);
                        copy.withJsonValue(jsonData);
                    } catch (Exception e) {
                        copy.withValue(value);
                    }

                    return copy.build();
                })
                .collect(Collectors.toList());
        }
    }

    /**
     * Create a new builder
     */
    public static KafkaMessageBuilder kafkaMessage() {
        return new KafkaMessageBuilder();
    }

    /**
     * Produce messages to Kafka topic via template
     *
     * @param kafkaTemplate Spring KafkaTemplate
     * @param topic Topic name
     * @param count Number of messages to produce
     * @param dataTemplate Template data for each message
     */
    public static void produceTestMessages(
            KafkaTemplate<String, String> kafkaTemplate,
            String topic,
            int count,
            Map<String, Object> dataTemplate) {

        for (int i = 0; i < count; i++) {
            try {
                Map<String, Object> data = new HashMap<>(dataTemplate);
                data.put("sequence", i);
                data.put("timestamp", System.currentTimeMillis());

                String message = objectMapper.writeValueAsString(data);
                kafkaTemplate.send(topic, String.valueOf(i % 100), message);
            } catch (Exception e) {
                throw new RuntimeException("Failed to produce test message", e);
            }
        }
    }

    /**
     * Create test topics with Kafka admin client
     *
     * @param topics Topic names to create
     */
    public static Map<String, Object> createTestTopic(String topic) {
        Map<String, Object> topicConfig = new HashMap<>();
        topicConfig.put("name", topic);
        topicConfig.put("partitions", 3);
        topicConfig.put("replicationFactor", 1);
        return topicConfig;
    }

    /**
     * Sample order event data
     */
    public static Map<String, Object> sampleOrderEvent() {
        return Map.of(
            "orderId", UUID.randomUUID().toString(),
            "customerId", "cust-" + System.currentTimeMillis(),
            "amount", 99.99,
            "status", "created",
            "timestamp", System.currentTimeMillis()
        );
    }

    /**
     * Sample payment event data
     */
    public static Map<String, Object> samplePaymentEvent() {
        return Map.of(
            "paymentId", UUID.randomUUID().toString(),
            "orderId", UUID.randomUUID().toString(),
            "amount", 99.99,
            "status", "processed",
            "timestamp", System.currentTimeMillis()
        );
    }
}
```

---

## PHASE 3: Create Base Test Classes

### Step 1: Abstract Base Integration Test

Create `src/test/java/com/example/integration/BaseIntegrationTest.java`:

```java
package com.example.integration;

import com.example.integration.config.TestcontainersConfiguration;
import com.example.integration.config.TestDatabaseReset;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Base class for all integration tests
 *
 * Provides:
 * - Shared Kafka and PostgreSQL containers
 * - Automatic database reset before each test
 * - Spring Boot context with test properties
 * - Easy access to autowired beans
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
public abstract class BaseIntegrationTest {

    @Autowired
    protected TestDatabaseReset testDatabaseReset;

    /**
     * Reset database before each test
     *
     * Ensures test isolation without recreating containers
     */
    @BeforeEach
    protected void resetDatabase() {
        testDatabaseReset.resetAllTables();
    }

    /**
     * Verify containers are available for test
     */
    protected boolean areContainersReady() {
        return TestcontainersConfiguration.isRunning();
    }
}
```

---

## PHASE 4: Create First Integration Test

### Example Test: Kafka Consumer

Create `src/test/java/com/example/integration/kafka/KafkaConsumerIntegrationTest.java`:

```java
package com.example.integration.kafka;

import com.example.domain.KafkaMessage;
import com.example.integration.BaseIntegrationTest;
import com.example.integration.config.TestDataFixtures;
import com.example.repository.KafkaMessageRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

/**
 * Integration test for Kafka message consumption and persistence
 *
 * Tests the full flow:
 * 1. Produce message to Kafka
 * 2. Kafka consumer processes message
 * 3. Message persisted to PostgreSQL
 */
class KafkaConsumerIntegrationTest extends BaseIntegrationTest {

    private static final String TEST_TOPIC = "test-orders";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaMessageRepository messageRepository;

    /**
     * Test: Single message consumption and persistence
     */
    @Test
    void shouldConsumeAndPersistSingleMessage() {
        // ARRANGE
        Map<String, Object> orderData = TestDataFixtures.sampleOrderEvent();

        // ACT: Produce message
        kafkaTemplate.send(
            TEST_TOPIC,
            String.valueOf(orderData.get("orderId")),
            "{\"order\": \"test\"}"
        );

        // ASSERT: Verify persistence within 5 seconds
        await()
            .atMost(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(100))
            .untilAsserted(() -> {
                List<KafkaMessage> messages =
                    messageRepository.findByTopic(TEST_TOPIC);
                assertThat(messages).hasSize(1);
            });
    }

    /**
     * Test: Batch message consumption
     */
    @Test
    void shouldConsumeBatchMessages() {
        // ARRANGE
        int messageCount = 50;

        // ACT: Produce batch
        TestDataFixtures.produceTestMessages(
            kafkaTemplate,
            TEST_TOPIC,
            messageCount,
            TestDataFixtures.sampleOrderEvent()
        );

        // ASSERT
        await()
            .atMost(Duration.ofSeconds(10))
            .untilAsserted(() -> {
                List<KafkaMessage> messages =
                    messageRepository.findByTopic(TEST_TOPIC);
                assertThat(messages).hasSize(messageCount);
            });
    }
}
```

---

## PHASE 5: Test Configuration Files

### Create Test Application Properties

Create `src/test/resources/application-test.properties`:

```properties
# Test Profile Configuration

# Logging
logging.level.root=WARN
logging.level.com.example=DEBUG
logging.level.org.springframework.kafka=INFO

# JPA/Hibernate
spring.jpa.hibernate.ddl-auto=create-drop
spring.jpa.show-sql=false
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect
spring.jpa.properties.hibernate.jdbc.batch_size=20
spring.jpa.properties.hibernate.order_inserts=true
spring.jpa.properties.hibernate.order_updates=true

# DataSource (overridden by TestcontainersConfiguration)
spring.datasource.hikari.maximum-pool-size=5
spring.datasource.hikari.minimum-idle=2

# Kafka Consumer
spring.kafka.consumer.group-id=test-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=true
spring.kafka.consumer.auto-commit-interval-ms=100

# Kafka Producer
spring.kafka.producer.acks=all
spring.kafka.producer.retries=3

# Connection Timeouts
spring.kafka.properties.connections.max.idle.ms=30000
spring.kafka.properties.session.timeout.ms=30000
spring.kafka.properties.request.timeout.ms=40000

# Test-specific settings
server.port=0
management.endpoints.web.exposure.include=health,info,metrics
```

### Create Database Init Script

Create `src/test/resources/db/init-test-schema.sql`:

```sql
-- Test database initialization script
-- Runs once when PostgreSQL container starts

CREATE TABLE IF NOT EXISTS kafka_messages (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    key VARCHAR(255),
    value TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(topic, partition, offset)
);

CREATE INDEX IF NOT EXISTS idx_kafka_messages_topic
    ON kafka_messages(topic);

CREATE INDEX IF NOT EXISTS idx_kafka_messages_timestamp
    ON kafka_messages(timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_kafka_messages_topic_timestamp
    ON kafka_messages(topic, timestamp DESC);

CREATE TABLE IF NOT EXISTS consumer_offsets (
    id BIGSERIAL PRIMARY KEY,
    consumer_group VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(consumer_group, topic, partition)
);

CREATE TABLE IF NOT EXISTS message_cache (
    id BIGSERIAL PRIMARY KEY,
    cache_key VARCHAR(255) NOT NULL UNIQUE,
    cache_value JSONB,
    ttl_ms BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Allow test user to create/drop tables
GRANT ALL PRIVILEGES ON DATABASE kafka_ui_db TO testuser;
```

---

## PHASE 6: Maven Commands Reference

```bash
# Run all unit tests (excludes integration tests)
mvn clean test

# Run all integration tests
mvn clean verify

# Run specific test class
mvn test -Dtest=KafkaConsumerIntegrationTest

# Run tests matching pattern
mvn test -Dtest=*Kafka*

# Run with detailed logging
mvn test -X -Dorg.slf4j.simpleLogger.defaultLogLevel=debug

# Run in parallel (4 threads)
mvn test -DthreadCount=4 -Dparallel=classes

# Skip slow tests
mvn test -Dgroups="!slow"

# Generate test report
mvn surefire-report:report
# View at: target/site/surefire-report.html
```

---

## PHASE 7: Verification Checklist

After implementing all steps, verify:

```bash
# 1. Check dependencies are resolved
mvn dependency:tree | grep testcontainers

# 2. Verify Docker is accessible
docker ps

# 3. Run first integration test
mvn verify -Dtest=KafkaConsumerIntegrationTest

# 4. Check test output includes container startup
mvn verify -Dtest=KafkaConsumerIntegrationTest -X | grep -i "container"

# 5. Verify database reset works
mvn verify -Dtest=*Test

# 6. Run all tests in parallel
mvn clean verify -DthreadCount=4 -Dparallel=classes

# 7. Verify test execution time <5 minutes
time mvn clean verify
```

---

## Troubleshooting

### Problem: "Docker daemon is not available"
**Solution**:
```bash
docker ps
docker info
```

### Problem: "Port 5432 already in use"
**Solution**: Ensure dynamic port binding (no fixed ports in container configuration)

### Problem: Tests pass individually, fail when run together
**Solution**: Verify `@BeforeEach` database reset is being called
```bash
mvn test -Dtest=TestName -X | grep "resetDatabase"
```

### Problem: Tests slow on first run (60+ seconds)
**Solution**: This is normal (container startup). Enable reuse for faster subsequent runs:
```
echo "testcontainers.reuse.enable=true" > .testcontainers.properties
```

---

## Success Criteria

You've successfully implemented Testcontainers when:

1. ✓ Tests run with shared Kafka and PostgreSQL containers
2. ✓ Each test takes 1-10 seconds (after container startup)
3. ✓ Database resets automatically before each test
4. ✓ Tests pass locally and in CI/CD pipeline
5. ✓ Full test suite completes in <5 minutes with 4 threads
6. ✓ No manual Docker setup required (Docker daemon running is all)
7. ✓ Test isolation verified (tests can run in any order, parallel)

---

## Next Steps

1. Implement Phase 1-2 (Maven + Base Infrastructure)
2. Run first test: `mvn verify -Dtest=KafkaConsumerIntegrationTest`
3. Implement Phase 3-4 (More integration tests)
4. Add Phase 5-6 (Additional test classes and configurations)
5. Integrate into CI/CD pipeline (GitHub Actions)

