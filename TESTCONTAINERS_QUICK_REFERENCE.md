# Testcontainers Quick Reference Card
## Copy-Paste Templates for Common Scenarios

---

## 1. POM.XML - Complete Test Configuration

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.example</groupId>
    <artifactId>kafka-streaming-ui</artifactId>
    <version>1.0.0</version>

    <parent>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-parent</artifactId>
        <version>3.3.0</version>
        <relativePath/>
    </parent>

    <properties>
        <java.version>21</java.version>
        <maven.compiler.source>21</maven.compiler.source>
        <maven.compiler.target>21</maven.compiler.target>
        <testcontainers.version>1.20.0</testcontainers.version>

        <!-- JUnit 5 Parallel Execution -->
        <junit.jupiter.execution.parallel.enabled>true</junit.jupiter.execution.parallel.enabled>
        <junit.jupiter.execution.parallel.mode.default>concurrent</junit.jupiter.execution.parallel.mode.default>
        <junit.jupiter.execution.parallel.mode.classes.default>concurrent</junit.jupiter.execution.parallel.mode.classes.default>
    </properties>

    <dependencyManagement>
        <dependencies>
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

        <!-- Database -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>42.7.3</version>
        </dependency>

        <!-- TEST DEPENDENCIES -->

        <!-- Spring Boot Test -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Testcontainers -->
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>kafka</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>postgresql</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-testcontainers</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>

        <!-- Async Assertions -->
        <dependency>
            <groupId>org.awaitility</groupId>
            <artifactId>awaitility</artifactId>
            <version>4.14.1</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Maven Surefire: Unit Tests -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-surefire-plugin</artifactId>
                <version>3.1.2</version>
                <configuration>
                    <parallel>classes</parallel>
                    <threadCount>4</threadCount>
                    <reuseForks>true</reuseForks>
                    <forkedProcessTimeoutInSeconds>300</forkedProcessTimeoutInSeconds>
                    <argLine>-XX:+UseG1GC -Xmx2G -Xms512M</argLine>
                    <includes>
                        <include>**/*Test.java</include>
                    </includes>
                    <excludes>
                        <exclude>**/*IntegrationTest.java</exclude>
                    </excludes>
                </configuration>
            </plugin>

            <!-- Maven Failsafe: Integration Tests -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-failsafe-plugin</artifactId>
                <version>3.1.2</version>
                <configuration>
                    <parallel>classes</parallel>
                    <threadCount>4</threadCount>
                    <reuseForks>true</reuseForks>
                    <includes>
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

---

## 2. Base Integration Test Class

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

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
public abstract class BaseIntegrationTest {

    @Autowired
    protected TestDatabaseReset testDatabaseReset;

    @BeforeEach
    protected void resetDatabase() {
        testDatabaseReset.resetAllTables();
    }
}
```

---

## 3. Container Configuration Class

```java
package com.example.integration.config;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@TestConfiguration
public class TestcontainersConfiguration {

    @Container
    public static final PostgreSQLContainer<?> POSTGRES =
        new PostgreSQLContainer<>("postgres:17-alpine")
            .withDatabaseName("testdb")
            .withUsername("testuser")
            .withPassword("testpass")
            .withInitScript("db/init-test.sql");

    @Container
    public static final KafkaContainer KAFKA =
        new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.0")
        );

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",
            () -> "jdbc:postgresql://" + POSTGRES.getHost() + ":" +
                  POSTGRES.getFirstMappedPort() + "/testdb");
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }
}
```

---

## 4. Minimal Integration Test Example

```java
package com.example.integration;

import com.example.domain.KafkaMessage;
import com.example.repository.KafkaMessageRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.*;

class MyIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private KafkaMessageRepository repository;

    @Test
    void shouldConsumeKafkaMessage() {
        // ARRANGE
        String topic = "test-topic";
        String message = "{\"id\": 1}";

        // ACT
        kafkaTemplate.send(topic, "key1", message);

        // ASSERT
        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> {
                List<KafkaMessage> messages = repository.findByTopic(topic);
                assertThat(messages).hasSize(1);
            });
    }
}
```

---

## 5. Application Configuration for Tests

File: `src/test/resources/application-test.properties`

```properties
# Logging
logging.level.root=WARN
logging.level.com.example=DEBUG
logging.level.org.springframework.kafka=INFO

# Database
spring.jpa.hibernate.ddl-auto=create-drop
spring.jpa.show-sql=false

# Kafka
spring.kafka.consumer.group-id=test-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=true

# Server
server.port=0
```

File: `src/test/resources/db/init-test.sql`

```sql
CREATE TABLE kafka_messages (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    key VARCHAR(255),
    value TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_kafka_messages_topic ON kafka_messages(topic);
CREATE INDEX idx_kafka_messages_timestamp ON kafka_messages(timestamp DESC);
```

---

## 6. Testing Common Scenarios

### Test: Simple Message Persistence

```java
@Test
void shouldPersistMessage() {
    kafkaTemplate.send("orders", "key1", "{\"amount\": 99.99}");

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            List<KafkaMessage> messages = repository.findByTopic("orders");
            assertThat(messages).hasSize(1);
        });
}
```

### Test: Batch Processing

```java
@Test
void shouldProcessBatch() {
    // Produce 100 messages
    for (int i = 0; i < 100; i++) {
        kafkaTemplate.send("events", String.valueOf(i), "{\"id\":" + i + "}");
    }

    // Assert all processed
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
            assertThat(repository.findByTopic("events")).hasSize(100);
        });
}
```

### Test: Multiple Topics

```java
@Test
void shouldHandleMultipleTopics() {
    kafkaTemplate.send("orders", "k1", "order");
    kafkaTemplate.send("payments", "k2", "payment");

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            assertThat(repository.findByTopic("orders")).hasSize(1);
            assertThat(repository.findByTopic("payments")).hasSize(1);
        });
}
```

### Test: API Endpoint

```java
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
class APIIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void shouldRetrieveMessages() {
        // Produce message
        kafkaTemplate.send("api-topic", "key", "value");

        // Wait for persistence
        await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> repository.findByTopic("api-topic").size() == 1);

        // Call API
        KafkaMessage[] response = restTemplate.getForObject(
            "/api/messages?topic=api-topic",
            KafkaMessage[].class
        );

        assertThat(response).hasSize(1);
    }
}
```

---

## 7. Maven Commands

```bash
# Run all unit tests
mvn clean test

# Run all integration tests
mvn clean verify

# Run specific test
mvn test -Dtest=MyIntegrationTest

# Run with 4 parallel threads
mvn test -DthreadCount=4

# Run excluding slow tests
mvn test "-Dgroups=!slow"

# Run with debug output
mvn test -X

# Generate test report
mvn surefire-report:report
open target/site/surefire-report.html
```

---

## 8. GitHub Actions Workflow

File: `.github/workflows/integration-tests.yml`

```yaml
name: Integration Tests

on:
  push:
    branches: [develop, main]
  pull_request:
    branches: [develop, main]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java-version: [21]

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK ${{ matrix.java-version }}
        uses: actions/setup-java@v4
        with:
          java-version: ${{ matrix.java-version }}
          distribution: 'temurin'
          cache: maven

      - name: Run integration tests
        run: |
          mvn clean verify \
            --batch-mode \
            --show-version \
            -DskipITs=false
        timeout-minutes: 10

      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: test-results
          path: target/surefire-reports/
```

---

## 9. Local Development Setup

### .gitignore Entry

```
# Testcontainers local reuse configuration
.testcontainers.properties
```

### .testcontainers.properties (local development only, DO NOT COMMIT)

```properties
# Enable container reuse for faster local development
testcontainers.reuse.enable=true
```

### Quick Start Script

```bash
#!/bin/bash
# setup-tests.sh

echo "Starting Docker..."
docker daemon

echo "Running integration tests..."
mvn clean verify

echo "Generating test report..."
mvn surefire-report:report

echo "Opening report..."
open target/site/surefire-report.html
```

---

## 10. Troubleshooting Checklist

```bash
# 1. Verify Docker is running
docker ps

# 2. Check dependency versions
mvn dependency:tree | grep testcontainers

# 3. Run single test for debugging
mvn test -Dtest=MyIntegrationTest -X

# 4. Check container logs
docker logs <container-id>

# 5. Verify network connectivity
docker network ls

# 6. Clean up dangling containers
docker container prune -f

# 7. Check port availability (Linux/Mac)
lsof -i :5432
lsof -i :9093
```

---

## 11. Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| **First run** | 40-50 sec | Container startup + tests |
| **Subsequent runs (reuse)** | 30-40 sec | Containers already running |
| **CI/CD run (fresh containers)** | 60-90 sec | 4 parallel threads |
| **Single test execution** | 100-500 ms | After container startup |
| **Database reset** | <1 sec | TRUNCATE operation |
| **Message throughput** | >500 msg/sec | 1000+ messages in <2 sec |

---

## 12. Key Files to Create

```
project-root/
├── pom.xml                          (use template from section 1)
├── .gitignore                       (add .testcontainers.properties)
│
├── src/
│   ├── test/
│   │   ├── java/com/example/
│   │   │   └── integration/
│   │   │       ├── BaseIntegrationTest.java
│   │   │       ├── config/
│   │   │       │   ├── TestcontainersConfiguration.java
│   │   │       │   ├── TestDatabaseReset.java
│   │   │       │   └── TestDataFixtures.java
│   │   │       ├── kafka/
│   │   │       │   └── KafkaConsumerIntegrationTest.java
│   │   │       ├── persistence/
│   │   │       │   └── KafkaMessageRepositoryIntegrationTest.java
│   │   │       └── api/
│   │   │           └── MessagesAPIIntegrationTest.java
│   │   │
│   │   └── resources/
│   │       ├── application-test.properties
│   │       └── db/
│   │           └── init-test.sql
│   │
│   └── main/
│       ├── java/...
│       └── resources/
│           └── application.properties
│
└── .github/
    └── workflows/
        └── integration-tests.yml
```

---

## 13. One-Minute Integration Test

Want to get started immediately? Copy this entire test:

```java
package com.example.integration;

import com.example.integration.config.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

import static org.awaitility.Awaitility.*;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
class QuickIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void reset() {
        try {
            jdbcTemplate.execute("TRUNCATE TABLE kafka_messages RESTART IDENTITY CASCADE");
        } catch (Exception e) {
            // Ignore
        }
    }

    @Test
    void testKafkaIntegration() {
        kafkaTemplate.send("test", "key1", "value1");

        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() -> {
                Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM kafka_messages",
                    Integer.class
                );
                assert count != null && count > 0;
            });
    }
}
```

Run it:
```bash
mvn clean test -Dtest=QuickIntegrationTest
```

---

## Success Indicators

After implementing these templates, you'll see:

✓ Tests run in 40-90 seconds total
✓ Containers start automatically
✓ Database reset happens between tests
✓ Messages consumed from Kafka
✓ Zero Docker manual setup needed
✓ Parallel test execution (4 threads)
✓ Works locally and in CI/CD
✓ Clear failure messages

Good luck! 🚀

