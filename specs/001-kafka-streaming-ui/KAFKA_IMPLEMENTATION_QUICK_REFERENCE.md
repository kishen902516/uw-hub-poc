# Quick Reference: Spring Kafka Consumer Implementation

**TL;DR**: Copy-paste configurations for implementing the recommended Kafka consumer setup.

---

## 1. application.yml (Copy This Entire Section)

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

    consumer:
      group-id: ${KAFKA_CONSUMER_GROUP:uw-hub-streaming-group}

      # Batch settings (1000 msg/sec optimization)
      max-poll-records: 500
      fetch-max-bytes: 52428800
      fetch-min-bytes: 10485760
      fetch-max-wait-ms: 500

      # Offset management (no message loss)
      enable-auto-commit: false
      auto-offset-reset: earliest
      isolation-level: read_committed

      # Timing
      session-timeout-ms: 30000
      heartbeat-interval-ms: 10000
      max-poll-interval-ms: 300000

      # Deserialization
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer

      properties:
        spring.json.trusted.packages: "com.uwhub.domain.event"
        spring.json.use.type.headers: false

    # Listener configuration
    listener:
      type: batch
      poll-timeout: 3000
      concurrency: 8
      idle-event-interval: 30000
      ack-mode: manual

    # Producer (for DLQ)
    producer:
      acks: all
      retries: 3
      compression-type: snappy
      properties:
        linger.ms: 10
        batch.size: 32768

# Actuator for monitoring
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus

  metrics:
    tags:
      application: uw-hub-streaming
      environment: production
```

---

## 2. pom.xml Dependencies

```xml
<!-- Spring Kafka -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
    <version>3.1.3</version>
</dependency>

<!-- Micrometer Prometheus (metrics) -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>

<!-- Spring Data JPA (for repository) -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-data-jpa</artifactId>
</dependency>

<!-- PostgreSQL Driver -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.6.0</version>
</dependency>

<!-- Testcontainers -->
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <version>1.19.3</version>
    <scope>test</scope>
</dependency>
```

---

## 3. KafkaConsumerConfig.java

```java
package com.uwhub.infrastructure.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                 kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                 kafkaProperties.getConsumerGroupId());

        // Batch settings
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10485760);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // Offset management
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Timing
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        // Deserializers
        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            new JsonDeserializer<>(String.class, false)
        );
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>>
        kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(8);

        ContainerProperties containerProperties = factory.getContainerProperties();
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL);
        containerProperties.setPollTimeout(3000);
        containerProperties.setIdleEventInterval(30000L);

        factory.setCommonErrorHandler(kafkaErrorHandler());

        return factory;
    }

    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate) {

        FixedBackOff backOff = new FixedBackOff(5000, 3);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (r, e) -> new TopicPartition(r.getTopic() + ".dlq", 0)
            ),
            backOff
        );

        errorHandler.addNotRetryableExceptions(
            DataIntegrityViolationException.class,
            JsonMappingException.class
        );

        return errorHandler;
    }
}
```

---

## 4. KafkaMessageListener.java (Batch Processor)

```java
package com.uwhub.infrastructure.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaMessageListener {

    private final MessageService messageService;

    public KafkaMessageListener(MessageService messageService) {
        this.messageService = messageService;
    }

    @KafkaListener(
        topics = "${kafka.topics:topic1,topic2}",
        groupId = "${kafka.consumer.group-id:uw-hub-streaming-group}"
    )
    public void processMessageBatch(
            List<ConsumerRecord<String, String>> records,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {

        long startTime = System.currentTimeMillis();

        try {
            // Process batch
            messageService.processBatch(records);

            // Manual acknowledge after successful processing
            ack.acknowledge();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Batch processed: messages={}, partition={}, duration={}ms",
                     records.size(), partition, duration);

        } catch (Exception e) {
            // Don't acknowledge - will retry
            log.error("Batch processing failed, no ack sent", e);
            throw new RuntimeException(e);
        }
    }
}
```

---

## 5. MessageService.java

```java
package com.uwhub.application.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MessageService {

    private final MessageRepository messageRepository;
    private final SseEventPublisher sseEventPublisher;
    private final KafkaMetricsPublisher metricsPublisher;

    public MessageService(
            MessageRepository messageRepository,
            SseEventPublisher sseEventPublisher,
            KafkaMetricsPublisher metricsPublisher) {
        this.messageRepository = messageRepository;
        this.sseEventPublisher = sseEventPublisher;
        this.metricsPublisher = metricsPublisher;
    }

    @Transactional
    public void processBatch(List<ConsumerRecord<String, String>> records) {
        long startTime = System.currentTimeMillis();

        try {
            // Convert to domain objects
            List<KafkaMessage> messages = records.stream()
                .map(this::mapToDomain)
                .collect(Collectors.toList());

            // Batch insert (upsert to handle duplicates)
            messageRepository.saveAllUpsert(messages);

            // Publish SSE events
            messages.forEach(sseEventPublisher::publishMessage);

            // Record metrics
            long duration = System.currentTimeMillis() - startTime;
            metricsPublisher.recordBatchProcessing(records, duration);

        } catch (DataIntegrityViolationException e) {
            log.error("Database integrity error", e);
            metricsPublisher.recordError(records.get(0).topic(), "DataIntegrityError");
            throw e;  // Non-retryable
        } catch (Exception e) {
            log.error("Unexpected error", e);
            metricsPublisher.recordError(records.get(0).topic(), e.getClass().getSimpleName());
            throw new RuntimeException(e);  // Retryable
        }
    }

    private KafkaMessage mapToDomain(ConsumerRecord<String, String> record) {
        return KafkaMessage.builder()
            .topic(record.topic())
            .partition(record.partition())
            .offset(record.offset())
            .key(record.key())
            .value(record.value())
            .timestamp(record.timestamp())
            .build();
    }
}
```

---

## 6. KafkaMessage Entity

```java
package com.uwhub.domain.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "kafka_messages", indexes = {
    @Index(name = "idx_topic_timestamp", columnList = "topic, created_at DESC"),
    @Index(name = "idx_offset", columnList = "offset"),
    @Index(name = "idx_partition", columnList = "partition")
})
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaMessage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private Integer partition;

    @Column(nullable = false, unique = true)
    private Long offset;

    @Column
    private String key;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String value;

    @Column(nullable = false)
    private Long timestamp;

    @Column(nullable = false, updatedDate = true)
    private LocalDateTime createdAt;

    @Version
    private Long version;
}
```

---

## 7. MessageRepository (Batch Upsert)

```java
package com.uwhub.infrastructure.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface MessageRepository extends JpaRepository<KafkaMessage, Long> {

    @Query(nativeQuery = true, value = """
        INSERT INTO kafka_messages (topic, partition, offset, key, value, timestamp, created_at)
        VALUES (:#{#message.topic}, :#{#message.partition}, :#{#message.offset},
                :#{#message.key}, :#{#message.value}, :#{#message.timestamp}, NOW())
        ON CONFLICT (offset) DO UPDATE SET
            value = EXCLUDED.value,
            timestamp = EXCLUDED.timestamp
    """)
    void upsert(KafkaMessage message);

    @Transactional
    default void saveAllUpsert(List<KafkaMessage> messages) {
        for (KafkaMessage message : messages) {
            upsert(message);
        }
    }
}
```

---

## 8. KafkaMetricsPublisher.java

```java
package com.uwhub.infrastructure.kafka.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaMetricsPublisher {

    private final MeterRegistry meterRegistry;

    public KafkaMetricsPublisher(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void recordBatchProcessing(
            List<ConsumerRecord<String, ?>> records,
            long processingTimeMs) {

        String topic = records.get(0).topic();

        meterRegistry.counter(
            "kafka.batch.processed.total",
            "topic", topic
        ).increment(records.size());

        meterRegistry.timer(
            "kafka.batch.processing.time",
            "topic", topic
        ).record(processingTimeMs, TimeUnit.MILLISECONDS);

        double msgPerSec = (records.size() * 1000.0) / processingTimeMs;
        meterRegistry.gauge(
            "kafka.throughput.msg_per_sec",
            "topic", topic,
            () -> msgPerSec
        );
    }

    public void recordError(String topic, String errorType) {
        meterRegistry.counter(
            "kafka.errors.total",
            "topic", topic,
            "error_type", errorType
        ).increment();
    }
}
```

---

## 9. integration-test.yml (Test Configuration)

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS}

  datasource:
    url: ${DB_JDBC_URL}
    username: ${DB_USERNAME}
    password: ${DB_PASSWORD}

  jpa:
    hibernate:
      ddl-auto: create-drop
    database-platform: org.hibernate.dialect.PostgreSQLDialect
```

---

## 10. KafkaConsumerIntegrationTest.java

```java
package com.uwhub.infrastructure.kafka;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
public class KafkaConsumerIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
        DockerImageName.parse("postgres:15-alpine")
    )
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                    kafka::getBootstrapServersString);
        registry.add("spring.datasource.url",
                    postgres::getJdbcUrl);
        registry.add("spring.datasource.username",
                    postgres::getUsername);
        registry.add("spring.datasource.password",
                    postgres::getPassword);
    }

    @Test
    void testBatchMessageProcessing() throws Exception {
        produceMessages(500);

        await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> messageRepository.count() == 500);

        assertThat(messageRepository.count()).isEqualTo(500);
    }

    @Test
    void test1000MsgSecThroughput() throws Exception {
        long startTime = System.currentTimeMillis();
        produceMessages(5000);

        await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> messageRepository.count() == 5000);

        long duration = System.currentTimeMillis() - startTime;
        double throughput = (5000 * 1000.0) / duration;

        assertThat(throughput).isGreaterThan(1000);
    }
}
```

---

## Deployment: Docker & Kubernetes

### Dockerfile

```dockerfile
FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

COPY target/uw-hub-backend-*.jar app.jar

EXPOSE 8080

ENV JAVA_OPTS="-Xmx2g -Xms1g"

ENTRYPOINT ["java", "${JAVA_OPTS}", "-jar", "app.jar"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: uw-hub-kafka-consumer
spec:
  replicas: 3
  selector:
    matchLabels:
      app: uw-hub-kafka-consumer
  template:
    metadata:
      labels:
        app: uw-hub-kafka-consumer
    spec:
      containers:
      - name: consumer
        image: uw-hub-backend:latest
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: kafka:9092
        - name: KAFKA_CONSUMER_GROUP
          value: uw-hub-streaming-group
        - name: KAFKA_TOPICS
          value: "topic1,topic2"
        resources:
          requests:
            cpu: "2"
            memory: "2Gi"
          limits:
            cpu: "4"
            memory: "4Gi"
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
```

---

## Monitoring: Prometheus & Grafana

### Prometheus Scrape Config

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'uw-hub-streaming'
    static_configs:
      - targets: ['localhost:8080']
    metrics_path: '/actuator/prometheus'
```

### Key Grafana Queries

```promql
# Throughput
rate(kafka_consumer_records_consumed_total[1m])

# Consumer Lag
kafka_consumer_lag{topic="topic1"}

# Error Rate
rate(kafka_errors_total[1m])

# P95 Latency
histogram_quantile(0.95, rate(kafka_batch_processing_time_bucket[1m]))
```

---

## Troubleshooting Checklist

| Issue | Check | Solution |
|-------|-------|----------|
| Messages not consumed | Consumer logs, Kafka broker connectivity | Verify bootstrap-servers, group-id |
| Messages in DLQ | DLQ topic, exception headers | Check non-retryable exception list |
| High consumer lag | throughput metrics, DB insert time | Increase concurrency, check DB connection pool |
| Rebalancing loops | session.timeout.ms vs. heartbeat.interval.ms | Ensure heartbeat < session/3 |
| Memory issues | Heap usage metrics, batch size | Reduce max-poll-records from 500 to 250 |
| Duplicate messages | DB upsert working? | Verify ON CONFLICT clause in SQL |

---

## Environment Variables (Docker/Kubernetes)

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_CONSUMER_GROUP=uw-hub-streaming-group
KAFKA_TOPICS=topic1,topic2

# Database
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=uwhub
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secure_password

# Consumer Tuning
KAFKA_CONSUMER_CONCURRENCY=8
KAFKA_CONSUMER_MAX_POLL_RECORDS=500

# Monitoring
MANAGEMENT_METRICS_EXPORT_PROMETHEUS_ENABLED=true
LOGGING_LEVEL_ORG_SPRINGFRAMEWORK_KAFKA=INFO
```

---

## Key Takeaways

✅ **Do This**:
- Use batch listeners for >500 msg/sec
- Manual offset commit for reliability
- DLQ for poison message handling
- Horizontal scaling with 3+ pods
- Prometheus metrics + alerting

❌ **Don't Do This**:
- Auto-commit (message loss risk)
- Single message listeners (slow)
- No error handling (pipeline blocks)
- Single pod (no HA)
- No monitoring (blind to problems)
