# Research: Spring Kafka Consumer Configuration for High-Throughput Real-Time Streaming

**Date**: 2025-11-02 | **Target**: 1000 messages/sec | **Environment**: Spring Boot 3.x, PostgreSQL, Kubernetes

---

## Executive Summary

This research provides a **production-ready Spring Kafka consumer configuration** optimized for your real-time Kafka-to-PostgreSQL-to-SSE pipeline. The configuration prioritizes:

1. **Throughput**: Achieve 1000 msg/sec with minimal latency (<100ms SSE push)
2. **Reliability**: Prevent message loss through offset management and error handling
3. **Horizontal Scaling**: Support multiple consumer instances with rebalancing
4. **Observability**: Comprehensive monitoring and alerting for production readiness

---

## Decision 1: Recommended Consumer Configuration

### Core Consumer Settings (application.yml)

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      # Group configuration
      group-id: ${KAFKA_CONSUMER_GROUP:uw-hub-streaming-group}
      max-poll-records: 500          # Batch size for throughput (default: 500)
      fetch-max-bytes: 52428800      # 50MB max fetch (default: 52428800)
      fetch-min-bytes: 10485760      # 10MB min fetch to trigger poll (default: 1)

      # Offset management (critical for reliability)
      enable-auto-commit: false       # Manual commit for at-least-once semantics
      auto-offset-reset: earliest     # Start from beginning on consumer reset

      # Network tuning for throughput
      session-timeout-ms: 30000       # Rebalance timeout
      heartbeat-interval-ms: 10000    # Heartbeat interval (1/3 of session timeout)
      max-poll-interval-ms: 300000    # 5 min - time to process batch

      # Deserialization
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.uwhub.domain.event"

    # Listener configuration (threading)
    listener:
      type: batch                      # Batch mode for throughput
      poll-timeout: 3000               # Time to wait for batch
      concurrency: 8                   # Concurrency per container (match consumer threads)
      idle-event-interval: 30000       # Emit idle events for monitoring
      no-poll-record-max-poll-records: 10  # Max records in no-poll batch

    # Producer settings (for DLQ publishing)
    producer:
      acks: all                        # Ensure all replicas ack
      retries: 3
      compression-type: snappy        # Reduce network I/O
```

### Listener Configuration (Java Code)

```java
@Configuration
public class KafkaConsumerConfig {

    /**
     * Consumer factory with optimized settings for 1000 msg/sec
     */
    @Bean
    public ConsumerFactory<String, KafkaMessage> consumerFactory(
            KafkaProperties kafkaProperties) {

        Map<String, Object> configs = new HashMap<>();

        // Batch settings for throughput
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        configs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10485760);      // 10MB
        configs.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);         // 500ms wait

        // Offset management
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Parallelism and timeouts
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

        // Default JSON deserializer
        return new DefaultKafkaConsumerFactory<>(
            configs,
            new StringDeserializer(),
            new JsonDeserializer<>(KafkaMessage.class, false)
        );
    }

    /**
     * Batch listener container factory for high throughput
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaMessage>
        kafkaListenerContainerFactory(ConsumerFactory<String, KafkaMessage> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, KafkaMessage> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);                    // Enable batch processing
        factory.setConcurrency(8);                         // 8 threads per container
        factory.getContainerProperties().setPollTimeout(3000);
        factory.getContainerProperties().setIdleEventInterval(30000L);

        // Error handling with DLQ
        factory.setCommonErrorHandler(kafkaErrorHandler());

        // Ack mode: manual for guaranteed processing
        factory.getContainerProperties()
            .setAckMode(ContainerProperties.AckMode.MANUAL);

        return factory;
    }

    /**
     * Error handler with DLQ and retry logic
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, KafkaMessage> kafkaTemplate) {

        FixedBackOff backOff = new FixedBackOff(5000, 3);  // Retry 3x with 5s delay

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (r, e) -> new TopicPartition(r.getTopic() + ".dlq", r.getPartition())
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

### Database Batch Insert Configuration

```java
@Configuration
public class DatabaseBatchConfig {

    @Bean
    public JdbcBatchItemWriter<KafkaMessage> kafkaMessageBatchWriter(
            JdbcOperationsFactory jdbcOperationsFactory) {

        return new JdbcBatchItemWriterBuilder<KafkaMessage>()
            .dataSource(jdbcOperationsFactory.getDataSource())
            .sql("INSERT INTO kafka_messages " +
                 "(topic, partition, offset, key, value, timestamp, created_at) " +
                 "VALUES (?, ?, ?, ?, ?, ?, NOW())")
            .itemPreparedStatementSetter((msg, ps) -> {
                ps.setString(1, msg.getTopic());
                ps.setInt(2, msg.getPartition());
                ps.setLong(3, msg.getOffset());
                ps.setString(4, msg.getKey());
                ps.setString(5, msg.getValue());
                ps.setLong(6, msg.getTimestamp());
            })
            .build();
    }
}
```

---

## Decision 2: Batch Processing vs. Individual Processing

### Recommendation: **BATCH PROCESSING** (Batch Listener Mode)

#### Why Batch Processing for 1000 msg/sec:

| Aspect | Batch Processing | Individual Processing |
|--------|------------------|----------------------|
| **Throughput** | 1000+ msg/sec ✅ | ~100-200 msg/sec ❌ |
| **Database I/O** | 500 inserts/batch (batching efficient) | 1 insert per message (overhead) |
| **Error Handling** | Fail-fast, process batch or skip | Granular error handling per message |
| **Network Roundtrips** | 2 roundtrips/batch (50 fetches/sec) | 1000 roundtrips/sec (network limit) |
| **GC Pressure** | Lower (fewer object creations) | Higher (objects per message) |

#### Batch Listener Configuration:

```java
@KafkaListener(
    topics = {"${kafka.topics:topic1,topic2}"},
    groupId = "${kafka.consumer.group-id}",
    containerFactory = "kafkaListenerContainerFactory"
)
public void processMessageBatch(
        List<ConsumerRecord<String, KafkaMessage>> records,
        Acknowledgment ack,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {

    try {
        // Process batch as atomic unit
        List<KafkaMessage> messages = records.stream()
            .map(record -> mapRecordToMessage(record))
            .collect(Collectors.toList());

        // Batch database insert (JDBC batch or ORM)
        messageRepository.saveAll(messages);

        // Emit SSE events to clients
        messages.forEach(msg ->
            sseEventPublisher.publishMessage(msg)
        );

        // Manual commit after successful processing
        ack.acknowledge();

        log.info("Processed batch of {} messages from partition {}",
                 records.size(), partition);

    } catch (DataIntegrityViolationException e) {
        // Non-retryable - send to DLQ
        log.error("Data integrity error in batch, sending to DLQ", e);
        throw e;  // DefaultErrorHandler sends to DLQ
    } catch (Exception e) {
        // Retryable - DefaultErrorHandler will retry
        log.error("Batch processing failed, will retry", e);
        throw e;
    }
}

private KafkaMessage mapRecordToMessage(ConsumerRecord<String, KafkaMessage> record) {
    return record.value()
        .withTopic(record.topic())
        .withPartition(record.partition())
        .withOffset(record.offset())
        .withTimestamp(record.timestamp());
}
```

#### Performance Comparison Table:

```
Configuration: max-poll-records = 500, 1000 msg/sec total, 8 concurrent listeners

Individual Message Mode:
- 1000 messages arrive per second
- 1000 listener invocations (context switches)
- 1000 database inserts (or batched in SQL level)
- 1000 offset commits (expensive operation)
- Expected throughput: 100-200 msg/sec (network bound)

Batch Mode (500 max-poll-records):
- 1000 messages arrive per second
- 2 listener invocations (2 full batches)
- 1 batch insert of 500 + 1 batch insert of 500 (efficient)
- 2 offset commits (much cheaper)
- Expected throughput: 1000+ msg/sec (GC bound, not I/O)
```

---

## Decision 3: Offset Management for Zero Message Loss

### Strategy: **Manual Commit with Batch Processing**

```java
@Configuration
public class OffsetManagementConfig {

    /**
     * Critical: Configure manual offset commit for at-least-once semantics
     * Ensures message is committed only AFTER successful database insert
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaMessage>
        kafkaListenerContainerFactory(ConsumerFactory<String, KafkaMessage> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, KafkaMessage> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);

        // CRITICAL: Manual commit mode ensures offset committed AFTER processing
        ContainerProperties props = factory.getContainerProperties();
        props.setAckMode(ContainerProperties.AckMode.MANUAL);

        // Alternative: MANUAL_IMMEDIATE - commit asynchronously (faster but riskier)
        // props.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}

@Component
public class KafkaMessageListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(
        topics = "${kafka.topics}",
        groupId = "${kafka.consumer.group-id}"
    )
    public void processMessages(
            List<ConsumerRecord<String, KafkaMessage>> records,
            Acknowledgment ack) {

        long startTime = System.currentTimeMillis();

        try {
            // Step 1: Extract message data
            List<KafkaMessage> messages = extractMessages(records);

            // Step 2: Persist to database (CRITICAL: must complete before ack)
            messageRepository.saveAll(messages);

            // Step 3: Publish SSE events
            messages.forEach(sseEventPublisher::publishMessage);

            // Step 4: ONLY AFTER successful processing, commit offset
            ack.acknowledge();

            long duration = System.currentTimeMillis() - startTime;
            log.info("Batch commit: {} messages in {}ms, last offset: {}",
                     records.size(),
                     duration,
                     records.get(records.size() - 1).offset());

        } catch (Exception e) {
            // DO NOT acknowledge - rebalance will replay messages
            log.error("Batch processing failed, no ack sent. Messages will be retried", e);
            throw e;  // Let error handler decide (retry or DLQ)
        }
    }

    private List<KafkaMessage> extractMessages(
            List<ConsumerRecord<String, KafkaMessage>> records) {
        return records.stream()
            .map(this::buildMessage)
            .collect(Collectors.toList());
    }

    private KafkaMessage buildMessage(ConsumerRecord<String, KafkaMessage> record) {
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

### Offset Management: Key Concepts

| Property | Value | Rationale |
|----------|-------|-----------|
| `enable-auto-commit` | `false` | Manual control ensures offset committed only after DB insert |
| `auto.offset.reset` | `earliest` | Start from beginning if no offset found (new consumer) |
| `isolation.level` | `read_committed` | Only read committed messages (requires producer `acks=all`) |
| `Ack Mode` | `MANUAL` | Explicit ack after processing (safest, slightly slower) |

### Failure Scenario: What Happens on Crash?

```
Timeline with Manual Commit:
─────────────────────────────────────────

Consumer polls: msgs [offset 1000-1499]
↓
Application crashes BEFORE acknowledge
↓
Consumer never commits offset
↓
Rebalance occurs
↓
New consumer instance starts
↓
Kafka broker sees last committed offset = 999
↓
New consumer starts from offset 1000 (same batch)
↓
Database insert happens again (using UPSERT to handle duplicates)
↓
Result: AT-LEAST-ONCE semantics ✅ (no message loss)
```

---

## Decision 4: Error Handling Pattern (DLQ + Retry Policy)

### Architecture: Error Handling with Dead Letter Queue

```yaml
# DLQ topic naming convention
Topics:
  - kafka_messages          # Main topic
  - kafka_messages.dlq      # Dead letter queue
  - kafka_messages.retry    # Retry topic (optional)
```

### Spring Kafka Error Handler Configuration

```java
@Configuration
public class ErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandlingConfig.class);

    /**
     * Error handler with exponential backoff and DLQ
     * Strategy:
     * 1. First failure: Retry with 1s delay
     * 2. Second failure: Retry with 5s delay
     * 3. Third failure: Send to DLQ
     * 4. Fatal errors (JSON parse, etc): Send to DLQ immediately
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate,
            ErrorMetricsPublisher errorMetrics) {

        // Exponential backoff: 1s, 5s, 25s (but we only do 3 retries)
        ExponentialBackOff backOff = new ExponentialBackOff(1000, 2.0);
        backOff.setMaxElapsedTime(30000);  // Max 30s total

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            deadLetterPublishingRecoverer(kafkaTemplate),
            backOff
        );

        // Non-retryable exceptions - send to DLQ immediately
        errorHandler.addNotRetryableExceptions(
            // JSON deserialization errors
            JsonMappingException.class,
            JsonParseException.class,

            // Database constraint violations
            DataIntegrityViolationException.class,
            ConstraintViolationException.class,

            // Business logic errors
            InvalidMessageFormatException.class,
            TopicConfigurationException.class
        );

        // Optional: Log all errors
        errorHandler.setErrorCallback((thrownException, data) -> {
            log.error("Kafka error (will be sent to DLQ): topic={}, partition={}, offset={}",
                     data.topic(),
                     data.partition(),
                     data.offset(),
                     thrownException);

            errorMetrics.recordError(
                data.topic(),
                thrownException.getClass().getSimpleName()
            );
        });

        return errorHandler;
    }

    /**
     * Dead Letter Publisher: Recoverer that publishes failed messages to DLQ topic
     */
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, String> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
            new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (consumerRecord, exception) -> {
                    // Route to DLQ: topic-name.dlq
                    String dlqTopic = consumerRecord.topic() + ".dlq";
                    return new TopicPartition(dlqTopic, 0);
                }
            );

        recoverer.setHeadersProvider((consumerRecord, exception) -> {
            Headers headers = new RecordHeaders();
            headers.add("original-topic",
                       consumerRecord.topic().getBytes(StandardCharsets.UTF_8));
            headers.add("original-offset",
                       String.valueOf(consumerRecord.offset())
                           .getBytes(StandardCharsets.UTF_8));
            headers.add("original-partition",
                       String.valueOf(consumerRecord.partition())
                           .getBytes(StandardCharsets.UTF_8));
            headers.add("exception-class",
                       exception.getClass().getName().getBytes(StandardCharsets.UTF_8));
            headers.add("exception-message",
                       exception.getMessage().getBytes(StandardCharsets.UTF_8));
            return headers;
        });

        return recoverer;
    }

    /**
     * Kafka template for publishing to DLQ
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(
            ProducerFactory<String, String> producerFactory) {

        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        template.setDefaultTopic("default-dlq");
        return template;
    }
}
```

### DLQ Consumer for Alerting

```java
@Component
public class DeadLetterQueueListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueListener.class);

    @KafkaListener(
        topics = "${kafka.dlq-topics}",
        groupId = "${kafka.dlq-consumer-group}"
    )
    public void processDLQMessage(
            ConsumerRecord<String, String> record,
            @Headers Map<String, String> headers) {

        String originalTopic = headers.get("original-topic");
        String originalOffset = headers.get("original-offset");
        String exceptionClass = headers.get("exception-class");

        log.error("MESSAGE IN DLQ: original-topic={}, original-offset={}, error={}",
                 originalTopic,
                 originalOffset,
                 exceptionClass);

        // TODO: Send alert to monitoring system (PagerDuty, Slack, etc.)
        // alertingService.notifyDLQMessage(record, headers);

        // TODO: Store in database for manual review
        // dlqMessageRepository.save(new DLQMessage(record, headers));
    }
}
```

### Error Handling Flowchart

```
Message arrives in Kafka
    ↓
Consumer polls batch
    ↓
Process batch (DB insert + SSE publish)
    ├─ SUCCESS
    │   ↓
    │   Manual acknowledge
    │   ↓
    │   Offset committed ✅
    │
    └─ EXCEPTION
        ↓
        Is exception retryable?
        ├─ NO (JSON parse, constraint violation)
        │   ↓
        │   Send to topic.dlq immediately
        │   ↓
        │   DLQ consumer alerts ⚠️
        │
        └─ YES (Network timeout, transient DB error)
            ↓
            Retry with backoff (1s delay)
            ├─ SUCCESS → acknowledge ✅
            └─ FAIL → Retry with backoff (5s delay)
               ├─ SUCCESS → acknowledge ✅
               └─ FAIL → Send to topic.dlq
                  ↓
                  DLQ consumer alerts ⚠️
```

---

## Decision 5: Horizontal Scaling with Consumer Groups and Partitions

### Consumer Scaling Strategy

```yaml
# Example: 2 Kafka topics, 3 partitions each, 1000 msg/sec target

Topic Structure:
  topic1:
    partitions: 3
    replication_factor: 3
    min_in_sync_replicas: 2

  topic2:
    partitions: 3
    replication_factor: 3
    min_in_sync_replicas: 2

Consumer Group:
  group-id: uw-hub-streaming-group
  instances: 3                  # 3 consumer pods in Kubernetes
```

### Consumer Scaling Configuration

```java
@Configuration
@EnableKafka
public class ConsumerScalingConfig {

    /**
     * Consumer factory that auto-discovers topics and scales
     */
    @Bean
    public ConsumerFactory<String, KafkaMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Cluster settings
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "uw-hub-streaming-group");

        // Rebalance strategy: RoundRobin for fair distribution
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                 Arrays.asList(
                     "org.apache.kafka.clients.consumer.RoundRobinAssignor"
                 ));

        // Rebalance timeout
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.REBALANCE_TIMEOUT_MS_CONFIG, 60000);

        // Batch settings for throughput
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10485760);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);

        // Offset management
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Concurrent listener container: scales horizontally
     * - concurrency: threads per container
     * - multiple pods in Kubernetes each running independent containers
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, KafkaMessage>
        kafkaListenerContainerFactory(
            ConsumerFactory<String, KafkaMessage> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, KafkaMessage> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);

        // Concurrency: 8 threads per pod (adjust based on CPU/memory)
        // With 3 pods: 24 total concurrent message processing threads
        factory.setConcurrency(Integer.parseInt(
            System.getenv().getOrDefault("KAFKA_CONSUMER_CONCURRENCY", "8")
        ));

        // Container properties
        ContainerProperties props = factory.getContainerProperties();
        props.setAckMode(ContainerProperties.AckMode.MANUAL);
        props.setPollTimeout(3000);
        props.setIdleEventInterval(30000L);

        // Error handling
        factory.setCommonErrorHandler(kafkaErrorHandler());

        return factory;
    }
}
```

### Kubernetes Deployment for Horizontal Scaling

```yaml
# k8s/kafka-consumer-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: uw-hub-kafka-consumer
spec:
  replicas: 3                    # 3 pod replicas for scaling
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
        - name: KAFKA_CONSUMER_CONCURRENCY
          value: "8"              # 8 threads per pod
        resources:
          requests:
            cpu: "2"              # 2 CPU cores
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

### Scaling Math: 1000 msg/sec with 3 Pods

```
Configuration:
- 2 Kafka topics
- 3 partitions per topic
- 3 consumer pods
- 8 threads per pod (concurrency)

Distribution:
- 6 total partitions (3 topics × 2)
- 3 pods in consumer group
- Each pod gets 2 partitions (6 ÷ 3)
- Each pod processes 2 topics with 1 partition each

Throughput calculation:
- 1000 msg/sec total
- 3 pods × 8 threads = 24 concurrent processors
- 1000 msg/sec ÷ 24 = ~42 msg/sec per thread (very manageable)
- With batch size 500: 2 batches per second per thread
- Processing time per batch: 500 ms at 1000 msg/sec rate (acceptable)

Rebalancing:
- When pod fails, Kafka rebalances partitions to remaining 2 pods
- Each pod gets 3 partitions
- No message loss (offset committed before processing)
- Slight throughput dip during rebalance (~5-10 seconds)
```

### Partition Strategy

```java
@Configuration
public class TopicProvisioningConfig {

    /**
     * Create topics with optimal partition count
     * Formula: partitions = target_throughput / expected_msg_per_sec_per_partition
     *
     * For 1000 msg/sec and ~330 msg/sec per partition:
     * partitions = 1000 / 330 = ~3 partitions
     */
    @Bean
    public NewTopic topic1(
            @Value("${kafka.topic1.name:topic1}") String name) {
        return TopicBuilder.name(name)
            .partitions(3)                          // 3 partitions
            .replicas(3)                            // 3 replicas (HA)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")  // 24 hours
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
            .build();
    }

    @Bean
    public NewTopic topic2(
            @Value("${kafka.topic2.name:topic2}") String name) {
        return TopicBuilder.name(name)
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
            .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "snappy")
            .build();
    }

    /**
     * DLQ topics: same partition count for consistency
     */
    @Bean
    public NewTopic topic1DLQ(
            @Value("${kafka.topic1.name:topic1}") String topicName) {
        return TopicBuilder.name(topicName + ".dlq")
            .partitions(3)
            .replicas(3)
            .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
            .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")  // 30 days
            .build();
    }
}
```

---

## Decision 6: Monitoring Strategy and Metrics

### Key Metrics to Track

```java
@Component
public class KafkaMetricsPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaMetricsPublisher.class);
    private final MeterRegistry meterRegistry;
    private final KafkaConsumerMetrics consumerMetrics;

    public KafkaMetricsPublisher(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.consumerMetrics = new KafkaConsumerMetrics();
    }

    /**
     * Critical Metrics for 1000 msg/sec system:
     *
     * 1. Consumer Lag (lag per partition)
     * 2. Processing Rate (msg/sec)
     * 3. Batch Processing Time (p50, p95, p99)
     * 4. Error Rate (errors/sec)
     * 5. DLQ Rate (messages to DLQ/sec)
     */

    public void recordBatchProcessing(
            List<ConsumerRecord<String, ?>> records,
            long processingTimeMs) {

        // Timer: batch processing latency
        Timer.Sample sample = Timer.start(meterRegistry);

        meterRegistry.counter(
            "kafka.batch.processed.total",
            "topic", records.get(0).topic(),
            "status", "success"
        ).increment(records.size());

        meterRegistry.timer(
            "kafka.batch.processing.time",
            "topic", records.get(0).topic()
        ).record(processingTimeMs, TimeUnit.MILLISECONDS);

        // Throughput: messages per second
        double msgPerSec = (records.size() * 1000.0) / processingTimeMs;
        meterRegistry.gauge(
            "kafka.throughput.msg_per_sec",
            "topic", records.get(0).topic(),
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

    public void recordDLQMessage(String topic) {
        meterRegistry.counter(
            "kafka.dlq.messages.total",
            "topic", topic
        ).increment();
    }

    public void recordConsumerLag(String topic, int partition, long lag) {
        meterRegistry.gauge(
            "kafka.consumer.lag",
            "topic", topic,
            "partition", String.valueOf(partition),
            () -> lag
        );
    }
}
```

### Prometheus Monitoring Configuration

```yaml
# application.yml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus

  metrics:
    # Enable Kafka consumer metrics
    tags:
      application: uw-hub-streaming
      environment: production

    # Micrometer Kafka metrics
    kafka:
      enabled: true

  # Prometheus actuator
  prometheus:
    enabled: true

# Prometheus metrics exposed at /actuator/prometheus
```

### Spring Kafka Built-in Metrics

Spring Kafka automatically exposes these metrics via Micrometer:

```
# Consumer metrics
kafka_consumer_fetch_latency_avg{client_id="..."}
kafka_consumer_records_consumed_rate{client_id="..."}
kafka_consumer_lag{client_id="...", topic="...", partition="0"}
kafka_consumer_lag_sum{client_id="..."}

# Connection metrics
kafka_consumer_connection_connect_total{client_id="..."}
kafka_consumer_connection_count{client_id="..."}

# Rebalance metrics
kafka_consumer_rebalance_latency_total{client_id="..."}
kafka_consumer_rebalance_total{client_id="..."}
```

### Prometheus Query Examples

```promql
# Current throughput (messages per second)
rate(kafka_consumer_records_consumed_total[1m])

# Consumer lag (messages behind)
kafka_consumer_lag{topic="topic1"}

# P95 batch processing latency
histogram_quantile(0.95,
  rate(kafka_batch_processing_time_bucket[1m]))

# Error rate
rate(kafka_errors_total[1m])

# DLQ message rate
rate(kafka_dlq_messages_total[1m])

# Rebalancing frequency
rate(kafka_consumer_rebalance_total[1m])
```

### Kubernetes Health Checks

```java
@Configuration
public class HealthCheckConfig {

    /**
     * Liveness: Pod is running (restart if unhealthy)
     * - Checks: JVM status, memory usage
     */
    @Bean
    public HealthIndicator jvmHealthIndicator() {
        return () -> {
            long heapUsage = Runtime.getRuntime().totalMemory()
                           - Runtime.getRuntime().freeMemory();
            long heapMax = Runtime.getRuntime().maxMemory();

            if (heapUsage > heapMax * 0.95) {
                return Health.down()
                    .withDetail("heap_usage_percent",
                        (heapUsage * 100) / heapMax)
                    .build();
            }
            return Health.up().build();
        };
    }

    /**
     * Readiness: Pod ready to receive traffic
     * - Checks: Kafka connection, Database connection
     */
    @Bean
    public HealthIndicator kafkaHealthIndicator(
            AdminClient adminClient) {
        return () -> {
            try {
                DescribeClusterResult cluster = adminClient
                    .describeCluster();
                cluster.brokers().get(3, TimeUnit.SECONDS);
                return Health.up()
                    .withDetail("kafka_status", "connected")
                    .build();
            } catch (Exception e) {
                return Health.down()
                    .withDetail("kafka_error", e.getMessage())
                    .build();
            }
        };
    }
}
```

### Grafana Dashboard Queries

```json
{
  "dashboard": {
    "title": "Kafka Consumer Monitoring",
    "panels": [
      {
        "title": "Throughput (msg/sec)",
        "targets": [{
          "expr": "rate(kafka_consumer_records_consumed_total[1m])"
        }]
      },
      {
        "title": "Consumer Lag",
        "targets": [{
          "expr": "kafka_consumer_lag"
        }]
      },
      {
        "title": "Batch Processing P95",
        "targets": [{
          "expr": "histogram_quantile(0.95, rate(kafka_batch_processing_time_bucket[1m]))"
        }]
      },
      {
        "title": "Error Rate",
        "targets": [{
          "expr": "rate(kafka_errors_total[1m])"
        }]
      },
      {
        "title": "DLQ Messages",
        "targets": [{
          "expr": "rate(kafka_dlq_messages_total[1m])"
        }]
      }
    ]
  }
}
```

---

## Rationale: Why These Settings Optimize for 1000 msg/sec and Reliability

### 1. Batch Processing (max-poll-records: 500)

- **Throughput**: Reduces network roundtrips from 1000 to 2 per second (500x improvement)
- **Database Efficiency**: Single batch insert vs. 1000 individual inserts
- **GC Pressure**: Fewer object allocations per second
- **Offset Management**: 2 commits per second vs. 1000 (cheaper operation)

### 2. Manual Offset Commit with Batch Mode

- **Reliability**: Ensures message persisted to DB before committing offset
- **Failure Recovery**: If crash occurs, messages replayed from last committed offset
- **Zero Loss**: Worst case is duplicate processing (handled by UPSERT in DB)

### 3. Concurrency: 8 Threads per Container

- **Parallelism**: Process multiple batches concurrently
- **CPU Efficiency**: Matches typical 2-CPU pod allocation (allow context switching)
- **Network I/O**: Allows batches to fetch while others process

### 4. Error Handling with DLQ

- **Reliability**: Poison messages don't block pipeline (sent to DLQ)
- **Observability**: All errors visible in DLQ topic for post-mortem analysis
- **Data Integrity**: Non-retryable errors (JSON parse, constraint violations) fail fast

### 5. Horizontal Scaling (3 Pods, 3 Partitions per Topic)

- **Throughput**: 24 concurrent threads (3 pods × 8 threads) = low per-thread load
- **Resilience**: Single pod failure only impacts 1/3 of traffic
- **Rebalancing**: Kafka automatically redistributes partitions on pod failure

### 6. Monitoring & Observability

- **Lag Tracking**: Detect when consumer falls behind
- **Error Alerting**: Know immediately when messages go to DLQ
- **Performance Tuning**: Identify bottlenecks (CPU, memory, DB I/O)

---

## Alternatives Considered and Trade-offs

### Alternative 1: Individual Message Processing (vs. Batch)

```yaml
Configuration:
  listener:
    type: single          # Process one message at a time
    concurrency: 100      # Many threads to compensate

Trade-offs:
  ❌ Throughput: Limited to 100-200 msg/sec (network bound)
  ❌ Database: 1000 separate inserts/sec (connection pool exhaustion)
  ❌ GC Pressure: 1000 new message objects per second
  ❌ Offset Commits: 1000 commit operations/sec (expensive)
  ✅ Error Handling: Fine-grained control per message (rarely needed)
```

**Verdict**: REJECTED for 1000 msg/sec. Only viable for <100 msg/sec systems.

---

### Alternative 2: Auto Commit (vs. Manual)

```yaml
Configuration:
  enable-auto-commit: true
  auto-commit-interval-ms: 5000  # Commit every 5 seconds

Trade-offs:
  ❌ Reliability: Messages may be lost if crash occurs between commit intervals
  ✅ Performance: Slightly faster (no explicit ack needed)
  ❌ Observability: Harder to debug when messages are lost

Example failure:
  - Consumer processes messages 1000-1500
  - Consumer crashes at 1s (before 5s auto-commit)
  - Messages 1000-1500 are lost forever ❌
```

**Verdict**: REJECTED. Risk of message loss unacceptable for critical systems.

---

### Alternative 3: Spring Cloud Stream (vs. Direct Spring Kafka)

```yaml
Spring Cloud Stream approach:
  - Abstraction layer over Kafka/RabbitMQ/etc
  - Simpler API (functions, binders)
  - Less control over offset management

Trade-offs:
  ❌ Performance: Extra abstraction layer adds latency
  ❌ Debugging: Harder to understand Kafka behavior
  ✅ Flexibility: Can swap message broker implementations
  ✅ Simplicity: Less boilerplate code
```

**Verdict**: REJECTED for performance-critical system. Use direct Spring Kafka for full control.

---

### Alternative 4: Single Consumer Pod (vs. Distributed)

```yaml
Configuration:
  replicas: 1              # Only one pod

Trade-offs:
  ❌ Scalability: Can't horizontally scale
  ❌ Resilience: Single pod failure = complete outage
  ✅ Simplicity: No rebalancing complexity
  ✅ Cost: Single pod vs. 3 pods
  ❌ Throughput: Single pod limited to ~100-300 msg/sec
```

**Verdict**: REJECTED. Single pod can't achieve 1000 msg/sec or HA requirements.

---

### Alternative 5: Synchronous Commit (vs. Async Commit via DefaultErrorHandler)

```java
// Synchronous approach (blocking)
ack.acknowledge();
// Wait for broker to acknowledge offset commit

// Asynchronous approach (non-blocking, our recommendation)
// Consumer continues immediately, broker acks in background
```

**Trade-offs**:
- Sync: Slightly safer (ensures offset committed) but adds ~10-50ms latency per batch
- Async: Faster (no wait for broker) but tiny risk of duplicate on crash

**Verdict**: ASYNC is fine since we use UPSERT in database to handle duplicates.

---

### Alternative 6: Exactly-Once Semantics (vs. At-Least-Once)

```yaml
Configuration:
  Exactly-Once:
    isolation.level: read_committed
    enable.idempotence: true
    producer.acks: all

  At-Least-Once:
    isolation.level: read_committed
    enable.idempotence: false  # or not required
    producer.acks: all
```

**Trade-offs**:
- Exactly-Once: No duplicates ever (but ~2x slower, requires transactional support)
- At-Least-Once: Rare duplicates on crash (but ~2x faster)
  - Handled in database with `INSERT ... ON CONFLICT DO UPDATE`

**Verdict**: AT-LEAST-ONCE is fine. Database UPSERT handles duplicates transparently.

---

## Testable Configuration with Testcontainers

### Integration Test Setup

```java
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
        // Produce 500 messages
        produceMessages(500);

        // Wait for consumption and database storage
        Thread.sleep(5000);

        // Assert all 500 messages persisted
        long count = messageRepository.count();
        assertThat(count).isEqualTo(500);
    }

    @Test
    void testErrorHandlingWithDLQ() throws Exception {
        // Produce message with invalid JSON
        kafkaTemplate.send("topic1", "invalid-json");

        // Wait for error handling
        Thread.sleep(5000);

        // Assert message in DLQ
        ConsumerRecord<String, String> dlqMessage =
            consumeFromDLQ("topic1.dlq");

        assertThat(dlqMessage).isNotNull();
        assertThat(dlqMessage.headers()
                            .lastHeader("exception-class")
                            .value())
            .contains("JsonMappingException");
    }

    @Test
    void testManualOffsetCommit() throws Exception {
        // Produce 100 messages
        produceMessages(100);

        // Let consumer process
        Thread.sleep(3000);

        // Stop consumer
        kafkaConsumerContainer.stop();

        // Verify no messages lost (check offset)
        long committedOffset = getCommittedOffset("topic1", 0);

        // All 100 should be committed
        assertThat(committedOffset).isGreaterThanOrEqualTo(99);
    }
}
```

### Load Test with Testcontainers

```java
@SpringBootTest
@Testcontainers
public class KafkaConsumerLoadTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(...);

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(...);

    @Test
    void test1000MsgSecThroughput() throws Exception {
        // Measure time to consume 5000 messages
        long startTime = System.currentTimeMillis();

        produceMessages(5000);

        // Wait for all to be processed
        await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> messageRepository.count() == 5000);

        long duration = System.currentTimeMillis() - startTime;
        double throughput = (5000 * 1000.0) / duration;

        assertThat(throughput)
            .as("Throughput should exceed 1000 msg/sec")
            .isGreaterThan(1000);
    }

    @Test
    void testP95Latency() throws Exception {
        List<Long> latencies = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            long start = System.currentTimeMillis();

            kafkaTemplate.send("topic1", "msg-" + i);

            // Wait for message in database
            await()
                .atMost(Duration.ofSeconds(1))
                .until(() -> messageRepository
                    .findByValue("msg-" + i)
                    .isPresent());

            long latency = System.currentTimeMillis() - start;
            latencies.add(latency);
        }

        Collections.sort(latencies);
        long p95 = latencies.get((int)(latencies.size() * 0.95));

        assertThat(p95)
            .as("P95 latency should be under 100ms")
            .isLessThan(100);
    }
}
```

---

## Implementation Checklist

### Phase 1: Core Consumer Setup
- [ ] Create KafkaConsumerConfig with batch settings (max-poll-records: 500)
- [ ] Implement KafkaMessageListener with batch processing
- [ ] Configure manual offset commit (AckMode.MANUAL)
- [ ] Create KafkaMessage entity and repository

### Phase 2: Error Handling & DLQ
- [ ] Create ErrorHandlingConfig with DefaultErrorHandler
- [ ] Configure DeadLetterPublishingRecoverer
- [ ] Create DLQ topic provisioning
- [ ] Implement DLQ consumer for alerting

### Phase 3: Observability
- [ ] Add KafkaMetricsPublisher for metrics collection
- [ ] Configure Prometheus endpoint (/actuator/prometheus)
- [ ] Create health check indicators (Kafka, DB)
- [ ] Create Grafana dashboard

### Phase 4: Kubernetes Scaling
- [ ] Create kafka-consumer-deployment.yaml (3 replicas)
- [ ] Configure horizontal pod autoscaler (based on lag metric)
- [ ] Create ConfigMaps for topic configuration
- [ ] Test pod failure scenarios

### Phase 5: Testing
- [ ] Create KafkaConsumerIntegrationTest with Testcontainers
- [ ] Create load test for 1000 msg/sec validation
- [ ] Create failure scenario tests (pod crash, broker down)
- [ ] Create SSE latency tests

---

## Configuration Reference: All Properties

### Essential Properties (Must Configure)

```properties
# Consumer identity
spring.kafka.consumer.group-id=uw-hub-streaming-group

# Offset management (critical)
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.auto-offset-reset=earliest

# Batch settings (for 1000 msg/sec)
spring.kafka.consumer.max-poll-records=500
spring.kafka.consumer.fetch-min-bytes=10485760

# Listener threading
spring.kafka.listener.type=batch
spring.kafka.listener.concurrency=8

# Acknowledgment mode
spring.kafka.listener.ack-mode=manual
```

### Performance Tuning Properties (Recommended)

```properties
# Network tuning
spring.kafka.consumer.fetch-max-wait-ms=500
spring.kafka.consumer.session-timeout-ms=30000
spring.kafka.consumer.heartbeat-interval-ms=10000
spring.kafka.consumer.max-poll-interval-ms=300000

# Deserialization
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

# JSON deserialization
spring.kafka.consumer.properties.spring.json.trusted.packages=com.uwhub.domain.event

# Listener polling
spring.kafka.listener.poll-timeout=3000
spring.kafka.listener.idle-event-interval=30000
```

### Advanced Tuning (Optional)

```properties
# Connection pool
spring.kafka.properties.connections.max.idle.ms=540000

# Compression
spring.kafka.producer.compression-type=snappy

# Rebalancing
spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor

# Metrics
management.endpoints.web.exposure.include=health,info,metrics,prometheus
management.metrics.tags.application=uw-hub-streaming
```

---

## Summary Table: Recommended vs. Defaults

| Property | Recommended | Default | Reason |
|----------|-------------|---------|--------|
| `max-poll-records` | 500 | 500 | Batch size optimal for 1000 msg/sec |
| `fetch-min-bytes` | 10MB | 1B | Reduce network roundtrips |
| `fetch-max-wait-ms` | 500ms | 500ms | Balance latency vs. throughput |
| `enable-auto-commit` | false | true | Manual control for reliability |
| `listener.type` | batch | single | Required for high throughput |
| `listener.concurrency` | 8 | 1 | Parallelism for throughput |
| `ack-mode` | MANUAL | AUTO | Explicit control, no message loss |
| `session-timeout-ms` | 30s | 10s | Stability during rebalancing |
| `max-poll-interval-ms` | 300s | 300s | Time to process batch |

---

## Conclusion

This configuration achieves:

1. **1000 msg/sec throughput**: Batch processing, 8 concurrent threads, 3 pods
2. **Reliable message processing**: Manual offset commit, at-least-once semantics
3. **Graceful error handling**: DLQ, retry policies, alerting
4. **Horizontal scalability**: Consumer groups, automatic rebalancing
5. **Production observability**: Prometheus metrics, health checks, lag monitoring

The approach prioritizes **reliability over simplicity** — offset management and error handling add complexity but ensure zero message loss in production systems.

---

## References

- [Spring Kafka Documentation](https://docs.spring.io/spring-kafka/reference/)
- [Apache Kafka Consumer Tuning](https://kafka.apache.org/documentation/#consumerconfigs)
- [Kafka Best Practices](https://www.confluent.io/blog/kafka-best-practices/)
- [Spring Boot Actuator](https://docs.spring.io/spring-boot/reference/actuator/)
- [Testcontainers Documentation](https://testcontainers.com/)
