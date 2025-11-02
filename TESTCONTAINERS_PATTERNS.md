# Testcontainers Common Patterns & Best Practices
## For Spring Boot Integration Testing

---

## Pattern 1: Async Message Assertion with Awaitility

```java
import static org.awaitility.Awaitility.*;
import java.time.Duration;

@Test
void shouldProcessMessageAsynchronously() {
    // ARRANGE
    kafkaTemplate.send("orders", "key1", "{\"data\": \"test\"}");

    // ACT & ASSERT: Wait for async processing (up to 5 seconds)
    await()
        .atMost(Duration.ofSeconds(5))           // Max wait time
        .pollInterval(Duration.ofMillis(100))    // Check every 100ms
        .pollDelay(Duration.ofMillis(10))        // Initial delay before first check
        .untilAsserted(() -> {
            List<KafkaMessage> messages = repository.findByTopic("orders");
            assertThat(messages).hasSize(1);
        });
}
```

**When to use**:
- Testing async message processing
- Database persistence from Kafka consumers
- Event-driven workflows

**Tuning parameters**:
- `atMost`: Adjust based on expected latency (100ms for fast, 10s for slow)
- `pollInterval`: Balance between CPU and latency (100ms is good default)
- `pollDelay`: Skip initial delay if message appears immediately

---

## Pattern 2: Batch Message Processing with Throughput Verification

```java
@Test
void shouldProcessHighThroughputMessages() {
    // ARRANGE
    int messageCount = 1000;
    long startTime = System.currentTimeMillis();

    // ACT: Produce messages rapidly
    for (int i = 0; i < messageCount; i++) {
        kafkaTemplate.send("events", String.valueOf(i), "{\"id\":" + i + "}");
    }

    // ASSERT: Verify all messages processed with acceptable throughput
    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofMillis(500))  // Check less frequently for batch
        .untilAsserted(() -> {
            List<KafkaMessage> processed = repository.findAll();
            assertThat(processed).hasSize(messageCount);

            // Calculate throughput
            long duration = System.currentTimeMillis() - startTime;
            double throughput = messageCount * 1000.0 / duration;

            // Assert minimum throughput (>200 messages/second)
            assertThat(throughput)
                .as("Messages per second: " + throughput)
                .isGreaterThan(200);
        });
}
```

**Rationale**:
- Verifies system meets performance requirements
- Catches degradation early
- Adjustable throughput threshold

---

## Pattern 3: Transaction Isolation for Database Tests

```java
@Test
@Transactional  // Automatically rollback after test
void shouldInsertMessageInTransaction() {
    // ARRANGE
    KafkaMessage message = new KafkaMessage()
        .setTopic("orders")
        .setValue("test");

    // ACT
    KafkaMessage saved = repository.save(message);

    // ASSERT
    assertThat(saved.getId()).isNotNull();

    // Cleanup: Automatic rollback via @Transactional
}

@Test
@Transactional(propagation = NOT_SUPPORTED)  // Do NOT use transaction
void shouldConsumerMessageOutsideTransaction() {
    // Use this for testing async consumers that commit offsets
    // Consumer should NOT participate in test transaction
}
```

**When to use**:
- Simple CRUD operation tests
- Read-only queries
- Tests that don't involve async consumers

**Avoid for**:
- Consumer tests (consumers run outside transaction)
- Multi-service tests (only one transaction)
- Tests needing manual cleanup

---

## Pattern 4: Testing Message Serialization/Deserialization

```java
@Test
void shouldSerializeAndDeserializeComplexMessage() {
    // ARRANGE
    Map<String, Object> complexData = Map.of(
        "orderId", "ORD-12345",
        "customer", Map.of("id", "CUST-001", "name", "John Doe"),
        "items", List.of(
            Map.of("sku", "SKU-001", "qty", 2, "price", 49.99),
            Map.of("sku", "SKU-002", "qty", 1, "price", 29.99)
        ),
        "metadata", Map.of("source", "web", "timestamp", System.currentTimeMillis())
    );

    ObjectMapper mapper = new ObjectMapper();
    String serialized = mapper.writeValueAsString(complexData);

    // ACT: Send to Kafka and retrieve
    kafkaTemplate.send("orders", "ORD-12345", serialized);

    // ASSERT: Verify deserialization in consumer
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            KafkaMessage stored = repository.findByKeyAndTopic("ORD-12345", "orders")
                .orElseThrow();

            // Parse stored value
            Map<String, Object> deserialized = mapper.readValue(
                stored.getValue(),
                new TypeReference<>() {}
            );

            // Verify nested structures preserved
            assertThat(deserialized)
                .containsEntry("orderId", "ORD-12345")
                .containsKey("customer")
                .containsKey("items");

            List<?> items = (List<?>) deserialized.get("items");
            assertThat(items).hasSize(2);
        });
}
```

**Key points**:
- Tests against real Kafka serialization
- Catches schema changes early
- Validates nested object handling

---

## Pattern 5: Error Handling and Recovery

```java
@Test
void shouldHandleCorruptedMessageGracefully() {
    // ARRANGE: Send invalid JSON
    kafkaTemplate.send("orders", "invalid-key", "not-json{corrupt");

    // ACT & ASSERT: Consumer should NOT crash
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            // Verify error was logged/stored
            List<String> errors = errorRepository.findByTopic("orders");
            assertThat(errors).isNotEmpty();

            // Verify healthy messages still processed
            List<KafkaMessage> validMessages = repository.findByTopic("orders");
            // Should have processed other valid messages
        });
}

@Test
void shouldRetryFailedDatabaseInsert() {
    // ARRANGE
    // Simulate database temporarily unavailable
    // (May need mocking or testcontainers-specific setup)

    // ACT
    kafkaTemplate.send("orders", "retry-key", "{\"data\": \"test\"}");

    // ASSERT: Verify retry succeeds eventually
    await()
        .atMost(Duration.ofSeconds(15))  // Longer for retries
        .pollInterval(Duration.ofMillis(500))
        .untilAsserted(() -> {
            KafkaMessage message = repository.findByKey("retry-key");
            assertThat(message).isNotNull();
        });
}

@Test
void shouldForwardToDeadLetterQueueOnPermanentFailure() {
    // ARRANGE
    kafkaTemplate.send("orders", "dlq-key", "{\"invalid\": \"schema\"}");

    // ASSERT: Message moved to DLQ
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() -> {
            KafkaMessage dlqMessage = dlqRepository.findByKey("dlq-key");
            assertThat(dlqMessage).isNotNull();
            assertThat(dlqMessage.getTopic()).isEqualTo("orders-dlq");
        });
}
```

**Patterns**:
- Verify graceful degradation
- Assert retry mechanisms work
- Validate dead-letter queue routing

---

## Pattern 6: Message Ordering and Partition Management

```java
@Test
void shouldMaintainMessageOrderingPerPartition() {
    // ARRANGE: Send messages with same key (goes to same partition)
    String orderId = "ORDER-001";
    List<String> events = List.of("created", "confirmed", "shipped", "delivered");

    // ACT: Send in order
    for (String event : events) {
        Map<String, Object> data = Map.of(
            "orderId", orderId,
            "event", event,
            "timestamp", System.currentTimeMillis()
        );
        kafkaTemplate.send(
            "order-events",
            orderId,  // Same key = same partition = ordering preserved
            objectMapper.writeValueAsString(data)
        );
    }

    // ASSERT: Verify order preserved
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            List<KafkaMessage> messages = repository.findByKeyOrderByOffset(orderId);
            assertThat(messages).hasSize(4);

            List<String> storedEvents = messages.stream()
                .map(msg -> objectMapper.readValue(msg.getValue(), Map.class))
                .map(map -> (String) map.get("event"))
                .collect(Collectors.toList());

            assertThat(storedEvents).isEqualTo(events);
        });
}

@Test
void shouldDistributeMessagesAcrossPartitions() {
    // ARRANGE: Send with different keys (distributes to multiple partitions)
    int messageCount = 30;

    // ACT
    for (int i = 0; i < messageCount; i++) {
        kafkaTemplate.send(
            "events",
            String.valueOf(i % 10),  // Key 0-9 = distributed
            "{\"id\": " + i + "}"
        );
    }

    // ASSERT: Verify partition distribution
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            List<KafkaMessage> messages = repository.findByTopic("events");

            // Get partition distribution
            Map<Integer, Long> partitionCounts = messages.stream()
                .collect(Collectors.groupingBy(
                    KafkaMessage::getPartition,
                    Collectors.counting()
                ));

            // Should use multiple partitions
            assertThat(partitionCounts).size().isGreaterThanOrEqualTo(2);

            // Verify no partition has all messages (distributed)
            for (Long count : partitionCounts.values()) {
                assertThat(count)
                    .as("Partition should not have all messages")
                    .isLessThan(messageCount);
            }
        });
}
```

---

## Pattern 7: Consumer Offset Management

```java
@Test
void shouldTrackAndPersistConsumerOffsets() {
    // ARRANGE: Send messages with tracked offsets
    kafkaTemplate.send("tracked-topic", "key1", "msg1");
    kafkaTemplate.send("tracked-topic", "key2", "msg2");
    kafkaTemplate.send("tracked-topic", "key3", "msg3");

    // ACT & ASSERT
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            // Verify messages stored with correct offset metadata
            List<KafkaMessage> messages = repository.findByTopic("tracked-topic");
            assertThat(messages).hasSize(3);

            // Verify offsets are sequential (or at least valid)
            for (KafkaMessage msg : messages) {
                assertThat(msg.getOffset()).isGreaterThanOrEqualTo(0);
                assertThat(msg.getPartition()).isBetween(0, 2);
            }

            // Verify consumer offset persisted
            ConsumerOffset offset = offsetRepository.findLatestByGroup("test-group");
            assertThat(offset.getOffset()).isGreaterThanOrEqualTo(2);
        });
}

@Test
void shouldResumeFromLastOffsetAfterRestart() {
    // ARRANGE: Send initial batch
    TestDataFixtures.produceTestMessages(kafkaTemplate, "resume-topic", 5, Map.of());

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            assertThat(repository.findByTopic("resume-topic")).hasSize(5);
        });

    // Get current offset
    ConsumerOffset offsetBefore = offsetRepository.findLatestByGroup("test-group");

    // ACT: Send more messages
    TestDataFixtures.produceTestMessages(kafkaTemplate, "resume-topic", 5, Map.of());

    // ASSERT: Only new messages consumed (resume from offset)
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            List<KafkaMessage> allMessages = repository.findByTopic("resume-topic");
            assertThat(allMessages).hasSize(10);

            ConsumerOffset offsetAfter = offsetRepository.findLatestByGroup("test-group");
            assertThat(offsetAfter.getOffset())
                .isGreaterThan(offsetBefore.getOffset());
        });
}
```

---

## Pattern 8: Multi-Topic Integration

```java
@Test
void shouldCorrelateMessagesAcrossTopics() {
    // ARRANGE: Order placed on one topic, payment on another
    String orderId = "ORD-12345";

    Map<String, Object> orderEvent = Map.of(
        "orderId", orderId,
        "amount", 99.99,
        "status", "placed"
    );

    Map<String, Object> paymentEvent = Map.of(
        "orderId", orderId,
        "paymentId", "PAY-001",
        "amount", 99.99,
        "status", "authorized"
    );

    // ACT: Send to different topics
    kafkaTemplate.send("orders", orderId,
        objectMapper.writeValueAsString(orderEvent));
    kafkaTemplate.send("payments", orderId,
        objectMapper.writeValueAsString(paymentEvent));

    // ASSERT: Verify correlation
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            KafkaMessage order = repository
                .findByKeyAndTopic(orderId, "orders").orElseThrow();
            KafkaMessage payment = repository
                .findByKeyAndTopic(orderId, "payments").orElseThrow();

            assertThat(order).isNotNull();
            assertThat(payment).isNotNull();

            // Verify business logic can correlate
            Map<String, Object> orderData = objectMapper
                .readValue(order.getValue(), Map.class);
            Map<String, Object> paymentData = objectMapper
                .readValue(payment.getValue(), Map.class);

            assertThat(orderData.get("amount"))
                .isEqualTo(paymentData.get("amount"));
        });
}

@Test
void shouldFilterAndViewMessagesPerTopic() {
    // ARRANGE: Mixed topics
    List.of("orders", "payments", "shipments").forEach(topic ->
        TestDataFixtures.produceTestMessages(kafkaTemplate, topic, 10, Map.of())
    );

    // ASSERT: Each topic viewable separately
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(() -> {
            assertThat(repository.findByTopic("orders")).hasSize(10);
            assertThat(repository.findByTopic("payments")).hasSize(10);
            assertThat(repository.findByTopic("shipments")).hasSize(10);
            assertThat(repository.findAll()).hasSize(30);
        });
}
```

---

## Pattern 9: Testing API Endpoints with Testcontainers

```java
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Testcontainers
class KafkaMessageAPIIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void shouldRetrieveMessagesViaAPI() {
        // ARRANGE
        TestDataFixtures.produceTestMessages(
            kafkaTemplate, "api-topic", 5, Map.of()
        );

        // Wait for persistence
        await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> repository.findByTopic("api-topic").size() == 5);

        // ACT: Call API
        ResponseEntity<KafkaMessage[]> response = restTemplate.getForEntity(
            "/api/messages?topic=api-topic",
            KafkaMessage[].class
        );

        // ASSERT
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).hasSize(5);
    }

    @Test
    void shouldPaginateMessages() {
        // ARRANGE
        TestDataFixtures.produceTestMessages(
            kafkaTemplate, "paginated", 25, Map.of()
        );

        await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> repository.findByTopic("paginated").size() == 25);

        // ACT
        ResponseEntity<Page> response = restTemplate.getForEntity(
            "/api/messages?topic=paginated&page=0&size=10",
            Page.class
        );

        // ASSERT
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody().getTotalElements()).isEqualTo(25);
        assertThat(response.getBody().getTotalPages()).isEqualTo(3);
    }

    @Test
    void shouldFilterByTopic() {
        // ARRANGE: Multiple topics
        kafkaTemplate.send("orders", "key1", "msg1");
        kafkaTemplate.send("payments", "key2", "msg2");
        kafkaTemplate.send("orders", "key3", "msg3");

        await()
            .atMost(Duration.ofSeconds(5))
            .until(() -> repository.findAll().size() == 3);

        // ACT
        ResponseEntity<KafkaMessage[]> response = restTemplate.getForEntity(
            "/api/messages?topic=orders",
            KafkaMessage[].class
        );

        // ASSERT
        assertThat(response.getBody())
            .hasSize(2)
            .allMatch(msg -> msg.getTopic().equals("orders"));
    }
}
```

---

## Pattern 10: Performance Testing

```java
@Test
@Tag("slow")  // Exclude from fast test runs
void shouldMaintainPerformanceUnderLoad() {
    // ARRANGE
    int messageCount = 5000;
    long startTime = System.currentTimeMillis();
    StopWatch stopWatch = new StopWatch();

    // ACT: Produce high volume
    stopWatch.start("produce");
    for (int i = 0; i < messageCount; i++) {
        kafkaTemplate.send(
            "perf-topic",
            String.valueOf(i % 100),
            "{\"index\":" + i + "}"
        );
    }
    stopWatch.stop();

    // ASSERT: Consumption performance
    stopWatch.start("consume");
    await()
        .atMost(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(() -> {
            List<KafkaMessage> persisted = repository.findByTopic("perf-topic");
            assertThat(persisted).hasSize(messageCount);
        });
    stopWatch.stop();

    long totalTime = System.currentTimeMillis() - startTime;

    // Verify performance thresholds
    long productionTime = stopWatch.getTaskInfo()[0].getTimeMillis();
    long consumptionTime = stopWatch.getTaskInfo()[1].getTimeMillis();

    double productionThroughput = messageCount * 1000.0 / productionTime;
    double consumptionThroughput = messageCount * 1000.0 / consumptionTime;

    assertThat(productionThroughput)
        .as("Production throughput (msgs/sec): " + productionThroughput)
        .isGreaterThan(1000);

    assertThat(consumptionThroughput)
        .as("Consumption throughput (msgs/sec): " + consumptionThroughput)
        .isGreaterThan(500);

    System.out.println("Performance Report:");
    System.out.println(stopWatch.prettyPrint());
}
```

---

## Quick Reference: Assertions

```java
// Message existence
assertThat(repository.findByTopic("topic")).isNotEmpty();
assertThat(repository.findByKey("key")).isPresent();

// Message content
assertThat(message.getValue()).contains("expected-text");
assertThat(message.getTopic()).isEqualTo("expected-topic");

// Collections
assertThat(messages).hasSize(5);
assertThat(messages).allMatch(m -> m.getTopic().equals("topic"));
assertThat(messages).extracting(KafkaMessage::getTopic)
    .containsOnly("topic");

// Ordering
assertThat(messages)
    .isSortedAccordingTo((a, b) -> a.getTimestamp().compareTo(b.getTimestamp()));

// JSON content
ObjectMapper mapper = new ObjectMapper();
Map<String, Object> content = mapper.readValue(message.getValue(), Map.class);
assertThat(content)
    .containsEntry("orderId", "123")
    .containsKeys("status", "timestamp");

// Async/Timing
assertThat(stopWatch.getTotalTimeMillis())
    .isLessThan(5000);
```

---

## Summary of Best Practices

1. **Use Awaitility for async operations** - Not Thread.sleep()
2. **Classify tests by speed** - @Tag("fast"), @Tag("slow")
3. **Test isolation via @BeforeEach** - Not per-test containers
4. **Use Transactional only for CRUD** - Not for consumer tests
5. **Verify real Kafka behavior** - Don't mock it
6. **Assert throughput metrics** - Not just correctness
7. **Test error paths** - Serialization failures, network issues
8. **Organize tests by layer** - kafka/, persistence/, api/
9. **Use builders for test data** - TestDataFixtures
10. **Document test intent** - Clear test names and comments

