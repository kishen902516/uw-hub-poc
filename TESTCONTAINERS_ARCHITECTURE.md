# Testcontainers Architecture Diagrams
## Visual Guide to Integration Testing Setup

---

## 1. Overall Test Execution Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Test Suite Execution                         │
│                                                                 │
│  mvn clean verify                                              │
│    ↓                                                           │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ Maven Surefire (Parallel Execution)                      │ │
│  │                                                          │ │
│  │ threadCount = 4  (4 test classes run simultaneously)    │ │
│  │                                                          │ │
│  │  Class1Test     Class2Test    Class3Test   Class4Test   │ │
│  │      ↓              ↓             ↓           ↓         │ │
│  │   [Running]     [Running]     [Running]   [Running]     │ │
│  │      ↓              ↓             ↓           ↓         │ │
│  │   [PASS]        [PASS]        [PASS]      [PASS]        │ │
│  └──────────────────────────────────────────────────────────┘ │
│    ↓                                                           │
│  All tests run against SHARED containers                      │
│  └─────────────────────────────────────────┬──────────────────┘
│                                            │
│                     ┌──────────────────────┴──────────────────┐
│                     ↓                                         ↓
│         ┌──────────────────────┐           ┌──────────────────────┐
│         │  PostgreSQL          │           │  Kafka Cluster       │
│         │  Container           │           │  Container           │
│         │                      │           │                      │
│         │ Database: testdb     │           │ Broker: localhost:   │
│         │ User: testuser       │           │ 9093 (random)        │
│         │ Port: 5432 (random)  │           │                      │
│         │                      │           │ Topics:              │
│         │ Tables:              │           │ - test-orders        │
│         │ - kafka_messages     │           │ - test-payments      │
│         │ - consumer_offsets   │           │ - test-events        │
│         │ - message_cache      │           │                      │
│         └──────────────────────┘           └──────────────────────┘
│              ↑                                      ↑
│              │                                     │
│         Spring Boot Application Context           │
│         (Cached across test class)         Message Flow
│              │                                     │
│         Database Reset (@BeforeEach)              │
│         TRUNCATE → Test → TRUNCATE                │
│         (<1 second per test)                      │
│                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Container Lifecycle: First Run vs Subsequent Runs

### First Run (No Container Reuse)
```
Timeline: 0 ───────────────────────────────────────────→ 50 seconds

  0-15s:  ┌─────────────────────────────────┐
          │ Pull Docker Images from Registry │
          │ postgres:17-alpine               │
          │ confluentinc/cp-kafka:7.7.0      │
          └─────────────────────────────────┘
              ↓
 15-35s: ┌─────────────────────────────────┐
         │ Start Containers & Initialize   │
         │ - PostgreSQL startup (10s)      │
         │ - Kafka startup (10s)           │
         │ - Init scripts run              │
         └─────────────────────────────────┘
             ↓
 35-40s: ┌─────────────────────────────────┐
         │ Spring Boot Context Creation    │
         │ - Load application.properties   │
         │ - Wire beans                    │
         │ - Connect to containers         │
         └─────────────────────────────────┘
             ↓
 40-50s: ┌─────────────────────────────────┐
         │ Test Execution (50 test methods)│
         │ Each method: 100-500ms          │
         └─────────────────────────────────┘
             ↓
         [Test Report Generated]
```

### Subsequent Run (With .testcontainers.properties)
```
Timeline: 0 ───────────────────────────────────────→ 30 seconds

  0s:    ┌─────────────────────────────────┐
         │ Check for Existing Containers   │
         │ Found! PostgreSQL & Kafka       │
         │ already running                 │
         └─────────────────────────────────┘
             ↓
  0.5s:  ┌─────────────────────────────────┐
         │ Spring Boot Context Creation    │
         │ (Cached from first run)         │
         │ Connect to existing containers  │
         └─────────────────────────────────┘
             ↓
  1-30s: ┌─────────────────────────────────┐
         │ Test Execution (50 test methods)│
         │ With per-test cleanup:          │
         │ 1. Reset DB (TRUNCATE, <1s)    │
         │ 2. Run test (100-500ms)        │
         │ 3. Repeat                       │
         └─────────────────────────────────┘
             ↓
         [Test Report Generated]

Result: 25-30 seconds (vs 50 seconds first run)
```

---

## 3. Test Isolation Strategy: Per-Test Cleanup

```
┌─────────────────────────────────────────────────────────────┐
│              Single Test Method Execution                   │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ @BeforeEach (10-50ms)                                       │
│ ├─ TRUNCATE kafka_messages RESTART IDENTITY CASCADE        │
│ ├─ DELETE FROM consumer_offsets                            │
│ └─ Verify database is empty                                │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ @Test (100-500ms)                                           │
│ ├─ ARRANGE: Create test data                               │
│ ├─ ACT: Send message to Kafka                              │
│ ├─ ASSERT: Verify message persisted                        │
│ │ └─ Use Awaitility.await() for async assertions          │
│ └─ Method completes                                         │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ @AfterEach (implicit - no manual cleanup)                   │
│ ├─ Spring rolls back any uncommitted transactions          │
│ ├─ Kafka topics remain (but consumers read from earliest) │
│ └─ Ready for next test                                     │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
              [Test passes, move to next method]

Benefits:
  ✓ Shared container state preserved
  ✓ Fast cleanup (TRUNCATE ~0.5 seconds)
  ✓ Complete isolation (no data leakage)
  ✓ Predictable test behavior
```

---

## 4. Message Flow: Producer → Kafka → Consumer → Database

```
Test Producer                  Kafka Cluster              Consumer              Database
     │                              │                         │                    │
     │ 1. Send message              │                         │                    │
     ├─────────────────────────────>│                         │                    │
     │   topic: "orders"            │                         │                    │
     │   key: "order-123"           │                         │                    │
     │   value: JSON payload        │                         │                    │
     │                              │                         │                    │
     │                              │ 2. Consumer polls       │                    │
     │                              │<────────────────────────┤                    │
     │                              │                         │                    │
     │                              │ 3. Return message       │                    │
     │                              ├────────────────────────>│                    │
     │                              │   offset: 0             │                    │
     │                              │   partition: 1          │                    │
     │                              │   timestamp: 1234567    │                    │
     │                              │                         │                    │
     │                              │                         │ 4. Persist        │
     │                              │                         ├───────────────────>│
     │                              │                         │ INSERT INTO        │
     │                              │                         │ kafka_messages     │
     │                              │                         │                    │
     │                              │                         │ 5. Return rowid   │
     │                              │                         │<───────────────────┤
     │                              │                         │                    │
     │                              │ 6. Commit offset        │                    │
     │                              │<────────────────────────┤                    │
     │                              │                         │                    │
     │ 7. Assert via await()        │                         │                    │
     │    (polls database)          │                         │                    │
     ├────────────────────────────────────────────────────────────────────────────>│
     │    SELECT * FROM kafka_messages                                             │
     │    WHERE topic = 'orders'                                                   │
     │<────────────────────────────────────────────────────────────────────────────┤
     │    ✓ Message found, test passes                                             │
     │                                                                             │

Timing:
  Step 1-2: 10-50ms (message produced, polled)
  Step 3-5: 50-100ms (consumed, persisted)
  Step 6: 10ms (offset committed)
  Step 7: 100-500ms (await polls until found)
  Total: 170-660ms per test (typically ~300ms)
```

---

## 5. Parallel Test Execution (4 Threads)

```
Time →

Thread 1   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
           │ Test 1.1 │  │ Test 1.2 │  │ Test 1.3 │  │ Test 1.4 │
           └──────────┘  └──────────┘  └──────────┘  └──────────┘

Thread 2   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
           │ Test 2.1 │  │ Test 2.2 │  │ Test 2.3 │  │ Test 2.4 │
           └──────────┘  └──────────┘  └──────────┘  └──────────┘

Thread 3   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
           │ Test 3.1 │  │ Test 3.2 │  │ Test 3.3 │  │ Test 3.4 │
           └──────────┘  └──────────┘  └──────────┘  └──────────┘

Thread 4   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
           │ Test 4.1 │  │ Test 4.2 │  │ Test 4.3 │  │ Test 4.4 │
           └──────────┘  └──────────┘  └──────────┘  └──────────┘


Key Points:
  ✓ Each thread has independent Spring context
  ✓ All threads share same PostgreSQL container
  ✓ All threads share same Kafka container
  ✓ Database reset happens per-test (before @BeforeEach)
  ✓ Kafka topics accumulate messages (reset between classes)
  ✓ No port conflicts (dynamic port assignment)

Safety Measures:
  ✗ DO NOT parallelize individual test METHODS
    (causes race conditions in container setup)
  ✓ DO parallelize test CLASSES
    (each class gets its own Spring context)
```

---

## 6. Container Network Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                     Docker Network                           │
│                     (Host mode or Bridge)                    │
│                                                              │
│  ┌────────────────────────┐      ┌──────────────────────┐   │
│  │   PostgreSQL           │      │   Kafka Broker       │   │
│  │   Container            │      │   Container          │   │
│  │                        │      │                      │   │
│  │  Hostname:             │      │  Hostname:           │   │
│  │  postgres              │      │  kafka               │   │
│  │  (resolved via DNS)    │      │  (resolved via DNS)  │   │
│  │                        │      │                      │   │
│  │  Port: 5432            │      │  Port: 9093 (PLAINTEXT)  │
│  │  (exposed)             │      │  Port: 29093 (DOCKER)    │
│  │                        │      │                      │   │
│  │  ↑                     │      │  ↑                   │   │
│  │  │                     │      │  │                   │   │
│  │  └─────────────────────┘      │  └───────────────────┘   │
│  │                               │                          │
│  └──────────────────────────────────────────────────────────┘
│         ↑                                   ↑
│         │                                   │
│    Spring Boot Test Application
│    Running on JVM
│
│    Connection Strings:
│    ├─ JDBC: jdbc:postgresql://localhost:5432/testdb
│    │         (localhost because Docker exposes to host)
│    │
│    └─ Kafka: localhost:9093
│              (PLAINTEXT listener from Spring context)
```

---

## 7. Database Schema: Integration Test Tables

```
┌─────────────────────────────────────────────────────────┐
│                    TestDB Schema                         │
└─────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│ Table: kafka_messages (Primary)                          │
├──────────────────────────────────────────────────────────┤
│ id          │ BIGSERIAL PRIMARY KEY                      │
│ topic       │ VARCHAR(255) NOT NULL (indexed)            │
│ partition   │ INT NOT NULL                               │
│ offset      │ BIGINT NOT NULL                            │
│ key         │ VARCHAR(255)                               │
│ value       │ TEXT (JSON payload)                        │
│ timestamp   │ TIMESTAMP WITH TIME ZONE (indexed DESC)    │
│ created_at  │ TIMESTAMP WITH TIME ZONE DEFAULT now()    │
│                                                          │
│ INDEXES:                                                │
│ ├─ idx_kafka_messages_topic                             │
│ ├─ idx_kafka_messages_timestamp DESC                    │
│ └─ idx_kafka_messages_topic_timestamp (composite)       │
│                                                          │
│ UNIQUE: (topic, partition, offset)                      │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│ Table: consumer_offsets (Supporting)                     │
├──────────────────────────────────────────────────────────┤
│ id               │ BIGSERIAL PRIMARY KEY                 │
│ consumer_group   │ VARCHAR(255) NOT NULL                 │
│ topic            │ VARCHAR(255) NOT NULL                 │
│ partition        │ INT NOT NULL                          │
│ offset           │ BIGINT NOT NULL                       │
│ timestamp        │ TIMESTAMP WITH TIME ZONE DEFAULT now()│
│                                                          │
│ UNIQUE: (consumer_group, topic, partition)              │
└──────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────┐
│ Table: message_cache (Optional)                          │
├──────────────────────────────────────────────────────────┤
│ id           │ BIGSERIAL PRIMARY KEY                     │
│ cache_key    │ VARCHAR(255) UNIQUE NOT NULL             │
│ cache_value  │ JSONB                                    │
│ ttl_ms       │ BIGINT                                   │
│ created_at   │ TIMESTAMP WITH TIME ZONE DEFAULT now()  │
└──────────────────────────────────────────────────────────┘

Data Flow During Test:
  Test produces message → Kafka
  Consumer polls message → Reads topic
  Consumer deserializes → KafkaMessage entity
  INSERT INTO kafka_messages
  Test queries table → Asserts message persisted
```

---

## 8. Spring Boot Context: Dependency Injection Flow

```
┌─────────────────────────────────────────────────────────┐
│         Test Class Instantiation & Setup               │
│         (Once per test class)                          │
└──────────────┬──────────────────────────────────────────┘
               ↓
       Spring detects @SpringBootTest
               ↓
    ┌──────────────────────────────┐
    │ Load Application Properties  │
    ├──────────────────────────────┤
    │ application.properties       │ (application-test.properties)
    │ + spring.datasource.url      │ ← Dynamic via @DynamicPropertySource
    │ + spring.kafka.bootstrap     │ ← Dynamic via @DynamicPropertySource
    └──────────────────────────────┘
               ↓
    ┌──────────────────────────────┐
    │ Create ApplicationContext    │
    ├──────────────────────────────┤
    │ Scan @Component, @Bean, etc  │
    │ Resolve dependencies         │
    │ Create singleton beans       │
    └──────────────────────────────┘
               ↓
    ┌──────────────────────────────┐
    │ Connect to Containers        │
    ├──────────────────────────────┤
    │ DataSource → PostgreSQL      │
    │ KafkaTemplate → Kafka        │
    │ Repositories → Database      │
    └──────────────────────────────┘
               ↓
    ┌──────────────────────────────┐
    │ Autowire Test Class Fields   │
    ├──────────────────────────────┤
    │ @Autowired KafkaTemplate     │ ✓ Ready
    │ @Autowired Repository        │ ✓ Ready
    │ @Autowired JdbcTemplate      │ ✓ Ready
    └──────────────────────────────┘
               ↓
    ┌──────────────────────────────┐
    │ Execute @BeforeEach          │
    ├──────────────────────────────┤
    │ resetDatabase()              │
    │ TRUNCATE tables              │
    │ Clear Kafka offsets          │
    └──────────────────────────────┘
               ↓
    ┌──────────────────────────────┐
    │ Execute @Test Method         │
    ├──────────────────────────────┤
    │ [Actual test logic]          │
    └──────────────────────────────┘
               ↓
    [Test method completes]
               ↓
    [Next test method in queue]
    [Repeat @BeforeEach → @Test]


Context Caching:
  ✓ If next test uses identical config → Context reused
  ✓ If config differs → New context created
  ✓ Typical: 1 context per test class
```

---

## 9. Test Failure Flow: Debugging Path

```
Test Execution
      ↓
  Assertion fails
      ↓
┌─────────────────────────────────────────┐
│ Was the message produced to Kafka?      │
│ ├─ Check Kafka logs: docker logs <id>  │
│ └─ Check KafkaTemplate.send() returned │
├─────────────────────────────────────────┤
│ Did consumer poll the message?          │
│ ├─ Check consumer logs                 │
│ └─ Verify auto.offset.reset=earliest   │
├─────────────────────────────────────────┤
│ Was message persisted to database?      │
│ ├─ SELECT FROM kafka_messages           │
│ ├─ Check for constraint violations      │
│ └─ Verify all fields inserted           │
├─────────────────────────────────────────┤
│ Did await() timeout?                    │
│ ├─ Increase atMost(Duration)           │
│ ├─ Add pollDelay()                     │
│ └─ Check consumer log lag               │
├─────────────────────────────────────────┤
│ Test isolation issue?                   │
│ ├─ Verify @BeforeEach runs             │
│ ├─ Check TRUNCATE executed             │
│ └─ Ensure no @Transactional interference│
└─────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────┐
│ Gather Debugging Information            │
│ ├─ Test output (System.out)             │
│ ├─ Spring logs (application.log)        │
│ ├─ Database state (SELECT *)            │
│ ├─ Kafka topics (kafka-topics.sh)       │
│ └─ Container logs (docker logs)         │
└─────────────────────────────────────────┘
      ↓
┌─────────────────────────────────────────┐
│ Root Cause Examples                     │
│                                         │
│ 1. Consumer lag (slow processing)       │
│    Fix: Increase await().atMost()      │
│                                         │
│ 2. Serialization error                  │
│    Fix: Check JSON format               │
│                                         │
│ 3. Constraint violation                 │
│    Fix: Verify unique constraints       │
│                                         │
│ 4. Container not running                │
│    Fix: docker ps, check memory         │
│                                         │
│ 5. Port conflict (local)                │
│    Fix: Remove .testcontainers.props   │
│                                         │
│ 6. Stale data from prev test            │
│    Fix: Verify TRUNCATE CASCADE runs   │
└─────────────────────────────────────────┘
```

---

## 10. CI/CD Integration: GitHub Actions Flow

```
┌───────────────────────────────────────────────────────────┐
│ Git: Push to develop/main branch                          │
└────────────┬────────────────────────────────────────────┘
             ↓
┌───────────────────────────────────────────────────────────┐
│ GitHub Actions Trigger                                    │
│ on: push                                                  │
│     branches: [develop, main]                            │
└────────────┬────────────────────────────────────────────┘
             ↓
┌───────────────────────────────────────────────────────────┐
│ Provision Ubuntu Runner (ubuntu-latest)                  │
│ ├─ CPU: 2 cores                                          │
│ ├─ Memory: 7 GB                                          │
│ └─ Disk: 14 GB                                           │
└────────────┬────────────────────────────────────────────┘
             ↓
┌───────────────────────────────────────────────────────────┐
│ Setup Java 21 & Maven Cache                              │
│ ├─ JDK: temurin-21                                       │
│ ├─ Maven: use cached dependencies                        │
│ └─ Time saved: 1-2 minutes                               │
└────────────┬────────────────────────────────────────────┘
             ↓
┌───────────────────────────────────────────────────────────┐
│ Run Integration Tests                                     │
│ mvn clean verify -DskipITs=false                         │
│                                                          │
│ Timeline:                                                │
│ ├─ Docker pull (postgres, kafka): 10-15s                │
│ ├─ Container startup: 15-20s                            │
│ ├─ Database setup: 2-3s                                 │
│ ├─ Parallel test execution (4 threads): 30-45s          │
│ └─ Total: 60-90 seconds                                 │
│                                                          │
│ Timeout: 10 minutes (600 seconds)                        │
└────────────┬────────────────────────────────────────────┘
             ↓
┌───────────────────────────────────────────────────────────┐
│ Process Results                                           │
│ ├─ Parse surefire-reports/TEST-*.xml                    │
│ ├─ Count: passed, failed, skipped                       │
│ ├─ Generate HTML report                                 │
│ └─ Upload artifacts (test-results/)                     │
└────────────┬────────────────────────────────────────────┘
             ↓
       ┌─────────────┬─────────────┐
       ↓             ↓
   ALL PASS      ANY FAIL       TIMEOUT
      ↓             ↓              ↓
   Merge        Fail Check    Fail Check
   Allowed      (Block Merge) (Block Merge)
   ✓            ✗             ✗

Optional: Send notifications to Slack
```

---

## 11. Performance Optimization Timeline

```
Version 1.0 (Initial)
├─ Per-test containers: 8-12 seconds each
├─ Serial test execution
├─ Total time: 8-12 min for 50 tests
└─ Status: Too slow ✗

         ↓ Optimization Phase 1: Shared Containers

Version 1.1 (Shared Static Containers)
├─ Container startup: 15-20 seconds (once)
├─ Per-test cleanup: 0.5 seconds (TRUNCATE)
├─ Test execution: 20-30 seconds (50 tests)
├─ Total time: 40-50 seconds
└─ Status: Better, but can improve ✓

         ↓ Optimization Phase 2: Parallel Execution

Version 1.2 (Shared + Parallel)
├─ Container startup: 15-20 seconds (once)
├─ Test execution: 10-15 seconds (4 threads)
├─ Total time: 30-40 seconds
└─ Status: Good for local dev ✓

         ↓ Optimization Phase 3: Alpine + Caching

Version 1.3 (Alpine + Maven Cache)
├─ Container startup: 10 seconds (Alpine images)
├─ Maven dependency cache: hit (CI/CD)
├─ Test execution: 10-15 seconds (4 threads)
├─ Total time: 25-30 seconds (local)
├─ Total time: 60-90 seconds (CI/CD first run)
└─ Status: Optimal ✓✓✓


Trade-off Matrix:
┌──────────────────────┬─────────┬──────────┬──────────────┐
│ Strategy             │ Speed   │ Cost     │ Complexity   │
├──────────────────────┼─────────┼──────────┼──────────────┤
│ Per-test containers  │ Slowest │ High CPU │ Low          │
│ Shared serial        │ Medium  │ Medium   │ Medium       │
│ Shared parallel      │ Fast    │ Low      │ Medium-High  │
│ Shared + Alpine      │ Fastest │ Very Low │ High         │
└──────────────────────┴─────────┴──────────┴──────────────┘

Recommendation: Shared + Parallel + Alpine
(Best balance of speed, resource usage, and complexity)
```

---

## 12. Comparison: Testcontainers Architecture vs Alternatives

```
TESTCONTAINERS (Recommended)
┌─────────────────────────────────────┐
│ Maven Test                          │
├─────────────────────────────────────┤
│ ↓                                   │
│ Testcontainers API                  │
│ ├─ @Container static field         │
│ ├─ @DynamicPropertySource          │
│ └─ JUnit 5 extension                │
│ ↓                                   │
│ Docker CLI                          │
│ ├─ docker pull [image]              │
│ ├─ docker run -p [port] [image]    │
│ └─ docker rm [container]            │
│ ↓                                   │
│ Real PostgreSQL + Kafka             │
├─────────────────────────────────────┤
│ Advantages:                         │
│ ✓ Production-representative        │
│ ✓ Programmatic control             │
│ ✓ Fast with caching                │
│ ✓ Parallel-friendly (dynamic ports)│
│ ✓ CI/CD compatible                 │
│                                     │
│ Execution time: 40-90 seconds       │
└─────────────────────────────────────┘

DOCKER COMPOSE
┌─────────────────────────────────────┐
│ Manual: docker-compose up           │
├─────────────────────────────────────┤
│ docker-compose.yml                  │
│ ├─ services                         │
│ │  ├─ postgres                      │
│ │  └─ kafka                         │
│ └─ networks                         │
│ ↓                                   │
│ Real PostgreSQL + Kafka             │
│ ↓                                   │
│ Maven Test                          │
├─────────────────────────────────────┤
│ Advantages:                         │
│ ✓ Clear configuration               │
│ ✓ Local dev friendly                │
│                                     │
│ Disadvantages:                      │
│ ✗ Manual setup required             │
│ ✗ Version management                │
│ ✗ Different CI/CD path              │
│ ✗ Not deterministic                 │
│                                     │
│ Execution time: Varies (manual)     │
└─────────────────────────────────────┘

EMBEDDED KAFKA + H2
┌─────────────────────────────────────┐
│ Maven Test                          │
├─────────────────────────────────────┤
│ ↓                                   │
│ @EmbeddedKafka                      │
│ Spring's embedded Kafka             │
│ (not production Kafka)              │
│                                     │
│ @TestPropertySource                 │
│ H2 in-memory database               │
│ (not PostgreSQL)                    │
├─────────────────────────────────────┤
│ Advantages:                         │
│ ✓ Fast (in-process, in-memory)     │
│ ✓ No Docker required                │
│                                     │
│ Disadvantages:                      │
│ ✗ Not production-representative    │
│ ✗ Missing PostgreSQL features       │
│ ✗ Dialect mismatches               │
│ ✗ False test confidence            │
│                                     │
│ Execution time: 10-15 seconds       │
└─────────────────────────────────────┘

WINNER: Testcontainers
  - Provides production parity
  - Only 5-10x slower than embedded
  - Catches real production bugs
  - Worth the extra 40-60 seconds
```

---

## Summary

This architecture provides:

1. **Deterministic**: Same result every time
2. **Isolated**: No cross-test contamination
3. **Fast**: 30-90 seconds for full suite
4. **Scalable**: Parallel execution supported
5. **Production-Representative**: Real Kafka + PostgreSQL
6. **CI/CD Ready**: Automated test execution
7. **Debuggable**: Clear logs and error messages

The shared static container pattern is the optimal balance between execution speed and test reliability for integration testing.

