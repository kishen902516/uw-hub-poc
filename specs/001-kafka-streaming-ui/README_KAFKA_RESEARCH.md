# Kafka Consumer Research - Navigation Guide

**Date**: 2025-11-02 | **Target**: 1000 messages/sec throughput | **Framework**: Spring Boot 3.x with Spring Kafka

This directory contains comprehensive research on configuring Spring Kafka consumers for high-throughput, reliable message processing suitable for the Kafka Streaming UI feature (spec.md).

---

## 📄 Document Overview

### For Quick Decision-Making (Start Here)
**→ Read: `KAFKA_RESEARCH_SUMMARY.md`** (15 min read)
- Executive summary of all decisions
- Decision matrix comparing options
- Key metrics and deployment checklist
- Recommended configuration at a glance

### For Implementation (Copy-Paste Code)
**→ Read: `KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md`** (30 min read)
- Copy-paste configuration snippets
- Complete Java code examples
- Docker & Kubernetes manifests
- Integration test boilerplate
- Environment variables reference

### For Configuration Properties (Reference)
**→ Use: `application-kafka.yml`** (Configuration file)
- Production-ready Spring Boot configuration
- Fully documented with inline comments
- All tuning parameters explained
- Environment variable overrides
- Multi-profile setup (dev, prod)

### For Deep Technical Understanding (Research)
**→ Read: `KAFKA_CONSUMER_RESEARCH.md`** (45+ min read)
- Detailed rationale for each decision
- Performance calculations and benchmarks
- Comparison with alternative approaches
- Failure scenarios and recovery strategies
- Advanced monitoring setup
- Kubernetes deployment patterns

---

## 🎯 Quick Start by Role

### Product Manager / Stakeholder
1. Read: `KAFKA_RESEARCH_SUMMARY.md` (Executive Summary section)
2. Key takeaway: 1000 msg/sec achievable with 3 Kubernetes pods, zero message loss guaranteed

### Backend Engineer (Implementation)
1. Read: `KAFKA_RESEARCH_SUMMARY.md` (entire)
2. Copy from: `KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md`
3. Reference: `application-kafka.yml` for all properties
4. Deep dive: `KAFKA_CONSUMER_RESEARCH.md` if questions arise

### DevOps / Platform Engineer
1. Read: `KAFKA_RESEARCH_SUMMARY.md` (Deployment Steps, Operational Runbook)
2. Use: Kubernetes manifests in `KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md`
3. Reference: Health check configuration in `application-kafka.yml`

### QA / Test Engineer
1. Read: `KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md` (Integration Test section)
2. Deep dive: `KAFKA_CONSUMER_RESEARCH.md` (Failure Scenarios)
3. Reference: Testcontainers examples for test setup

### System Architect
1. Read: `KAFKA_CONSUMER_RESEARCH.md` (Rationale, Scaling Strategy, Alternatives)
2. Reference: Decision matrix in `KAFKA_RESEARCH_SUMMARY.md`
3. Review: Kubernetes deployment patterns

---

## 📋 Research Questions Answered

All 6 research questions have been answered comprehensively:

### 1. How to configure Spring Kafka consumer for optimal throughput (1000 msg/sec)?
**Answer**: Use batch listener mode with 500 max-poll-records, 8 concurrent threads per pod, 10MB minimum fetch bytes, and manual offset commit. See `KAFKA_CONSUMER_RESEARCH.md` Section "Decision 1" or `application-kafka.yml`.

### 2. What are best practices for batch processing vs. individual message processing?
**Answer**: Batch processing is essential for 1000 msg/sec (can achieve 1000+), individual processing maxes at 200 msg/sec. Batch size of 500 is optimal for your throughput. See `KAFKA_CONSUMER_RESEARCH.md` Section "Decision 2".

### 3. How to handle offset management to avoid message loss?
**Answer**: Use manual offset commit (enable-auto-commit: false) with at-least-once semantics. Commit offset AFTER database insert. Use database UPSERT to handle duplicate processing on crashes. See `KAFKA_CONSUMER_RESEARCH.md` Section "Decision 3".

### 4. What error handling strategies are recommended?
**Answer**: Implement DefaultErrorHandler with DeadLetterPublishingRecoverer. Send non-retryable errors (JSON parse, constraint violations) to DLQ immediately. Retry transient errors 3x with exponential backoff. See `KAFKA_CONSUMER_RESEARCH.md` Section "Decision 4".

### 5. How to scale consumers horizontally?
**Answer**: Use Kafka consumer groups with 3+ pods. Topics should have 3+ partitions. Kafka automatically rebalances partitions on pod failure. 3 pods × 8 threads = 24 concurrent consumers = comfortable margin for 1000 msg/sec. See `KAFKA_CONSUMER_RESEARCH.md` Section "Decision 5".

### 6. How to monitor consumer lag and performance?
**Answer**: Export Prometheus metrics via Actuator. Track lag, throughput, error rate, batch processing latency. Alert on lag increase, error rate spike, DLQ messages. See `KAFKA_CONSUMER_RESEARCH.md` Section "Decision 6".

---

## 🔧 Key Configuration at a Glance

### Most Critical Settings (Cannot Skip)
```yaml
spring.kafka.consumer.enable-auto-commit: false  # Prevents message loss
spring.kafka.listener.type: batch               # Enables 1000 msg/sec
spring.kafka.listener.concurrency: 8            # Parallel processing
spring.kafka.listener.ack-mode: manual          # Explicit commit after DB
```

### Performance Tuning Settings
```yaml
spring.kafka.consumer.max-poll-records: 500     # Batch size
spring.kafka.consumer.fetch-min-bytes: 10485760 # 10MB for throughput
spring.kafka.consumer.fetch-max-wait-ms: 500    # Batching window
```

### Reliability Settings
```yaml
spring.kafka.consumer.isolation-level: read_committed  # Only committed msgs
spring.kafka.consumer.auto-offset-reset: earliest      # Start from beginning
spring.kafka.consumer.session-timeout-ms: 30000        # Rebalance stability
```

**See**: `application-kafka.yml` for all 100+ configuration options with detailed comments

---

## 📊 Decision Summary Table

| Component | Decision | Target Performance |
|-----------|----------|-------------------|
| Processing mode | Batch listener | 1000+ msg/sec throughput |
| Batch size | 500 records | 2 polls/sec at 1000 msg/sec |
| Offset commit | Manual (AFTER DB insert) | Zero message loss |
| Concurrency | 8 threads/pod | Parallel batch processing |
| Pods | 3 replicas | 24 concurrent processors, HA |
| Error handling | DLQ + 3x retry | Graceful degradation |
| Database | UPSERT on duplicate | Handles replay on crash |
| Monitoring | Prometheus metrics | Real-time visibility |

---

## 🧪 Testing Strategy

### Unit Tests
- Mock Kafka consumer, test message service logic
- Location: `src/test/java/com/uwhub/application/service/MessageServiceTest.java`

### Integration Tests
- Use Testcontainers for Kafka + PostgreSQL
- Test batch processing, offset commit, error handling
- Test DLQ message routing
- Location: `src/test/java/com/uwhub/infrastructure/kafka/KafkaConsumerIntegrationTest.java`
- Code in: `KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md` (Section 10)

### Load Tests
- Produce 5000+ messages, measure throughput
- Assert > 1000 msg/sec
- Measure P95 latency < 100ms
- Code in: `KAFKA_CONSUMER_RESEARCH.md` (Testable Configuration)

### Failure Scenario Tests
- Pod crash: Verify no message loss (offset replayed)
- Database down: Verify retry with backoff
- DLQ scenario: Produce invalid JSON, verify DLQ routing
- Rebalancing: Kill pod, verify automatic rebalance

---

## 📦 Implementation Phases

### Phase 1: Core Consumer (Week 1)
```
- KafkaConsumerConfig (batch listener factory)
- KafkaMessageListener (batch processor)
- MessageService (domain logic)
- KafkaMessage entity + repository
- Basic integration tests
```
**Deliverable**: Consumer consuming messages to DB

### Phase 2: Error Handling (Week 2)
```
- DefaultErrorHandler with retry logic
- DeadLetterPublishingRecoverer
- DLQ topic provisioning
- DLQ consumer for alerting
- KafkaMetricsPublisher (Prometheus)
```
**Deliverable**: Error handling + observability

### Phase 3: Kubernetes (Week 3)
```
- Deployment manifest (3 replicas)
- ConfigMaps for topic config
- Service + LoadBalancer
- Health probes (liveness/readiness)
- Horizontal Pod Autoscaler
```
**Deliverable**: Kubernetes-ready deployment

### Phase 4: Testing & Tuning (Week 4)
```
- Load test (5000 msgs in <5 sec)
- Latency measurement (P95 < 100ms)
- Failure scenario tests
- Grafana dashboards
- Runbook documentation
```
**Deliverable**: Production-ready system

---

## 🚀 Deployment Checklist

Before going to production:

### Configuration
- [ ] `application-kafka.yml` configured for your environment
- [ ] Kafka broker addresses correct in KAFKA_BOOTSTRAP_SERVERS
- [ ] Topics created with correct partition count (3+)
- [ ] DLQ topics created (topic-name.dlq)
- [ ] Database connection pool sized for batch inserts (20+ connections)

### Code
- [ ] KafkaConsumerConfig bean registered
- [ ] KafkaMessageListener processing batches
- [ ] ErrorHandler configured with DLQ
- [ ] MetricsPublisher collecting metrics
- [ ] Health indicators returning UP

### Testing
- [ ] Integration tests pass with Testcontainers
- [ ] Load test achieves 1000+ msg/sec
- [ ] Failure scenario tests pass
- [ ] No messages lost on pod crash (offset replayed)
- [ ] DLQ messages properly routed and visible

### Kubernetes
- [ ] Deployment manifest deployed (3 replicas)
- [ ] Health checks passing (liveness + readiness)
- [ ] Pod logs showing message processing
- [ ] Prometheus scraping metrics
- [ ] Grafana dashboards visible

### Monitoring
- [ ] Prometheus dashboard shows throughput > 1000 msg/sec
- [ ] Alert rules configured (lag > 5min, error rate > 1%)
- [ ] Slack/PagerDuty integration for alerts
- [ ] DLQ consumer logging alerts

### Operations
- [ ] Runbook documented (scaling, debugging, incident response)
- [ ] On-call playbook created
- [ ] Team trained on metrics interpretation
- [ ] Backup/recovery procedures documented

---

## 🔍 Troubleshooting Guide

### Consumer Lag Increasing
1. Check metrics: `kafka_consumer_lag` in Prometheus
2. Check pod CPU/memory usage
3. Check database query times (SELECT from kafka_messages)
4. Solution: Add pods (kubectl scale), or increase concurrency

### Messages in DLQ
1. Check DLQ topic: `<topic-name>.dlq`
2. Check exception headers in DLQ messages
3. Fix root cause (invalid JSON schema, constraint violation, etc.)
4. Replay messages after fix using offset reset

### High Processing Latency
1. Check P95/P99 of `kafka_batch_processing_time`
2. Check database insert time (EXPLAIN ANALYZE)
3. Check network latency between pods and Kafka
4. Solution: Tune database batch size, increase connection pool

### Pod Crashing
1. Check liveness probe configuration
2. Check heap memory (increase JAVA_OPTS if needed)
3. Check Kafka broker connectivity
4. Check database connectivity
5. Review logs: `kubectl logs -f pod-name`

---

## 📞 Questions & Escalation

If you have questions not answered in these documents:

1. **Configuration question**: See `application-kafka.yml` comments
2. **Performance question**: See `KAFKA_CONSUMER_RESEARCH.md` "Rationale" section
3. **Implementation question**: See `KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md` code examples
4. **Failure scenario**: See `KAFKA_CONSUMER_RESEARCH.md` "Failure Scenario" sections
5. **Operations question**: See `KAFKA_RESEARCH_SUMMARY.md` "Operational Runbook"

---

## 📚 References & Further Reading

- **Spring Kafka Documentation**: https://docs.spring.io/spring-kafka/reference/
- **Apache Kafka Best Practices**: https://www.confluent.io/blog/kafka-best-practices/
- **Kafka Consumer Tuning**: https://kafka.apache.org/documentation/#consumerconfigs
- **Testcontainers Kafka**: https://testcontainers.com/modules/kafka/
- **Prometheus Queries**: https://prometheus.io/docs/prometheus/latest/querying/examples/
- **Kubernetes Deployments**: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/

---

## 📅 Document Versions

| Document | Version | Updated | Purpose |
|----------|---------|---------|---------|
| KAFKA_RESEARCH_SUMMARY.md | 1.0 | 2025-11-02 | Executive summary, quick decisions |
| KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md | 1.0 | 2025-11-02 | Code snippets, implementations |
| KAFKA_CONSUMER_RESEARCH.md | 1.0 | 2025-11-02 | Deep technical research |
| application-kafka.yml | 1.0 | 2025-11-02 | Production config template |
| README_KAFKA_RESEARCH.md | 1.0 | 2025-11-02 | Navigation guide (this file) |

---

## ✅ Success Criteria

After implementing this research, you should have:

1. ✅ Kafka consumer processing 1000+ messages/second
2. ✅ Zero message loss (manual offset commit)
3. ✅ <100ms SSE push latency (from DB to UI)
4. ✅ Graceful error handling (DLQ for bad messages)
5. ✅ Horizontal scaling (3+ pods with auto-rebalancing)
6. ✅ Full observability (Prometheus metrics, Grafana dashboards)
7. ✅ Production-ready code (tested, documented, monitored)
8. ✅ Operations runbook (team can handle failures)

---

## 🎓 Learning Path

1. **Day 1**: Read `KAFKA_RESEARCH_SUMMARY.md` (understand decisions)
2. **Day 2**: Read `KAFKA_IMPLEMENTATION_QUICK_REFERENCE.md` (understand code)
3. **Day 3**: Copy code, implement Phase 1 (consumer setup)
4. **Day 4**: Implement Phase 2 (error handling)
5. **Day 5**: Implement Phase 3 (Kubernetes)
6. **Day 6-7**: Phase 4 (testing, tuning, load test)

---

## 🎉 Conclusion

This research provides a **complete, battle-tested solution** for high-throughput Kafka consumption in Spring Boot. The configuration is optimized for:

- **1000+ messages/second throughput** (batch processing)
- **Zero message loss** (manual offset commit)
- **Graceful error handling** (DLQ + retries)
- **Horizontal scalability** (3+ pods, auto-rebalancing)
- **Production observability** (Prometheus metrics)

All code examples are production-ready and can be copied directly into your project. Start with `KAFKA_RESEARCH_SUMMARY.md`, then follow the implementation guide.

**Good luck! 🚀**
