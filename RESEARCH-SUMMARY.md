# SSE Research Summary - Executive Brief

**Date**: 2025-11-02
**Project**: UW-Hub Kafka Streaming UI
**Target**: 100+ concurrent SSE connections, <100ms latency, Spring Boot 3.x

---

## TL;DR - The Recommendation

**Use SseEmitter with in-memory registry and async queue broadcasting for Phase 1.**

This achieves all requirements (100 clients, <100ms latency) with minimal complexity, is easy to test, and provides a clear path to scale to 500+ clients via Redis pub/sub or WebFlux in Phase 3.

---

## Key Findings

### 1. SSE Implementation Choice

| Approach | For Whom | Verdict |
|----------|----------|---------|
| **SseEmitter** | <200 clients, single instance | ✅ **RECOMMENDED** |
| **WebFlux** | 500+ clients, reactive everywhere | ⚠️ Future if needed |
| **WebSocket** | Bidirectional comms required | ❌ Over-engineered |
| **Polling** | Any deployment | ❌ Can't meet <100ms latency |

**Winner**: SseEmitter
- Simple, battle-tested, production-proven
- Adequate for 100-200 concurrent clients on one instance
- Can be implemented in 2-3 days
- No additional dependencies beyond Spring Framework

---

### 2. Connection Registry Strategy

| Pattern | Best For | Verdict |
|---------|----------|---------|
| **In-Memory** | Single instance, <150 clients | ✅ **RECOMMENDED** |
| **Redis Pub/Sub** | 500+ clients, multi-instance | ⚠️ Introduce at scale |
| **Hybrid** | 150-200 clients, transition phase | ⚠️ If needed between |

**Winner**: In-Memory Registry
- No external dependencies (no Redis infrastructure)
- O(1) lookup and broadcast
- Perfect for POC/MVP
- Clean upgrade path: Wrap with Redis pub/sub later

---

### 3. Broadcasting Strategy

| Method | Latency | Complexity | Verdict |
|--------|---------|-----------|---------|
| **Sequential** | 100ms+ | Low | ❌ Marginal |
| **Parallel** | 20-30ms | Medium | ⚠️ Good |
| **Async Queue** | 5-10ms enqueue | Medium | ✅ **RECOMMENDED** |
| **Reactive Streams** | 10-20ms | High | ⚠️ Overkill now |

**Winner**: Async Queue (Fire-and-Forget)
- Kafka → DB thread returns immediately
- SSE broadcast happens on background thread
- Achieves <100ms total latency
- Easy to monitor (queue depth metric)
- Prevents thread pool starvation

---

### 4. Client Reconnection Pattern

| Strategy | Recovery Time | Message Loss | Verdict |
|----------|----------------|--------------|---------|
| **Stateless (Event Buffer)** | <1 second | ~10 minutes | ✅ **RECOMMENDED** |
| **Stateful (Session Queue)** | Immediate | 1+ hours | ⚠️ Phase 2+ |
| **Polling Fallback** | 5+ seconds | None | ❌ Complex |

**Winner**: Stateless Event Buffer
- Keep last 1000 messages in memory
- Browser's EventSource sends Last-Event-ID automatically
- Client replays missed messages on reconnect
- Sufficient for <10 minute offline windows
- No session database needed

---

## Latency Breakdown (100ms Total Target)

```
Kafka message arrives
     ↓ (0ms)
Kafka consumer processes message (~1-2ms)
     ↓
Message persisted to PostgreSQL (~5-10ms)
     ↓
Queued for SSE broadcast (~<1ms, non-blocking)
     ↓
Background broadcaster picks up message (~1-5ms)
     ↓
Message sent to 100 SSE clients (~20-50ms parallel)
     ↓
Browser receives and updates UI (~5-10ms)

TOTAL: ~35-80ms ✅ (target: <100ms)
Headroom: ~20-65ms for network jitter, GC pauses
```

---

## Resource Requirements (100 Concurrent Clients)

### Memory
- **Heap**: 512MB `-Xms512m -Xmx1024m`
- Per-connection overhead: ~5KB
- 100 connections: ~500KB
- Event buffer (1000 msgs): ~5MB (varies by message size)
- Total memory: 512MB (42% of heap) ✅

### CPU
- **Tomcat thread pool**: 150 threads max
  - 100 SSE connections (1 thread each)
  - 50 threads for other HTTP requests
- **CPU usage**: 45-55% on t3.medium (2 vCPU)
- **GC impact**: 20-30ms pauses every 5-10 seconds

### Network
- **Per client**: ~1 KB/message baseline
- **1000 msg/sec → all 100 clients**: ~100 KB/sec broadcast traffic
- **Keep-alive ping**: Optional 30-60 second heartbeat

### Database
- **Write throughput**: 1000 msg/sec
- **Connection pool**: 10-20 connections (HikariCP default)
- **Query time**: <200ms P95 with proper indexing

---

## Implementation Checklist (Phase 1)

### Backend Components (Spring Boot)
- [ ] `SseConnectionRegistry` - Manage active connections
- [ ] `SseEventBuffer` - Replay on reconnect
- [ ] `AsyncSseBroadcaster` - Background queue worker
- [ ] `SseController` - `/api/sse/subscribe` endpoint
- [ ] `KafkaMessageHandler` - Kafka listener integration
- [ ] Metrics & observability
- [ ] Error handling & circuit breaker
- [ ] Health check endpoints

### Frontend Components (Next.js/React)
- [ ] `useSSE` hook - Connection + auto-reconnection
- [ ] Message display component
- [ ] Connection status indicator
- [ ] Error handling & retry UI

### Testing
- [ ] Unit tests (no browser needed)
- [ ] Integration tests (Testcontainers)
- [ ] Load test (100 concurrent)
- [ ] Latency profiling

### Configuration
- [ ] Tomcat thread pool tuning
- [ ] JVM heap/GC settings
- [ ] Prometheus metrics export
- [ ] Structured logging (JSON)

### Deployment
- [ ] Docker image build
- [ ] Kubernetes manifests
- [ ] Health checks
- [ ] Resource limits/requests

---

## Scaling Path (Future Phases)

### Phase 2 (Week 3-4): Hardening
- Add circuit breaker for failed broadcasts
- Prometheus + Grafana dashboards
- Production logging & tracing
- Load test to 150+ clients

### Phase 3 (Week 5-6): Horizontal Scaling (if needed)
**When**: If Phase 2 load test shows saturation above 150 clients

**Option A: Redis Pub/Sub** (recommended for simplicity)
```
Kafka → DB → Redis Pub/Sub
           ↓
        Instance1 (in-memory registry)
        Instance2 (in-memory registry)
        Instance3 (in-memory registry)
           ↓
        Load Balancer (sticky sessions)
           ↓
        Clients
```
- Add `RedisSSEBroadcaster`
- Each instance has local registry
- Updates: 1-2 week effort

**Option B: WebFlux** (if <20ms latency is critical)
```
Refactor to Spring WebFlux + Reactor
- Non-blocking I/O model
- 5000+ connections per instance
- Steeper learning curve
- 2-3 week effort
```

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| Single instance bottleneck at 150+ clients | Medium | Medium | Phase 3 Redis scaling |
| GC pauses exceed 100ms target | Low | High | Tune G1GC, monitor metrics |
| Client reconnection infinite loop | Low | High | Exponential backoff, max retry |
| Message loss during broadcast | Low | High | Event buffer + circuit breaker |
| Thread pool exhaustion | Low | Medium | Proper sizing + monitoring |
| Network proxy timeout | Medium | Low | Keep-alive header config |

**Overall Risk Level**: 🟢 LOW
- Well-understood patterns (SseEmitter)
- Clear testing strategy
- Simple fallback (add Redis)

---

## Cost Estimate (12 months)

### Phase 1 Infrastructure
```
AWS:
├─ Spring Boot (t3.medium): $30/month × 12 = $360
├─ PostgreSQL (RDS t3.micro): $20/month × 12 = $240
├─ Kafka (MSK minimal): $50/month × 12 = $600
├─ Network/misc: $10/month × 12 = $120
└─ Monitoring (open-source): Free

Engineering:
├─ Backend dev (2.5 weeks): $5,000
├─ Frontend dev (1.5 weeks): $3,000
├─ Testing/QA (1 week): $2,000
└─ DevOps/deployment (0.5 week): $1,000

TOTAL: ~$12,320 (infrastructure + engineering)
```

### Phase 3 (Redis Scaling) - If Needed
```
Additional AWS:
├─ Redis (ElastiCache): $20/month × 12 = $240
├─ 2 more instances: $60/month × 12 = $720
└─ Load balancer: $25/month × 12 = $300

Engineering:
├─ Redis integration: $2,000
├─ Testing/QA: $1,000
└─ DevOps: $500

Additional Cost: ~$4,760
```

---

## Success Metrics (Phase 1 Definition of Done)

- ✅ 100 concurrent SSE clients sustained for >10 minutes
- ✅ <100ms latency (P95) from DB insert to browser update
- ✅ 0% message loss during steady state
- ✅ Auto-reconnection works (clients survive 5-minute outage)
- ✅ All integration tests pass
- ✅ Prometheus metrics exposed
- ✅ Kubernetes deployment working
- ✅ Load test report completed

---

## Technology Stack Summary

```
Backend:
├─ Spring Boot 3.x
├─ Spring MVC (SseEmitter)
├─ Spring Kafka
├─ PostgreSQL 15+
├─ Micrometer/Prometheus (metrics)
└─ JUnit 5 + Testcontainers (testing)

Frontend:
├─ Next.js 14+
├─ React 18+
├─ TypeScript
├─ shadcn/ui (components)
└─ Vitest + Playwright (testing)

Infrastructure:
├─ Docker (containerization)
├─ Kubernetes (orchestration)
├─ PostgreSQL (data storage)
├─ Kafka (message source)
└─ Prometheus + Grafana (monitoring)

NOT in Phase 1:
└─ Redis (Phase 3 only)
```

---

## Decision Summary

| Decision | Rationale |
|----------|-----------|
| **SseEmitter** | Proven, simple, meets requirements for 100 clients |
| **In-Memory Registry** | No external dependencies, easy to test |
| **Async Queue Broadcaster** | <100ms latency, prevents blocking |
| **Event Buffer Reconnection** | Sufficient for <10 min windows, no DB overhead |
| **Stateless Architecture** | Survives restarts, scales horizontally later |
| **Single Instance Phase 1** | MVP approach, scale out if validated |

---

## Next Steps

1. **Alignment** (Today): Team reviews this research
2. **Design Review** (Day 1): Whiteboard architecture with team
3. **Implementation Sprint** (Days 2-8): Code Phase 1 components
4. **Testing** (Days 9-10): Unit + integration tests
5. **Load Testing** (Days 11): Verify 100 clients + latency
6. **Demo** (Day 12): Show working prototype to stakeholders
7. **Refinement** (Week 2): Handle feedback, add hardening
8. **Production Ready** (Week 3): Deploy to staging

---

## References & Documentation

1. **RESEARCH-SSE-SCALING.md** - Detailed technical analysis
2. **RESEARCH-SSE-CODE-EXAMPLES.md** - Production-ready code patterns
3. **RESEARCH-SSE-DECISION-MATRIX.md** - Scaling guide & troubleshooting
4. Spring Kafka: https://spring.io/projects/spring-kafka
5. EventSource API: https://html.spec.whatwg.org/multipage/server-sent-events.html
6. Tomcat Documentation: https://tomcat.apache.org/tomcat-9.0-doc/

---

**Document Status**: Ready for implementation
**Last Updated**: 2025-11-02
**Prepared By**: Research Team
**Review Status**: ⏳ Pending team alignment
