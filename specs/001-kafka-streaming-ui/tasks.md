---
description: "Task list for Kafka Streaming UI implementation with Debezium CDC messages"
---

# Tasks: Kafka Streaming UI (CDC Messages)

**Input**: Design documents from `/specs/001-kafka-streaming-ui/`
**Prerequisites**: plan.md, spec.md, research.md

**CDC Message Format**: Debezium CDC format with table info, operation (INSERT/UPDATE/DELETE), before/after states, position metadata

**Tests**: This feature follows Test-First Development (TDD). All test tasks are REQUIRED and must be written FIRST before implementation.

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3, US4)
- Include exact file paths in descriptions

## Path Conventions

- **Backend**: `backend/src/main/java/` and `backend/src/test/java/`
- **Frontend**: `frontend/src/`
- **Infrastructure**: `k8s/` for Kubernetes manifests

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create backend directory structure following Clean Architecture: backend/src/main/java/{domain,application,infrastructure,presentation}
- [X] T002 Initialize Spring Boot 3.x project with Maven in backend/pom.xml (dependencies: spring-boot-starter-web, spring-kafka, postgresql, actuator, jackson-databind)
- [X] T003 [P] Initialize Next.js 14+ project with TypeScript in frontend/ directory
- [X] T004 [P] Install shadcn/ui dependencies and configure Tailwind CSS in frontend/
- [X] T005 [P] Create Docker Compose file for local development (Kafka, Zookeeper, PostgreSQL) in docker-compose.yml
- [X] T006 [P] Configure backend application properties in backend/src/main/resources/application.yml (Kafka, PostgreSQL, SSE settings)
- [X] T007 [P] Configure frontend environment variables template in frontend/.env.example
- [X] T008 Create Kubernetes manifests directory structure in k8s/{deployments,services,configmaps,secrets}

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Backend Foundation - Domain Layer

- [X] T009 Create domain value object TableInfo in backend/src/main/java/domain/valueobject/TableInfo.java (database, schema, table)
- [X] T010 Create domain value object Position in backend/src/main/java/domain/valueobject/Position.java (sourcePartition, offset with lsn/txId/timestamp)
- [X] T011 Create domain value object Metadata in backend/src/main/java/domain/valueobject/Metadata.java (schemaVersion, connector, source, version)
- [X] T012 Create domain entity CdcMessage in backend/src/main/java/domain/entity/CdcMessage.java (id, topic, tableInfo, operation, timestamp, position, beforeData, afterData, metadata)
- [X] T013 Create domain repository interface CdcMessageRepository in backend/src/main/java/domain/repository/CdcMessageRepository.java (save, findAll with pagination, findByTopic, findByOperation, findByTable)

### Backend Foundation - Infrastructure Layer

- [X] T014 Implement PostgreSQL schema migration in backend/src/main/resources/db/migration/V1__create_cdc_messages_table.sql (id, topic, database, schema, table_name, operation, timestamp, position JSONB, before_data JSONB, after_data JSONB, metadata JSONB, indexes on topic/operation/timestamp, partitioned by timestamp)
- [X] T015 [P] Create JPA entity CdcMessageEntity in backend/src/main/java/infrastructure/persistence/entity/CdcMessageEntity.java (with Hibernate JSONB type handlers for position/before/after/metadata)
- [X] T016 [P] Create JPA converter for JSONB types in backend/src/main/java/infrastructure/persistence/converter/JsonbConverter.java
- [X] T017 [P] Implement JPA repository CdcMessageJpaRepository in backend/src/main/java/infrastructure/persistence/CdcMessageJpaRepository.java
- [X] T018 [P] Create domain to entity mapper in backend/src/main/java/infrastructure/persistence/mapper/CdcMessageMapper.java
- [X] T019 [P] Create configuration class KafkaConfig in backend/src/main/java/infrastructure/config/KafkaConfig.java (deserializer for Debezium CDC JSON format)
- [X] T020 [P] Create CDC message deserializer in backend/src/main/java/infrastructure/kafka/CdcMessageDeserializer.java (parse Debezium format)
- [X] T021 [P] Create configuration class WebMvcConfig for CORS in backend/src/main/java/infrastructure/config/WebMvcConfig.java
- [X] T022 [P] Implement error handling infrastructure GlobalExceptionHandler in backend/src/main/java/presentation/rest/GlobalExceptionHandler.java
- [X] T023 [P] Implement logging infrastructure with correlation IDs in backend/src/main/java/infrastructure/logging/CorrelationIdFilter.java
- [X] T024 Implement health check endpoint in backend/src/main/java/presentation/rest/HealthController.java (Kafka, PostgreSQL status)
- [X] T025 [P] Create Prometheus metrics configuration in backend/src/main/java/infrastructure/config/MetricsConfig.java

### Frontend Foundation

- [X] T026 Configure shadcn/ui components.json in frontend/components.json
- [X] T027 [P] Create TypeScript types for CDC messages in frontend/src/types/cdc.ts (CdcMessage, TableInfo, Position, Metadata, Operation enum)
- [X] T028 [P] Create Zustand store for CDC message state in frontend/src/store/cdcMessageStore.ts (messages, filter by operation/table, connection status)
- [X] T029 [P] Create custom useSSE hook in frontend/src/hooks/useSSE.ts (with auto-reconnect, exponential backoff per research.md)
- [X] T030 [P] Add shadcn/ui Table component using shadcn MCP in frontend/src/components/ui/table.tsx
- [X] T031 [P] Add shadcn/ui Badge component using shadcn MCP in frontend/src/components/ui/badge.tsx
- [X] T032 [P] Add shadcn/ui Card component using shadcn MCP in frontend/src/components/ui/card.tsx
- [X] T033 [P] Add shadcn/ui Select component using shadcn MCP in frontend/src/components/ui/select.tsx
- [X] T034 [P] Add shadcn/ui Skeleton component using shadcn MCP in frontend/src/components/ui/skeleton.tsx
- [X] T035 [P] Install and configure React Virtuoso in frontend/package.json

### Testing Foundation

- [X] T036 Configure Testcontainers for integration tests in backend/src/test/java/infrastructure/testcontainers/TestContainersConfig.java
- [X] T037 [P] Configure Vitest for frontend unit tests in frontend/vitest.config.ts
- [X] T038 [P] Configure Playwright for E2E tests in frontend/playwright.config.ts (with test helpers for producing CDC messages)
- [X] T039 [P] Create Playwright test helpers in frontend/e2e/helpers/kafka-producer.ts (produce INSERT/UPDATE/DELETE CDC messages)

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Real-Time CDC Message Monitoring (Priority: P1) 🎯 MVP

**Goal**: System operators can view CDC messages (INSERT/UPDATE/DELETE) from Kafka topics in real-time with operation type, table info, and before/after states

**Independent Test**: Produce an UPDATE CDC message to Kafka topic, verify it appears in UI within 100ms showing before/after differences

### Tests for User Story 1 (TDD - Write FIRST, Ensure FAIL)

- [ ] T040 [P] [US1] Contract test for Debezium CDC message deserialization in backend/src/test/java/infrastructure/kafka/CdcMessageDeserializationTest.java (test INSERT/UPDATE/DELETE formats)
- [ ] T041 [P] [US1] Contract test for PostgreSQL JSONB storage in backend/src/test/java/infrastructure/persistence/CdcMessagePersistenceContractTest.java
- [ ] T042 [P] [US1] Integration test for Kafka CDC → PostgreSQL flow using Testcontainers in backend/src/test/java/integration/KafkaCdcToPostgresIntegrationTest.java
- [ ] T043 [P] [US1] Integration test for SSE endpoint CDC message delivery in backend/src/test/java/integration/SseCdcMessageDeliveryTest.java
- [ ] T044 [P] [US1] E2E test for real-time CDC message display with Playwright in frontend/e2e/real-time-cdc-messages.spec.ts (produce INSERT/UPDATE/DELETE, verify UI updates)

### Implementation for User Story 1

#### Backend - Kafka Consumer

- [ ] T045 [P] [US1] Implement CdcMessageConsumer in backend/src/main/java/infrastructure/kafka/CdcMessageConsumer.java (consume from Kafka, deserialize Debezium format)
- [ ] T046 [US1] Implement ConsumeMessageUseCase in backend/src/main/java/application/usecase/ConsumeMessageUseCase.java (validate CDC message, extract operation/table info, store)
- [ ] T047 [US1] Wire CdcMessageConsumer to ConsumeMessageUseCase in backend/src/main/java/infrastructure/kafka/CdcConsumerService.java
- [ ] T048 [US1] Add error handling for CDC consumer in backend/src/main/java/infrastructure/kafka/CdcErrorHandler.java (dead-letter queue for malformed messages)

#### Backend - SSE Implementation

- [ ] T049 [P] [US1] Create SSE emitter registry in backend/src/main/java/infrastructure/sse/SseEmitterRegistry.java (manage multiple clients, track subscriptions)
- [ ] T050 [US1] Implement SSE controller endpoint GET /api/sse/cdc-messages in backend/src/main/java/presentation/rest/SseController.java
- [ ] T051 [US1] Implement CDC message broadcasting service in backend/src/main/java/infrastructure/sse/CdcMessageBroadcaster.java
- [ ] T052 [US1] Integrate CdcMessageBroadcaster with ConsumeMessageUseCase to push new CDC messages
- [ ] T053 [US1] Add SSE heartbeat mechanism (30s interval) in SseEmitterRegistry

#### Frontend - Real-Time CDC Display

- [ ] T054 [P] [US1] Create OperationBadge component in frontend/src/components/OperationBadge.tsx (color-coded: INSERT=green, UPDATE=blue, DELETE=red)
- [ ] T055 [P] [US1] Create CdcMessageRow component in frontend/src/components/CdcMessageRow.tsx (display operation, table, timestamp, before/after preview)
- [ ] T056 [P] [US1] Create CdcMessageStream component in frontend/src/components/CdcMessageStream.tsx (integrate useSSE, Virtuoso, Zustand)
- [ ] T057 [P] [US1] Create ARIA live region component in frontend/src/components/AriaLiveRegion.tsx (accessibility for screen readers, announce CDC operations)
- [ ] T058 [US1] Integrate CdcMessageStream with useSSE hook to connect to backend SSE endpoint
- [ ] T059 [US1] Implement virtualized table rendering with React Virtuoso (10k CDC messages support)
- [ ] T060 [US1] Add connection status indicator (connected/disconnected badge) in CdcMessageStream component
- [ ] T061 [US1] Add auto-scroll behavior when new CDC messages arrive (stay at top if already at top)
- [ ] T062 [US1] Create main page in frontend/src/app/page.tsx integrating CdcMessageStream component

**Checkpoint**: User Story 1 complete - Real-time CDC monitoring functional with operation types and table info

---

## Phase 4: User Story 2 - Before/After Diff View for UPDATEs (Priority: P2)

**Goal**: Data analysts can expand UPDATE operations to see side-by-side comparison of before/after field changes

**Independent Test**: Click on UPDATE operation row, verify diff modal shows changed fields highlighted (e.g., last_name: "Sivalingam 2" → "Sivalingam Name change")

### Tests for User Story 2 (TDD - Write FIRST, Ensure FAIL)

- [ ] T063 [P] [US2] Unit test for diff calculation utility in frontend/src/utils/diff.test.ts
- [ ] T064 [P] [US2] Unit test for BeforeAfterDiff component in frontend/src/components/BeforeAfterDiff.test.tsx
- [ ] T065 [P] [US2] E2E test for UPDATE diff view with Playwright in frontend/e2e/update-diff-view.spec.ts (produce UPDATE, click row, verify diff modal)

### Implementation for User Story 2

#### Frontend - Diff Calculation & Display

- [ ] T066 [P] [US2] Create diff calculation utility in frontend/src/utils/calculateDiff.ts (compare before/after JSON objects, identify changed/added/removed fields)
- [ ] T067 [P] [US2] Add shadcn/ui Dialog component using shadcn MCP in frontend/src/components/ui/dialog.tsx
- [ ] T068 [P] [US2] Add shadcn/ui ScrollArea component using shadcn MCP in frontend/src/components/ui/scroll-area.tsx
- [ ] T069 [P] [US2] Create BeforeAfterDiff component in frontend/src/components/BeforeAfterDiff.tsx (side-by-side view with highlighted changes)
- [ ] T070 [US2] Integrate BeforeAfterDiff with CdcMessageRow (click UPDATE row to open diff modal)
- [ ] T071 [US2] Add keyboard navigation for diff modal (Escape to close, Tab for field navigation)
- [ ] T072 [US2] Add field-level highlighting (green=added, red=removed, yellow=changed)

**Checkpoint**: User Stories 1 AND 2 complete - Real-time CDC monitoring with UPDATE diff view

---

## Phase 5: User Story 3 - Historical CDC Message Viewing & Filtering (Priority: P3)

**Goal**: Users can view historical CDC messages with filters by operation type (INSERT/UPDATE/DELETE) and table name

**Independent Test**: Apply filter "operation=UPDATE AND table=customers", verify only UPDATE operations on customers table are displayed

### Tests for User Story 3 (TDD - Write FIRST, Ensure FAIL)

- [ ] T073 [P] [US3] Contract test for GET /api/cdc-messages?operation=UPDATE&table=customers in backend/src/test/java/presentation/rest/CdcMessageControllerContractTest.java
- [ ] T074 [P] [US3] Integration test for historical message retrieval with filters in backend/src/test/java/integration/HistoricalCdcMessageRetrievalTest.java
- [ ] T075 [P] [US3] Unit test for FilterPanel component in frontend/src/components/FilterPanel.test.tsx
- [ ] T076 [P] [US3] E2E test for combined filters with Playwright in frontend/e2e/cdc-filters.spec.ts

### Implementation for User Story 3

#### Backend - Historical API with Filters

- [ ] T077 [P] [US3] Implement GetCdcMessagesUseCase in backend/src/main/java/application/usecase/GetCdcMessagesUseCase.java (pagination, sorting, filters: operation, table, database, dateRange)
- [ ] T078 [US3] Implement REST endpoint GET /api/cdc-messages in backend/src/main/java/presentation/rest/CdcMessageController.java (query params: page, size, operation, table, database, fromDate, toDate)
- [ ] T079 [US3] Add database indexes for efficient filtered queries in backend/src/main/resources/db/migration/V2__add_cdc_message_indexes.sql (composite index on operation+table_name+timestamp)
- [ ] T080 [US3] Implement query optimization for filtered queries in CdcMessageJpaRepository
- [ ] T081 [US3] Add GET /api/tables endpoint in CdcMessageController to list unique table names (database.schema.table)

#### Frontend - Filter UI

- [ ] T082 [P] [US3] Create FilterPanel component in frontend/src/components/FilterPanel.tsx (operation checkboxes, table dropdown, date range picker)
- [ ] T083 [P] [US3] Add shadcn/ui Checkbox component using shadcn MCP in frontend/src/components/ui/checkbox.tsx
- [ ] T084 [P] [US3] Create OperationFilter component in frontend/src/components/OperationFilter.tsx (multi-select: INSERT, UPDATE, DELETE)
- [ ] T085 [P] [US3] Create TableFilter component in frontend/src/components/TableFilter.tsx (dropdown with database.schema.table format)
- [ ] T086 [US3] Integrate FilterPanel with Zustand store (update filter state, trigger refetch)
- [ ] T087 [US3] Update CdcMessageStream to apply filters to displayed messages
- [ ] T088 [US3] Add "Clear Filters" button to reset all filters
- [ ] T089 [US3] Display active filters as badges (e.g., "UPDATE", "customers table")
- [ ] T090 [US3] Implement SSE client-specific filtering (clients subscribe to specific operations/tables)

**Checkpoint**: User Stories 1, 2, AND 3 complete - All CDC filtering and diff features work

---

## Phase 6: User Story 4 - Pagination & Performance (Priority: P4)

**Goal**: System handles large volumes of CDC messages with pagination, lazy loading, and performance optimizations

**Independent Test**: Load 50,000 CDC messages, verify pagination works smoothly and UI remains responsive (<200ms page transitions)

### Tests for User Story 4 (TDD - Write FIRST, Ensure FAIL)

- [ ] T091 [P] [US4] Integration test for pagination in backend/src/test/java/integration/CdcMessagePaginationTest.java
- [ ] T092 [P] [US4] Load test for 1000 msg/sec throughput in backend/src/test/java/performance/CdcLoadTest.java
- [ ] T093 [P] [US4] E2E test for lazy loading with Playwright in frontend/e2e/lazy-loading.spec.ts

### Implementation for User Story 4

#### Backend - Pagination & Performance

- [ ] T094 [P] [US4] Add pagination support to CdcMessageRepository in backend/src/main/java/domain/repository/CdcMessageRepository.java (Page<CdcMessage> findAll with filters)
- [ ] T095 [US4] Implement cursor-based pagination for real-time streams in GetCdcMessagesUseCase (use timestamp+id cursor)
- [ ] T096 [US4] Add database connection pooling configuration in backend/src/main/resources/application.yml (HikariCP settings)
- [ ] T097 [US4] Implement batch insert optimization for high-volume CDC messages in CdcMessageJpaRepository

#### Frontend - Pagination & Lazy Loading

- [ ] T098 [P] [US4] Add shadcn/ui Pagination component using shadcn MCP in frontend/src/components/ui/pagination.tsx
- [ ] T099 [P] [US4] Create Pagination controls in frontend/src/components/CdcPagination.tsx
- [ ] T100 [US4] Implement infinite scroll with React Virtuoso (fetch next page when scrolling to bottom)
- [ ] T101 [US4] Add loading skeleton states using shadcn/ui Skeleton component for pagination transitions
- [ ] T102 [US4] Optimize Zustand store with selectors to prevent unnecessary re-renders

**Checkpoint**: User Stories 1-4 complete - Full CDC monitoring with performance optimizations

---

## Phase 7: User Story 5 - Scalability & Observability (Priority: P5)

**Goal**: System is production-ready with scalability, monitoring, and operational excellence

**Independent Test**: Deploy multiple backend instances, produce 10,000 CDC messages, verify no duplicates and all messages processed

### Tests for User Story 5 (TDD - Write FIRST, Ensure FAIL)

- [ ] T103 [P] [US5] Integration test for Kafka consumer group rebalancing in backend/src/test/java/integration/ConsumerRebalancingTest.java
- [ ] T104 [P] [US5] E2E test for 100 concurrent SSE clients with Playwright in frontend/e2e/concurrent-clients.spec.ts
- [ ] T105 [P] [US5] Performance test for sustained 1000 msg/sec load in backend/src/test/java/performance/SustainedLoadTest.java

### Implementation for User Story 5

#### Backend - Scalability

- [ ] T106 [P] [US5] Implement consumer group configuration for horizontal scaling in KafkaConfig (consumer group ID, partition assignment)
- [ ] T107 [P] [US5] Implement partition assignment strategy in KafkaConfig (cooperative-sticky rebalancing)
- [ ] T108 [P] [US5] Add consumer lag monitoring metrics to Prometheus in backend/src/main/java/infrastructure/metrics/KafkaMetrics.java

#### Observability & Monitoring

- [ ] T109 [P] [US5] Add structured JSON logging for all CDC messages with correlation IDs (include operation, table, txId)
- [ ] T110 [P] [US5] Expose Prometheus metrics endpoint /actuator/prometheus (message count by operation/table, processing latency, consumer lag)
- [ ] T111 [P] [US5] Create Grafana dashboard configuration in k8s/monitoring/grafana-dashboard.json (CDC operations timeline, table activity heatmap)
- [ ] T112 [US5] Implement distributed tracing with trace IDs across Kafka → DB → SSE flow
- [ ] T113 [US5] Add alerting rules for consumer lag and message processing latency in k8s/monitoring/prometheus-rules.yaml

#### Database Optimization

- [ ] T114 [P] [US5] Implement PostgreSQL table partitioning by timestamp in backend/src/main/resources/db/migration/V3__partition_cdc_messages_table.sql (monthly partitions)
- [ ] T115 [P] [US5] Create data retention policy in backend/src/main/java/application/usecase/CleanupOldCdcMessagesUseCase.java (configurable TTL, preserve last snapshot per table)
- [ ] T116 [US5] Schedule cleanup job to run daily in backend/src/main/java/infrastructure/scheduling/CdcMessageCleanupScheduler.java

**Checkpoint**: All user stories complete - Production-ready CDC monitoring system

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Security, deployment, documentation, and final validation

### Security

- [ ] T117 [P] Implement JWT authentication filter in backend/src/main/java/infrastructure/security/JwtAuthenticationFilter.java
- [ ] T118 [P] Configure Spring Security for REST and SSE endpoints in backend/src/main/java/infrastructure/config/SecurityConfig.java
- [ ] T119 [P] Implement XSS sanitization for CDC message content in frontend/src/utils/sanitize.ts (sanitize before_data/after_data fields)
- [ ] T120 Add rate limiting for SSE connections in backend/src/main/java/infrastructure/security/RateLimitFilter.java (max 5 connections per user)

### Deployment

- [ ] T121 [P] Create Kubernetes Deployment manifest in k8s/deployments/backend-deployment.yaml (with resource limits, liveness/readiness probes)
- [ ] T122 [P] Create Kubernetes Service manifest in k8s/services/backend-service.yaml (ClusterIP for backend, LoadBalancer for frontend)
- [ ] T123 [P] Create Kubernetes ConfigMap in k8s/configmaps/backend-config.yaml (Kafka bootstrap servers, topic names)
- [ ] T124 [P] Create Kubernetes Secret template in k8s/secrets/backend-secrets.yaml (PostgreSQL credentials, Kafka SASL)
- [ ] T125 [P] Create Docker build configuration in backend/Dockerfile (multi-stage build with JRE 21 slim)
- [ ] T126 [P] Create Docker build configuration in frontend/Dockerfile (multi-stage build with Node 20 alpine)
- [ ] T127 Build and tag Docker images for backend and frontend

### Documentation

- [ ] T128 [P] Create API documentation in backend/docs/API.md (OpenAPI/Swagger format for REST endpoints)
- [ ] T129 [P] Create CDC message format documentation in docs/CDC_MESSAGE_FORMAT.md (Debezium structure with examples)
- [ ] T130 [P] Create deployment guide in k8s/README.md (step-by-step Kubernetes deployment)
- [ ] T131 [P] Create troubleshooting guide in docs/TROUBLESHOOTING.md (common issues: consumer lag, SSE disconnects, JSONB errors)
- [ ] T132 Update main README.md with architecture overview, CDC message flow diagram, quickstart guide

### Final Validation

- [ ] T133 Run all contract tests and verify 100% pass rate
- [ ] T134 Run all integration tests with Testcontainers (Kafka, PostgreSQL)
- [ ] T135 Run E2E tests with Playwright (real-time updates, filters, diff view, pagination)
- [ ] T136 Run load test (1000 msg/sec for 5 minutes with INSERT/UPDATE/DELETE mix) and verify no errors
- [ ] T137 Verify health check endpoints return healthy status
- [ ] T138 Validate WCAG 2.1 AA accessibility with axe DevTools (keyboard navigation, screen reader announcements)
- [ ] T139 Deploy to Kubernetes and verify all pods healthy (backend, PostgreSQL, Kafka)

---

## CDC Message Format Reference

### INSERT Example
```json
{
  "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
  "operation": "INSERT",
  "timestamp": "2025-11-02T04:45:54.287Z",
  "position": {
    "sourcePartition": "{server=postgres-localhost-cdcdb}",
    "offset": { "txId": 751, "lsn": 27696384, "snapshot": true }
  },
  "after": { "customer_id": 1, "first_name": "John", ... },
  "metadata": { "version": "1.0.0", "connector": "postgresql" }
}
```

### UPDATE Example
```json
{
  "table": { "database": "cdcdb", "schema": "public", "table": "customers" },
  "operation": "UPDATE",
  "timestamp": "2025-11-02T08:54:07.298Z",
  "position": {
    "sourcePartition": "{server=postgres-localhost-cdcdb}",
    "offset": { "lsn_commit": 27709280, "txId": 759 }
  },
  "before": { "customer_id": 6, "last_name": "Sivalingam 2", ... },
  "after": { "customer_id": 6, "last_name": "Sivalingam Name change", ... },
  "metadata": { "version": "1.0.0", "connector": "postgresql" }
}
```

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational completion
- **User Story 2 (Phase 4)**: Depends on US1 completion (needs CDC message display)
- **User Story 3 (Phase 5)**: Depends on US1 completion (can run parallel to US2)
- **User Story 4 (Phase 6)**: Depends on US1, US2, US3 completion
- **User Story 5 (Phase 7)**: Depends on US1-4 completion
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Foundation only - Real-time CDC monitoring
- **User Story 2 (P2)**: Requires US1 - Diff view builds on message display
- **User Story 3 (P3)**: Requires US1 - Filtering needs existing display
- **User Story 4 (P4)**: Requires US1, US2, US3 - Performance layer
- **User Story 5 (P5)**: Requires US1-4 - Production readiness

### Within Each User Story

- **Tests FIRST**: All test tasks must be written and FAIL before implementation begins
- **Domain before infrastructure**: Value objects → Entities → Repositories
- **Backend before frontend**: API endpoints before UI components
- **Core before integration**: Individual components before cross-story integration

### Parallel Opportunities

#### Setup Phase (Phase 1)
- T003, T004, T005, T006, T007, T008 can run in parallel

#### Foundational Phase (Phase 2)
- T009, T010, T011 can run in parallel (domain value objects)
- T015, T016, T017, T018, T019, T020, T021, T022, T023, T025 can run in parallel (infrastructure)
- T027, T028, T029, T030, T031, T032, T033, T034, T035 can run in parallel (frontend)
- T037, T038, T039 can run in parallel (testing)

#### User Story 1 Tests
- T040, T041, T042, T043, T044 can run in parallel

#### User Story 1 Implementation
- T045 (Kafka consumer) and T049 (SSE registry) can run in parallel
- T054, T055, T056, T057 can run in parallel (frontend components)

#### User Story 2 Tests
- T063, T064, T065 can run in parallel

#### User Story 2 Implementation
- T066, T067, T068, T069 can run in parallel

#### User Story 3 Tests
- T073, T074, T075, T076 can run in parallel

#### User Story 3 Implementation
- T077, T082, T083, T084, T085 can run in parallel

#### User Story 4 Tests
- T091, T092, T093 can run in parallel

#### User Story 4 Implementation
- T098, T099 can run in parallel

#### User Story 5 Tests
- T103, T104, T105 can run in parallel

#### User Story 5 Implementation
- T106, T107, T108, T109, T110, T114, T115 can run in parallel

#### Polish Phase
- T117, T118, T119, T121, T122, T123, T124, T125, T126, T128, T129, T130, T131 can run in parallel

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Write all US1 tests (T040-T044) - verify they FAIL
4. Complete Phase 3: User Story 1 implementation
5. **STOP and VALIDATE**: Run all US1 tests - verify they PASS
6. Deploy/demo MVP (real-time CDC monitoring with operation types)

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP - Real-time CDC monitoring!)
3. Add User Story 2 → Test independently → Deploy/Demo (UPDATE diff view!)
4. Add User Story 3 → Test independently → Deploy/Demo (Filtering by operation/table!)
5. Add User Story 4 → Test independently → Deploy/Demo (Pagination & performance!)
6. Add User Story 5 → Test independently → Deploy/Demo (Production-ready scalability!)
7. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1 (real-time CDC monitoring)
   - Developer B: User Story 3 (filtering - can start in parallel)
3. After US1 completes:
   - Developer A: User Story 2 (diff view - depends on US1)
   - Developer B: Continues US3
4. After US1-3 complete:
   - Developer A: User Story 4 (pagination)
   - Developer B: User Story 5 (scalability)
5. Final: Team collaborates on Phase 8 (Polish)

---

## Notes

- **TDD REQUIRED**: All test tasks MUST be written first and FAIL before implementation
- **[P] tasks**: Different files, no dependencies - safe to parallelize
- **[Story] label**: Maps task to specific user story for traceability
- **CDC Format**: Debezium CDC messages with before/after, operation, table info
- **JSONB Storage**: PostgreSQL JSONB for flexible schema storage of before/after data
- **Operation Types**: INSERT (only after), UPDATE (before + after), DELETE (only before)
- **Commit strategy**: Commit after each task or logical group
- **Checkpoints**: Stop at any checkpoint to validate story independently
- **Architecture**: Clean Architecture (domain → application → infrastructure → presentation)
- **shadcn/ui**: Use MCP to add components (prefer adding over manual creation)
- **Playwright**: E2E tests produce real CDC messages to Kafka for realistic testing

---

## Task Count Summary

- **Total Tasks**: 139
- **Phase 1 (Setup)**: 8 tasks
- **Phase 2 (Foundational)**: 31 tasks (domain, infrastructure, frontend, testing)
- **Phase 3 (User Story 1)**: 23 tasks (5 tests + 18 implementation)
- **Phase 4 (User Story 2)**: 10 tasks (3 tests + 7 implementation)
- **Phase 5 (User Story 3)**: 18 tasks (4 tests + 14 implementation)
- **Phase 6 (User Story 4)**: 12 tasks (3 tests + 9 implementation)
- **Phase 7 (User Story 5)**: 14 tasks (3 tests + 11 implementation)
- **Phase 8 (Polish)**: 23 tasks
- **Parallel Opportunities**: ~60 tasks marked [P]

---

## Suggested MVP Scope

**Minimum Viable Product (MVP)**: User Story 1 only
- Real-time CDC message monitoring from Kafka topics
- Display operation types (INSERT/UPDATE/DELETE) with color-coded badges
- Show table info (database.schema.table)
- SSE-based live updates
- Virtualized table display (10k CDC messages)
- Basic accessibility (WCAG 2.1 AA)
- Health check endpoints
- Estimated effort: ~45% of total tasks (Phases 1, 2, 3)

**Why this is valuable**: Demonstrates core CDC monitoring value with operation visibility, production-ready quality (tests, accessibility, observability), and foundation for diff view and filtering

---

## CDC-Specific Features Summary

1. **Operation-Aware Display**: Color-coded badges (INSERT=green, UPDATE=blue, DELETE=red)
2. **Table Metadata**: Show database.schema.table for every message
3. **Before/After States**: Store and display pre/post change states for UPDATEs
4. **Diff View**: Side-by-side comparison with field-level highlighting
5. **Operation Filtering**: Filter by INSERT/UPDATE/DELETE
6. **Table Filtering**: Filter by specific tables
7. **JSONB Storage**: Flexible schema storage for varying CDC payloads
8. **Position Tracking**: Store LSN, txId, timestamp for CDC ordering
