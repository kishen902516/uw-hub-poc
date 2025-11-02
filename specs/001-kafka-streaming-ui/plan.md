# Implementation Plan: Kafka Streaming UI

**Branch**: `001-kafka-streaming-ui` | **Date**: 2025-11-02 | **Spec**: [spec.md](./spec.md)

## Summary

Build a real-time data streaming application that consumes messages from Kafka topics, stores them in PostgreSQL, and displays them in a web UI with real-time updates via Server-Sent Events (SSE). The system will follow a library-first architecture with Spring Boot backend and Next.js frontend, implementing Clean Architecture principles with DDD tactical patterns.

## Technical Context

**Language/Version**: Java 21 (backend), TypeScript/Next.js 14+ (frontend)  
**Primary Dependencies**: Spring Boot 3.x, Spring Kafka, PostgreSQL Driver, Next.js, shadcn/ui, Tailwind CSS  
**Storage**: PostgreSQL 15+  
**Testing**: JUnit 5, Testcontainers, Vitest, Playwright  
**Target Platform**: Kubernetes (containerized deployment), Web browsers  
**Project Type**: Web (backend + frontend)  
**Performance Goals**: 1000 msg/sec throughput, <100ms SSE latency, <200ms p95 DB query time  
**Constraints**: <100ms SSE push latency, support 100 concurrent SSE clients, WCAG 2.1 AA accessibility  
**Scale/Scope**: POC for real-time streaming, 2+ Kafka topics, up to 10k messages in UI viewport

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### Principle I: Library-First Architecture
- **Status**: ⚠️ NEEDS CLARIFICATION
- **Plan**: Backend structured as library with CLI interface for Kafka consumption
- **Question**: Separate library JAR vs. integrated Spring Boot app with CLI commands?

### Principle II: CLI-Driven Interfaces
- **Status**: ⚠️ NEEDS CLARIFICATION
- **Plan**: CLI commands for: start consumer, check lag, export messages (JSON/CSV)
- **Question**: Spring Shell vs. picocli vs. custom implementation?

### Principle III: Test-First Development
- **Status**: ✅ COMPLIANT
- **Plan**: TDD workflow - tests before implementation for all layers

### Principle IV: Contract & Integration Testing
- **Status**: ✅ COMPLIANT
- **Contract Tests**: REST API, Kafka message schema, PostgreSQL schema
- **Integration Tests**: Kafka → DB → SSE → UI flow with Testcontainers

### Principle V: Observability & Simplicity
- **Status**: ✅ COMPLIANT
- **Observability**: Structured JSON logging, correlation IDs, Actuator metrics, Prometheus
- **Simplicity**: Single-node deployment, no caching, direct SSE (no Redis in v1)

### Principle VI: Clean Architecture & DDD
- **Status**: ✅ COMPLIANT
- **Layers**: Domain (Message entity, Repository interface) → Application (UseCases) → Infrastructure (Kafka, PostgreSQL, SSE) → Presentation (REST, CLI)

### Principle VII: Shadcn/UI MCP
- **Status**: ✅ COMPLIANT
- **Components**: Table, Badge, Card, ScrollArea, Skeleton (shadcn/ui)
- **Styling**: Tailwind CSS, dark mode, mobile-responsive
- **Accessibility**: WCAG 2.1 AA, keyboard navigation, screen reader support

### Initial Assessment
- **Blockers**: NEEDS CLARIFICATION on library-first approach and CLI framework
- **Proceed to Phase 0**: YES (resolve in research.md)

## Project Structure

See full directory tree in plan.md (too large for inline display).
Key structure:
- backend/ (Clean Architecture: domain/application/infrastructure/presentation)
- frontend/ (Next.js App Router, shadcn/ui components)
- k8s/ (Kubernetes manifests)

