# Research Summary: Real-Time SSE Updates with React, shadcn/ui & Next.js 14+

**Date**: 2025-11-02
**Status**: ✅ Complete
**Location**: `/specs/001-kafka-streaming-ui/research.md`

---

## Quick Reference

### 1. DECISION: Recommended Stack

| Component | Recommendation | Reason |
|-----------|-----------------|--------|
| **SSE Hook** | Custom `useSSE` hook | Type-safe, ~2KB, full control over reconnection logic |
| **State Management** | Zustand | 3KB bundle, fine-grained subscriptions, minimal boilerplate |
| **Virtualization** | React Virtuoso | Accessible, shadcn/ui compatible, 10k+ items support |
| **Accessibility** | ARIA Live Regions | WCAG 2.1 AA compliant, screen reader announcements |

### 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                    Next.js 14 Frontend                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────────────────────────────────────┐  │
│  │     MessageStream Component (shadcn/ui)          │  │
│  │  ┌─────────────────────────────────────────────┐ │  │
│  │  │  React Virtuoso Table                       │ │  │
│  │  │  - Only ~20 rows rendered (10k items total) │ │  │
│  │  │  - 60fps scrolling, <8MB memory             │ │  │
│  │  └─────────────────────────────────────────────┘ │  │
│  │                                                 │  │
│  │  ┌─────────────────────────────────────────────┐ │  │
│  │  │  ARIA Live Region (sr-only)                │ │  │
│  │  │  "New message: topic billing"              │ │  │
│  │  │  Announces to screen readers               │ │  │
│  │  └─────────────────────────────────────────────┘ │  │
│  └──────────────────────────────────────────────────┘  │
│                        ▲                               │
│  ┌────────────────────┼──────────────────────────────┐ │
│  │   useSSE Hook      │    Zustand Store             │ │
│  │  ┌─────────────┐   │  ┌──────────────────────┐   │ │
│  │  │ EventSource │───┤  │ messages[]           │   │ │
│  │  │ Auto-Reconnect  │  │ connectionStatus     │   │ │
│  │  │ Exponential │   │  │ announcementText     │   │ │
│  │  │ Backoff     │   │  │ filter               │   │ │
│  │  └─────────────┘   │  └──────────────────────┘   │ │
│  └────────────────────┼──────────────────────────────┘ │
│                        │                               │
└────────────────────────┼───────────────────────────────┘
                         │ SSE Streaming
                         │ (EventSource)
                         │
            ┌────────────▼──────────────┐
            │  Next.js API Route        │
            │ /api/sse/messages         │
            │ - Authentication (JWT)    │
            │ - Headers (keep-alive)    │
            │ - TransformStream/Events  │
            └────────────┬──────────────┘
                         │
            ┌────────────▼──────────────┐
            │  Spring Boot Backend      │
            │ - Kafka Consumer          │
            │ - PostgreSQL Write        │
            │ - Message Broadcast       │
            └──────────────────────────┘
```

### 3. Performance Targets

| Metric | Target | Status |
|--------|--------|--------|
| SSE Latency (network → DB) | <100ms | Network-dependent |
| UI Update Latency | ~10-20ms | Custom hook + Zustand |
| Table Render (10k items) | <80ms | React Virtuoso |
| Scroll Frame Rate | 60fps | Virtualization ensures |
| Memory (10k messages) | <15MB | Efficient rendering |
| Auto-reconnect Delay | <8s | Exponential backoff |
| WCAG 2.1 AA Compliance | 100% | ARIA live regions |

### 4. Key Implementation Details

#### Custom useSSE Hook
```typescript
// Returns: { status, reconnect, disconnect }
// Auto-connects on mount
// Auto-reconnects with exponential backoff (500ms → 8s)
// Parses JSON events
// Separate error handling
```

#### Zustand Store
```typescript
// Optimized subscriptions via selector hooks
// useMessages() → only messages
// useConnectionStatus() → only status
// useAnnouncement() → only announcements
// Prevents unnecessary re-renders
```

#### ARIA Live Regions
```typescript
// <div aria-live="polite" aria-atomic="true">
// "New message: topic=billing, 50 chars preview"
// Auto-clears after 1.5s
// Screen readers announce without interrupting
```

#### React Virtuoso Integration
```typescript
// <Virtuoso data={messages} itemContent={(index, msg) => ()} />
// Only renders visible rows (~20 at a time)
// Sticky headers support
// Accessibility built-in
```

### 5. Alternatives Considered & Rejected

| Approach | Why Not | Trade-off |
|----------|---------|-----------|
| react-hooks-sse library | Library adds complexity, less control | Simpler setup vs. custom hook |
| Context API alone | Causes full tree re-render per message | Simple vs. performant |
| Jotai | Atomic state model overkill for arrays | Complex vs. straightforward |
| TanStack Virtual | 8KB but needs manual ARIA config | Tiny bundle vs. accessibility |
| Redis Pub/Sub | Extra infrastructure for v1 | Scalable vs. simple |
| WebSockets | Firewall issues, bidirectional overhead | Fully interactive vs. one-way stream |

### 6. Accessibility (WCAG 2.1 AA)

✅ **Live Regions**: Announces new messages to screen readers
✅ **Semantic HTML**: Proper table structure with headers/bodies
✅ **Keyboard Navigation**: Tab, Arrow keys, Home, End supported
✅ **ARIA Labels**: Roles, labels, counts for screen readers
✅ **Status Announcements**: Connection status announced
✅ **Focus Management**: Logical tab order maintained

**Testing**: NVDA (Windows), VoiceOver (Mac), axe DevTools

### 7. Code Structure

```
app/
├── api/
│   └── sse/
│       └── messages/
│           └── route.ts          # SSE endpoint
├── components/
│   ├── MessageStream.tsx         # Main virtualized table
│   ├── TopicFilter.tsx           # Filter UI
│   └── ui/                        # shadcn/ui components
├── hooks/
│   └── useSSE.ts                 # Custom SSE hook
├── store/
│   └── messageStore.ts           # Zustand state
└── page.tsx                       # Home page

types/
└── message.ts                     # Message interface
```

### 8. Setup Instructions

```bash
# 1. Install dependencies
npm install zustand react-virtuoso react-intersection-observer

# 2. Add shadcn/ui components
npx shadcn-ui@latest add table badge card scroll-area skeleton

# 3. Copy code from research.md sections 5.1-5.5
# - useSSE hook
# - Zustand store
# - MessageStream component
# - API route
# - Home page

# 4. Test SSE endpoint
curl -H "Authorization: Bearer YOUR_TOKEN" \
  http://localhost:3000/api/sse/messages

# 5. Verify accessibility
# Open in browser with NVDA or VoiceOver enabled
```

### 9. Next Steps (Phases)

1. **Phase 1**: Frontend setup + custom hook implementation
2. **Phase 2**: Zustand store + MessageStream component
3. **Phase 3**: React Virtuoso virtualization + shadcn/ui integration
4. **Phase 4**: ARIA live regions + accessibility testing
5. **Phase 5**: Performance testing + load test (10k messages)
6. **Phase 6**: Documentation + deployment

### 10. Success Criteria

- [x] Research complete and documented
- [ ] Custom useSSE hook tested with mock events
- [ ] Zustand store with proper selectors
- [ ] MessageStream renders 10k messages at 60fps
- [ ] ARIA announcements verified with screen reader
- [ ] SSE latency <100ms (network dependent)
- [ ] Bundle size <250KB
- [ ] WCAG 2.1 AA audit passes

---

## Detailed Research Document

For complete analysis including:
- Detailed rationale for each decision
- Code examples and pseudocode
- Implementation checklist
- Performance benchmarks
- Reference links

👉 **See**: `/specs/001-kafka-streaming-ui/research.md`

---

## Questions?

Refer to the research document's "Open Questions & Next Steps" section or the README for each implementation phase.

**Status**: ✅ Ready for Phase 1 implementation
**Owner**: Development Team
**Last Updated**: 2025-11-02
