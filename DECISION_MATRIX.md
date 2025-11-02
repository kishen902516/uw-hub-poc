# Decision Matrix: SSE + React + shadcn/ui Integration

**Purpose**: Quick reference for architectural decisions with trade-off analysis
**Date**: 2025-11-02

---

## 1. SSE Hook Pattern Decision

### Question
Should we use a custom `useSSE` hook or a third-party library like `react-hooks-sse`?

### Options Evaluated

#### Option A: Custom useSSE Hook ✅ RECOMMENDED

**Implementation**:
```typescript
const { status, reconnect } = useSSE({
  url: '/api/sse/messages',
  onMessage: (data) => store.addMessage(data),
  onStatusChange: (status) => store.setConnectionStatus(status),
});
```

**Pros**:
- Full control over reconnection logic
- Type-safe with full TypeScript support
- Minimal bundle size (~2KB)
- Zero external dependencies
- Easy to customize for edge cases
- Clear separation of concerns
- Can implement exponential backoff easily

**Cons**:
- Need to maintain hook code
- No pre-built edge case handling

**Bundle Impact**: +2KB
**Implementation Time**: 2-3 hours
**Maintenance Burden**: Low (simple, stable API)

---

#### Option B: react-hooks-sse Library ❌ REJECTED

**Implementation**:
```typescript
const { data, isLoading, error } = useSSE(url);
```

**Pros**:
- Drop-in hook abstraction
- Some error handling included
- Established library

**Cons**:
- Less control over reconnection
- Bundle size 5-10KB (2.5-5x larger)
- Limited customization
- TypeScript support inconsistent
- Smaller community/updates
- Hidden complexity

**Decision Rationale**:
Custom hook provides better control + type safety + performance without significant additional complexity. Control is essential for auto-reconnect and error handling in production systems.

---

#### Option C: use-sse NPM Package ❌ REJECTED

**Pros**: Lightweight alternative
**Cons**: Even less community adoption, unmaintained

---

## 2. State Management Decision

### Question
Which state management solution is best for managing SSE messages and connection state?

### Options Evaluated

#### Option A: Zustand ✅ RECOMMENDED

**Store Structure**:
```typescript
const useMessageStore = create((set, get) => ({
  messages: [],
  filter: { topic: '' },
  connectionStatus: 'disconnected',
  announcementText: '',
  addMessage: (msg) => set((state) => ({
    messages: [msg, ...state.messages].slice(0, 10000),
  })),
}));

// Selector hooks prevent unnecessary re-renders
export const useMessages = () => useMessageStore((state) => state.messages);
export const useConnectionStatus = () => useMessageStore((state) => state.connectionStatus);
```

**Pros**:
- 3KB bundle (smallest viable option)
- Hook-based API (familiar to React devs)
- Granular subscriptions via selectors
- Minimal re-renders
- Built-in devtools
- Easy to test
- Middleware support (logging, persistence)
- No provider wrapper needed

**Cons**:
- Single store (though can be split)
- Requires selector discipline

**Bundle Impact**: +3KB
**Learning Curve**: Low (very simple API)
**Performance**: Excellent (fine-grained subscriptions)
**Scale**: 10k-100k messages (beyond this, pagination recommended)

---

#### Option B: Context API + useReducer ❌ REJECTED

**Pros**: Built-in, no dependencies
**Cons**:
- Full tree re-render on each message (60fps at 100+ messages/sec is impossible)
- Not designed for frequently-updating state
- Provider wrapping boilerplate
- Difficult to optimize without useMemo

**Performance Impact**: ❌ Unacceptable for SSE

---

#### Option C: Jotai ❌ REJECTED

**Pros**: Atomic state, React hooks API
**Cons**:
- Overkill for simple message array
- Atom composition learning curve
- 8KB+ bundle
- More re-renders than Zustand for this use case

**Recommendation**: Use Jotai only if complex interdependent state needed (v2+)

---

#### Option D: Redux Toolkit ❌ REJECTED

**Pros**: Powerful, widely known
**Cons**:
- Massive boilerplate for simple SSE state
- Actions, reducers, selectors, types
- 40KB+ bundle
- Overkill for this use case

**Recommendation**: Use only if team already deeply familiar and using for other features

---

## 3. Virtualization Library Decision

### Question
How to render 10k messages while maintaining 60fps and accessibility?

### Options Evaluated

#### Option A: React Virtuoso ✅ RECOMMENDED

**Implementation**:
```typescript
<Virtuoso
  style={{ height: '600px' }}
  data={messages}
  itemContent={(index, message) => (
    <TableRow>{/* message cells */}</TableRow>
  )}
  role="grid"
  aria-label="Message stream"
  aria-rowcount={messages.length}
/>
```

**Pros**:
- WCAG 2.1 AA accessible by default
- Native table integration
- Sticky header support
- Auto-sizing rows
- 25KB bundle
- Excellent documentation
- Production-tested
- Auto-scroll utilities
- Handles 10k+ items at 60fps

**Cons**:
- Slightly heavier than TanStack Virtual
- Less micro-optimization control

**Performance Metrics**:
- Initial render: 40-80ms
- Scroll: 60fps (modern hardware)
- Memory: ~8MB for 10k items
- Accessibility: WCAG 2.1 AA compliant

**Bundle Impact**: +25KB
**Learning Curve**: Low (simple component API)
**Accessibility**: Built-in ✅

---

#### Option B: TanStack Virtual (react-virtual) ⚠️ CONSIDERED

**Implementation**:
```typescript
const rowVirtualizer = useVirtualizer({
  count: messages.length,
  getScrollElement: () => parentRef.current,
  estimateSize: () => 44, // row height
});
```

**Pros**:
- Lightweight: 8KB
- Maximum control
- Flexible (works with anything)

**Cons**:
- No built-in accessibility
- Requires manual ARIA configuration
- No native table support
- More complex setup
- Row height calculation manual

**Accessibility**: Requires significant custom ARIA work ⚠️
**Recommendation**: Use only if bundle size critical AND accessibility handled separately

---

#### Option C: React Virtuoso + TanStack React Table ✅ COMPATIBLE

**Advanced Pattern** (if advanced sorting/filtering needed):
```typescript
// React Virtuoso for virtualization
// TanStack React Table for sort/filter/column logic
// Both work together seamlessly
```

---

#### Option D: Server-Side Pagination ❌ REJECTED

**Pros**: No client virtualization needed
**Cons**:
- Doesn't meet UX requirement: "display up to 10k messages"
- Extra API calls
- Network latency on pagination
- Worse UX (users want to scroll through all messages)

---

## 4. Accessibility Strategy Decision

### Question
How to properly announce SSE updates to screen reader users?

### Options Evaluated

#### Option A: ARIA Live Regions with Zustand ✅ RECOMMENDED

**Implementation**:
```typescript
// 1. Live region in DOM from page load
<div
  role="status"
  aria-live="polite"
  aria-atomic="true"
  className="sr-only"
>
  {announcementText}
</div>

// 2. Zustand triggers announcements
const store = create((set) => ({
  announcementText: '',
  addMessage: (msg) => {
    set({
      announcementText: `New message: ${msg.topic}: ${msg.value.substring(0, 50)}`,
    });
    setTimeout(() => set({ announcementText: '' }), 1500);
  },
}));
```

**Pros**:
- WCAG 2.1 AA compliant
- Screen readers automatically announce
- `aria-live="polite"` doesn't interrupt user
- Works with all major screen readers (NVDA, JAWS, VoiceOver)
- Semantic and standards-based
- Low performance overhead

**Cons**:
- Requires live region in DOM from page load
- Easy to break if not careful (hidden with display:none)

**Testing**: NVDA (Windows), VoiceOver (Mac)
**Compliance**: WCAG 2.1 AA ✅

---

#### Option B: ARIA announcer library (aria-announcer) ❌ REJECTED

**Pros**: Abstraction over live regions
**Cons**:
- Extra dependency
- Adds complexity
- Custom hook makes it unnecessary

---

#### Option C: No accessibility support ❌ REJECTED

This is unacceptable for WCAG 2.1 AA compliance.

---

## 5. Auto-Scroll Behavior Decision

### Question
When new messages arrive, should the table auto-scroll to bottom? How to not disrupt user reading?

### Options Evaluated

#### Option A: Smart Auto-Scroll with Intersection Observer ✅ RECOMMENDED

**Implementation**:
```typescript
import { useInView } from 'react-intersection-observer';

const MessageStream = () => {
  const { ref: bottomRef, inView: isAtBottom } = useInView();

  // Auto-scroll only if user at bottom
  useEffect(() => {
    if (isAtBottom && latestMessageRef.current) {
      latestMessageRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [isAtBottom, messages.length]);

  return (
    <Virtuoso
      data={messages}
      // ...
      atBottomStateChange={(isAt) => {
        // Zustand triggers auto-scroll logic
      }}
    />
  );
};
```

**Pros**:
- Respects user intent (if scrolled up, don't auto-scroll)
- Smooth scrolling doesn't jank
- Accessibility-friendly
- Works with Virtuoso natively

**Cons**:
- Requires Intersection Observer (browser API support excellent)

**UX Pattern**:
- User at bottom: Auto-scroll to new messages ✅
- User scrolled up reading history: Stay in place (don't auto-scroll) ✅
- Shows subtle notification "N new messages" badge if not at bottom

**Browser Support**: All modern browsers ✅

---

#### Option B: Always Auto-Scroll ❌ REJECTED

**Cons**:
- Disruptive when user reading old messages
- Poor UX
- Frustrating for accessibility users

---

#### Option C: Scroll Position Tracking ⚠️ CONSIDERED

Keep scroll position stable as new messages arrive.
**Complexity**: Medium (requires Virtuoso hooks)
**UX**: Good

---

## 6. Message Batching Decision

### Question
Should SSE send one message per event or batch multiple messages?

### Options Evaluated

#### Option A: One Message Per Event ✅ RECOMMENDED (v1)

**Implementation**: Each Kafka message → one SSE event
```
data: { id: "msg-1", timestamp: "...", topic: "...", value: "..." }\n\n
data: { id: "msg-2", timestamp: "...", topic: "...", value: "..." }\n\n
```

**Pros**:
- Simplest implementation
- Lowest latency (message → UI immediately)
- Matches Kafka 1-to-1

**Cons**:
- HTTP frame overhead per message
- At 1000 msg/sec: 1000 events/sec

**Recommendation**: Start with this; optimize if bandwidth becomes issue

---

#### Option B: Batch Messages (v2) ⚠️ FUTURE

Batch 10 messages per SSE event:
```
data: [
  { id: "msg-1", ... },
  { id: "msg-2", ... },
  ...
  { id: "msg-10", ... }
]\n\n
```

**Pros**:
- Reduced HTTP overhead
- Better for high-throughput scenarios

**Cons**:
- Slightly higher latency (+5-10ms)
- More complex parsing

**Recommendation**: Implement in v2 if bandwidth concerns arise

---

## 7. Backend Integration Decision

### Question
Should Next.js SSE route connect to Spring Boot backend directly or use intermediate layer?

### Options Evaluated

#### Option A: Direct Connection ✅ RECOMMENDED (v1)

```
Browser → Next.js Route Handler → Spring Boot SSE Endpoint
```

**Pros**:
- Simplest architecture
- Single connection per client
- Fewest moving parts

**Cons**:
- Must run same number of Spring Boot SSE connections as clients

---

#### Option B: Redis Pub/Sub Broker (v2) ⚠️ FUTURE

```
Browser → Next.js Route Handler → Redis Pub/Sub ← Spring Boot
```

**Pros**:
- Decouples backend from client count
- Enables multiple app instances
- Clients can reconnect to different Next.js instance

**Cons**:
- Extra infrastructure (Redis)
- Added latency (+20-50ms)
- Complexity

**Recommendation**: Implement when scaling beyond single backend instance

---

## Summary Decision Table

| Decision | Recommended | Bundle | Performance | Accessibility | Maintenance |
|----------|------------|--------|-------------|----------------|------------|
| SSE Hook | Custom useSSE | 2KB | Excellent | Built-in | Low |
| State Mgmt | Zustand | 3KB | Excellent | Via hooks | Low |
| Virtualization | React Virtuoso | 25KB | 60fps (10k items) | WCAG AA ✅ | Low |
| Accessibility | ARIA Live Regions | 0KB | Excellent | WCAG AA ✅ | Low |
| Auto-Scroll | Intersection Observer | 0KB | Smooth | Accessible | Medium |
| Message Pattern | One per event | N/A | <100ms latency | N/A | Simple |
| Backend | Direct Spring Boot | N/A | Optimal (v1) | N/A | Simple |

**Total Bundle Overhead**: 30KB (reasonable for critical feature)

---

## Risk Assessment

### High Confidence ✅
- Custom useSSE hook (simple EventSource API)
- Zustand for state (well-established pattern)
- React Virtuoso (production-tested, accessible)

### Medium Confidence ⚠️
- Scaling to 100+ concurrent SSE connections (may need Redis v2)
- ARIA announcements timing (requires testing)

### Low Risk ✅
- All decisions align with WCAG 2.1 AA
- No breaking changes to shadcn/ui
- No performance bottlenecks identified

---

## Implementation Timeline

| Phase | Component | Effort | Risk | Dependency |
|-------|-----------|--------|------|------------|
| 1 | useSSE hook | 2-3h | Low | None |
| 2 | Zustand store | 1-2h | Low | Phase 1 |
| 3 | MessageStream + Virtuoso | 3-4h | Low | Phase 2 |
| 4 | ARIA live regions | 1-2h | Medium | Phase 2 |
| 5 | Testing (10k messages) | 2-3h | Low | Phase 3-4 |
| **Total** | **Full Implementation** | **9-14h** | **Low** | **Sequential** |

---

## Appendix: Considered But Not Included

| Idea | Reason for Rejection |
|------|---------------------|
| Socket.io | Overkill; SSE sufficient for one-way streaming |
| GraphQL Subscriptions | Adds complexity; REST + SSE simpler |
| Service Workers + Push API | Different use case; better for notifications |
| React Query for state | Good for server state; Zustand better for SSE |
| CSS-in-JS | Tailwind already sufficient with shadcn/ui |

---

**Decision Authority**: Approved for Implementation
**Review Date**: 2025-11-02
**Next Review**: After Phase 1 implementation completion
