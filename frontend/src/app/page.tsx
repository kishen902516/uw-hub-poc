'use client';

import { CdcMessageStream } from '@/components/CdcMessageStream';

/**
 * Home Page
 *
 * Main application page displaying the CDC Message Stream component.
 *
 * Features:
 * - Real-time CDC message monitoring from Kafka topics
 * - Server-Sent Events (SSE) for live updates
 * - Virtualized table display (10k+ messages support)
 * - Connection status indicator
 * - WCAG 2.1 AA accessibility compliant
 *
 * User Stories Implemented:
 * - US1: Real-Time CDC Message Monitoring
 *   - View CDC messages (INSERT/UPDATE/DELETE) from Kafka topics in real-time
 *   - Operation type badges, table info, before/after states
 *   - <100ms SSE latency for updates
 */
export default function Home() {
  return (
    <main className="container mx-auto py-8 px-4">
      <div className="space-y-6">
        {/* Page Header */}
        <div className="space-y-2">
          <h1 className="text-3xl font-bold tracking-tight">CDC Message Stream</h1>
          <p className="text-muted-foreground">
            Real-time monitoring of Change Data Capture (CDC) messages from Kafka topics
          </p>
        </div>

        {/* Main Content: CDC Message Stream */}
        <CdcMessageStream />

        {/* Footer Info */}
        <div className="text-xs text-muted-foreground text-center pt-4 border-t">
          <p>
            Displaying CDC messages from Kafka with INSERT (green), UPDATE (blue), and DELETE (red) operations.
          </p>
          <p className="mt-1">
            Messages are streamed in real-time via Server-Sent Events (SSE) with automatic reconnection.
          </p>
        </div>
      </div>
    </main>
  );
}
