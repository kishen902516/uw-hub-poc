package com.uw.hub.presentation.rest;

import com.uw.hub.infrastructure.sse.SseEmitterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * REST Controller for Server-Sent Events (SSE) endpoints.
 *
 * Provides real-time streaming of CDC messages from Kafka to browser clients via SSE.
 *
 * SSE vs WebSocket:
 * - SSE is simpler for one-way server-to-client streaming
 * - Works over HTTP (no protocol upgrade needed)
 * - Better compatibility with proxies/firewalls
 * - Auto-reconnect built into browser EventSource API
 *
 * Endpoint Lifecycle:
 * 1. Client connects to /api/sse/cdc-messages
 * 2. SseEmitter created and registered in SseEmitterRegistry
 * 3. Client receives welcome message
 * 4. CdcMessageBroadcaster sends new messages as they arrive from Kafka
 * 5. Heartbeat sent every 30s to keep connection alive
 * 6. On disconnect/timeout/error, emitter is removed from registry
 *
 * Performance Considerations:
 * - Supports up to 100 concurrent SSE clients (configurable)
 * - Each emitter has 1-hour timeout
 * - Heartbeat prevents proxy/firewall timeout
 * - Async message broadcasting (non-blocking)
 *
 * Security:
 * - CORS configured in WebMvcConfig
 * - Authentication/authorization handled by Spring Security filters (if enabled)
 *
 * @see SseEmitterRegistry
 * @see com.uw.hub.infrastructure.sse.CdcMessageBroadcaster
 */
@RestController
@RequestMapping("/api/sse")
@RequiredArgsConstructor
@Slf4j
public class SseController {

    private final SseEmitterRegistry sseEmitterRegistry;

    /**
     * SSE endpoint for real-time CDC message streaming.
     *
     * Client Usage:
     * <pre>
     * const eventSource = new EventSource('/api/sse/cdc-messages');
     *
     * eventSource.addEventListener('connected', (event) => {
     *   console.log('Connected:', event.data);
     * });
     *
     * eventSource.addEventListener('cdc-message', (event) => {
     *   const message = JSON.parse(event.data);
     *   console.log('New CDC message:', message);
     * });
     *
     * eventSource.onerror = (error) => {
     *   console.error('SSE error:', error);
     * };
     * </pre>
     *
     * SSE Message Format:
     * <pre>
     * event: cdc-message
     * id: 123e4567-e89b-12d3-a456-426614174000
     * data: {"id":"...", "operation":"INSERT", "table":{"database":"cdcdb","schema":"public","table":"customers"}, ...}
     * </pre>
     *
     * Error Handling:
     * - Client auto-reconnects on connection loss (EventSource built-in)
     * - Server removes dead emitters automatically
     * - Heartbeat prevents idle connection timeout
     *
     * @return SseEmitter for streaming CDC messages
     */
    @GetMapping(value = "/cdc-messages", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamCdcMessages() {
        log.info("New SSE client connecting to /api/sse/cdc-messages");

        try {
            // Create and register new SSE emitter
            SseEmitter emitter = sseEmitterRegistry.createEmitter();

            log.info("SSE client successfully connected. Total active clients: {}",
                    sseEmitterRegistry.getClientCount());

            return emitter;

        } catch (Exception e) {
            log.error("Failed to create SSE emitter for client", e);
            throw new RuntimeException("Failed to establish SSE connection", e);
        }
    }

    /**
     * Get SSE registry metrics (for monitoring/debugging).
     *
     * Returns:
     * - connectedClients: Number of active SSE connections
     * - heartbeatIntervalMs: Heartbeat interval in milliseconds
     * - timeoutMs: SSE emitter timeout in milliseconds
     *
     * @return Registry metrics
     */
    @GetMapping("/metrics")
    public ResponseEntity<SseEmitterRegistry.RegistryMetrics> getMetrics() {
        SseEmitterRegistry.RegistryMetrics metrics = sseEmitterRegistry.getMetrics();
        log.debug("SSE metrics requested: connectedClients={}", metrics.connectedClients());
        return ResponseEntity.ok(metrics);
    }
}
