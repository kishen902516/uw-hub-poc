package com.uw.hub.infrastructure.sse;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Registry for managing multiple SSE (Server-Sent Events) client connections.
 *
 * Responsibilities:
 * 1. Track active SSE emitters (one per connected client)
 * 2. Handle client connect/disconnect lifecycle
 * 3. Send heartbeat messages to keep connections alive (30s interval)
 * 4. Provide metrics on connected clients
 * 5. Thread-safe operations for concurrent access
 *
 * SSE Connection Lifecycle:
 * 1. Client connects → Create SseEmitter → Register in map
 * 2. Send initial welcome message
 * 3. Periodic heartbeat (every 30s)
 * 4. On client disconnect/timeout/error → Remove from map
 *
 * Heartbeat Strategy:
 * - Prevents proxy/firewall from closing idle connections
 * - Format: comment-style SSE message (": heartbeat\n\n")
 * - Interval: 30 seconds (configurable)
 *
 * Thread Safety:
 * - Uses ConcurrentHashMap for thread-safe emitter storage
 * - Multiple threads can broadcast messages simultaneously
 *
 * @see SseEmitter
 * @see CdcMessageBroadcaster
 */
@Component
@Slf4j
public class SseEmitterRegistry {

    /**
     * Map of emitter ID → SseEmitter
     * ConcurrentHashMap ensures thread-safe access
     */
    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();

    /**
     * Heartbeat scheduler to keep connections alive
     */
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

    /**
     * SSE emitter timeout: 1 hour (in milliseconds)
     * After this time, connection is automatically closed if no data sent
     */
    private static final long SSE_TIMEOUT = 60 * 60 * 1000L; // 1 hour

    /**
     * Heartbeat interval: 30 seconds (in milliseconds)
     */
    private static final long HEARTBEAT_INTERVAL_MS = 30_000L;

    /**
     * Constructor - starts heartbeat scheduler
     */
    public SseEmitterRegistry() {
        startHeartbeatScheduler();
        log.info("SseEmitterRegistry initialized with heartbeat interval: {}s", HEARTBEAT_INTERVAL_MS / 1000);
    }

    /**
     * Register a new SSE emitter for a connected client.
     *
     * Lifecycle callbacks:
     * - onCompletion: Normal disconnection (client closes connection)
     * - onTimeout: Connection timeout (no data sent within timeout period)
     * - onError: Error during transmission
     *
     * @return Configured SseEmitter with registered callbacks
     */
    public SseEmitter createEmitter() {
        String emitterId = UUID.randomUUID().toString();
        SseEmitter emitter = new SseEmitter(SSE_TIMEOUT);

        // Register lifecycle callbacks
        emitter.onCompletion(() -> {
            log.info("SSE emitter completed: emitterId={}", emitterId);
            removeEmitter(emitterId);
        });

        emitter.onTimeout(() -> {
            log.warn("SSE emitter timed out: emitterId={}", emitterId);
            removeEmitter(emitterId);
        });

        emitter.onError((exception) -> {
            log.error("SSE emitter error: emitterId={}, error={}", emitterId, exception.getMessage());
            removeEmitter(emitterId);
        });

        // Add to registry
        emitters.put(emitterId, emitter);
        log.info("SSE emitter registered: emitterId={}, totalClients={}", emitterId, emitters.size());

        // Send welcome message
        sendWelcomeMessage(emitter, emitterId);

        return emitter;
    }

    /**
     * Remove emitter from registry.
     */
    private void removeEmitter(String emitterId) {
        emitters.remove(emitterId);
        log.info("SSE emitter removed: emitterId={}, remainingClients={}", emitterId, emitters.size());
    }

    /**
     * Send welcome message to newly connected client.
     */
    private void sendWelcomeMessage(SseEmitter emitter, String emitterId) {
        try {
            SseEmitter.SseEventBuilder event = SseEmitter.event()
                    .name("connected")
                    .data("Connected to CDC message stream")
                    .id(emitterId);

            emitter.send(event);
            log.debug("Sent welcome message to emitter: {}", emitterId);

        } catch (IOException e) {
            log.error("Failed to send welcome message: emitterId={}, error={}", emitterId, e.getMessage());
            removeEmitter(emitterId);
        }
    }

    /**
     * Broadcast message to all connected clients.
     *
     * @param eventName SSE event name (e.g., "cdc-message")
     * @param data Message payload (will be serialized to JSON)
     * @return Number of clients who successfully received the message
     */
    public int broadcast(String eventName, Object data) {
        if (emitters.isEmpty()) {
            log.debug("No SSE clients connected - skipping broadcast");
            return 0;
        }

        log.debug("Broadcasting SSE event to {} clients: eventName={}", emitters.size(), eventName);

        int successCount = 0;
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            String emitterId = entry.getKey();
            SseEmitter emitter = entry.getValue();

            try {
                SseEmitter.SseEventBuilder event = SseEmitter.event()
                        .name(eventName)
                        .data(data)
                        .id(UUID.randomUUID().toString());

                emitter.send(event);
                successCount++;

            } catch (IOException e) {
                log.error("Failed to send SSE event to client: emitterId={}, eventName={}, error={}",
                        emitterId, eventName, e.getMessage());
                removeEmitter(emitterId);
            }
        }

        log.debug("Broadcasted SSE event to {}/{} clients: eventName={}", successCount, emitters.size(), eventName);
        return successCount;
    }

    /**
     * Start heartbeat scheduler to send periodic heartbeat messages.
     * Keeps connections alive and prevents timeouts from proxies/firewalls.
     */
    private void startHeartbeatScheduler() {
        heartbeatScheduler.scheduleAtFixedRate(
                this::sendHeartbeatToAllClients,
                HEARTBEAT_INTERVAL_MS,
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        log.info("Heartbeat scheduler started: interval={}s", HEARTBEAT_INTERVAL_MS / 1000);
    }

    /**
     * Send heartbeat to all connected clients.
     * Uses SSE comment format (": heartbeat\n\n") which is ignored by clients
     * but keeps the connection alive.
     */
    private void sendHeartbeatToAllClients() {
        if (emitters.isEmpty()) {
            return; // No clients to send heartbeat to
        }

        log.debug("Sending heartbeat to {} SSE clients", emitters.size());

        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            String emitterId = entry.getKey();
            SseEmitter emitter = entry.getValue();

            try {
                // Send comment-style heartbeat (invisible to clients)
                SseEmitter.SseEventBuilder heartbeat = SseEmitter.event()
                        .comment("heartbeat");

                emitter.send(heartbeat);

            } catch (IOException e) {
                log.warn("Failed to send heartbeat to client: emitterId={}, error={}", emitterId, e.getMessage());
                removeEmitter(emitterId);
            }
        }
    }

    /**
     * Get number of currently connected SSE clients.
     *
     * @return Client count
     */
    public int getClientCount() {
        return emitters.size();
    }

    /**
     * Check if any SSE clients are connected.
     *
     * @return true if at least one client is connected
     */
    public boolean hasClients() {
        return !emitters.isEmpty();
    }

    /**
     * Remove all emitters (used for cleanup/testing).
     */
    public void clearAll() {
        log.info("Clearing all SSE emitters: count={}", emitters.size());
        emitters.forEach((id, emitter) -> {
            try {
                emitter.complete();
            } catch (Exception e) {
                log.warn("Error completing emitter during clearAll: {}", e.getMessage());
            }
        });
        emitters.clear();
    }

    /**
     * Shutdown heartbeat scheduler (called on application shutdown).
     */
    public void shutdown() {
        log.info("Shutting down SSE emitter registry and heartbeat scheduler");
        heartbeatScheduler.shutdown();
        clearAll();
    }

    /**
     * Get registry metrics for monitoring.
     */
    public RegistryMetrics getMetrics() {
        return new RegistryMetrics(
                emitters.size(),
                HEARTBEAT_INTERVAL_MS,
                SSE_TIMEOUT
        );
    }

    /**
     * Metrics record for SSE registry.
     */
    public record RegistryMetrics(
        int connectedClients,
        long heartbeatIntervalMs,
        long timeoutMs
    ) {}
}
