package com.uw.hub.infrastructure.sse;

import com.uw.hub.domain.entity.CdcMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Service responsible for broadcasting CDC messages to all connected SSE clients.
 *
 * This component acts as a bridge between the Kafka consumer layer and the SSE presentation layer.
 *
 * Data Flow:
 * 1. Kafka consumer receives CDC message
 * 2. ConsumeMessageUseCase processes and stores message
 * 3. ConsumeMessageUseCase calls CdcMessageBroadcaster.broadcast()
 * 4. CdcMessageBroadcaster delegates to SseEmitterRegistry
 * 5. SseEmitterRegistry sends message to all connected clients
 *
 * Performance Considerations:
 * - Broadcasting is async and non-blocking
 * - Failed client emitters are automatically removed
 * - No backpressure handling (SSE is fire-and-forget)
 * - Messages are not queued if no clients connected
 *
 * Threading:
 * - This component is called from Kafka consumer thread
 * - SseEmitterRegistry handles thread-safe broadcasting
 * - No blocking operations in broadcast path
 *
 * Metrics & Monitoring:
 * - Logs broadcast events with client count
 * - Tracks success/failure rates
 * - Integration with Prometheus metrics (future)
 *
 * @see SseEmitterRegistry
 * @see com.uw.hub.application.usecase.ConsumeMessageUseCase
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CdcMessageBroadcaster {

    private final SseEmitterRegistry sseEmitterRegistry;

    /**
     * Broadcast a CDC message to all connected SSE clients.
     *
     * Message Format:
     * - Event name: "cdc-message"
     * - Event data: CdcMessage serialized to JSON
     * - Event ID: Auto-generated UUID
     *
     * Error Handling:
     * - Failed client emitters are automatically removed by SseEmitterRegistry
     * - No retry mechanism (SSE is fire-and-forget)
     * - Errors are logged but don't block message processing
     *
     * Performance:
     * - O(n) complexity where n = number of connected clients
     * - Non-blocking operation
     * - No message queuing if no clients connected
     *
     * Example SSE Event:
     * <pre>
     * event: cdc-message
     * id: 123e4567-e89b-12d3-a456-426614174000
     * data: {"id":"...","operation":"INSERT","table":{"database":"cdcdb",...},...}
     * </pre>
     *
     * @param cdcMessage The CDC message to broadcast
     * @return Number of clients who successfully received the message
     */
    public int broadcast(CdcMessage cdcMessage) {
        if (cdcMessage == null) {
            log.warn("Attempted to broadcast null CDC message - skipping");
            return 0;
        }

        // Check if any clients are connected
        if (!sseEmitterRegistry.hasClients()) {
            log.debug("No SSE clients connected - skipping broadcast for message: {}",
                    cdcMessage.getSummary());
            return 0;
        }

        log.debug("Broadcasting CDC message to {} clients: {}",
                sseEmitterRegistry.getClientCount(), cdcMessage.getSummary());

        try {
            // Delegate to SseEmitterRegistry for actual broadcasting
            int successCount = sseEmitterRegistry.broadcast("cdc-message", cdcMessage);

            log.debug("Successfully broadcast CDC message to {}/{} clients: {}",
                    successCount, sseEmitterRegistry.getClientCount(), cdcMessage.getSummary());

            return successCount;

        } catch (Exception e) {
            log.error("Error broadcasting CDC message: message={}, error={}",
                    cdcMessage.getSummary(), e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Broadcast a CDC message with custom event name (for future extensibility).
     *
     * Use cases:
     * - Different event types for different operations (INSERT/UPDATE/DELETE)
     * - Custom events for error messages or system notifications
     * - Table-specific event streams
     *
     * @param eventName Custom SSE event name
     * @param cdcMessage The CDC message to broadcast
     * @return Number of clients who successfully received the message
     */
    public int broadcast(String eventName, CdcMessage cdcMessage) {
        if (eventName == null || eventName.isBlank()) {
            log.warn("Invalid event name - using default 'cdc-message'");
            return broadcast(cdcMessage);
        }

        if (cdcMessage == null) {
            log.warn("Attempted to broadcast null CDC message - skipping");
            return 0;
        }

        if (!sseEmitterRegistry.hasClients()) {
            log.debug("No SSE clients connected - skipping broadcast for message: {}",
                    cdcMessage.getSummary());
            return 0;
        }

        log.debug("Broadcasting CDC message with event '{}' to {} clients: {}",
                eventName, sseEmitterRegistry.getClientCount(), cdcMessage.getSummary());

        try {
            int successCount = sseEmitterRegistry.broadcast(eventName, cdcMessage);

            log.debug("Successfully broadcast CDC message ({}) to {}/{} clients: {}",
                    eventName, successCount, sseEmitterRegistry.getClientCount(),
                    cdcMessage.getSummary());

            return successCount;

        } catch (Exception e) {
            log.error("Error broadcasting CDC message: eventName={}, message={}, error={}",
                    eventName, cdcMessage.getSummary(), e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Get number of currently connected SSE clients.
     * Useful for monitoring and conditional logic.
     *
     * @return Number of active SSE connections
     */
    public int getConnectedClientCount() {
        return sseEmitterRegistry.getClientCount();
    }

    /**
     * Check if any SSE clients are currently connected.
     * Useful for optimizing message processing (skip serialization if no clients).
     *
     * @return true if at least one client is connected
     */
    public boolean hasConnectedClients() {
        return sseEmitterRegistry.hasClients();
    }
}
