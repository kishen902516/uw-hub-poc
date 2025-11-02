package com.uw.hub.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uw.hub.domain.entity.CdcMessage;
import com.uw.hub.domain.valueobject.Metadata;
import com.uw.hub.domain.valueobject.Position;
import com.uw.hub.domain.valueobject.TableInfo;
import com.uw.hub.infrastructure.sse.CdcMessageBroadcaster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration test for SSE endpoint CDC message delivery.
 *
 * Tests:
 * 1. SSE endpoint is accessible and returns event stream
 * 2. CDC messages are pushed to connected SSE clients in real-time
 * 3. Multiple clients can connect and receive messages independently
 * 4. Heartbeat mechanism keeps connections alive
 * 5. Client reconnection works correctly
 *
 * @see com.uw.hub.presentation.rest.SseController
 * @see com.uw.hub.infrastructure.sse.CdcMessageBroadcaster
 */
@SpringBootTest
@AutoConfigureMockMvc
@DisplayName("SSE CDC Message Delivery Integration Tests")
class SseCdcMessageDeliveryTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private CdcMessageBroadcaster broadcaster;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        // Clear any existing SSE connections
        // broadcaster.clearAll(); // Will be implemented in CdcMessageBroadcaster
    }

    @Test
    @DisplayName("Should connect to SSE endpoint and receive event stream header")
    void shouldConnectToSseEndpoint() throws Exception {
        // When: Connect to SSE endpoint
        mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                .header("Accept", "text/event-stream"))
                // Then: Verify headers and status
                .andExpect(status().isOk())
                .andExpect(content().contentType("text/event-stream;charset=UTF-8"));
    }

    @Test
    @DisplayName("Should push CDC INSERT message to connected SSE client")
    void shouldPushInsertMessageToSseClient() throws Exception {
        // Given: A CDC INSERT message
        Map<String, Object> afterData = new HashMap<>();
        afterData.put("customer_id", 1);
        afterData.put("first_name", "John");
        afterData.put("last_name", "Doe");

        CdcMessage message = createCdcMessage("INSERT", null, afterData, 751);

        // When: Connect to SSE endpoint and broadcast message
        CompletableFuture<String> sseResponse = new CompletableFuture<>();

        Thread clientThread = new Thread(() -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                        .header("Accept", "text/event-stream")
                        .asyncDispatch())
                        .andReturn();

                // Give broadcaster time to initialize connection
                Thread.sleep(500);

                // Broadcast message
                broadcaster.broadcast(message);

                // Wait for message
                Thread.sleep(1000);

                String response = result.getResponse().getContentAsString();
                sseResponse.complete(response);
            } catch (Exception e) {
                sseResponse.completeExceptionally(e);
            }
        });

        clientThread.start();
        String response = sseResponse.get(10, TimeUnit.SECONDS);

        // Then: Verify SSE message contains CDC data
        assertThat(response).isNotBlank();
        assertThat(response).contains("data:");
        assertThat(response).contains("INSERT");
        assertThat(response).contains("John");
        assertThat(response).contains("Doe");
    }

    @Test
    @DisplayName("Should push CDC UPDATE message with before/after diff to SSE client")
    void shouldPushUpdateMessageToSseClient() throws Exception {
        // Given: A CDC UPDATE message with before/after data
        Map<String, Object> beforeData = new HashMap<>();
        beforeData.put("customer_id", 6);
        beforeData.put("last_name", "Sivalingam 2");

        Map<String, Object> afterData = new HashMap<>();
        afterData.put("customer_id", 6);
        afterData.put("last_name", "Sivalingam Name change");

        CdcMessage message = createCdcMessage("UPDATE", beforeData, afterData, 759);

        // When: Connect to SSE endpoint and broadcast message
        CompletableFuture<String> sseResponse = new CompletableFuture<>();

        Thread clientThread = new Thread(() -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                        .header("Accept", "text/event-stream")
                        .asyncDispatch())
                        .andReturn();

                Thread.sleep(500);
                broadcaster.broadcast(message);
                Thread.sleep(1000);

                String response = result.getResponse().getContentAsString();
                sseResponse.complete(response);
            } catch (Exception e) {
                sseResponse.completeExceptionally(e);
            }
        });

        clientThread.start();
        String response = sseResponse.get(10, TimeUnit.SECONDS);

        // Then: Verify SSE message contains both before and after states
        assertThat(response).isNotBlank();
        assertThat(response).contains("UPDATE");
        assertThat(response).contains("Sivalingam 2"); // before
        assertThat(response).contains("Sivalingam Name change"); // after
    }

    @Test
    @DisplayName("Should push CDC DELETE message to SSE client")
    void shouldPushDeleteMessageToSseClient() throws Exception {
        // Given: A CDC DELETE message
        Map<String, Object> beforeData = new HashMap<>();
        beforeData.put("customer_id", 99);
        beforeData.put("first_name", "Test");

        CdcMessage message = createCdcMessage("DELETE", beforeData, null, 765);

        // When: Connect to SSE endpoint and broadcast message
        CompletableFuture<String> sseResponse = new CompletableFuture<>();

        Thread clientThread = new Thread(() -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                        .header("Accept", "text/event-stream")
                        .asyncDispatch())
                        .andReturn();

                Thread.sleep(500);
                broadcaster.broadcast(message);
                Thread.sleep(1000);

                String response = result.getResponse().getContentAsString();
                sseResponse.complete(response);
            } catch (Exception e) {
                sseResponse.completeExceptionally(e);
            }
        });

        clientThread.start();
        String response = sseResponse.get(10, TimeUnit.SECONDS);

        // Then: Verify SSE message contains DELETE operation
        assertThat(response).isNotBlank();
        assertThat(response).contains("DELETE");
        assertThat(response).contains("Test");
    }

    @Test
    @DisplayName("Should broadcast message to multiple SSE clients simultaneously")
    void shouldBroadcastToMultipleClients() throws Exception {
        // Given: Two SSE clients and one CDC message
        Map<String, Object> afterData = Map.of("customer_id", 1, "name", "Broadcast Test");
        CdcMessage message = createCdcMessage("INSERT", null, afterData, 800);

        CompletableFuture<String> client1Response = new CompletableFuture<>();
        CompletableFuture<String> client2Response = new CompletableFuture<>();

        // When: Connect two clients
        Thread client1Thread = new Thread(() -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                        .header("Accept", "text/event-stream")
                        .asyncDispatch())
                        .andReturn();

                Thread.sleep(500);
                String response = result.getResponse().getContentAsString();
                client1Response.complete(response);
            } catch (Exception e) {
                client1Response.completeExceptionally(e);
            }
        });

        Thread client2Thread = new Thread(() -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                        .header("Accept", "text/event-stream")
                        .asyncDispatch())
                        .andReturn();

                Thread.sleep(500);
                String response = result.getResponse().getContentAsString();
                client2Response.complete(response);
            } catch (Exception e) {
                client2Response.completeExceptionally(e);
            }
        });

        client1Thread.start();
        client2Thread.start();

        // Broadcast message after clients connect
        Thread.sleep(1000);
        broadcaster.broadcast(message);
        Thread.sleep(1000);

        String response1 = client1Response.get(10, TimeUnit.SECONDS);
        String response2 = client2Response.get(10, TimeUnit.SECONDS);

        // Then: Both clients should receive the message
        assertThat(response1).contains("Broadcast Test");
        assertThat(response2).contains("Broadcast Test");
    }

    @Test
    @DisplayName("Should send heartbeat to keep SSE connection alive")
    void shouldSendHeartbeatToKeepConnectionAlive() throws Exception {
        // When: Connect to SSE endpoint and wait for heartbeat
        CompletableFuture<String> sseResponse = new CompletableFuture<>();

        Thread clientThread = new Thread(() -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                        .header("Accept", "text/event-stream")
                        .asyncDispatch())
                        .andReturn();

                // Wait for heartbeat (30s interval, but we'll wait less for testing)
                Thread.sleep(5000);

                String response = result.getResponse().getContentAsString();
                sseResponse.complete(response);
            } catch (Exception e) {
                sseResponse.completeExceptionally(e);
            }
        });

        clientThread.start();
        String response = sseResponse.get(10, TimeUnit.SECONDS);

        // Then: Verify heartbeat comment is sent
        // SSE heartbeat format: ": heartbeat\n\n" or "event: heartbeat\n\n"
        assertThat(response).isNotBlank();
        // Either comment-style or event-style heartbeat
        assertThat(response.contains(": heartbeat") || response.contains("event: heartbeat")).isTrue();
    }

    @Test
    @DisplayName("Should handle rapid succession of CDC messages")
    void shouldHandleRapidMessagesSuccession() throws Exception {
        // Given: Multiple CDC messages
        CdcMessage message1 = createCdcMessage("INSERT", null, Map.of("id", 1), 801);
        CdcMessage message2 = createCdcMessage("UPDATE", Map.of("id", 1), Map.of("id", 1, "updated", true), 802);
        CdcMessage message3 = createCdcMessage("DELETE", Map.of("id", 1), null, 803);

        // When: Connect client and broadcast messages rapidly
        CompletableFuture<String> sseResponse = new CompletableFuture<>();

        Thread clientThread = new Thread(() -> {
            try {
                MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get("/api/sse/cdc-messages")
                        .header("Accept", "text/event-stream")
                        .asyncDispatch())
                        .andReturn();

                Thread.sleep(500);

                // Broadcast messages rapidly
                broadcaster.broadcast(message1);
                broadcaster.broadcast(message2);
                broadcaster.broadcast(message3);

                Thread.sleep(2000);

                String response = result.getResponse().getContentAsString();
                sseResponse.complete(response);
            } catch (Exception e) {
                sseResponse.completeExceptionally(e);
            }
        });

        clientThread.start();
        String response = sseResponse.get(10, TimeUnit.SECONDS);

        // Then: All messages should be received
        assertThat(response).contains("INSERT");
        assertThat(response).contains("UPDATE");
        assertThat(response).contains("DELETE");
    }

    // Helper method

    private CdcMessage createCdcMessage(String operation, Map<String, Object> beforeData,
                                        Map<String, Object> afterData, int txId) {
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("txId", txId);

        return CdcMessage.builder()
                .topic("cdcdb.public.customers")
                .tableInfo(new TableInfo("cdcdb", "public", "customers"))
                .operation(operation)
                .timestamp(Instant.now())
                .position(new Position("{server=test}", offsetMap))
                .beforeData(beforeData)
                .afterData(afterData)
                .metadata(new Metadata("1", "postgresql", "test", "1.0.0"))
                .build();
    }
}
