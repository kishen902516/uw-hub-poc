import { useEffect, useRef, useState, useCallback } from "react";
import { ConnectionStatus } from "@/types/cdc";

/**
 * Options for useSSE hook
 */
export interface UseSSEOptions {
  url: string;
  retryCount?: number;
  maxRetryDelay?: number;
  onMessage: (data: any) => void;
  onError?: (error: Event) => void;
  onStatusChange?: (status: ConnectionStatus) => void;
}

/**
 * Custom hook for Server-Sent Events (SSE) with auto-reconnect
 * Implements exponential backoff for reconnection attempts
 */
export const useSSE = ({
  url,
  retryCount = 5,
  maxRetryDelay = 8000,
  onMessage,
  onError,
  onStatusChange,
}: UseSSEOptions) => {
  const [status, setStatus] = useState<ConnectionStatus>("disconnected");
  const eventSourceRef = useRef<EventSource | null>(null);
  const retryCountRef = useRef(0);
  const retryTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  // Use refs for callbacks to avoid recreating connect/disconnect on every render
  const onMessageRef = useRef(onMessage);
  const onErrorRef = useRef(onError);
  const onStatusChangeRef = useRef(onStatusChange);

  // Update refs when callbacks change
  useEffect(() => {
    onMessageRef.current = onMessage;
    onErrorRef.current = onError;
    onStatusChangeRef.current = onStatusChange;
  }, [onMessage, onError, onStatusChange]);

  /**
   * Calculate exponential backoff delay
   * Formula: 500ms * 2^attempt, capped at maxRetryDelay
   */
  const calculateBackoffDelay = useCallback(
    (attempt: number): number => {
      const delay = Math.min(500 * Math.pow(2, attempt), maxRetryDelay);
      return delay;
    },
    [maxRetryDelay]
  );

  /**
   * Connect to SSE endpoint
   */
  const connect = useCallback(() => {
    if (eventSourceRef.current) {
      console.log("SSE connection already exists - skipping");
      return;
    }

    console.log(`Connecting to SSE: ${url}`);
    setStatus("connecting");
    onStatusChangeRef.current?.("connecting");

    try {
      const eventSource = new EventSource(url);

      // Connection opened
      eventSource.addEventListener("open", () => {
        console.log("SSE connection opened");
        setStatus("connected");
        onStatusChangeRef.current?.("connected");
        retryCountRef.current = 0; // Reset retry counter on success
      });

      // Listen for "connected" event from backend
      eventSource.addEventListener("connected", (event: MessageEvent) => {
        console.log("SSE connected event received:", event.data);
      });

      // Listen for CDC messages (custom event name: "cdc-message")
      eventSource.addEventListener("cdc-message", (event: MessageEvent) => {
        try {
          const data = JSON.parse(event.data);
          console.log("SSE cdc-message received:", data);
          onMessageRef.current(data);
        } catch (err) {
          console.error("Failed to parse SSE cdc-message:", err);
          onErrorRef.current?.(event);
        }
      });

      // Also listen for default "message" events (fallback)
      eventSource.addEventListener("message", (event: MessageEvent) => {
        try {
          const data = JSON.parse(event.data);
          console.log("SSE default message received:", data);
          onMessageRef.current(data);
        } catch (err) {
          console.error("Failed to parse SSE message:", err);
          onErrorRef.current?.(event);
        }
      });

      // Connection error
      eventSource.addEventListener("error", (event: Event) => {
        console.error("SSE connection error:", event);
        eventSource.close();
        eventSourceRef.current = null;
        setStatus("error");
        onStatusChangeRef.current?.("error");
        onErrorRef.current?.(event);

        // Auto-reconnect with exponential backoff
        if (retryCountRef.current < retryCount) {
          const delay = calculateBackoffDelay(retryCountRef.current);
          console.log(
            `Reconnecting in ${delay}ms (attempt ${retryCountRef.current + 1}/${retryCount})`
          );
          retryCountRef.current += 1;

          retryTimeoutRef.current = setTimeout(() => {
            connect();
          }, delay);
        } else {
          console.error("Max retry attempts reached");
          setStatus("disconnected");
          onStatusChangeRef.current?.("disconnected");
        }
      });

      eventSourceRef.current = eventSource;
    } catch (err) {
      console.error("Failed to create EventSource:", err);
      setStatus("error");
      onStatusChangeRef.current?.("error");
    }
  }, [url, retryCount, calculateBackoffDelay]);

  /**
   * Disconnect from SSE endpoint
   */
  const disconnect = useCallback(() => {
    console.log("Disconnecting from SSE");
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    if (retryTimeoutRef.current) {
      clearTimeout(retryTimeoutRef.current);
      retryTimeoutRef.current = null;
    }
    setStatus("disconnected");
    onStatusChangeRef.current?.("disconnected");
  }, []);

  /**
   * Reconnect to SSE endpoint
   */
  const reconnect = useCallback(() => {
    console.log("Manually reconnecting to SSE");
    disconnect();
    retryCountRef.current = 0;
    connect();
  }, [connect, disconnect]);

  // Auto-connect on mount and when URL changes
  useEffect(() => {
    console.log("useSSE: useEffect triggered (url changed or mount)");

    // Only connect if not already connected
    if (!eventSourceRef.current) {
      connect();
    }

    // Cleanup on unmount
    return () => {
      console.log("useSSE: Cleaning up connection on unmount");
      disconnect();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [url]); // Only reconnect when URL changes

  return { status, reconnect, disconnect };
};
