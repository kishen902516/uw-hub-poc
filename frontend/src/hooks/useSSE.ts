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
      console.log("SSE connection already exists");
      return;
    }

    console.log(`Connecting to SSE: ${url}`);
    setStatus("connecting");
    onStatusChange?.("connecting");

    try {
      const eventSource = new EventSource(url);

      // Connection opened
      eventSource.addEventListener("open", () => {
        console.log("SSE connection opened");
        setStatus("connected");
        onStatusChange?.("connected");
        retryCountRef.current = 0; // Reset retry counter on success
      });

      // Message received
      eventSource.addEventListener("message", (event: MessageEvent) => {
        try {
          const data = JSON.parse(event.data);
          console.log("SSE message received:", data);
          onMessage(data);
        } catch (err) {
          console.error("Failed to parse SSE message:", err);
          onError?.(event);
        }
      });

      // Connection error
      eventSource.addEventListener("error", (event: Event) => {
        console.error("SSE connection error:", event);
        eventSource.close();
        eventSourceRef.current = null;
        setStatus("error");
        onStatusChange?.("error");
        onError?.(event);

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
          onStatusChange?.("disconnected");
        }
      });

      eventSourceRef.current = eventSource;
    } catch (err) {
      console.error("Failed to create EventSource:", err);
      setStatus("error");
      onStatusChange?.("error");
    }
  }, [url, retryCount, onMessage, onError, onStatusChange, calculateBackoffDelay]);

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
    onStatusChange?.("disconnected");
  }, [onStatusChange]);

  /**
   * Reconnect to SSE endpoint
   */
  const reconnect = useCallback(() => {
    console.log("Manually reconnecting to SSE");
    disconnect();
    retryCountRef.current = 0;
    connect();
  }, [connect, disconnect]);

  // Auto-connect on mount
  useEffect(() => {
    connect();

    // Cleanup on unmount
    return () => {
      disconnect();
    };
  }, [connect, disconnect]);

  return { status, reconnect, disconnect };
};
