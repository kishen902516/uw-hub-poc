import { create } from "zustand";
import { devtools, persist } from "zustand/middleware";
import { CdcMessage, ConnectionStatus, CdcFilter, Operation } from "@/types/cdc";

/**
 * Zustand store for CDC message state management
 * Provides centralized state for messages, filters, and connection status
 */

export interface CdcMessageStore {
  // State
  messages: CdcMessage[];
  filter: CdcFilter;
  connectionStatus: ConnectionStatus;
  lastMessageTime: number;
  announcementText: string; // For ARIA live regions

  // Computed
  filteredMessages: () => CdcMessage[];
  messageCount: () => number;
  isConnected: () => boolean;

  // Actions
  addMessage: (message: CdcMessage) => void;
  addMessages: (messages: CdcMessage[]) => void;
  setFilter: (filter: Partial<CdcFilter>) => void;
  clearFilter: () => void;
  setConnectionStatus: (status: ConnectionStatus) => void;
  clearMessages: () => void;
  setAnnouncementText: (text: string) => void;
}

export const useCdcMessageStore = create<CdcMessageStore>()(
  devtools(
    persist(
      (set, get) => ({
        // Initial state
        messages: [],
        filter: {},
        connectionStatus: "disconnected",
        lastMessageTime: 0,
        announcementText: "",

        // Computed selectors
        filteredMessages: () => {
          const { messages, filter } = get();

          return messages.filter((message) => {
            // Filter by topic
            if (filter.topic && message.topic !== filter.topic) {
              return false;
            }

            // Filter by operation
            if (filter.operation && message.operation !== filter.operation) {
              return false;
            }

            // Filter by database
            if (filter.database && message.tableInfo.database !== filter.database) {
              return false;
            }

            // Filter by schema
            if (filter.schema && message.tableInfo.schema !== filter.schema) {
              return false;
            }

            // Filter by table
            if (filter.table && message.tableInfo.table !== filter.table) {
              return false;
            }

            // Filter by date range
            if (filter.fromDate) {
              const messageDate = new Date(message.timestamp);
              if (messageDate < filter.fromDate) {
                return false;
              }
            }

            if (filter.toDate) {
              const messageDate = new Date(message.timestamp);
              if (messageDate > filter.toDate) {
                return false;
              }
            }

            return true;
          });
        },

        messageCount: () => get().messages.length,

        isConnected: () => get().connectionStatus === "connected",

        // Actions
        addMessage: (message: CdcMessage) => {
          const announceText = `New ${message.operation} on ${message.tableInfo.table}`;

          set((state) => ({
            messages: [message, ...state.messages].slice(0, 10000), // Keep last 10k
            lastMessageTime: Date.now(),
            announcementText: announceText,
          }));

          // Clear announcement after 1.5s
          setTimeout(() => {
            set({ announcementText: "" });
          }, 1500);
        },

        addMessages: (newMessages: CdcMessage[]) => {
          const announceText = `Received ${newMessages.length} messages`;

          set((state) => ({
            messages: [...newMessages, ...state.messages].slice(0, 10000),
            lastMessageTime: Date.now(),
            announcementText: announceText,
          }));

          setTimeout(() => {
            set({ announcementText: "" });
          }, 1500);
        },

        setFilter: (newFilter: Partial<CdcFilter>) => {
          set((state) => ({
            filter: { ...state.filter, ...newFilter },
          }));
        },

        clearFilter: () => {
          set({ filter: {} });
        },

        setConnectionStatus: (status: ConnectionStatus) => {
          set({ connectionStatus: status });

          // Announce status changes to screen readers
          if (status === "connected") {
            set({ announcementText: "Connected to message stream" });
            setTimeout(() => set({ announcementText: "" }), 1500);
          } else if (status === "disconnected") {
            set({ announcementText: "Connection lost. Attempting to reconnect..." });
          } else if (status === "error") {
            set({ announcementText: "Connection error occurred" });
          }
        },

        clearMessages: () => {
          set({ messages: [] });
        },

        setAnnouncementText: (text: string) => {
          set({ announcementText: text });
        },
      }),
      {
        name: "cdc-message-store",
        partialize: (state) => ({
          messages: state.messages,
          filter: state.filter,
        }),
      }
    )
  )
);

// Selector hooks for granular subscriptions (prevents unnecessary re-renders)
export const useMessages = () => useCdcMessageStore((state) => state.messages);
export const useFilteredMessages = () => useCdcMessageStore((state) => state.filteredMessages());
export const useConnectionStatus = () => useCdcMessageStore((state) => state.connectionStatus);
export const useAnnouncement = () => useCdcMessageStore((state) => state.announcementText);
export const useFilter = () => useCdcMessageStore((state) => state.filter);
export const useMessageCount = () => useCdcMessageStore((state) => state.messageCount());
