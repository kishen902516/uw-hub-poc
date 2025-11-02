/**
 * TypeScript types for CDC messages matching backend domain model
 */

export enum Operation {
  INSERT = "INSERT",
  UPDATE = "UPDATE",
  DELETE = "DELETE",
}

export interface TableInfo {
  database: string;
  schema: string;
  table: string;
}

export interface Position {
  sourcePartition: string;
  offset: {
    lsn?: number;
    lsn_commit?: number;
    txId?: number;
    timestamp?: number;
    snapshot?: boolean;
  };
}

export interface Metadata {
  schemaVersion: string;
  connector: string;
  source: string;
  version: string;
}

export interface CdcMessage {
  id: string;
  topic: string;
  tableInfo: TableInfo;
  operation: Operation;
  timestamp: string; // ISO 8601 format
  position: Position;
  beforeData?: Record<string, any>;
  afterData?: Record<string, any>;
  metadata: Metadata;
}

/**
 * Connection status for SSE
 */
export type ConnectionStatus = "connecting" | "connected" | "disconnected" | "error";

/**
 * Filter options for CDC messages
 */
export interface CdcFilter {
  topic?: string;
  operation?: Operation;
  database?: string;
  schema?: string;
  table?: string;
  fromDate?: Date;
  toDate?: Date;
}

/**
 * Helper to get fully qualified table name
 */
export function getFullyQualifiedTableName(tableInfo: TableInfo): string {
  return `${tableInfo.database}.${tableInfo.schema}.${tableInfo.table}`;
}

/**
 * Helper to format operation with color
 */
export function getOperationColor(operation: Operation): string {
  switch (operation) {
    case Operation.INSERT:
      return "green";
    case Operation.UPDATE:
      return "blue";
    case Operation.DELETE:
      return "red";
    default:
      return "gray";
  }
}

/**
 * Diff result for UPDATE operations
 */
export interface FieldDiff {
  field: string;
  before: any;
  after: any;
  type: "changed" | "added" | "removed";
}

/**
 * Pagination parameters
 */
export interface PaginationParams {
  page: number;
  size: number;
}

/**
 * API response wrapper
 */
export interface PagedResponse<T> {
  content: T[];
  page: number;
  size: number;
  totalElements: number;
  totalPages: number;
}
