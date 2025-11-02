/**
 * Playwright test helpers for producing CDC messages to Kafka
 * Used in E2E tests to simulate real CDC events
 */

import { CdcMessage, Operation } from '../../src/types/cdc';

/**
 * Kafka producer configuration
 */
export interface KafkaProducerConfig {
  bootstrapServers: string;
  topic: string;
}

/**
 * Default Kafka configuration for tests
 */
const defaultConfig: KafkaProducerConfig = {
  bootstrapServers: process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092',
  topic: process.env.KAFKA_TEST_TOPIC || 'test.cdc.messages',
};

/**
 * Create a test CDC INSERT message
 */
export function createInsertMessage(
  database: string = 'testdb',
  schema: string = 'public',
  table: string = 'customers',
  afterData: Record<string, any> = { id: 1, name: 'Test User' }
): CdcMessage {
  return {
    id: crypto.randomUUID(),
    topic: `${database}.${schema}.${table}`,
    tableInfo: {
      database,
      schema,
      table,
    },
    operation: Operation.INSERT,
    timestamp: new Date().toISOString(),
    position: {
      sourcePartition: `{server=${database}}`,
      offset: {
        lsn: Math.floor(Math.random() * 1000000),
        txId: Math.floor(Math.random() * 10000),
        timestamp: Date.now(),
        snapshot: false,
      },
    },
    afterData,
    metadata: {
      schemaVersion: '1.0.0',
      connector: 'postgresql',
      source: 'debezium',
      version: '2.4.0',
    },
  };
}

/**
 * Create a test CDC UPDATE message
 */
export function createUpdateMessage(
  database: string = 'testdb',
  schema: string = 'public',
  table: string = 'customers',
  beforeData: Record<string, any> = { id: 1, name: 'Old Name' },
  afterData: Record<string, any> = { id: 1, name: 'New Name' }
): CdcMessage {
  return {
    id: crypto.randomUUID(),
    topic: `${database}.${schema}.${table}`,
    tableInfo: {
      database,
      schema,
      table,
    },
    operation: Operation.UPDATE,
    timestamp: new Date().toISOString(),
    position: {
      sourcePartition: `{server=${database}}`,
      offset: {
        lsn: Math.floor(Math.random() * 1000000),
        txId: Math.floor(Math.random() * 10000),
        timestamp: Date.now(),
      },
    },
    beforeData,
    afterData,
    metadata: {
      schemaVersion: '1.0.0',
      connector: 'postgresql',
      source: 'debezium',
      version: '2.4.0',
    },
  };
}

/**
 * Create a test CDC DELETE message
 */
export function createDeleteMessage(
  database: string = 'testdb',
  schema: string = 'public',
  table: string = 'customers',
  beforeData: Record<string, any> = { id: 1, name: 'Deleted User' }
): CdcMessage {
  return {
    id: crypto.randomUUID(),
    topic: `${database}.${schema}.${table}`,
    tableInfo: {
      database,
      schema,
      table,
    },
    operation: Operation.DELETE,
    timestamp: new Date().toISOString(),
    position: {
      sourcePartition: `{server=${database}}`,
      offset: {
        lsn: Math.floor(Math.random() * 1000000),
        txId: Math.floor(Math.random() * 10000),
        timestamp: Date.now(),
      },
    },
    beforeData,
    metadata: {
      schemaVersion: '1.0.0',
      connector: 'postgresql',
      source: 'debezium',
      version: '2.4.0',
    },
  };
}

/**
 * Send a CDC message to the backend API
 * This simulates producing a message to Kafka by directly posting to the backend
 */
export async function sendCdcMessage(
  message: CdcMessage,
  apiUrl: string = 'http://localhost:8080'
): Promise<void> {
  const response = await fetch(`${apiUrl}/api/test/cdc-messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(message),
  });

  if (!response.ok) {
    throw new Error(`Failed to send CDC message: ${response.statusText}`);
  }
}

/**
 * Send multiple CDC messages in batch
 */
export async function sendBatchCdcMessages(
  messages: CdcMessage[],
  apiUrl: string = 'http://localhost:8080'
): Promise<void> {
  await Promise.all(messages.map((msg) => sendCdcMessage(msg, apiUrl)));
}

/**
 * Wait for SSE connection to be established
 */
export async function waitForSseConnection(page: any, timeout: number = 5000): Promise<void> {
  await page.waitForFunction(
    () => {
      const statusElement = document.querySelector('[data-testid="connection-status"]');
      return statusElement?.textContent?.includes('connected');
    },
    { timeout }
  );
}

/**
 * Wait for a specific number of messages to appear in the UI
 */
export async function waitForMessageCount(
  page: any,
  count: number,
  timeout: number = 10000
): Promise<void> {
  await page.waitForFunction(
    (expectedCount: number) => {
      const rows = document.querySelectorAll('[data-testid="cdc-message-row"]');
      return rows.length >= expectedCount;
    },
    count,
    { timeout }
  );
}
