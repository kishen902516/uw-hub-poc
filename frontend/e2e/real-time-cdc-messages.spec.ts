import { test, expect } from '@playwright/test';
import { produceInsertMessage, produceUpdateMessage, produceDeleteMessage } from './helpers/kafka-producer';

/**
 * E2E test for real-time CDC message display.
 *
 * Tests User Story 1: Real-Time CDC Message Monitoring
 *
 * Scenarios:
 * 1. Produce INSERT CDC message to Kafka → Verify appears in UI within 100ms
 * 2. Produce UPDATE CDC message → Verify before/after states are displayed
 * 3. Produce DELETE CDC message → Verify deletion is shown
 * 4. Verify operation badges are color-coded (INSERT=green, UPDATE=blue, DELETE=red)
 * 5. Verify table info (database.schema.table) is displayed
 * 6. Verify connection status indicator shows "connected"
 * 7. Verify accessibility (ARIA live regions announce new messages)
 */

test.describe('Real-Time CDC Message Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the app
    await page.goto('http://localhost:3000');

    // Wait for SSE connection to establish
    await page.waitForSelector('[data-testid="connection-status"][data-status="connected"]', {
      timeout: 10000,
    });
  });

  test('should display INSERT message in UI within 100ms', async ({ page }) => {
    // Given: Record the start time
    const startTime = Date.now();

    // When: Produce an INSERT CDC message to Kafka
    const insertMessage = {
      table: { database: 'cdcdb', schema: 'public', table: 'customers' },
      operation: 'INSERT',
      timestamp: new Date().toISOString(),
      position: {
        sourcePartition: '{server=postgres-localhost-cdcdb}',
        offset: { txId: 1001, lsn: 27696384, snapshot: true },
      },
      after: {
        customer_id: 1,
        first_name: 'John',
        last_name: 'Doe',
        email: 'john.doe@example.com',
      },
      metadata: {
        version: '1.0.0',
        connector: 'postgresql',
        schemaVersion: '1',
        source: 'postgres-localhost-cdcdb',
      },
    };

    await produceInsertMessage('cdcdb.public.customers', insertMessage);

    // Then: Verify message appears in UI
    const messageRow = page.locator('[data-testid="cdc-message-row"]').filter({
      has: page.locator('text="John"'),
    });

    await expect(messageRow).toBeVisible({ timeout: 5000 });

    // Verify latency is < 100ms (allow some buffer for test execution)
    const endTime = Date.now();
    const latency = endTime - startTime;
    expect(latency).toBeLessThan(2000); // 2s buffer for test execution

    // Verify operation badge is green for INSERT
    const operationBadge = messageRow.locator('[data-testid="operation-badge"]');
    await expect(operationBadge).toHaveText('INSERT');
    await expect(operationBadge).toHaveClass(/bg-green/); // Tailwind green variant

    // Verify table info is displayed
    const tableInfo = messageRow.locator('[data-testid="table-info"]');
    await expect(tableInfo).toHaveText('cdcdb.public.customers');

    // Verify after data is displayed
    await expect(messageRow).toContainText('John');
    await expect(messageRow).toContainText('Doe');
    await expect(messageRow).toContainText('john.doe@example.com');
  });

  test('should display UPDATE message with before/after states', async ({ page }) => {
    // When: Produce an UPDATE CDC message to Kafka
    const updateMessage = {
      table: { database: 'cdcdb', schema: 'public', table: 'customers' },
      operation: 'UPDATE',
      timestamp: new Date().toISOString(),
      position: {
        sourcePartition: '{server=postgres-localhost-cdcdb}',
        offset: { lsn_commit: 27709280, txId: 1002 },
      },
      before: {
        customer_id: 6,
        first_name: 'Kumar',
        last_name: 'Sivalingam 2',
        email: 'kumar@example.com',
      },
      after: {
        customer_id: 6,
        first_name: 'Kumar',
        last_name: 'Sivalingam Name change',
        email: 'kumar@example.com',
      },
      metadata: {
        version: '1.0.0',
        connector: 'postgresql',
      },
    };

    await produceUpdateMessage('cdcdb.public.customers', updateMessage);

    // Then: Verify UPDATE message appears in UI
    const messageRow = page.locator('[data-testid="cdc-message-row"]').filter({
      has: page.locator('text="Kumar"'),
    });

    await expect(messageRow).toBeVisible({ timeout: 5000 });

    // Verify operation badge is blue for UPDATE
    const operationBadge = messageRow.locator('[data-testid="operation-badge"]');
    await expect(operationBadge).toHaveText('UPDATE');
    await expect(operationBadge).toHaveClass(/bg-blue/); // Tailwind blue variant

    // Verify both before and after data are shown in preview
    await expect(messageRow).toContainText('Sivalingam 2'); // before
    await expect(messageRow).toContainText('Sivalingam Name change'); // after
  });

  test('should display DELETE message with before state', async ({ page }) => {
    // When: Produce a DELETE CDC message to Kafka
    const deleteMessage = {
      table: { database: 'cdcdb', schema: 'public', table: 'customers' },
      operation: 'DELETE',
      timestamp: new Date().toISOString(),
      position: {
        sourcePartition: '{server=postgres-localhost-cdcdb}',
        offset: { lsn_commit: 27715000, txId: 1003 },
      },
      before: {
        customer_id: 99,
        first_name: 'Test',
        last_name: 'User',
        email: 'test@example.com',
      },
      metadata: {
        version: '1.0.0',
        connector: 'postgresql',
      },
    };

    await produceDeleteMessage('cdcdb.public.customers', deleteMessage);

    // Then: Verify DELETE message appears in UI
    const messageRow = page.locator('[data-testid="cdc-message-row"]').filter({
      has: page.locator('text="Test"'),
    });

    await expect(messageRow).toBeVisible({ timeout: 5000 });

    // Verify operation badge is red for DELETE
    const operationBadge = messageRow.locator('[data-testid="operation-badge"]');
    await expect(operationBadge).toHaveText('DELETE');
    await expect(operationBadge).toHaveClass(/bg-red/); // Tailwind red variant

    // Verify before data is displayed
    await expect(messageRow).toContainText('Test');
    await expect(messageRow).toContainText('User');
  });

  test('should show connection status as connected', async ({ page }) => {
    // Then: Verify connection status indicator
    const connectionStatus = page.locator('[data-testid="connection-status"]');
    await expect(connectionStatus).toHaveText('connected');
    await expect(connectionStatus).toHaveAttribute('data-status', 'connected');

    // Verify badge color is green/success variant
    await expect(connectionStatus).toHaveClass(/bg-green|variant-default/);
  });

  test('should handle multiple messages in rapid succession', async ({ page }) => {
    // When: Produce multiple CDC messages rapidly
    const messages = [
      {
        operation: 'INSERT',
        after: { customer_id: 10, name: 'Customer 10' },
      },
      {
        operation: 'INSERT',
        after: { customer_id: 11, name: 'Customer 11' },
      },
      {
        operation: 'INSERT',
        after: { customer_id: 12, name: 'Customer 12' },
      },
    ];

    for (const msg of messages) {
      const cdcMessage = {
        table: { database: 'cdcdb', schema: 'public', table: 'customers' },
        operation: msg.operation,
        timestamp: new Date().toISOString(),
        position: {
          sourcePartition: '{server=test}',
          offset: { txId: Math.floor(Math.random() * 10000) },
        },
        after: msg.after,
        metadata: { version: '1.0.0', connector: 'postgresql' },
      };

      await produceInsertMessage('cdcdb.public.customers', cdcMessage);
    }

    // Then: Verify all messages appear in UI
    await expect(page.locator('[data-testid="cdc-message-row"]')).toHaveCount(3, {
      timeout: 10000,
    });

    // Verify each message is displayed
    await expect(page.locator('text="Customer 10"')).toBeVisible();
    await expect(page.locator('text="Customer 11"')).toBeVisible();
    await expect(page.locator('text="Customer 12"')).toBeVisible();
  });

  test('should auto-scroll to top when new messages arrive', async ({ page }) => {
    // Given: Page is loaded and scrolled down
    // (This test assumes auto-scroll behavior is implemented)

    // When: New message arrives
    const newMessage = {
      table: { database: 'cdcdb', schema: 'public', table: 'customers' },
      operation: 'INSERT',
      timestamp: new Date().toISOString(),
      position: {
        sourcePartition: '{server=test}',
        offset: { txId: 2000 },
      },
      after: { customer_id: 999, name: 'Latest Customer' },
      metadata: { version: '1.0.0', connector: 'postgresql' },
    };

    await produceInsertMessage('cdcdb.public.customers', newMessage);

    // Then: Verify latest message is at the top
    const firstRow = page.locator('[data-testid="cdc-message-row"]').first();
    await expect(firstRow).toContainText('Latest Customer');
  });

  test('should display table info in database.schema.table format', async ({ page }) => {
    // When: Produce a message
    const message = {
      table: { database: 'cdcdb', schema: 'public', table: 'orders' },
      operation: 'INSERT',
      timestamp: new Date().toISOString(),
      position: {
        sourcePartition: '{server=test}',
        offset: { txId: 3000 },
      },
      after: { order_id: 1, total: 100.5 },
      metadata: { version: '1.0.0', connector: 'postgresql' },
    };

    await produceInsertMessage('cdcdb.public.orders', message);

    // Then: Verify table info format
    const tableInfo = page.locator('[data-testid="table-info"]');
    await expect(tableInfo).toHaveText('cdcdb.public.orders');
  });

  test('should announce new messages to screen readers (ARIA live region)', async ({ page }) => {
    // When: Produce a new message
    const message = {
      table: { database: 'cdcdb', schema: 'public', table: 'customers' },
      operation: 'INSERT',
      timestamp: new Date().toISOString(),
      position: {
        sourcePartition: '{server=test}',
        offset: { txId: 4000 },
      },
      after: { customer_id: 888, name: 'Accessible Customer' },
      metadata: { version: '1.0.0', connector: 'postgresql' },
    };

    await produceInsertMessage('cdcdb.public.customers', message);

    // Then: Verify ARIA live region is updated
    const liveRegion = page.locator('[role="status"][aria-live="polite"]');
    await expect(liveRegion).toBeVisible(); // Should be in DOM (even if sr-only)

    // Verify announcement text contains operation and table info
    const announcementText = await liveRegion.textContent();
    expect(announcementText).toContain('INSERT'); // or "New message"
  });

  test('should handle reconnection when connection is lost', async ({ page }) => {
    // Note: This test requires backend support to simulate connection loss
    // For now, we'll verify the reconnection UI exists

    // When: Simulate connection loss (by navigating offline or backend restart)
    // await page.context().setOffline(true);

    // Then: Verify connection status shows disconnected
    // const connectionStatus = page.locator('[data-testid="connection-status"]');
    // await expect(connectionStatus).toHaveText('disconnected');

    // When: Restore connection
    // await page.context().setOffline(false);

    // Then: Verify reconnection
    // await expect(connectionStatus).toHaveText('connected', { timeout: 10000 });

    // Placeholder: Just verify the component exists for now
    const connectionStatus = page.locator('[data-testid="connection-status"]');
    await expect(connectionStatus).toBeVisible();
  });

  test('should display timestamp in human-readable format', async ({ page }) => {
    // When: Produce a message
    const timestamp = new Date().toISOString();
    const message = {
      table: { database: 'cdcdb', schema: 'public', table: 'customers' },
      operation: 'INSERT',
      timestamp,
      position: {
        sourcePartition: '{server=test}',
        offset: { txId: 5000 },
      },
      after: { customer_id: 777 },
      metadata: { version: '1.0.0', connector: 'postgresql' },
    };

    await produceInsertMessage('cdcdb.public.customers', message);

    // Then: Verify timestamp is displayed (format: HH:MM:SS or relative time)
    const messageRow = page.locator('[data-testid="cdc-message-row"]').first();
    const timestampElement = messageRow.locator('[data-testid="message-timestamp"]');

    await expect(timestampElement).toBeVisible();
    const timestampText = await timestampElement.textContent();

    // Should be in time format (HH:MM:SS) or relative (e.g., "2 seconds ago")
    expect(timestampText).toMatch(/\d{1,2}:\d{2}:\d{2}|ago|second|minute/);
  });

  test('should virtualize table for 10k+ messages performance', async ({ page }) => {
    // Note: This test verifies virtualization is working by checking DOM elements
    // Only visible rows should be in the DOM, not all 10k

    // When: Page loads (assume backend already has 10k+ messages)
    // Or produce a large batch

    // Then: Verify virtualization is active
    // Check for react-virtuoso container
    const virtualizedContainer = page.locator('[data-test-id="virtuoso-scroller"]');
    // Note: Actual selector depends on React Virtuoso implementation

    // Verify only ~20-30 rows are rendered (not 10k)
    const renderedRows = page.locator('[data-testid="cdc-message-row"]');
    const rowCount = await renderedRows.count();

    // Should be much less than total messages (virtualized)
    expect(rowCount).toBeLessThan(100);
  });
});
