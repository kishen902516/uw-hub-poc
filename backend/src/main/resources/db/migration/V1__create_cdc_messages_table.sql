-- V1: Create CDC messages table with JSONB support and partitioning
-- Date: 2025-11-02
-- Description: Core table for storing Debezium CDC messages with optimized indexes

-- Create CDC messages table with declarative partitioning by timestamp (monthly)
CREATE TABLE IF NOT EXISTS cdc_messages (
    id UUID DEFAULT gen_random_uuid(),
    topic VARCHAR(255) NOT NULL,

    -- Table info
    database_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,

    -- CDC operation
    operation VARCHAR(10) NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),

    -- Timestamp for partitioning and filtering
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- JSONB columns for flexible schema
    position JSONB NOT NULL,
    before_data JSONB,
    after_data JSONB,
    metadata JSONB NOT NULL,

    -- Audit fields
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT valid_insert CHECK (
        operation != 'INSERT' OR (before_data IS NULL AND after_data IS NOT NULL)
    ),
    CONSTRAINT valid_update CHECK (
        operation != 'UPDATE' OR (before_data IS NOT NULL AND after_data IS NOT NULL)
    ),
    CONSTRAINT valid_delete CHECK (
        operation != 'DELETE' OR (before_data IS NOT NULL AND after_data IS NULL)
    ),
    -- Primary key must include partition column
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Create indexes for common query patterns
CREATE INDEX idx_cdc_messages_topic ON cdc_messages(topic);
CREATE INDEX idx_cdc_messages_operation ON cdc_messages(operation);
CREATE INDEX idx_cdc_messages_timestamp ON cdc_messages(timestamp DESC);
CREATE INDEX idx_cdc_messages_table ON cdc_messages(database_name, schema_name, table_name);

-- Composite index for filtered queries
CREATE INDEX idx_cdc_messages_composite ON cdc_messages(operation, table_name, timestamp DESC);

-- JSONB GIN indexes for efficient querying
CREATE INDEX idx_cdc_messages_position_gin ON cdc_messages USING GIN(position);
CREATE INDEX idx_cdc_messages_before_data_gin ON cdc_messages USING GIN(before_data);
CREATE INDEX idx_cdc_messages_after_data_gin ON cdc_messages USING GIN(after_data);

-- Create initial partition for current month
DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    partition_name TEXT;
BEGIN
    -- Current month partition
    start_date := DATE_TRUNC('month', CURRENT_DATE);
    end_date := start_date + INTERVAL '1 month';
    partition_name := 'cdc_messages_' || TO_CHAR(start_date, 'YYYY_MM');

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF cdc_messages
         FOR VALUES FROM (%L) TO (%L)',
        partition_name, start_date, end_date
    );

    -- Next month partition (pre-create to avoid gaps)
    start_date := end_date;
    end_date := start_date + INTERVAL '1 month';
    partition_name := 'cdc_messages_' || TO_CHAR(start_date, 'YYYY_MM');

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF cdc_messages
         FOR VALUES FROM (%L) TO (%L)',
        partition_name, start_date, end_date
    );
END $$;

-- Create view for easy querying with JSON data
CREATE OR REPLACE VIEW v_cdc_messages_summary AS
SELECT
    id,
    topic,
    database_name || '.' || schema_name || '.' || table_name AS full_table_name,
    operation,
    timestamp,
    position->>'txId' AS transaction_id,
    position->'offset'->>'lsn' AS lsn,
    metadata->>'connector' AS connector,
    created_at
FROM cdc_messages;

-- Create function to automatically create future partitions
CREATE OR REPLACE FUNCTION create_next_partition()
RETURNS void AS $$
DECLARE
    max_partition_date DATE;
    next_start_date DATE;
    next_end_date DATE;
    partition_name TEXT;
BEGIN
    -- Find the latest partition end date
    SELECT MAX(TO_DATE(SUBSTRING(tablename FROM '\d{4}_\d{2}'), 'YYYY_MM') + INTERVAL '1 month')
    INTO max_partition_date
    FROM pg_tables
    WHERE tablename LIKE 'cdc_messages_%'
    AND schemaname = 'public';

    -- Create next partition
    IF max_partition_date IS NOT NULL THEN
        next_start_date := max_partition_date;
        next_end_date := next_start_date + INTERVAL '1 month';
        partition_name := 'cdc_messages_' || TO_CHAR(next_start_date, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF cdc_messages
             FOR VALUES FROM (%L) TO (%L)',
            partition_name, next_start_date, next_end_date
        );

        RAISE NOTICE 'Created partition: %', partition_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Add comment for documentation
COMMENT ON TABLE cdc_messages IS 'Stores Change Data Capture messages from Kafka topics in Debezium format';
COMMENT ON COLUMN cdc_messages.position IS 'JSONB containing LSN, txId, offset, and snapshot flag';
COMMENT ON COLUMN cdc_messages.before_data IS 'Row state before change (NULL for INSERT)';
COMMENT ON COLUMN cdc_messages.after_data IS 'Row state after change (NULL for DELETE)';
COMMENT ON COLUMN cdc_messages.metadata IS 'CDC connector metadata (version, connector type, schema version)';
