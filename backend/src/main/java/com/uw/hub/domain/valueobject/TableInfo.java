package com.uw.hub.domain.valueobject;

import jakarta.validation.constraints.NotBlank;
import lombok.Value;

/**
 * Value object representing database table information in CDC messages.
 * Immutable by design following DDD principles.
 */
@Value
public class TableInfo {

    @NotBlank(message = "Database name cannot be blank")
    String database;

    @NotBlank(message = "Schema name cannot be blank")
    String schema;

    @NotBlank(message = "Table name cannot be blank")
    String table;

    /**
     * Returns the fully qualified table name in format: database.schema.table
     */
    public String getFullyQualifiedName() {
        return String.format("%s.%s.%s", database, schema, table);
    }

    /**
     * Returns the schema-qualified table name in format: schema.table
     */
    public String getSchemaQualifiedName() {
        return String.format("%s.%s", schema, table);
    }

    @Override
    public String toString() {
        return getFullyQualifiedName();
    }
}
