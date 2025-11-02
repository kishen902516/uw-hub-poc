package com.uw.hub.domain.valueobject;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.Value;

/**
 * Value object representing CDC message metadata.
 * Contains information about the CDC connector and schema version.
 * Immutable by design following DDD principles.
 */
@Value
public class Metadata {

    @NotBlank(message = "Schema version cannot be blank")
    @JsonProperty("schemaVersion")
    String schemaVersion;

    @NotBlank(message = "Connector name cannot be blank")
    @JsonProperty("connector")
    String connector;

    @JsonProperty("source")
    String source;

    @NotBlank(message = "Version cannot be blank")
    @JsonProperty("version")
    String version;

    /**
     * Factory method for Debezium metadata
     */
    public static Metadata debezium(String version) {
        return new Metadata("1.0.0", "postgresql", "debezium", version);
    }

    @Override
    public String toString() {
        return String.format("Metadata[connector=%s, version=%s, schemaVersion=%s]",
            connector, version, schemaVersion);
    }
}
