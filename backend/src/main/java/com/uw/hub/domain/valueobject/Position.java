package com.uw.hub.domain.valueobject;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import lombok.Value;

import java.util.Map;

/**
 * Value object representing CDC message position metadata.
 * Contains source partition info and offset details (LSN, txId, timestamp).
 * Immutable by design following DDD principles.
 */
@Value
public class Position {

    @NotNull(message = "Source partition cannot be null")
    @JsonProperty("sourcePartition")
    String sourcePartition;

    @NotNull(message = "Offset cannot be null")
    @JsonProperty("offset")
    Offset offset;

    /**
     * Nested value object for offset details
     */
    @Value
    public static class Offset {

        @JsonProperty("lsn")
        Long lsn; // Log Sequence Number for PostgreSQL

        @JsonProperty("lsn_commit")
        Long lsnCommit; // Commit LSN for PostgreSQL

        @JsonProperty("txId")
        Long txId; // Transaction ID

        @JsonProperty("timestamp")
        Long timestamp; // Event timestamp

        @JsonProperty("snapshot")
        Boolean snapshot; // Whether this is from initial snapshot

        // Additional fields for flexibility
        Map<String, Object> additionalProperties;

        /**
         * Returns the primary LSN (prefer lsnCommit over lsn)
         */
        public Long getPrimaryLsn() {
            return lsnCommit != null ? lsnCommit : lsn;
        }
    }

    /**
     * Returns a string representation suitable for logging
     */
    @Override
    public String toString() {
        return String.format("Position[partition=%s, txId=%s, lsn=%s]",
            sourcePartition,
            offset.getTxId(),
            offset.getPrimaryLsn());
    }
}
