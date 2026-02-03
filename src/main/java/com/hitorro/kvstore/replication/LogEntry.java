package com.hitorro.kvstore.replication;

import java.util.Optional;

/**
 * Represents a single entry in the Write-Ahead Log (WAL).
 * Used for replication between database instances.
 */
public class LogEntry {
    private final long sequenceNumber;
    private final LogEntryType type;
    private final byte[] key;
    private final byte[] value;
    private final byte[] metadata;

    public LogEntry(long sequenceNumber, LogEntryType type, byte[] key, byte[] value, byte[] metadata) {
        this.sequenceNumber = sequenceNumber;
        this.type = type;
        this.key = key;
        this.value = value;
        this.metadata = metadata;
    }

    /**
     * Gets the sequence number of this log entry.
     *
     * @return The sequence number
     */
    public long getSequenceNumber() {
        return sequenceNumber;
    }

    /**
     * Gets the type of operation.
     *
     * @return The log entry type
     */
    public LogEntryType getType() {
        return type;
    }

    /**
     * Gets the key.
     *
     * @return The key bytes
     */
    public byte[] getKey() {
        return key;
    }

    /**
     * Gets the value.
     *
     * @return The value bytes (may be null for DELETE operations)
     */
    public byte[] getValue() {
        return value;
    }

    /**
     * Gets optional metadata attached to this entry.
     *
     * @return Optional metadata bytes
     */
    public Optional<byte[]> getMetadata() {
        return Optional.ofNullable(metadata);
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "sequenceNumber=" + sequenceNumber +
                ", type=" + type +
                ", keySize=" + (key != null ? key.length : 0) +
                ", valueSize=" + (value != null ? value.length : 0) +
                ", hasMetadata=" + (metadata != null) +
                '}';
    }
}
