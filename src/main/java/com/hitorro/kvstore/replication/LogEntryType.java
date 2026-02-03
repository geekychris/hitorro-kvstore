package com.hitorro.kvstore.replication;

/**
 * Type of WAL log entry.
 */
public enum LogEntryType {
    /**
     * Put operation
     */
    PUT,
    
    /**
     * Delete operation
     */
    DELETE,
    
    /**
     * Merge operation
     */
    MERGE
}
