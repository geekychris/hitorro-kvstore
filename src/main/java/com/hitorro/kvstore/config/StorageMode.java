package com.hitorro.kvstore.config;

/**
 * Storage mode for the database.
 */
public enum StorageMode {
    /**
     * Disk-based storage - data persisted to disk
     */
    DISK,
    
    /**
     * Memory-based storage - data kept in memory (useful for testing or caching)
     */
    MEMORY
}
