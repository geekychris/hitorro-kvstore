package com.hitorro.kvstore;

import com.hitorro.kvstore.config.CompressionType;
import com.hitorro.kvstore.config.StorageMode;

/**
 * Configuration for a database instance.
 * Use the Builder to create instances.
 */
public class DatabaseConfig {
    private final String path;
    private final CompressionType compressionType;
    private final StorageMode storageMode;
    private final boolean createIfMissing;
    private final long blockCacheSize;
    private final long writeBufferSize;
    private final int maxWriteBufferNumber;
    private final int blockSize;
    private final double bitsPerKey;
    private final boolean enableWAL;
    private final String walDirectory;
    private final long walTTLSeconds;
    private final long walSizeLimitMB;
    private final boolean syncWrites;
    private final boolean enableTransactions;

    private DatabaseConfig(Builder builder) {
        this.path = builder.path;
        this.compressionType = builder.compressionType;
        this.storageMode = builder.storageMode;
        this.createIfMissing = builder.createIfMissing;
        this.blockCacheSize = builder.blockCacheSize;
        this.writeBufferSize = builder.writeBufferSize;
        this.maxWriteBufferNumber = builder.maxWriteBufferNumber;
        this.blockSize = builder.blockSize;
        this.bitsPerKey = builder.bitsPerKey;
        this.enableWAL = builder.enableWAL;
        this.walDirectory = builder.walDirectory;
        this.walTTLSeconds = builder.walTTLSeconds;
        this.walSizeLimitMB = builder.walSizeLimitMB;
        this.syncWrites = builder.syncWrites;
        this.enableTransactions = builder.enableTransactions;
    }

    public String getPath() {
        return path;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public StorageMode getStorageMode() {
        return storageMode;
    }

    public boolean isCreateIfMissing() {
        return createIfMissing;
    }

    public long getBlockCacheSize() {
        return blockCacheSize;
    }

    public long getWriteBufferSize() {
        return writeBufferSize;
    }

    public int getMaxWriteBufferNumber() {
        return maxWriteBufferNumber;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public double getBitsPerKey() {
        return bitsPerKey;
    }

    public boolean isEnableWAL() {
        return enableWAL;
    }

    public String getWalDirectory() {
        return walDirectory;
    }

    public long getWalTTLSeconds() {
        return walTTLSeconds;
    }

    public long getWalSizeLimitMB() {
        return walSizeLimitMB;
    }

    public boolean isSyncWrites() {
        return syncWrites;
    }

    public boolean isEnableTransactions() {
        return enableTransactions;
    }

    /**
     * Creates a new builder for DatabaseConfig.
     *
     * @param path The database path
     * @return A new builder
     */
    public static Builder builder(String path) {
        return new Builder(path);
    }

    /**
     * Builder for DatabaseConfig.
     */
    public static class Builder {
        private final String path;
        private CompressionType compressionType = CompressionType.SNAPPY;
        private StorageMode storageMode = StorageMode.DISK;
        private boolean createIfMissing = true;
        private long blockCacheSize = 8 * 1024 * 1024; // 8MB default
        private long writeBufferSize = 64 * 1024 * 1024; // 64MB default
        private int maxWriteBufferNumber = 3;
        private int blockSize = 4 * 1024; // 4KB default
        private double bitsPerKey = 10.0; // Bloom filter bits per key
        private boolean enableWAL = true;
        private String walDirectory = null; // null means use db directory
        private long walTTLSeconds = 0; // 0 means no TTL
        private long walSizeLimitMB = 0; // 0 means no limit
        private boolean syncWrites = false;
        private boolean enableTransactions = false;

        private Builder(String path) {
            if (path == null || path.isEmpty()) {
                throw new IllegalArgumentException("Database path cannot be null or empty");
            }
            this.path = path;
        }

        /**
         * Sets the compression type.
         *
         * @param compressionType The compression type
         * @return This builder
         */
        public Builder compressionType(CompressionType compressionType) {
            this.compressionType = compressionType;
            return this;
        }

        /**
         * Sets the storage mode.
         *
         * @param storageMode The storage mode
         * @return This builder
         */
        public Builder storageMode(StorageMode storageMode) {
            this.storageMode = storageMode;
            return this;
        }

        /**
         * Sets whether to create the database if it doesn't exist.
         *
         * @param createIfMissing Whether to create if missing
         * @return This builder
         */
        public Builder createIfMissing(boolean createIfMissing) {
            this.createIfMissing = createIfMissing;
            return this;
        }

        /**
         * Sets the block cache size in bytes.
         *
         * @param blockCacheSize The block cache size
         * @return This builder
         */
        public Builder blockCacheSize(long blockCacheSize) {
            this.blockCacheSize = blockCacheSize;
            return this;
        }

        /**
         * Sets the write buffer size in bytes.
         *
         * @param writeBufferSize The write buffer size
         * @return This builder
         */
        public Builder writeBufferSize(long writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }

        /**
         * Sets the maximum number of write buffers.
         *
         * @param maxWriteBufferNumber The max write buffer number
         * @return This builder
         */
        public Builder maxWriteBufferNumber(int maxWriteBufferNumber) {
            this.maxWriteBufferNumber = maxWriteBufferNumber;
            return this;
        }

        /**
         * Sets the block size in bytes.
         *
         * @param blockSize The block size
         * @return This builder
         */
        public Builder blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        /**
         * Sets the bloom filter bits per key.
         *
         * @param bitsPerKey The bits per key
         * @return This builder
         */
        public Builder bitsPerKey(double bitsPerKey) {
            this.bitsPerKey = bitsPerKey;
            return this;
        }

        /**
         * Sets whether to enable WAL (Write-Ahead Log).
         *
         * @param enableWAL Whether to enable WAL
         * @return This builder
         */
        public Builder enableWAL(boolean enableWAL) {
            this.enableWAL = enableWAL;
            return this;
        }

        /**
         * Sets the WAL directory path.
         *
         * @param walDirectory The WAL directory (null to use db directory)
         * @return This builder
         */
        public Builder walDirectory(String walDirectory) {
            this.walDirectory = walDirectory;
            return this;
        }

        /**
         * Sets the WAL TTL in seconds for archival.
         *
         * @param walTTLSeconds The WAL TTL (0 for no TTL)
         * @return This builder
         */
        public Builder walTTLSeconds(long walTTLSeconds) {
            this.walTTLSeconds = walTTLSeconds;
            return this;
        }

        /**
         * Sets the WAL size limit in megabytes for archival.
         *
         * @param walSizeLimitMB The WAL size limit (0 for no limit)
         * @return This builder
         */
        public Builder walSizeLimitMB(long walSizeLimitMB) {
            this.walSizeLimitMB = walSizeLimitMB;
            return this;
        }

        /**
         * Sets whether to sync writes (fsync after each write).
         *
         * @param syncWrites Whether to sync writes
         * @return This builder
         */
        public Builder syncWrites(boolean syncWrites) {
            this.syncWrites = syncWrites;
            return this;
        }

        /**
         * Sets whether to enable transaction support.
         *
         * @param enableTransactions Whether to enable transactions
         * @return This builder
         */
        public Builder enableTransactions(boolean enableTransactions) {
            this.enableTransactions = enableTransactions;
            return this;
        }

        /**
         * Builds the DatabaseConfig.
         *
         * @return The DatabaseConfig
         */
        public DatabaseConfig build() {
            return new DatabaseConfig(this);
        }
    }
}
