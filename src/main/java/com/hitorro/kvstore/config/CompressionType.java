package com.hitorro.kvstore.config;

/**
 * Compression types supported by the key-value store.
 */
public enum CompressionType {
    /**
     * No compression
     */
    NONE(org.rocksdb.CompressionType.NO_COMPRESSION),
    
    /**
     * Snappy compression - fast compression with reasonable compression ratio
     */
    SNAPPY(org.rocksdb.CompressionType.SNAPPY_COMPRESSION),
    
    /**
     * LZ4 compression - very fast compression
     */
    LZ4(org.rocksdb.CompressionType.LZ4_COMPRESSION),
    
    /**
     * LZ4HC compression - higher compression ratio than LZ4, slower compression
     */
    LZ4HC(org.rocksdb.CompressionType.LZ4HC_COMPRESSION),
    
    /**
     * ZSTD compression - best compression ratio, configurable speed/ratio trade-off
     */
    ZSTD(org.rocksdb.CompressionType.ZSTD_COMPRESSION),
    
    /**
     * Zlib compression - good compression ratio, slower than Snappy
     */
    ZLIB(org.rocksdb.CompressionType.ZLIB_COMPRESSION);

    private final org.rocksdb.CompressionType rocksDBType;

    CompressionType(org.rocksdb.CompressionType rocksDBType) {
        this.rocksDBType = rocksDBType;
    }

    /**
     * Gets the RocksDB compression type.
     *
     * @return The RocksDB compression type
     */
    public org.rocksdb.CompressionType getRocksDBType() {
        return rocksDBType;
    }
}
