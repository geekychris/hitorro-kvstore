package com.hitorro.kvstore;

import com.hitorro.kvstore.config.StorageMode;
import com.hitorro.kvstore.replication.ReplicationSource;
import org.apache.log4j.Logger;
import org.rocksdb.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * RocksDB-based implementation of KVStore.
 * Provides all key-value operations with proper resource management.
 */
public class RocksDBStore implements KVStore {
    private static final Logger logger = Logger.getLogger(RocksDBStore.class);


    private final RocksDB db;
    private final Options options;
    private final WriteOptions writeOptions;
    private final ReadOptions readOptions;
    private final DatabaseConfig config;
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final List<AutoCloseable> resources = new ArrayList<>();

    public RocksDBStore(DatabaseConfig config) throws RocksDBException {
        RocksDBLoader.loadLibrary();
        
        this.config = config;
        this.options = createOptions(config);
        this.writeOptions = createWriteOptions(config);
        this.readOptions = new ReadOptions();
        
        // Add to resources for cleanup
        resources.add(options);
        resources.add(writeOptions);
        resources.add(readOptions);

        // Open or create the database
        if (config.isEnableTransactions()) {
            // Use TransactionDB for transaction support
            TransactionDBOptions txnDbOptions = new TransactionDBOptions();
            resources.add(txnDbOptions);
            TransactionDB txnDb = TransactionDB.open(options, txnDbOptions, config.getPath());
            this.db = txnDb;
        } else {
            this.db = RocksDB.open(options, config.getPath());
        }
        
        resources.add(db);
        
        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        
        logger.info("Opened RocksDB at: " + config.getPath());
    }

    private Options createOptions(DatabaseConfig config) {
        Options options = new Options();
        
        options.setCreateIfMissing(config.isCreateIfMissing());
        options.setCompressionType(config.getCompressionType().getRocksDBType());
        
        // Memory settings
        if (config.getStorageMode() == StorageMode.MEMORY) {
            // For in-memory mode, disable WAL and use larger write buffers
            options.setWalTtlSeconds(0);
            options.setWalSizeLimitMB(0);
        }
        
        // Cache settings
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCache(new LRUCache(config.getBlockCacheSize()));
        tableConfig.setBlockSize(config.getBlockSize());
        tableConfig.setFilterPolicy(new BloomFilter(config.getBitsPerKey()));
        options.setTableFormatConfig(tableConfig);
        
        // Write buffer settings
        options.setWriteBufferSize(config.getWriteBufferSize());
        options.setMaxWriteBufferNumber(config.getMaxWriteBufferNumber());
        
        // WAL settings
        if (config.getWalDirectory() != null) {
            options.setWalDir(config.getWalDirectory());
        }
        if (config.getWalTTLSeconds() > 0) {
            options.setWalTtlSeconds(config.getWalTTLSeconds());
        }
        if (config.getWalSizeLimitMB() > 0) {
            options.setWalSizeLimitMB(config.getWalSizeLimitMB());
        }
        
        return options;
    }

    private WriteOptions createWriteOptions(DatabaseConfig config) {
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(config.isSyncWrites());
        writeOptions.setDisableWAL(!config.isEnableWAL());
        return writeOptions;
    }

    @Override
    public Result<byte[]> get(byte[] key) {
        if (!open.get()) {
            return Result.failure("Store is closed");
        }
        
        try {
            byte[] value = db.get(readOptions, key);
            if (value == null) {
                return Result.failure("Key not found");
            }
            return Result.success(value);
        } catch (RocksDBException e) {
            logger.error("Error getting key", e);
            return Result.failure("Failed to get key: " + e.getMessage());
        }
    }

    @Override
    public Result<Void> put(byte[] key, byte[] value) {
        if (!open.get()) {
            return Result.failure("Store is closed");
        }
        
        try {
            db.put(writeOptions, key, value);
            return Result.success(null);
        } catch (RocksDBException e) {
            logger.error("Error putting key-value", e);
            return Result.failure("Failed to put key-value: " + e.getMessage());
        }
    }

    @Override
    public <T> Result<Void> put(T object, Function<T, byte[]> keyExtractor, Function<T, byte[]> valueSerializer) {
        if (!open.get()) {
            return Result.failure("Store is closed");
        }
        
        try {
            byte[] key = keyExtractor.apply(object);
            byte[] value = valueSerializer.apply(object);
            return put(key, value);
        } catch (Exception e) {
            logger.error("Error putting object", e);
            return Result.failure("Failed to put object: " + e.getMessage());
        }
    }

    @Override
    public Result<Void> delete(byte[] key) {
        if (!open.get()) {
            return Result.failure("Store is closed");
        }
        
        try {
            db.delete(writeOptions, key);
            return Result.success(null);
        } catch (RocksDBException e) {
            logger.error("Error deleting key", e);
            return Result.failure("Failed to delete key: " + e.getMessage());
        }
    }

    @Override
    public Result<List<byte[]>> batchGet(List<byte[]> keys) {
        if (!open.get()) {
            return Result.failure("Store is closed");
        }
        
        try {
            List<byte[]> values = db.multiGetAsList(readOptions, keys);
            return Result.success(values);
        } catch (RocksDBException e) {
            logger.error("Error in batch get", e);
            return Result.failure("Failed batch get: " + e.getMessage());
        }
    }

    @Override
    public Result<Void> batchPut(Map<byte[], byte[]> entries, boolean transactional) {
        if (!open.get()) {
            return Result.failure("Store is closed");
        }
        
        if (transactional && config.isEnableTransactions() && db instanceof TransactionDB) {
            return batchPutTransactional(entries);
        } else {
            return batchPutNonTransactional(entries);
        }
    }

    private Result<Void> batchPutTransactional(Map<byte[], byte[]> entries) {
        TransactionDB txnDb = (TransactionDB) db;
        Transaction txn = null;
        
        try {
            WriteOptions txnWriteOptions = new WriteOptions();
            txnWriteOptions.setSync(config.isSyncWrites());
            txn = txnDb.beginTransaction(txnWriteOptions);
            
            for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
                txn.put(entry.getKey(), entry.getValue());
            }
            
            txn.commit();
            return Result.success(null);
        } catch (RocksDBException e) {
            logger.error("Error in transactional batch put", e);
            if (txn != null) {
                try {
                    txn.rollback();
                } catch (RocksDBException rollbackEx) {
                    logger.error("Error rolling back transaction", rollbackEx);
                }
            }
            return Result.failure("Failed transactional batch put: " + e.getMessage());
        } finally {
            if (txn != null) {
                txn.close();
            }
        }
    }

    private Result<Void> batchPutNonTransactional(Map<byte[], byte[]> entries) {
        try (WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<byte[], byte[]> entry : entries.entrySet()) {
                batch.put(entry.getKey(), entry.getValue());
            }
            db.write(writeOptions, batch);
            return Result.success(null);
        } catch (RocksDBException e) {
            logger.error("Error in non-transactional batch put", e);
            return Result.failure("Failed batch put: " + e.getMessage());
        }
    }

    @Override
    public Result<Void> batchDelete(List<byte[]> keys, boolean transactional) {
        if (!open.get()) {
            return Result.failure("Store is closed");
        }
        
        if (transactional && config.isEnableTransactions() && db instanceof TransactionDB) {
            return batchDeleteTransactional(keys);
        } else {
            return batchDeleteNonTransactional(keys);
        }
    }

    private Result<Void> batchDeleteTransactional(List<byte[]> keys) {
        TransactionDB txnDb = (TransactionDB) db;
        Transaction txn = null;
        
        try {
            WriteOptions txnWriteOptions = new WriteOptions();
            txnWriteOptions.setSync(config.isSyncWrites());
            txn = txnDb.beginTransaction(txnWriteOptions);
            
            for (byte[] key : keys) {
                txn.delete(key);
            }
            
            txn.commit();
            return Result.success(null);
        } catch (RocksDBException e) {
            logger.error("Error in transactional batch delete", e);
            if (txn != null) {
                try {
                    txn.rollback();
                } catch (RocksDBException rollbackEx) {
                    logger.error("Error rolling back transaction", rollbackEx);
                }
            }
            return Result.failure("Failed transactional batch delete: " + e.getMessage());
        } finally {
            if (txn != null) {
                txn.close();
            }
        }
    }

    private Result<Void> batchDeleteNonTransactional(List<byte[]> keys) {
        try (WriteBatch batch = new WriteBatch()) {
            for (byte[] key : keys) {
                batch.delete(key);
            }
            db.write(writeOptions, batch);
            return Result.success(null);
        } catch (RocksDBException e) {
            logger.error("Error in non-transactional batch delete", e);
            return Result.failure("Failed batch delete: " + e.getMessage());
        }
    }

    @Override
    public Iterator<byte[]> streamValues(List<byte[]> keys) {
        return new Iterator<byte[]>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < keys.size();
            }

            @Override
            public byte[]next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                byte[] key = keys.get(index++);
                Result<byte[]> result = get(key);
                return result.getValue().orElse(null);
            }
        };
    }

    @Override
    public Iterator<byte[]> scanByPrefix(byte[] prefix) {
        RocksIterator iterator = db.newIterator(readOptions);
        iterator.seek(prefix);
        
        return new Iterator<byte[]>() {
            private boolean hasChecked = false;
            private boolean hasNextValue = false;

            @Override
            public boolean hasNext() {
                if (!hasChecked) {
                    hasChecked = true;
                    hasNextValue = iterator.isValid() && startsWith(iterator.key(), prefix);
                }
                return hasNextValue;
            }

            @Override
            public byte[] next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                byte[] value = iterator.value();
                iterator.next();
                hasChecked = false;
                return value;
            }
        };
    }

    @Override
    public Iterator<Map.Entry<byte[], byte[]>> scanByPrefixWithKeys(byte[] prefix) {
        RocksIterator iterator = db.newIterator(readOptions);
        iterator.seek(prefix);
        
        return new Iterator<Map.Entry<byte[], byte[]>>() {
            private boolean hasChecked = false;
            private boolean hasNextValue = false;

            @Override
            public boolean hasNext() {
                if (!hasChecked) {
                    hasChecked = true;
                    hasNextValue = iterator.isValid() && startsWith(iterator.key(), prefix);
                }
                return hasNextValue;
            }

            @Override
            public Map.Entry<byte[], byte[]> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Map.Entry<byte[], byte[]> entry = new AbstractMap.SimpleEntry<>(iterator.key(), iterator.value());
                iterator.next();
                hasChecked = false;
                return entry;
            }
        };
    }

    @Override
    public void consumeValues(List<byte[]> keys, Consumer<byte[]> consumer) {
        for (byte[] key : keys) {
            Result<byte[]> result = get(key);
            result.getValue().ifPresent(consumer);
        }
    }

    @Override
    public void consumePrefixValues(byte[] prefix, Consumer<byte[]> consumer) {
        try (RocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(prefix);
            while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
                consumer.accept(iterator.value());
                iterator.next();
            }
        }
    }

    @Override
    public void consumePrefixEntries(byte[] prefix, Consumer<Map.Entry<byte[], byte[]>> consumer) {
        try (RocksIterator iterator = db.newIterator(readOptions)) {
            iterator.seek(prefix);
            while (iterator.isValid() && startsWith(iterator.key(), prefix)) {
                consumer.accept(new AbstractMap.SimpleEntry<>(iterator.key(), iterator.value()));
                iterator.next();
            }
        }
    }

    @Override
    public Stream<byte[]> streamByPrefix(byte[] prefix) {
        Iterable<byte[]> iterable = () -> scanByPrefix(prefix);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    @Override
    public Stream<Map.Entry<byte[], byte[]>> streamByPrefixWithKeys(byte[] prefix) {
        Iterable<Map.Entry<byte[], byte[]>> iterable = () -> scanByPrefixWithKeys(prefix);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public long getLatestSequenceNumber() {
        return db.getLatestSequenceNumber();
    }

    /**
     * Creates a replication source for this database.
     * Enables WAL tailing for replication purposes.
     *
     * @return A ReplicationSource instance
     */
    public ReplicationSource createReplicationSource() {
        return new ReplicationSource(db, options);
    }

    @Override
    public void close() {
        if (open.compareAndSet(true, false)) {
            logger.info("Closing RocksDB at: " + config.getPath());
            
            // Close resources in reverse order
            for (int i = resources.size() - 1; i >= 0; i--) {
                try {
                    resources.get(i).close();
                } catch (Exception e) {
                    logger.error("Error closing resource", e);
                }
            }
            resources.clear();
        }
    }

    /**
     * Checks if a key starts with the given prefix.
     */
    private boolean startsWith(byte[] key, byte[] prefix) {
        if (key.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (key[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
