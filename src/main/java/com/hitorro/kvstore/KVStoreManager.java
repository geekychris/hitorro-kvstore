package com.hitorro.kvstore;

import org.apache.log4j.Logger;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages multiple KVStore instances.
 * Provides lifecycle management and ensures proper resource cleanup.
 */
public class KVStoreManager implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(KVStoreManager.class);
    
    private final ConcurrentHashMap<String, KVStore> stores = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public KVStoreManager() {
        // Register shutdown hook for cleanup
        Runtime.getRuntime().addShutdownHook(new Thread(this::closeAll));
    }

    /**
     * Opens or creates a database with the given name and configuration.
     *
     * @param name The database name
     * @param config The database configuration
     * @return Result containing the KVStore or failure
     */
    public Result<KVStore> openDatabase(String name, DatabaseConfig config) {
        if (closed) {
            return Result.failure("Manager is closed");
        }

        if (name == null || name.isEmpty()) {
            return Result.failure("Database name cannot be null or empty");
        }

        if (stores.containsKey(name)) {
            return Result.failure("Database already open: " + name);
        }

        try {
            KVStore store = new RocksDBStore(config);
            stores.put(name, store);
            logger.info("Opened database: " + name);
            return Result.success(store);
        } catch (RocksDBException e) {
            logger.error("Failed to open database: " + name, e);
            return Result.failure("Failed to open database: " + e.getMessage());
        }
    }

    /**
     * Gets an already opened database by name.
     *
     * @param name The database name
     * @return Result containing the KVStore or failure if not found
     */
    public Result<KVStore> getDatabase(String name) {
        if (closed) {
            return Result.failure("Manager is closed");
        }

        KVStore store = stores.get(name);
        if (store == null) {
            return Result.failure("Database not found: " + name);
        }
        return Result.success(store);
    }

    /**
     * Closes a database by name.
     *
     * @param name The database name
     * @return Result indicating success or failure
     */
    public Result<Void> closeDatabase(String name) {
        if (name == null || name.isEmpty()) {
            return Result.failure("Database name cannot be null or empty");
        }

        KVStore store = stores.remove(name);
        if (store == null) {
            return Result.failure("Database not found: " + name);
        }

        try {
            store.close();
            logger.info("Closed database: " + name);
            return Result.success(null);
        } catch (Exception e) {
            logger.error("Error closing database: " + name, e);
            return Result.failure("Failed to close database: " + e.getMessage());
        }
    }

    /**
     * Lists all currently open databases.
     *
     * @return List of database names
     */
    public List<String> listDatabases() {
        return new ArrayList<>(stores.keySet());
    }

    /**
     * Checks if a database is open.
     *
     * @param name The database name
     * @return true if open, false otherwise
     */
    public boolean isDatabaseOpen(String name) {
        return stores.containsKey(name);
    }

    /**
     * Gets the number of open databases.
     *
     * @return The number of open databases
     */
    public int getDatabaseCount() {
        return stores.size();
    }

    /**
     * Closes all open databases.
     */
    public void closeAll() {
        if (closed) {
            return;
        }

        logger.info("Closing all databases (" + stores.size() + " total)");
        
        for (String name : new ArrayList<>(stores.keySet())) {
            closeDatabase(name);
        }
        
        stores.clear();
        closed = true;
    }

    /**
     * Checks if the manager is closed.
     *
     * @return true if closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closeAll();
    }
}
