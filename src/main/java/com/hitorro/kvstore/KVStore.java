package com.hitorro.kvstore;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Key-value store interface providing various access patterns.
 * All methods use the Result type for error handling without exceptions.
 */
public interface KVStore extends AutoCloseable {

    /**
     * Gets a value by key.
     *
     * @param key The key
     * @return Result containing the value or failure
     */
    Result<byte[]> get(byte[] key);

    /**
     * Puts a key-value pair.
     *
     * @param key The key
     * @param value The value
     * @return Result indicating success or failure
     */
    Result<Void> put(byte[] key, byte[] value);

    /**
     * Puts an object using a key extraction function.
     *
     * @param object The object to store
     * @param keyExtractor Function to extract the key from the object
     * @param valueSerializer Function to serialize the object to bytes
     * @param <T> The object type
     * @return Result indicating success or failure
     */
    <T> Result<Void> put(T object, Function<T, byte[]> keyExtractor, Function<T, byte[]> valueSerializer);

    /**
     * Deletes a key-value pair.
     *
     * @param key The key
     * @return Result indicating success or failure
     */
    Result<Void> delete(byte[] key);

    /**
     * Batch get operation.
     *
     * @param keys The list of keys
     * @return Result containing a list of values (null for missing keys)
     */
    Result<List<byte[]>> batchGet(List<byte[]> keys);

    /**
     * Batch put operation.
     *
     * @param entries The map of key-value pairs
     * @param transactional Whether to use a transaction (all or nothing)
     * @return Result indicating success or failure
     */
    Result<Void> batchPut(Map<byte[], byte[]> entries, boolean transactional);

    /**
     * Batch delete operation.
     *
     * @param keys The list of keys to delete
     * @param transactional Whether to use a transaction (all or nothing)
     * @return Result indicating success or failure
     */
    Result<Void> batchDelete(List<byte[]> keys, boolean transactional);

    /**
     * Streams values for a list of keys using an iterator.
     *
     * @param keys The list of keys
     * @return An iterator over the values
     */
    Iterator<byte[]> streamValues(List<byte[]> keys);

    /**
     * Scans by prefix and returns an iterator of values.
     *
     * @param prefix The key prefix
     * @return An iterator over matching values
     */
    Iterator<byte[]> scanByPrefix(byte[] prefix);

    /**
     * Scans by prefix and returns an iterator of key-value pairs.
     *
     * @param prefix The key prefix
     * @return An iterator over matching key-value pairs
     */
    Iterator<Map.Entry<byte[], byte[]>> scanByPrefixWithKeys(byte[] prefix);

    /**
     * Consumes values for a list of keys.
     *
     * @param keys The list of keys
     * @param consumer The consumer to process each value
     */
    void consumeValues(List<byte[]> keys, Consumer<byte[]> consumer);

    /**
     * Consumes values matching a prefix.
     *
     * @param prefix The key prefix
     * @param consumer The consumer to process each value
     */
    void consumePrefixValues(byte[] prefix, Consumer<byte[]> consumer);

    /**
     * Consumes key-value pairs matching a prefix.
     *
     * @param prefix The key prefix
     * @param consumer The consumer to process each key-value pair
     */
    void consumePrefixEntries(byte[] prefix, Consumer<Map.Entry<byte[], byte[]>> consumer);

    /**
     * Streams values matching a prefix using Java Stream API.
     *
     * @param prefix The key prefix
     * @return A stream of matching values
     */
    Stream<byte[]> streamByPrefix(byte[] prefix);

    /**
     * Streams key-value pairs matching a prefix using Java Stream API.
     *
     * @param prefix The key prefix
     * @return A stream of matching key-value pairs
     */
    Stream<Map.Entry<byte[], byte[]>> streamByPrefixWithKeys(byte[] prefix);

    /**
     * Checks if the store is open.
     *
     * @return true if open, false otherwise
     */
    boolean isOpen();

    /**
     * Gets the latest sequence number (for replication).
     *
     * @return The latest sequence number
     */
    long getLatestSequenceNumber();

    /**
     * Closes the store and releases resources.
     */
    @Override
    void close();
}
