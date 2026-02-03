package com.hitorro.kvstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Typed wrapper around KVStore that handles serialization/deserialization automatically.
 * Supports String keys and JSON serialization for values.
 *
 * @param <V> The value type
 */
public class TypedKVStore<V> implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(TypedKVStore.class);
    
    private final KVStore store;
    private final Class<V> valueClass;
    private final ObjectMapper objectMapper;

    public TypedKVStore(KVStore store, Class<V> valueClass) {
        this.store = store;
        this.valueClass = valueClass;
        this.objectMapper = new ObjectMapper();
    }

    public TypedKVStore(KVStore store, Class<V> valueClass, ObjectMapper objectMapper) {
        this.store = store;
        this.valueClass = valueClass;
        this.objectMapper = objectMapper;
    }

    /**
     * Gets a value by String key.
     *
     * @param key The key
     * @return Result containing the deserialized value or failure
     */
    public Result<V> get(String key) {
        return store.get(keyToBytes(key)).flatMap(this::deserializeValue);
    }

    /**
     * Puts a key-value pair.
     *
     * @param key The key
     * @param value The value
     * @return Result indicating success or failure
     */
    public Result<Void> put(String key, V value) {
        Result<byte[]> serialized = serializeValue(value);
        if (serialized.isFailure()) {
            return Result.failure(serialized.getError().orElse("Serialization failed"));
        }
        return store.put(keyToBytes(key), serialized.getValue().get());
    }

    /**
     * Puts an object using a key extraction function.
     *
     * @param object The object to store
     * @param keyExtractor Function to extract the key string from the object
     * @return Result indicating success or failure
     */
    public Result<Void> put(V object, Function<V, String> keyExtractor) {
        try {
            String key = keyExtractor.apply(object);
            return put(key, object);
        } catch (Exception e) {
            return Result.failure("Key extraction failed: " + e.getMessage());
        }
    }

    /**
     * Deletes a key-value pair.
     *
     * @param key The key
     * @return Result indicating success or failure
     */
    public Result<Void> delete(String key) {
        return store.delete(keyToBytes(key));
    }

    /**
     * Batch get operation.
     *
     * @param keys The list of keys
     * @return Result containing a list of values
     */
    public Result<List<V>> batchGet(List<String> keys) {
        List<byte[]> keyBytes = keys.stream()
                .map(this::keyToBytes)
                .collect(Collectors.toList());
        
        return store.batchGet(keyBytes).flatMap(valuesList -> {
            List<V> values = new ArrayList<>();
            for (byte[] bytes : valuesList) {
                if (bytes != null) {
                    Result<V> deserialized = deserializeValue(bytes);
                    if (deserialized.isFailure()) {
                        return Result.failure(deserialized.getError().orElse("Deserialization failed"));
                    }
                    values.add(deserialized.getValue().orElse(null));
                } else {
                    values.add(null);
                }
            }
            return Result.success(values);
        });
    }

    /**
     * Batch put operation.
     *
     * @param entries The map of key-value pairs
     * @param transactional Whether to use a transaction
     * @return Result indicating success or failure
     */
    public Result<Void> batchPut(Map<String, V> entries, boolean transactional) {
        Map<byte[], byte[]> byteEntries = new HashMap<>();
        
        for (Map.Entry<String, V> entry : entries.entrySet()) {
            Result<byte[]> serialized = serializeValue(entry.getValue());
            if (serialized.isFailure()) {
                return Result.failure("Serialization failed for key: " + entry.getKey());
            }
            byteEntries.put(keyToBytes(entry.getKey()), serialized.getValue().get());
        }
        
        return store.batchPut(byteEntries, transactional);
    }

    /**
     * Batch delete operation.
     *
     * @param keys The list of keys
     * @param transactional Whether to use a transaction
     * @return Result indicating success or failure
     */
    public Result<Void> batchDelete(List<String> keys, boolean transactional) {
        List<byte[]> keyBytes = keys.stream()
                .map(this::keyToBytes)
                .collect(Collectors.toList());
        return store.batchDelete(keyBytes, transactional);
    }

    /**
     * Scans by prefix and returns an iterator of values.
     *
     * @param prefix The key prefix
     * @return An iterator over matching values
     */
    public Iterator<V> scanByPrefix(String prefix) {
        Iterator<byte[]> byteIterator = store.scanByPrefix(keyToBytes(prefix));
        
        return new Iterator<V>() {
            @Override
            public boolean hasNext() {
                return byteIterator.hasNext();
            }

            @Override
            public V next() {
                byte[] bytes = byteIterator.next();
                return deserializeValue(bytes).getValue().orElse(null);
            }
        };
    }

    /**
     * Scans by prefix and returns an iterator of key-value pairs.
     *
     * @param prefix The key prefix
     * @return An iterator over matching key-value pairs
     */
    public Iterator<Map.Entry<String, V>> scanByPrefixWithKeys(String prefix) {
        Iterator<Map.Entry<byte[], byte[]>> byteIterator = store.scanByPrefixWithKeys(keyToBytes(prefix));
        
        return new Iterator<Map.Entry<String, V>>() {
            @Override
            public boolean hasNext() {
                return byteIterator.hasNext();
            }

            @Override
            public Map.Entry<String, V> next() {
                Map.Entry<byte[], byte[]> entry = byteIterator.next();
                String key = bytesToKey(entry.getKey());
                V value = deserializeValue(entry.getValue()).getValue().orElse(null);
                return new AbstractMap.SimpleEntry<>(key, value);
            }
        };
    }

    /**
     * Consumes values matching a prefix.
     *
     * @param prefix The key prefix
     * @param consumer The consumer to process each value
     */
    public void consumePrefixValues(String prefix, Consumer<V> consumer) {
        store.consumePrefixValues(keyToBytes(prefix), bytes -> {
            deserializeValue(bytes).getValue().ifPresent(consumer);
        });
    }

    /**
     * Consumes key-value pairs matching a prefix.
     *
     * @param prefix The key prefix
     * @param consumer The consumer to process each key-value pair
     */
    public void consumePrefixEntries(String prefix, Consumer<Map.Entry<String, V>> consumer) {
        store.consumePrefixEntries(keyToBytes(prefix), entry -> {
            String key = bytesToKey(entry.getKey());
            deserializeValue(entry.getValue()).getValue().ifPresent(value -> {
                consumer.accept(new AbstractMap.SimpleEntry<>(key, value));
            });
        });
    }

    /**
     * Streams values matching a prefix using Java Stream API.
     *
     * @param prefix The key prefix
     * @return A stream of matching values
     */
    public Stream<V> streamByPrefix(String prefix) {
        Iterable<V> iterable = () -> scanByPrefix(prefix);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Streams key-value pairs matching a prefix using Java Stream API.
     *
     * @param prefix The key prefix
     * @return A stream of matching key-value pairs
     */
    public Stream<Map.Entry<String, V>> streamByPrefixWithKeys(String prefix) {
        Iterable<Map.Entry<String, V>> iterable = () -> scanByPrefixWithKeys(prefix);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    /**
     * Checks if the underlying store is open.
     *
     * @return true if open, false otherwise
     */
    public boolean isOpen() {
        return store.isOpen();
    }

    /**
     * Gets the underlying KVStore.
     *
     * @return The underlying KVStore
     */
    public KVStore getUnderlyingStore() {
        return store;
    }

    @Override
    public void close() {
        store.close();
    }

    private byte[] keyToBytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private String bytesToKey(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private Result<byte[]> serializeValue(V value) {
        try {
            byte[] bytes = objectMapper.writeValueAsBytes(value);
            return Result.success(bytes);
        } catch (IOException e) {
            logger.error("Failed to serialize value", e);
            return Result.failure("Serialization failed: " + e.getMessage());
        }
    }

    private Result<V> deserializeValue(byte[] bytes) {
        try {
            V value = objectMapper.readValue(bytes, valueClass);
            return Result.success(value);
        } catch (IOException e) {
            logger.error("Failed to deserialize value", e);
            return Result.failure("Deserialization failed: " + e.getMessage());
        }
    }
}
