/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.util.core.iterator.sources.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.util.core.iterator.sinks.kvstore.RocksDbSharedRegistry;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Iterates every value in a named RocksDB KV store as {@link JsonNode}
 * rows. Enables the classic "one job writes into the KV, a downstream
 * job reads back" pattern without any pipeline framework.
 *
 * <p>Lives in {@code hitorro-kvstore} — symmetric with
 * {@link com.hitorro.util.core.iterator.sinks.kvstore.KvStoreSink}. Uses
 * {@link RocksDbSharedRegistry} so a sink and source hitting the same
 * physical DB in one JVM don't fight the exclusive lock.</p>
 */
public final class KvStoreSource implements Iterator<JsonNode>, AutoCloseable {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final RocksDbSharedRegistry.Entry entry;
    private final RocksIterator it;

    public KvStoreSource(String name, Path home) throws IOException {
        this.entry = RocksDbSharedRegistry.acquire(name,
                home.resolve("kv").resolve(name));
        this.it = entry.db.newIterator();
        this.it.seekToFirst();
    }

    @Override
    public boolean hasNext() {
        return it.isValid();
    }

    @Override
    public JsonNode next() {
        if (!it.isValid()) throw new java.util.NoSuchElementException();
        byte[] v = it.value();
        try {
            JsonNode row = JSON.readTree(v);
            it.next();
            return row;
        } catch (IOException e) {
            throw new RuntimeException("bad kv value: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try { it.close(); } catch (Exception ignored) { }
        RocksDbSharedRegistry.release(entry);
    }
}
