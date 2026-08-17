/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.util.core.iterator.sinks.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hitorro.util.core.iterator.sinks.JsonNodeSinkBase;
import com.hitorro.util.io.StoreException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RocksDB-backed KV sink for JSON rows. Extracts a key from each
 * incoming row via a dotted-path expression; value is the row's JSON
 * serialisation in UTF-8.
 *
 * <p>Lives in {@code hitorro-kvstore} — any caller with a
 * {@code Sink<JsonNode>} pipeline and RocksDB on the classpath can use
 * this without adopting any pipeline framework.</p>
 *
 * <p>Supports exactly-once via {@link #addIdempotent(String, long, JsonNode)}
 * — tracks the highest {@code seq} per {@code taskId} and skips
 * earlier re-plays. See {@link com.hitorro.util.core.iterator.sinks.Sink#addIdempotent}.</p>
 *
 * <p>Uses {@link RocksDbSharedRegistry} for handle sharing so multiple
 * sinks + sources on the same physical DB in one JVM don't fight for
 * the exclusive lock.</p>
 */
public class KvStoreSink extends JsonNodeSinkBase {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final String name;
    private final String keyExpr;
    private final Path   dir;

    private RocksDbSharedRegistry.Entry entry;

    /**
     * Per-task highest-seq-seen for exactly-once dedup. Retried tasks
     * with lower seqs are skipped in {@link #addIdempotent(String, long, JsonNode)}.
     * Ephemeral to this sink instance.
     */
    private final Map<String, Long> taskSeqSeen = new ConcurrentHashMap<>();

    public KvStoreSink(String name, String keyExpr, Path home) {
        this.name    = name;
        this.keyExpr = keyExpr == null ? "id" : keyExpr;
        this.dir     = home.resolve("kv").resolve(name);
    }

    public String name()      { return name; }
    public String keyExpr()   { return keyExpr; }
    public Path physicalDir() { return dir; }

    @Override
    public boolean start() throws IOException {
        entry = RocksDbSharedRegistry.acquire(name, dir);
        return true;
    }

    @Override
    protected void writeRow(JsonNode row) throws IOException {
        if (entry == null) start();
        String key = extractKey(row, keyExpr);
        try {
            entry.db.put(key.getBytes(StandardCharsets.UTF_8),
                         JSON.writeValueAsBytes(row));
        } catch (Exception e) {
            throw new IOException("rocksdb put " + name + " key=" + key + ": " + e.getMessage(), e);
        }
    }

    @Override
    public boolean addIdempotent(String taskId, long seq, JsonNode row) throws IOException, StoreException {
        if (taskId == null) return add(row);
        Long prev = taskSeqSeen.get(taskId);
        if (prev != null && prev >= seq) return true;   // already processed
        taskSeqSeen.put(taskId, seq);
        return add(row);
    }

    @Override
    public void close() throws IOException {
        if (entry != null) { RocksDbSharedRegistry.release(entry); entry = null; }
    }

    private static String extractKey(JsonNode row, String path) {
        JsonNode cur = row;
        for (String seg : path.split("\\.")) {
            if (cur == null || cur.isNull()) return "<null>";
            cur = cur.get(seg);
        }
        return (cur == null || cur.isNull()) ? "<null>" : cur.asText();
    }
}
