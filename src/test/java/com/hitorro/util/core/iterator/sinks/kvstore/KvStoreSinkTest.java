/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.util.core.iterator.sinks.kvstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct coverage for {@link KvStoreSink} + {@link RocksDbSharedRegistry}.
 * The pipelines-kvstore round-trip test proves the sink writes bytes
 * that the source can read; this suite pins the semantics a caller
 * depends on but which aren't obvious from a round-trip:
 *
 * <ul>
 *   <li>Key extraction from a dotted-path (walks nested objects)</li>
 *   <li>Missing / null key → "&lt;null&gt;" sentinel so nothing crashes</li>
 *   <li>Idempotent write dedupes by (taskId, seq) — retried mappers
 *       don't double-write</li>
 *   <li>Shared registry refcounts handles — two sinks on the same
 *       name in one JVM share a single RocksDB handle instead of
 *       fighting for the lock</li>
 * </ul>
 */
class KvStoreSinkTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    // -------------------------------------------------- Basic write + key

    @Test
    void write_thenReadBack_viaSource(@TempDir Path home) throws Exception {
        try (KvStoreSink sink = new KvStoreSink("cities", "iso3", home)) {
            sink.start();
            sink.add(row("iso3", "USA", "name", "United States"));
            sink.add(row("iso3", "CHN", "name", "China"));
            assertThat(sink.count()).isEqualTo(2);
        }
        try (var src = new com.hitorro.util.core.iterator.sources.kvstore.KvStoreSource("cities", home)) {
            java.util.List<JsonNode> read = new java.util.ArrayList<>();
            while (src.hasNext()) read.add(src.next());
            assertThat(read).hasSize(2);
            assertThat(read).extracting(r -> r.get("iso3").asText())
                    .containsExactlyInAnyOrder("USA", "CHN");
        }
    }

    @Test
    void keyExtraction_dottedPath_walksNestedObject(@TempDir Path home) throws Exception {
        // Prove the sink extracts a nested key correctly. If we can
        // put a row keyed on user.email and get it back with the same
        // key layout, the path-walker works end-to-end.
        try (KvStoreSink sink = new KvStoreSink("users", "user.email", home)) {
            sink.start();
            ObjectNode row = JSON.createObjectNode();
            row.putObject("user").put("email", "alice@example.com").put("name", "Alice");
            sink.add(row);
        }
        // Read the raw bytes at the expected key.
        var entry = RocksDbSharedRegistry.acquire("users", home.resolve("kv").resolve("users"));
        try {
            byte[] value = entry.db.get("alice@example.com".getBytes(StandardCharsets.UTF_8));
            assertThat(value).isNotNull();
            JsonNode stored = JSON.readTree(value);
            assertThat(stored.get("user").get("name").asText()).isEqualTo("Alice");
        } finally {
            RocksDbSharedRegistry.release(entry);
        }
    }

    @Test
    void keyExtraction_missingField_writesUnderNullSentinel(@TempDir Path home) throws Exception {
        // A row missing the key-expr field falls back to "<null>" —
        // documenting the behaviour so a spec bug (wrong keyExpr) is
        // visible as "everything under <null>" rather than a crash.
        try (KvStoreSink sink = new KvStoreSink("s", "user.missing", home)) {
            sink.start();
            sink.add(row("user", "not-object"));
        }
        var entry = RocksDbSharedRegistry.acquire("s", home.resolve("kv").resolve("s"));
        try {
            byte[] value = entry.db.get("<null>".getBytes(StandardCharsets.UTF_8));
            assertThat(value).isNotNull();
        } finally {
            RocksDbSharedRegistry.release(entry);
        }
    }

    @Test
    void keyExtraction_defaultsToId_whenKeyExprNull(@TempDir Path home) throws Exception {
        // KvStoreSink documents keyExpr==null → uses "id".
        try (KvStoreSink sink = new KvStoreSink("s", null, home)) {
            sink.start();
            sink.add(row("id", "u-42", "n", "n42"));
        }
        var entry = RocksDbSharedRegistry.acquire("s", home.resolve("kv").resolve("s"));
        try {
            byte[] v = entry.db.get("u-42".getBytes(StandardCharsets.UTF_8));
            assertThat(v).isNotNull();
        } finally {
            RocksDbSharedRegistry.release(entry);
        }
    }

    @Test
    void sameKeyTwice_secondWriteOverwrites(@TempDir Path home) throws Exception {
        // Not a bug — documented put semantics. Idempotent-mode is the
        // right knob when retries must be no-ops (see below).
        try (KvStoreSink sink = new KvStoreSink("s", "id", home)) {
            sink.start();
            sink.add(row("id", "k", "v", "first"));
            sink.add(row("id", "k", "v", "second"));
        }
        var entry = RocksDbSharedRegistry.acquire("s", home.resolve("kv").resolve("s"));
        try {
            byte[] v = entry.db.get("k".getBytes(StandardCharsets.UTF_8));
            assertThat(JSON.readTree(v).get("v").asText()).isEqualTo("second");
        } finally {
            RocksDbSharedRegistry.release(entry);
        }
    }

    // -------------------------------------------------- addIdempotent

    @Test
    void idempotent_sameTaskSameSeq_skipsSecondWrite(@TempDir Path home) throws Exception {
        // The exactly-once contract: mesh retries a task with the same
        // (taskId, seq) → sink recognises + skips the replay so the
        // downstream state doesn't get double-counted.
        try (KvStoreSink sink = new KvStoreSink("s", "id", home)) {
            sink.start();
            sink.addIdempotent("t-1", 5L, row("id", "k", "v", "first"));
            sink.addIdempotent("t-1", 5L, row("id", "k", "v", "replay"));
            // Count reflects one accepted write (replay was skipped
            // before the underlying `add`; JsonNodeSinkBase.add is what
            // bumps the counter).
            assertThat(sink.count()).isEqualTo(1);
        }
        var entry = RocksDbSharedRegistry.acquire("s", home.resolve("kv").resolve("s"));
        try {
            byte[] v = entry.db.get("k".getBytes(StandardCharsets.UTF_8));
            assertThat(JSON.readTree(v).get("v").asText()).isEqualTo("first");
        } finally {
            RocksDbSharedRegistry.release(entry);
        }
    }

    @Test
    void idempotent_higherSeqAfterLower_processed(@TempDir Path home) throws Exception {
        // seq monotonic within a task — a higher seq means new work,
        // must be processed.
        try (KvStoreSink sink = new KvStoreSink("s", "id", home)) {
            sink.start();
            sink.addIdempotent("t-1", 1L, row("id", "k", "v", "one"));
            sink.addIdempotent("t-1", 2L, row("id", "k", "v", "two"));
            sink.addIdempotent("t-1", 3L, row("id", "k", "v", "three"));
            assertThat(sink.count()).isEqualTo(3);
        }
        var entry = RocksDbSharedRegistry.acquire("s", home.resolve("kv").resolve("s"));
        try {
            byte[] v = entry.db.get("k".getBytes(StandardCharsets.UTF_8));
            assertThat(JSON.readTree(v).get("v").asText()).isEqualTo("three");
        } finally {
            RocksDbSharedRegistry.release(entry);
        }
    }

    @Test
    void idempotent_lowerSeqAfterHigher_skipped(@TempDir Path home) throws Exception {
        // Out-of-order retry: driver retries seq=1 after seq=3 landed.
        // Sink already saw 3, so 1 is a duplicate → skip.
        try (KvStoreSink sink = new KvStoreSink("s", "id", home)) {
            sink.start();
            sink.addIdempotent("t-1", 3L, row("id", "k", "v", "third"));
            sink.addIdempotent("t-1", 1L, row("id", "k", "v", "stale"));  // skipped
            assertThat(sink.count()).isEqualTo(1);
        }
        var entry = RocksDbSharedRegistry.acquire("s", home.resolve("kv").resolve("s"));
        try {
            byte[] v = entry.db.get("k".getBytes(StandardCharsets.UTF_8));
            assertThat(JSON.readTree(v).get("v").asText()).isEqualTo("third");
        } finally {
            RocksDbSharedRegistry.release(entry);
        }
    }

    @Test
    void idempotent_differentTasks_dedupIndependently(@TempDir Path home) throws Exception {
        // taskA seq=5 and taskB seq=5 are independent — both write.
        try (KvStoreSink sink = new KvStoreSink("s", "id", home)) {
            sink.start();
            sink.addIdempotent("t-a", 5L, row("id", "k1", "src", "a"));
            sink.addIdempotent("t-b", 5L, row("id", "k2", "src", "b"));
            assertThat(sink.count()).isEqualTo(2);
        }
    }

    @Test
    void idempotent_nullTaskId_fallsBackToPlainAdd(@TempDir Path home) throws Exception {
        // taskId null → no dedup, every call goes through as add().
        try (KvStoreSink sink = new KvStoreSink("s", "id", home)) {
            sink.start();
            sink.addIdempotent(null, 5L, row("id", "k", "v", "first"));
            sink.addIdempotent(null, 5L, row("id", "k", "v", "second"));
            assertThat(sink.count()).isEqualTo(2);
        }
    }

    // -------------------------------------------------- Handle sharing

    @Test
    void sharedRegistry_twoSinksOnSameName_shareOneHandle(@TempDir Path home) throws Exception {
        // The whole reason RocksDbSharedRegistry exists: RocksDB takes
        // an exclusive lock on its data dir. Two concurrent KvStoreSinks
        // on the same physical DB in one JVM would fail without sharing.
        Path dir = home.resolve("kv").resolve("shared");

        var e1 = RocksDbSharedRegistry.acquire("shared", dir);
        var e2 = RocksDbSharedRegistry.acquire("shared", dir);
        try {
            assertThat(e1).isSameAs(e2);        // same Entry, same db
            assertThat(e1.db).isSameAs(e2.db);
        } finally {
            RocksDbSharedRegistry.release(e1);
            RocksDbSharedRegistry.release(e2);
        }

        // After the second release the entry is gone — a fresh acquire
        // opens a new handle.
        var e3 = RocksDbSharedRegistry.acquire("shared", dir);
        try {
            assertThat(e3).isNotSameAs(e1);
        } finally {
            RocksDbSharedRegistry.release(e3);
        }
    }

    @Test
    void sharedRegistry_refcountedLifecycle(@TempDir Path home) throws Exception {
        // Prove the refcount handles a fanout: acquire 3 times, release
        // 3 times. The DB stays open across the intermediate release
        // (refs > 0) and only closes on the last one.
        Path dir = home.resolve("kv").resolve("rc");
        var a = RocksDbSharedRegistry.acquire("rc", dir);
        var b = RocksDbSharedRegistry.acquire("rc", dir);
        var c = RocksDbSharedRegistry.acquire("rc", dir);
        try {
            RocksDbSharedRegistry.release(a);
            RocksDbSharedRegistry.release(b);
            // Still open — c is still using it. Prove by writing.
            c.db.put("x".getBytes(), "y".getBytes());
            assertThat(new String(c.db.get("x".getBytes()))).isEqualTo("y");
        } finally {
            RocksDbSharedRegistry.release(c);
        }
        // Now closed. Reopen from scratch — fresh handle.
        var reopened = RocksDbSharedRegistry.acquire("rc", dir);
        try {
            // The persistent bytes survive the close/reopen.
            assertThat(new String(reopened.db.get("x".getBytes()))).isEqualTo("y");
        } finally {
            RocksDbSharedRegistry.release(reopened);
        }
    }

    @Test
    void sharedRegistry_concurrent_acquires_getSameEntry(@TempDir Path home) throws Exception {
        // Race-safety smoke test: 8 threads each acquire the same name
        // simultaneously. Every thread must see the same Entry, and the
        // final refcount cleanup must fully close.
        Path dir = home.resolve("kv").resolve("conc");
        int nThreads = 8;
        var entries = new RocksDbSharedRegistry.Entry[nThreads];
        var started = new java.util.concurrent.CountDownLatch(nThreads);
        var proceed = new java.util.concurrent.CountDownLatch(1);
        var done    = new java.util.concurrent.CountDownLatch(nThreads);
        AtomicBoolean anyError = new AtomicBoolean(false);

        for (int i = 0; i < nThreads; i++) {
            int idx = i;
            new Thread(() -> {
                try {
                    started.countDown();
                    proceed.await();
                    entries[idx] = RocksDbSharedRegistry.acquire("conc", dir);
                } catch (Exception e) { anyError.set(true); }
                finally { done.countDown(); }
            }).start();
        }
        started.await();
        proceed.countDown();
        done.await();

        assertThat(anyError.get()).isFalse();
        // Every entry is the SAME (or all null on failure, filtered by
        // isSameAs below).
        for (int i = 1; i < nThreads; i++) {
            assertThat(entries[i]).isSameAs(entries[0]);
        }
        // Release every acquire so the DB closes cleanly for the next test.
        for (var e : entries) RocksDbSharedRegistry.release(e);
    }

    // ------------------------------------------------------------ helpers

    private static ObjectNode row(Object... kv) {
        ObjectNode n = JSON.createObjectNode();
        for (int i = 0; i < kv.length; i += 2) {
            String k = (String) kv[i];
            Object v = kv[i + 1];
            if (v == null) n.putNull(k);
            else if (v instanceof Integer x) n.put(k, x);
            else n.put(k, String.valueOf(v));
        }
        return n;
    }
}
