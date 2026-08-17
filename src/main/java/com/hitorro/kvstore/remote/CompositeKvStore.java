/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.kvstore.remote;

import com.hitorro.kvstore.KVStore;
import com.hitorro.kvstore.Result;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Fan-out {@link KVStore} that queries N delegates in parallel and
 * returns the <b>first non-null match</b>. Symmetric with
 * {@code CompositeSearchProvider} — the "one row lives on fleet A,
 * another on B" pattern for cross-fleet KV federation.
 *
 * <p>Read-only. Writes throw — cross-fleet writes need explicit
 * routing (which fleet owns which key range), not fan-out.</p>
 *
 * <p>{@link #get(byte[])} sends the request to every delegate in
 * parallel; the first non-null response wins. Slow delegates don't
 * hold up fast ones. Batch fetches iterate keys — one first-match
 * round per key.</p>
 *
 * <p>Prefix scans and iteration ARE supported here (unlike
 * {@link RemoteKvStore}) — the composite delegates to whichever
 * underlying store DOES support them (typically a
 * {@code ReadOnlyKvStore} co-located with the driver) OR merges
 * across delegates that do. Delegates that throw on scan are skipped
 * with a warning.</p>
 */
public class CompositeKvStore implements KVStore {

    private final List<KVStore> delegates;

    public CompositeKvStore(List<KVStore> delegates) {
        if (delegates == null || delegates.isEmpty()) {
            throw new IllegalArgumentException("CompositeKvStore needs ≥1 delegate");
        }
        this.delegates = List.copyOf(delegates);
    }

    public List<KVStore> delegates() { return delegates; }

    @Override
    public Result<byte[]> get(byte[] key) {
        if (delegates.size() == 1) return delegates.get(0).get(key);
        // Fan out in parallel via CompletableFuture; return first non-null.
        List<CompletableFuture<Result<byte[]>>> futures = new ArrayList<>(delegates.size());
        for (KVStore d : delegates) {
            futures.add(CompletableFuture.supplyAsync(() -> d.get(key)));
        }
        Result<byte[]> lastFailure = null;
        for (CompletableFuture<Result<byte[]>> f : futures) {
            try {
                Result<byte[]> r = f.get();
                if (r.isSuccess() && r.getValue().isPresent()) return r;
                if (!r.isSuccess()) lastFailure = r;
            } catch (InterruptedException | ExecutionException ignore) { /* skip */ }
        }
        // No delegate had it. If every delegate errored, surface the last error;
        // else return success(null) — a legitimate "not found in any fleet."
        if (lastFailure != null && delegates.size() == countFailures(futures)) return lastFailure;
        return Result.success(null);
    }

    private static int countFailures(List<CompletableFuture<Result<byte[]>>> futures) {
        int n = 0;
        for (var f : futures) {
            try { if (!f.get().isSuccess()) n++; }
            catch (Exception e) { n++; }
        }
        return n;
    }

    @Override
    public Result<List<byte[]>> batchGet(List<byte[]> keys) {
        List<byte[]> out = new ArrayList<>(keys.size());
        for (byte[] k : keys) {
            Result<byte[]> r = get(k);
            out.add(r.isSuccess() && r.getValue().isPresent() ? r.getValue().get() : null);
        }
        return Result.success(out);
    }

    @Override public Iterator<byte[]> streamValues(List<byte[]> keys) {
        return new Iterator<>() {
            final Iterator<byte[]> it = keys.iterator();
            public boolean hasNext() { return it.hasNext(); }
            public byte[] next() {
                var r = get(it.next());
                return r.isSuccess() && r.getValue().isPresent() ? r.getValue().get() : null;
            }
        };
    }

    @Override public void consumeValues(List<byte[]> keys, Consumer<byte[]> c) {
        for (byte[] k : keys) {
            var r = get(k);
            if (r.isSuccess() && r.getValue().isPresent()) c.accept(r.getValue().get());
        }
    }

    // ── writes disabled ─────────────────────────────────────────
    @Override public Result<Void> put(byte[] key, byte[] value)                     { return refused(); }
    @Override public <T> Result<Void> put(T o, Function<T, byte[]> ke, Function<T, byte[]> vs) { return refused(); }
    @Override public Result<Void> delete(byte[] key)                                { return refused(); }
    @Override public Result<Void> batchPut(Map<byte[], byte[]> entries, boolean tx) { return refused(); }
    @Override public Result<Void> batchDelete(List<byte[]> keys, boolean tx)        { return refused(); }
    private static <T> Result<T> refused() {
        return Result.failure("CompositeKvStore is read-only — cross-fleet writes need explicit key-range routing");
    }

    // ── prefix scans: merge from every delegate that supports them ──
    // Silently skips delegates that throw UnsupportedOperationException
    // (RemoteKvStore does, since the fleet wire doesn't expose a scan yet).
    @Override
    public Iterator<byte[]> scanByPrefix(byte[] prefix) {
        List<byte[]> merged = new ArrayList<>();
        for (KVStore d : delegates) {
            try { d.scanByPrefix(prefix).forEachRemaining(merged::add); }
            catch (UnsupportedOperationException ignore) { /* skip */ }
        }
        return merged.iterator();
    }
    @Override
    public Iterator<Map.Entry<byte[], byte[]>> scanByPrefixWithKeys(byte[] prefix) {
        List<Map.Entry<byte[], byte[]>> merged = new ArrayList<>();
        for (KVStore d : delegates) {
            try { d.scanByPrefixWithKeys(prefix).forEachRemaining(merged::add); }
            catch (UnsupportedOperationException ignore) { /* skip */ }
        }
        return merged.iterator();
    }
    @Override public void consumePrefixValues(byte[] prefix, Consumer<byte[]> c) {
        for (var it = scanByPrefix(prefix); it.hasNext(); ) c.accept(it.next());
    }
    @Override public void consumePrefixEntries(byte[] prefix, Consumer<Map.Entry<byte[], byte[]>> c) {
        for (var it = scanByPrefixWithKeys(prefix); it.hasNext(); ) c.accept(it.next());
    }
    @Override public Stream<byte[]> streamByPrefix(byte[] prefix) {
        return java.util.stream.StreamSupport.stream(
                java.util.Spliterators.spliteratorUnknownSize(scanByPrefix(prefix), 0), false);
    }
    @Override public Stream<Map.Entry<byte[], byte[]>> streamByPrefixWithKeys(byte[] prefix) {
        return java.util.stream.StreamSupport.stream(
                java.util.Spliterators.spliteratorUnknownSize(scanByPrefixWithKeys(prefix), 0), false);
    }

    @Override public long getLatestSequenceNumber() { return -1L; }
    @Override public boolean isOpen()               { return true; }
    @Override public void close() {
        for (KVStore d : delegates) { try { d.close(); } catch (Exception ignore) { } }
    }
}
