/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.kvstore.remote;

import com.hitorro.kvstore.KVStore;
import com.hitorro.kvstore.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * HTTP client that implements {@link KVStore} against a remote
 * {@code hitorro-fleet-retrieval}'s document-fetch endpoint
 * ({@code GET /api/retrieval/documents/{indexName}/{key}}).
 *
 * <p>Symmetric with {@code RemoteSearchProvider} — one class wraps one
 * remote instance's KV. Compose N of these with a caller-chosen strategy
 * via {@link com.hitorro.kvstore.remote.CompositeKvStore} (first-match
 * for federated fetch) when your data is sharded across multiple
 * fleet-retrieval instances.</p>
 *
 * <p>Read-only. Write ops throw {@link UnsupportedOperationException}
 * — the fleet's KV wire endpoint doesn't accept writes; the primary
 * pipeline writer owns the physical KV.</p>
 *
 * <p>Prefix scans / iteration also throw — the current wire endpoint
 * is key-lookup only. When cross-fleet iteration matters, add a
 * {@code GET /api/retrieval/kv/{indexName}/scan?prefix=…} endpoint on
 * fleet-retrieval and extend this client to consume it.</p>
 */
public class RemoteKvStore implements KVStore {

    private static final Logger log = LoggerFactory.getLogger(RemoteKvStore.class);

    private final String baseUrl;         // e.g. http://fleet-retrieval-0.fleet-retrieval:8087
    private final String indexName;       // KV name (fleet convention: same as the Lucene index)
    private final HttpClient http;
    private final Duration requestTimeout;

    public RemoteKvStore(String baseUrl, String indexName) {
        this(baseUrl, indexName, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build(),
                Duration.ofSeconds(5));
    }

    public RemoteKvStore(String baseUrl, String indexName, HttpClient http, Duration requestTimeout) {
        this.baseUrl = stripTrailingSlash(baseUrl);
        this.indexName = indexName;
        this.http = http;
        this.requestTimeout = requestTimeout;
    }

    public String baseUrl()   { return baseUrl; }
    public String indexName() { return indexName; }

    @Override
    public Result<byte[]> get(byte[] key) {
        try {
            String encKey = URLEncoder.encode(new String(key, StandardCharsets.UTF_8), StandardCharsets.UTF_8);
            String url = baseUrl + "/api/retrieval/documents/" + indexName + "/" + encKey;
            HttpResponse<byte[]> resp = http.send(HttpRequest.newBuilder(URI.create(url))
                            .timeout(requestTimeout).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() == 404) return Result.success(null);
            if (resp.statusCode() >= 200 && resp.statusCode() < 300) return Result.success(resp.body());
            return Result.failure("HTTP " + resp.statusCode() + " from " + url);
        } catch (Exception e) {
            log.debug("RemoteKvStore.get {}: {}", new String(key), e.getMessage());
            return Result.failure(e.getMessage());
        }
    }

    @Override
    public Result<List<byte[]>> batchGet(List<byte[]> keys) {
        // No batch endpoint on the fleet — fan out per-key. Cheap enough
        // for small batches; N should be modest. Callers with big lists
        // should use consumeValues() for streaming.
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
            Result<byte[]> r = get(k);
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
        return Result.failure("RemoteKvStore is read-only — the primary pipeline writer owns the physical KV");
    }

    // ── prefix scans via GET /api/retrieval/kv/{index}/scan?prefix=… ────
    // Fleet responds with NDJson: one {"key":"…","value":{…}} per line.
    // Optional limit (default 1000) prevents runaway scans; caller can
    // paginate via a follow-up prefix if the returned page hits the cap.

    /**
     * Max entries pulled per {@code scanByPrefix*} call. Configurable
     * per-instance for large partitions.
     */
    private int scanPageLimit = 10_000;
    public void setScanPageLimit(int n) { this.scanPageLimit = n; }

    @Override public Iterator<byte[]> scanByPrefix(byte[] prefix) {
        return new ScanIterator<>(fetchScanPage(prefix), true);
    }
    @Override public Iterator<Map.Entry<byte[], byte[]>> scanByPrefixWithKeys(byte[] prefix) {
        return new ScanIterator<>(fetchScanPage(prefix), false);
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

    /** Pull one page of scan results from the fleet. Best-effort — empty list on error. */
    private java.util.List<byte[]> fetchScanPage(byte[] prefix) {
        try {
            String encPrefix = URLEncoder.encode(new String(prefix, StandardCharsets.UTF_8), StandardCharsets.UTF_8);
            String url = baseUrl + "/api/retrieval/kv/" + indexName + "/scan"
                    + "?prefix=" + encPrefix + "&limit=" + scanPageLimit;
            HttpResponse<byte[]> resp = http.send(HttpRequest.newBuilder(URI.create(url))
                            .timeout(requestTimeout).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) return java.util.List.of();
            return java.util.List.of(resp.body());
        } catch (Exception e) {
            log.debug("RemoteKvStore.scan {}: {}", new String(prefix), e.getMessage());
            return java.util.List.of();
        }
    }

    /**
     * Parses the fleet's NDJson scan response into either raw values
     * (values-only iteration) or {@code Map.Entry<byte[], byte[]>}
     * (keys-and-values). Single-page today — extend to multi-page when
     * the fleet endpoint gains cursor-style pagination.
     */
    private static final class ScanIterator<T> implements Iterator<T> {
        private final java.util.List<T> items;
        private int i;
        ScanIterator(java.util.List<byte[]> pages, boolean valuesOnly) {
            java.util.List<T> parsed = new java.util.ArrayList<>();
            var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            for (byte[] pageBytes : pages) {
                String[] lines = new String(pageBytes, StandardCharsets.UTF_8).split("\n");
                for (String line : lines) {
                    if (line.isBlank()) continue;
                    try {
                        var node = mapper.readTree(line);
                        var key = node.get("key");
                        var value = node.get("value");
                        if (value == null || value.isNull()) continue;
                        byte[] vBytes = mapper.writeValueAsBytes(value);
                        if (valuesOnly) {
                            @SuppressWarnings("unchecked") T item = (T) vBytes;
                            parsed.add(item);
                        } else {
                            byte[] kBytes = (key == null || key.isNull() ? "" : key.asText())
                                    .getBytes(StandardCharsets.UTF_8);
                            @SuppressWarnings("unchecked") T item = (T) Map.entry(kBytes, vBytes);
                            parsed.add(item);
                        }
                    } catch (Exception ignore) { /* skip bad lines */ }
                }
            }
            this.items = parsed;
        }
        @Override public boolean hasNext() { return i < items.size(); }
        @Override public T next() {
            if (i >= items.size()) throw new java.util.NoSuchElementException();
            return items.get(i++);
        }
    }

    @Override public long getLatestSequenceNumber() { return -1L; }
    @Override public boolean isOpen()               { return true; }
    @Override public void close()                   { /* no persistent resources — HttpClient closes with JVM */ }

    private static String stripTrailingSlash(String s) {
        return s == null || s.isEmpty() || s.charAt(s.length() - 1) != '/' ? s : s.substring(0, s.length() - 1);
    }
}
