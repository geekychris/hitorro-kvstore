/*
 * Copyright (c) 2006-2026 Chris Collins
 */
package com.hitorro.util.core.iterator.sinks.kvstore;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared RocksDB handles keyed by store name so a sink and a source
 * reading the same-named store in the same JVM don't fight for the
 * exclusive lock RocksDB imposes on its data directory.
 *
 * <p>Refcount-based lifecycle — closes when the last user releases.
 * General-purpose: any caller opening RocksDB from multiple points in
 * the same process can use this to avoid `LockException`. Not tied to
 * any pipeline framework.</p>
 */
public final class RocksDbSharedRegistry {

    private static final Map<String, Entry> ENTRIES = new ConcurrentHashMap<>();

    static { RocksDB.loadLibrary(); }

    private RocksDbSharedRegistry() { }

    public static Entry acquire(String name, Path dir) throws IOException {
        // Race-safe: the compute atomically opens the DB on first miss.
        Entry e = ENTRIES.compute(name, (k, cur) -> {
            if (cur == null) {
                try {
                    Files.createDirectories(dir);
                    Options opts = new Options().setCreateIfMissing(true);
                    RocksDB db = RocksDB.open(opts, dir.toAbsolutePath().toString());
                    return new Entry(name, dir, db, opts);
                } catch (RocksDBException | IOException x) {
                    throw new RuntimeException("open rocksdb " + dir + ": " + x.getMessage(), x);
                }
            }
            cur.refs++;
            return cur;
        });
        return e;
    }

    public static void release(Entry e) {
        ENTRIES.compute(e.name, (k, cur) -> {
            if (cur == null) return null;
            if (--cur.refs > 0) return cur;
            try { cur.db.close(); }   catch (Exception ignored) { }
            try { cur.opts.close(); } catch (Exception ignored) { }
            return null;    // remove from map
        });
    }

    /** Handle exposed to sink/source implementations. */
    public static final class Entry {
        public final String name;
        public final Path   dir;
        public final RocksDB db;
        public final Options opts;
        int refs = 1;

        Entry(String name, Path dir, RocksDB db, Options opts) {
            this.name = name; this.dir = dir; this.db = db; this.opts = opts;
        }
    }
}
