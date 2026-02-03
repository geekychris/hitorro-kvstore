package com.hitorro.kvstore.replication;

import com.hitorro.kvstore.Result;
import org.apache.log4j.Logger;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Provides WAL tailing for replication.
 * Wraps RocksDB's GetUpdatesSince API to enable following database changes.
 */
public class ReplicationSource implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(ReplicationSource.class);
    
    private final RocksDB db;
    private final Options options;
    private boolean closed = false;

    public ReplicationSource(RocksDB db, Options options) {
        this.db = db;
        this.options = options;
    }

    /**
     * Tails the WAL from a specific sequence number.
     *
     * @param sequenceNumber The sequence number to start from
     * @return An iterator over log entries
     */
    public Iterator<LogEntry> tailFrom(long sequenceNumber) {
        try {
            final TransactionLogIterator iterator = db.getUpdatesSince(sequenceNumber);
            
            return new Iterator<LogEntry>() {
                private LogEntry nextEntry = null;
                private boolean hasCheckedNext = false;

                @Override
                public boolean hasNext() {
                    if (hasCheckedNext) {
                        return nextEntry != null;
                    }
                    
                    if (iterator.isValid()) {
                        TransactionLogIterator.BatchResult batchResult = iterator.getBatch();
                        WriteBatch writeBatch = batchResult.writeBatch();
                        long seqNum = batchResult.sequenceNumber();
                        
                        // Extract entries from the write batch
                        List<LogEntry> entries = extractEntriesFromBatch(writeBatch, seqNum);
                        if (!entries.isEmpty()) {
                            nextEntry = entries.get(0);
                            hasCheckedNext = true;
                            iterator.next();
                            return true;
                        }
                        
                        iterator.next();
                        return hasNext();
                    }
                    
                    nextEntry = null;
                    hasCheckedNext = true;
                    return false;
                }

                @Override
                public LogEntry next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    hasCheckedNext = false;
                    return nextEntry;
                }
            };
        } catch (RocksDBException e) {
            logger.error("Failed to create WAL iterator", e);
            return new Iterator<LogEntry>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public LogEntry next() {
                    throw new NoSuchElementException();
                }
            };
        }
    }

    /**
     * Gets the latest sequence number in the database.
     *
     * @return The latest sequence number
     */
    public long getLatestSequenceNumber() {
        return db.getLatestSequenceNumber();
    }

    /**
     * Enables WAL archival to prevent deletion of old WAL files.
     * This is necessary for replication to prevent log files from being deleted
     * before they can be replicated.
     *
     * @param ttlSeconds WAL TTL in seconds (0 for no TTL)
     * @param maxSizeMB Max WAL size in megabytes (0 for no limit)
     * @return Result indicating success or failure
     */
    public Result<Void> enableArchival(long ttlSeconds, long maxSizeMB) {
        try {
            options.setWalTtlSeconds(ttlSeconds);
            options.setWalSizeLimitMB(maxSizeMB);
            return Result.success(null);
        } catch (Exception e) {
            return Result.failure("Failed to enable WAL archival: " + e.getMessage());
        }
    }

    /**
     * Extracts log entries from a WriteBatch.
     */
    private List<LogEntry> extractEntriesFromBatch(WriteBatch batch, long baseSeqNum) {
        List<LogEntry> entries = new ArrayList<>();
        
        try {
            batch.iterate(new WriteBatch.Handler() {
                private long currentSeq = baseSeqNum;
                
                @Override
                public void put(int columnFamilyId, byte[] key, byte[] value) {
                    entries.add(new LogEntry(currentSeq++, LogEntryType.PUT, key, value, null));
                }

                @Override
                public void put(byte[] key, byte[] value) {
                    entries.add(new LogEntry(currentSeq++, LogEntryType.PUT, key, value, null));
                }

                @Override
                public void delete(int columnFamilyId, byte[] key) {
                    entries.add(new LogEntry(currentSeq++, LogEntryType.DELETE, key, null, null));
                }

                @Override
                public void delete(byte[] key) {
                    entries.add(new LogEntry(currentSeq++, LogEntryType.DELETE, key, null, null));
                }

                @Override
                public void merge(int columnFamilyId, byte[] key, byte[] value) {
                    entries.add(new LogEntry(currentSeq++, LogEntryType.MERGE, key, value, null));
                }

                @Override
                public void merge(byte[] key, byte[] value) {
                    entries.add(new LogEntry(currentSeq++, LogEntryType.MERGE, key, value, null));
                }

                @Override
                public void logData(byte[] blob) {
                    // Custom metadata - could be attached to the last entry
                    if (!entries.isEmpty()) {
                        LogEntry last = entries.get(entries.size() - 1);
                        entries.set(entries.size() - 1, 
                            new LogEntry(last.getSequenceNumber(), last.getType(), 
                                last.getKey(), last.getValue(), blob));
                    }
                }

                @Override
                public void markCommitWithTimestamp(byte[] key, byte[] ts) {
                    // Not used for replication
                }

                @Override
                public void markBeginPrepare() {
                    // Not used for replication
                }

                @Override
                public void markEndPrepare(byte[] xid) {
                    // Not used for replication
                }

                @Override
                public void markNoop(boolean emptyBatch) {
                    // Not used for replication
                }

                @Override
                public void markRollback(byte[] xid) {
                    // Not used for replication
                }

                @Override
                public void markCommit(byte[] xid) {
                    // Not used for replication
                }

                @Override
                public void putBlobIndex(int columnFamilyId, byte[] key, byte[] value) {
                    // Not used for replication
                }

                @Override
                public void singleDelete(int columnFamilyId, byte[] key) {
                    // Treat as regular delete
                    entries.add(new LogEntry(currentSeq++, LogEntryType.DELETE, key, null, null));
                }

                @Override
                public void singleDelete(byte[] key) {
                    // Treat as regular delete
                    entries.add(new LogEntry(currentSeq++, LogEntryType.DELETE, key, null, null));
                }

                @Override
                public void deleteRange(int columnFamilyId, byte[] beginKey, byte[] endKey) {
                    // Not used for replication
                }

                @Override
                public void deleteRange(byte[] beginKey, byte[] endKey) {
                    // Not used for replication
                }
            });
        } catch (RocksDBException e) {
            logger.error("Error extracting entries from WriteBatch", e);
        }
        
        return entries;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            // Note: We don't close the db or options here as they are managed externally
        }
    }
}
