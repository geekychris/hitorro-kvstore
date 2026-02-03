package com.hitorro.kvstore.replication;

import com.hitorro.kvstore.KVStore;
import com.hitorro.kvstore.Result;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Applies log entries from a replication source to a target database.
 * This enables replica databases to stay in sync with a primary.
 */
public class ReplicationTarget {
    private static final Logger logger = Logger.getLogger(ReplicationTarget.class);
    
    private final KVStore targetStore;
    private long lastAppliedSequence = 0;

    public ReplicationTarget(KVStore targetStore) {
        this.targetStore = targetStore;
    }

    /**
     * Applies a single log entry to the target store.
     *
     * @param entry The log entry to apply
     * @return Result indicating success or failure
     */
    public Result<Void> applyLogEntry(LogEntry entry) {
        try {
            Result<Void> result;
            
            switch (entry.getType()) {
                case PUT:
                    result = targetStore.put(entry.getKey(), entry.getValue());
                    break;
                    
                case DELETE:
                    result = targetStore.delete(entry.getKey());
                    break;
                    
                case MERGE:
                    // For merge operations, we treat them as puts in the replica
                    // A more sophisticated implementation might handle merges differently
                    result = targetStore.put(entry.getKey(), entry.getValue());
                    logger.warn("MERGE operation treated as PUT during replication");
                    break;
                    
                default:
                    return Result.failure("Unknown log entry type: " + entry.getType());
            }
            
            if (result.isSuccess()) {
                lastAppliedSequence = entry.getSequenceNumber();
            }
            
            return result;
        } catch (Exception e) {
            return Result.failure("Failed to apply log entry: " + e.getMessage());
        }
    }

    /**
     * Applies a batch of log entries to the target store.
     * Uses batch operations for better performance.
     *
     * @param entries The list of log entries to apply
     * @return Result indicating success or failure
     */
    public Result<Void> applyBatch(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return Result.success(null);
        }

        try {
            Map<byte[], byte[]> puts = new HashMap<>();
            
            for (LogEntry entry : entries) {
                switch (entry.getType()) {
                    case PUT:
                        puts.put(entry.getKey(), entry.getValue());
                        break;
                        
                    case DELETE:
                        // For deletes in a batch, we need to apply them separately
                        // or collect them and use batchDelete
                        Result<Void> deleteResult = targetStore.delete(entry.getKey());
                        if (deleteResult.isFailure()) {
                            return deleteResult;
                        }
                        break;
                        
                    case MERGE:
                        // Treat merge as put
                        puts.put(entry.getKey(), entry.getValue());
                        logger.warn("MERGE operation treated as PUT during replication");
                        break;
                }
                
                lastAppliedSequence = entry.getSequenceNumber();
            }
            
            // Apply all puts in a batch
            if (!puts.isEmpty()) {
                Result<Void> result = targetStore.batchPut(puts, false);
                if (result.isFailure()) {
                    return result;
                }
            }
            
            return Result.success(null);
        } catch (Exception e) {
            return Result.failure("Failed to apply log entry batch: " + e.getMessage());
        }
    }

    /**
     * Gets the sequence number of the last successfully applied log entry.
     *
     * @return The last applied sequence number
     */
    public long getLastAppliedSequence() {
        return lastAppliedSequence;
    }

    /**
     * Sets the last applied sequence number.
     * Useful for resuming replication from a checkpoint.
     *
     * @param sequenceNumber The sequence number to set
     */
    public void setLastAppliedSequence(long sequenceNumber) {
        this.lastAppliedSequence = sequenceNumber;
    }
}
