package com.hitorro.kvstore;

import com.hitorro.kvstore.config.CompressionType;
import com.hitorro.kvstore.replication.LogEntry;
import com.hitorro.kvstore.replication.ReplicationSource;
import com.hitorro.kvstore.replication.ReplicationTarget;
import org.junit.jupiter.api.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive demonstration of replication capabilities.
 * Shows:
 * 1. Continuous writes to a primary database
 * 2. Continuous replication to a replica database
 * 3. Handling disconnection and reconnection
 * 4. Resuming from last known good point
 */
class ReplicationDemonstrationTest {
    private static final String PRIMARY_DB_PATH = "/tmp/hitorro-kvstore-primary";
    private static final String REPLICA_DB_PATH = "/tmp/hitorro-kvstore-replica";
    
    private RocksDBStore primaryStore;
    private RocksDBStore replicaStore;
    private ReplicationSource replicationSource;
    private ReplicationTarget replicationTarget;
    
    private ExecutorService executorService;

    @BeforeEach
    void setUp() throws Exception {
        // Clean up any existing databases
        deleteDirectory(new File(PRIMARY_DB_PATH));
        deleteDirectory(new File(REPLICA_DB_PATH));
        
        // Create primary database with WAL archival enabled
        DatabaseConfig primaryConfig = DatabaseConfig.builder(PRIMARY_DB_PATH)
                .compressionType(CompressionType.SNAPPY)
                .enableWAL(true)
                .walTTLSeconds(3600)  // Keep WAL for 1 hour
                .walSizeLimitMB(100)  // Keep up to 100MB of WAL
                .createIfMissing(true)
                .build();
        
        primaryStore = new RocksDBStore(primaryConfig);
        
        // Create replica database
        DatabaseConfig replicaConfig = DatabaseConfig.builder(REPLICA_DB_PATH)
                .compressionType(CompressionType.SNAPPY)
                .createIfMissing(true)
                .build();
        
        replicaStore = new RocksDBStore(replicaConfig);
        
        // Set up replication
        replicationSource = primaryStore.createReplicationSource();
        replicationSource.enableArchival(3600, 100);
        replicationTarget = new ReplicationTarget(replicaStore);
        
        // Create thread pool for background tasks
        executorService = Executors.newFixedThreadPool(3);
    }

    @AfterEach
    void tearDown() {
        if (executorService != null) {
            executorService.shutdownNow();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        if (replicationSource != null) {
            replicationSource.close();
        }
        
        if (primaryStore != null && primaryStore.isOpen()) {
            primaryStore.close();
        }
        
        if (replicaStore != null && replicaStore.isOpen()) {
            replicaStore.close();
        }
        
        // Clean up test databases
        deleteDirectory(new File(PRIMARY_DB_PATH));
        deleteDirectory(new File(REPLICA_DB_PATH));
    }

    @Test
    @DisplayName("Demonstrate continuous replication with disconnection and reconnection")
    void demonstrateContinuousReplication() throws Exception {
        System.out.println("\n=== Replication Demonstration ===\n");
        
        // Tracking variables
        AtomicLong writeCount = new AtomicLong(0);
        AtomicLong replicatedCount = new AtomicLong(0);
        AtomicBoolean writerRunning = new AtomicBoolean(true);
        AtomicBoolean replicatorConnected = new AtomicBoolean(true);
        AtomicLong lastReplicatedSeq = new AtomicLong(0);
        
        // Phase 1: Start continuous writer
        System.out.println("Phase 1: Starting continuous writer on primary database...");
        Future<?> writerTask = executorService.submit(() -> {
            int counter = 0;
            while (writerRunning.get()) {
                try {
                    String key = "user:" + counter;
                    String value = "User Data " + counter + " - Timestamp: " + System.currentTimeMillis();
                    
                    Result<Void> result = primaryStore.put(key.getBytes(), value.getBytes());
                    if (result.isSuccess()) {
                        long count = writeCount.incrementAndGet();
                        if (count % 10 == 0) {
                            System.out.println("  Primary: Written " + count + " records");
                        }
                    }
                    
                    counter++;
                    Thread.sleep(50); // Write every 50ms
                    
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    System.err.println("Writer error: " + e.getMessage());
                }
            }
            System.out.println("  Primary: Writer stopped. Total writes: " + writeCount.get());
        });
        
        // Phase 2: Start continuous replicator
        System.out.println("Phase 2: Starting continuous replicator...");
        Future<?> replicatorTask = executorService.submit(() -> {
            long currentSeq = lastReplicatedSeq.get();
            List<LogEntry> batch = new ArrayList<>();
            
            while (writerRunning.get() || replicatorConnected.get()) {
                try {
                    if (replicatorConnected.get()) {
                        // Tail from the last known sequence
                        Iterator<LogEntry> logIterator = replicationSource.tailFrom(currentSeq);
                        
                        while (logIterator.hasNext() && replicatorConnected.get()) {
                            LogEntry entry = logIterator.next();
                            batch.add(entry);
                            
                            // Apply in batches of 10 for efficiency
                            if (batch.size() >= 10) {
                                Result<Void> result = replicationTarget.applyBatch(batch);
                                if (result.isSuccess()) {
                                    currentSeq = batch.get(batch.size() - 1).getSequenceNumber();
                                    lastReplicatedSeq.set(currentSeq);
                                    replicatedCount.addAndGet(batch.size());
                                    
                                    if (replicatedCount.get() % 10 == 0) {
                                        System.out.println("  Replica: Replicated " + replicatedCount.get() + 
                                                         " operations (seq: " + currentSeq + ")");
                                    }
                                } else {
                                    System.err.println("  Replica: Failed to apply batch: " + 
                                                     result.getError().orElse("Unknown error"));
                                }
                                batch.clear();
                            }
                        }
                        
                        // Apply any remaining entries
                        if (!batch.isEmpty() && replicatorConnected.get()) {
                            Result<Void> result = replicationTarget.applyBatch(batch);
                            if (result.isSuccess()) {
                                currentSeq = batch.get(batch.size() - 1).getSequenceNumber();
                                lastReplicatedSeq.set(currentSeq);
                                replicatedCount.addAndGet(batch.size());
                            }
                            batch.clear();
                        }
                    }
                    
                    // Sleep before next poll
                    Thread.sleep(100);
                    
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    System.err.println("  Replica: Replicator error: " + e.getMessage());
                    try {
                        Thread.sleep(500); // Back off on error
                    } catch (InterruptedException ie) {
                        break;
                    }
                }
            }
            
            System.out.println("  Replica: Replicator stopped. Total replicated: " + replicatedCount.get());
        });
        
        // Let it run for a bit
        Thread.sleep(1000);
        System.out.println("\nPhase 3: Normal operation - writes and replication happening...");
        Thread.sleep(1000);
        
        // Phase 4: Simulate disconnection
        System.out.println("\nPhase 4: Simulating replication disconnection...");
        replicatorConnected.set(false);
        long disconnectWriteCount = writeCount.get();
        long disconnectReplicatedCount = replicatedCount.get();
        
        System.out.println("  Disconnected at: writes=" + disconnectWriteCount + 
                         ", replicated=" + disconnectReplicatedCount);
        System.out.println("  Primary continues writing during disconnection...");
        
        // Continue writing while disconnected
        Thread.sleep(1500);
        
        long duringDisconnectWrites = writeCount.get();
        System.out.println("  During disconnection: " + (duringDisconnectWrites - disconnectWriteCount) + 
                         " additional writes occurred");
        
        // Phase 5: Reconnect and catch up
        System.out.println("\nPhase 5: Reconnecting replicator...");
        System.out.println("  Replica will resume from sequence: " + lastReplicatedSeq.get());
        replicatorConnected.set(true);
        
        // Give time for catch-up
        Thread.sleep(2000);
        
        long afterReconnectReplicated = replicatedCount.get();
        System.out.println("  After reconnection: caught up " + 
                         (afterReconnectReplicated - disconnectReplicatedCount) + " operations");
        
        // Phase 6: Stop writer and let replication catch up completely
        System.out.println("\nPhase 6: Stopping writer, waiting for replication to catch up...");
        writerRunning.set(false);
        
        // Wait for writer to finish
        writerTask.get(5, TimeUnit.SECONDS);
        
        // Give replication time to catch up completely
        Thread.sleep(1000);
        
        // Stop replicator
        replicatorConnected.set(false);
        replicatorTask.get(5, TimeUnit.SECONDS);
        
        // Phase 7: Verify consistency
        System.out.println("\nPhase 7: Verifying data consistency between primary and replica...");
        
        long finalWriteCount = writeCount.get();
        long finalReplicatedCount = replicatedCount.get();
        
        System.out.println("  Total writes to primary: " + finalWriteCount);
        System.out.println("  Total operations replicated: " + finalReplicatedCount);
        
        // Verify some random keys exist in both databases
        int sampled = 0;
        int matching = 0;
        for (int i = 0; i < finalWriteCount; i += Math.max(1, (int)(finalWriteCount / 10))) {
            String key = "user:" + i;
            Result<byte[]> primaryValue = primaryStore.get(key.getBytes());
            Result<byte[]> replicaValue = replicaStore.get(key.getBytes());
            
            if (primaryValue.isSuccess() && replicaValue.isSuccess()) {
                String primaryStr = new String(primaryValue.getValue().get());
                String replicaStr = new String(replicaValue.getValue().get());
                
                if (primaryStr.equals(replicaStr)) {
                    matching++;
                }
                sampled++;
            }
        }
        
        System.out.println("  Sampled " + sampled + " keys");
        System.out.println("  Matching values: " + matching + " (" + 
                         (sampled > 0 ? (matching * 100 / sampled) : 0) + "%)");
        
        // Assertions
        assertThat(finalWriteCount).isGreaterThan(0);
        assertThat(finalReplicatedCount).isGreaterThan(0);
        assertThat(matching).isEqualTo(sampled);
        
        // Verify last written key exists in both
        String lastKey = "user:" + (finalWriteCount - 1);
        Result<byte[]> primaryLast = primaryStore.get(lastKey.getBytes());
        Result<byte[]> replicaLast = replicaStore.get(lastKey.getBytes());
        
        assertThat(primaryLast.isSuccess()).isTrue();
        assertThat(replicaLast.isSuccess()).isTrue();
        assertThat(replicaLast.getValue().get()).isEqualTo(primaryLast.getValue().get());
        
        System.out.println("\n✓ Replication demonstration completed successfully!");
        System.out.println("  - Continuous writes: working");
        System.out.println("  - Continuous replication: working");
        System.out.println("  - Disconnection handling: working");
        System.out.println("  - Reconnection and catch-up: working");
        System.out.println("  - Data consistency: verified");
        System.out.println("\n=== End of Demonstration ===\n");
    }

    @Test
    @DisplayName("Demonstrate manual replication checkpoint and resume")
    void demonstrateCheckpointAndResume() throws Exception {
        System.out.println("\n=== Checkpoint and Resume Demonstration ===\n");
        
        // Phase 1: Write some data to primary
        System.out.println("Phase 1: Writing initial data to primary...");
        for (int i = 0; i < 50; i++) {
            String key = "checkpoint:" + i;
            String value = "Value " + i;
            primaryStore.put(key.getBytes(), value.getBytes());
        }
        System.out.println("  Written 50 records to primary");
        
        // Phase 2: Replicate first batch
        System.out.println("\nPhase 2: Replicating first 25 records...");
        long startSeq = 0;
        Iterator<LogEntry> logIterator = replicationSource.tailFrom(startSeq);
        
        int replicated = 0;
        while (logIterator.hasNext() && replicated < 25) {
            LogEntry entry = logIterator.next();
            replicationTarget.applyLogEntry(entry);
            replicated++;
        }
        
        // Save checkpoint
        long checkpoint = replicationTarget.getLastAppliedSequence();
        System.out.println("  Replicated 25 records");
        System.out.println("  Checkpoint saved at sequence: " + checkpoint);
        
        // Phase 3: Simulate restart - write more data
        System.out.println("\nPhase 3: Simulating restart - writing more data...");
        for (int i = 50; i < 100; i++) {
            String key = "checkpoint:" + i;
            String value = "Value " + i;
            primaryStore.put(key.getBytes(), value.getBytes());
        }
        System.out.println("  Written 50 more records to primary (total: 100)");
        
        // Phase 4: Resume from checkpoint
        System.out.println("\nPhase 4: Resuming replication from checkpoint (seq: " + checkpoint + ")...");
        
        // Create new replication target to simulate restart
        ReplicationTarget newTarget = new ReplicationTarget(replicaStore);
        newTarget.setLastAppliedSequence(checkpoint); // Restore checkpoint
        
        logIterator = replicationSource.tailFrom(checkpoint);
        int catchUpCount = 0;
        
        while (logIterator.hasNext()) {
            LogEntry entry = logIterator.next();
            Result<Void> result = newTarget.applyLogEntry(entry);
            if (result.isSuccess()) {
                catchUpCount++;
            }
        }
        
        System.out.println("  Caught up " + catchUpCount + " operations from checkpoint");
        System.out.println("  New sequence: " + newTarget.getLastAppliedSequence());
        
        // Phase 5: Verify all data is replicated
        System.out.println("\nPhase 5: Verifying complete replication...");
        int verified = 0;
        for (int i = 0; i < 100; i++) {
            String key = "checkpoint:" + i;
            Result<byte[]> primaryValue = primaryStore.get(key.getBytes());
            Result<byte[]> replicaValue = replicaStore.get(key.getBytes());
            
            if (primaryValue.isSuccess() && replicaValue.isSuccess()) {
                String primaryStr = new String(primaryValue.getValue().get());
                String replicaStr = new String(replicaValue.getValue().get());
                assertThat(replicaStr).isEqualTo(primaryStr);
                verified++;
            }
        }
        
        System.out.println("  Verified all " + verified + " records match between primary and replica");
        assertThat(verified).isEqualTo(100);
        
        System.out.println("\n✓ Checkpoint and resume demonstration completed successfully!");
        System.out.println("  - Checkpoint saving: working");
        System.out.println("  - Resume from checkpoint: working");
        System.out.println("  - Data consistency after resume: verified");
        System.out.println("\n=== End of Demonstration ===\n");
    }

    @Test
    @DisplayName("Demonstrate handling of deletes and updates in replication")
    void demonstrateDeletesAndUpdates() throws Exception {
        System.out.println("\n=== Deletes and Updates Demonstration ===\n");
        
        // Phase 1: Initial writes
        System.out.println("Phase 1: Writing initial data...");
        for (int i = 0; i < 10; i++) {
            String key = "ops:" + i;
            String value = "Version 1 - Record " + i;
            primaryStore.put(key.getBytes(), value.getBytes());
        }
        System.out.println("  Written 10 initial records");
        
        // Phase 2: Replicate initial data
        System.out.println("\nPhase 2: Replicating initial data...");
        Iterator<LogEntry> logIterator = replicationSource.tailFrom(0);
        while (logIterator.hasNext()) {
            replicationTarget.applyLogEntry(logIterator.next());
        }
        long checkpointAfterInitial = replicationTarget.getLastAppliedSequence();
        System.out.println("  Initial replication complete (seq: " + checkpointAfterInitial + ")");
        
        // Phase 3: Update some records
        System.out.println("\nPhase 3: Updating records 0-4...");
        for (int i = 0; i < 5; i++) {
            String key = "ops:" + i;
            String value = "Version 2 - Updated Record " + i;
            primaryStore.put(key.getBytes(), value.getBytes());
        }
        System.out.println("  Updated 5 records");
        
        // Phase 4: Delete some records
        System.out.println("\nPhase 4: Deleting records 5-7...");
        for (int i = 5; i < 8; i++) {
            String key = "ops:" + i;
            primaryStore.delete(key.getBytes());
        }
        System.out.println("  Deleted 3 records");
        
        // Phase 5: Replicate updates and deletes
        System.out.println("\nPhase 5: Replicating updates and deletes...");
        logIterator = replicationSource.tailFrom(checkpointAfterInitial);
        int updateCount = 0;
        int deleteCount = 0;
        
        while (logIterator.hasNext()) {
            LogEntry entry = logIterator.next();
            switch (entry.getType()) {
                case PUT:
                    updateCount++;
                    break;
                case DELETE:
                    deleteCount++;
                    break;
                default:
                    break;
            }
            replicationTarget.applyLogEntry(entry);
        }
        
        System.out.println("  Replicated " + updateCount + " updates and " + deleteCount + " deletes");
        
        // Phase 6: Verify final state
        System.out.println("\nPhase 6: Verifying final state...");
        
        // Updated records (0-4) should have version 2
        for (int i = 0; i < 5; i++) {
            String key = "ops:" + i;
            Result<byte[]> replicaValue = replicaStore.get(key.getBytes());
            assertThat(replicaValue.isSuccess()).isTrue();
            String value = new String(replicaValue.getValue().get());
            assertThat(value).contains("Version 2");
            assertThat(value).contains("Updated");
        }
        System.out.println("  ✓ Records 0-4: Updated correctly");
        
        // Deleted records (5-7) should not exist
        for (int i = 5; i < 8; i++) {
            String key = "ops:" + i;
            Result<byte[]> replicaValue = replicaStore.get(key.getBytes());
            assertThat(replicaValue.isFailure()).isTrue();
        }
        System.out.println("  ✓ Records 5-7: Deleted correctly");
        
        // Unchanged records (8-9) should have version 1
        for (int i = 8; i < 10; i++) {
            String key = "ops:" + i;
            Result<byte[]> replicaValue = replicaStore.get(key.getBytes());
            assertThat(replicaValue.isSuccess()).isTrue();
            String value = new String(replicaValue.getValue().get());
            assertThat(value).contains("Version 1");
        }
        System.out.println("  ✓ Records 8-9: Unchanged correctly");
        
        System.out.println("\n✓ Deletes and updates demonstration completed successfully!");
        System.out.println("  - Update replication: working");
        System.out.println("  - Delete replication: working");
        System.out.println("  - State consistency: verified");
        System.out.println("\n=== End of Demonstration ===\n");
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }
}
