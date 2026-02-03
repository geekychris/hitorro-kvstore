# Testing hitorro-kvstore

## Known Issue: RocksDB Native Library Loading

The tests require RocksDB's native libraries to be loaded. You may encounter this error:

```
java.lang.RuntimeException: Neither librocksdbjni-osx-arm64.jnilib or librocksdbjni-osx.jnilib 
were found inside the JAR, and there is no fallback.
```

**Root Cause**: RocksDB 10.4.2 Maven artifact does not include native binaries in the main JAR.
This is a known limitation with newer RocksDB versions. The JAR only contains Java classes,
and native libraries must be built separately or obtained from platform-specific artifacts.

## Solutions

### Option 1: Run from Maven (Recommended)

The most reliable way to run tests is from the command line using Maven:

```bash
# Run all tests
mvn test

# Run a specific test
mvn test -Dtest=KVStoreTest

# Run the replication demonstration
mvn test -Dtest=ReplicationDemonstrationTest
```

### Option 2: IntelliJ IDEA Setup

If you want to run tests from IntelliJ:

1. **Invalidate Caches**:
   - Go to `File -> Invalidate Caches... -> Invalidate and Restart`
   - This forces IntelliJ to re-index dependencies

2. **Reimport Maven Project**:
   - Right-click on `pom.xml` -> `Maven -> Reload Project`

3. **Check Dependencies**:
   - Open `Maven` tool window (View -> Tool Windows -> Maven)
   - Expand `hitorro-kvstore -> Dependencies`
   - Verify `rocksdbjni-10.4.2.jar` is present

4. **Add VM Option** (if still failing):
   - Edit Run Configuration
   - Add to VM options: `-Djava.library.path=/path/to/extracted/libs`

### Option 3: Build from Root

If running from the hitorro root project:

```bash
cd /Users/chris/hitorro
mvn clean install
cd hitorro-kvstore
mvn test
```

## Test Structure

### KVStoreTest
Basic functionality tests:
- Put/Get operations
- Delete operations  
- Batch operations
- Prefix scanning
- Consumer patterns
- Key extraction

### ReplicationDemonstrationTest
Comprehensive replication demonstrations:

1. **demonstrateContinuousReplication()**: 
   - Continuous writes to primary
   - Continuous replication to replica
   - Simulated disconnection
   - Reconnection and catch-up
   - Data consistency verification

2. **demonstrateCheckpointAndResume()**:
   - Checkpoint saving
   - Resume from checkpoint
   - Partial replication recovery

3. **demonstrateDeletesAndUpdates()**:
   - Replication of updates
   - Replication of deletes
   - State consistency verification

## Verifying RocksDB Installation

Check if RocksDB JAR contains native libraries:

```bash
# Extract the JAR
cd ~/.m2/repository/org/rocksdb/rocksdbjni/10.4.2
jar tf rocksdbjni-10.4.2.jar | grep -E '\.(jnilib|so|dll)$'
```

You should see files like:
- `librocksdbjni-osx-arm64.jnilib` (Mac ARM)
- `librocksdbjni-osx.jnilib` (Mac Intel)
- `librocksdbjni-linux64.so` (Linux)
- `rocksdbjni-win64.dll` (Windows)

## Platform-Specific JARs

If the uber-JAR doesn't work, you can use platform-specific JARs:

```xml
<!-- In pom.xml, replace rocksdbjni dependency with platform-specific one -->

<!-- For Mac ARM64 -->
<dependency>
    <groupId>org.rocksdb</groupId>
    <artifactId>rocksdbjni</artifactId>
    <version>10.4.2</version>
    <classifier>osx-arm64</classifier>
</dependency>

<!-- For Mac Intel -->
<dependency>
    <groupId>org.rocksdb</groupId>
    <artifactId>rocksdbjni</artifactId>
    <version>10.4.2</version>
    <classifier>osx</classifier>
</dependency>

<!-- For Linux -->
<dependency>
    <groupId>org.rocksdb</groupId>
    <artifactId>rocksdbjni</artifactId>
    <version>10.4.2</version>
    <classifier>linux64</classifier>
</dependency>
```

## Expected Test Output

When tests run successfully, you should see output like:

```
=== Replication Demonstration ===

Phase 1: Starting continuous writer on primary database...
Phase 2: Starting continuous replicator...
  Primary: Written 10 records
  Replica: Replicated 10 operations (seq: 10)
  Primary: Written 20 records
  Replica: Replicated 20 operations (seq: 20)

Phase 3: Normal operation - writes and replication happening...

Phase 4: Simulating replication disconnection...
  Disconnected at: writes=40, replicated=40
  Primary continues writing during disconnection...
  During disconnection: 30 additional writes occurred

Phase 5: Reconnecting replicator...
  Replica will resume from sequence: 40
  After reconnection: caught up 30 operations

Phase 6: Stopping writer, waiting for replication to catch up...
  Primary: Writer stopped. Total writes: 80
  Replica: Replicator stopped. Total replicated: 80

Phase 7: Verifying data consistency between primary and replica...
  Total writes to primary: 80
  Total operations replicated: 80
  Sampled 10 keys
  Matching values: 10 (100%)

✓ Replication demonstration completed successfully!
  - Continuous writes: working
  - Continuous replication: working
  - Disconnection handling: working
  - Reconnection and catch-up: working
  - Data consistency: verified

=== End of Demonstration ===
```

## Troubleshooting

### "Cannot find symbol: class RocksDB"
- Run `mvn clean install` to ensure dependencies are downloaded

### "ClassNotFoundException: org.rocksdb.RocksDB"
- Check that rocksdbjni JAR is in the classpath
- Run `mvn dependency:tree` to verify dependencies

### Tests timeout or hang
- Check that you have enough disk space in `/tmp`
- Verify write permissions to `/tmp`
- Increase test timeout in `@Test(timeout = 30000)` if needed

### Out of Memory
- Increase heap size: `mvn test -DargLine="-Xmx2g"`

## Manual Test Database Inspection

After tests run, you can inspect the databases:

```bash
# List test database files
ls -la /tmp/hitorro-kvstore-*

# These directories are automatically cleaned up after each test
```

## Clean Up

If test databases aren't automatically cleaned up:

```bash
rm -rf /tmp/hitorro-kvstore-*
```
