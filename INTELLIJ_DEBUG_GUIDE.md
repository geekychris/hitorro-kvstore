# Running Tests in IntelliJ with Debugger

## Quick Setup

The tests now work with RocksDB 8.11.3, which includes native libraries. Follow these steps:

### 1. Refresh Maven Dependencies

After updating to RocksDB 8.11.3:

1. Open the **Maven** tool window (View → Tool Windows → Maven)
2. Click the **Reload All Maven Projects** button (circular arrows icon)
3. Wait for dependencies to download

### 2. Invalidate Caches (Optional but Recommended)

1. Go to **File → Invalidate Caches...**
2. Check **Invalidate and Restart**
3. Click **Invalidate and Restart**

### 3. Run a Test with Debugger

#### Option A: Run Single Test Method

1. Open the test file (e.g., `KVStoreTest.java`)
2. Click the **green play icon** next to any test method
3. Select **Debug 'testBasicPutAndGet()'**
4. Set breakpoints by clicking in the left margin next to line numbers

#### Option B: Run All Tests in a Class

1. Right-click on the test class name in the editor
2. Select **Debug 'KVStoreTest'**

#### Option C: Run Specific Test from Project View

1. In **Project** view, navigate to `src/test/java/com/hitorro/kvstore/`
2. Right-click on test file (e.g., `ReplicationDemonstrationTest.java`)
3. Select **Debug 'ReplicationDemonstrationTest'**

### 4. Debug the Replication Demonstration

The `ReplicationDemonstrationTest` is excellent for debugging:

```java
// Open: src/test/java/com/hitorro/kvstore/ReplicationDemonstrationTest.java
```

**Set breakpoints at interesting points**:

- **Line 150**: Inside the replication loop where log entries are processed
- **Line 159**: Where batches are applied to the replica
- **Line 214**: When disconnection is simulated
- **Line 231**: When reconnection happens
- **Line 332**: During checkpoint save
- **Line 361**: During checkpoint resume

**To debug**:
1. Click the green arrow next to `demonstrateContinuousReplication()`
2. Select **Debug**
3. Watch the variables in the **Variables** pane:
   - `writeCount` - number of writes to primary
   - `replicatedCount` - number of replicated operations
   - `lastReplicatedSeq` - current sequence number
   - `logIterator` - the WAL iterator

### 5. Common Debugging Tasks

#### View Variable Values

While paused at a breakpoint:
- Hover over any variable to see its value
- Open the **Variables** pane at the bottom to see all variables
- Right-click a variable → **Add to Watches** to track it

#### Step Through Code

Use these controls in the debugger toolbar:
- **F8** - Step Over (execute current line, don't go into methods)
- **F7** - Step Into (go into method calls)
- **Shift+F8** - Step Out (finish current method and return)
- **F9** - Resume (continue to next breakpoint)

#### Evaluate Expressions

While paused:
1. Select any expression in the code
2. Right-click → **Evaluate Expression** (or **Alt+F8**)
3. View or modify the result

#### Example: Watch Replication Progress

Set a breakpoint at line 159 in `ReplicationDemonstrationTest`:
```java
Result<Void> result = replicationTarget.applyBatch(batch);
```

Add watches for:
- `batch.size()` - see how many entries in each batch
- `replicatedCount.get()` - see total replicated
- `currentSeq` - see current sequence number

### 6. Debugging Tips for Replication

#### View LogEntry Contents

When stopped at a breakpoint where you have a `LogEntry`:
```java
LogEntry entry = logIterator.next();
```

Evaluate these expressions:
- `new String(entry.getKey())` - see the key as a string
- `new String(entry.getValue())` - see the value as a string
- `entry.getSequenceNumber()` - see the sequence number
- `entry.getType()` - see if it's PUT, DELETE, or MERGE

#### Monitor Thread Activity

For the `demonstrateContinuousReplication()` test which uses multiple threads:

1. When paused, open **Debugger → Threads** tab
2. You'll see:
   - **pool-1-thread-1** - the writer thread
   - **pool-1-thread-2** - the replicator thread
   - **main** - the test coordinator thread
3. Click on any thread to see its current state and stack trace

#### Debug Disconnection/Reconnection

Set breakpoints at:
```java
replicatorConnected.set(false);  // Line 213 - simulating disconnect
replicatorConnected.set(true);   // Line 231 - reconnecting
```

Watch how the replication loop handles the `replicatorConnected` flag.

### 7. Running All Tests

To run all tests with debugger:

1. Right-click on `src/test/java` folder
2. Select **Debug 'Tests in hitorro-kvstore'**
3. Tests will run sequentially
4. Any breakpoints in any test will pause execution

### 8. Console Output

The replication tests print detailed console output. View it in:
- **Run** tool window at the bottom
- You'll see all the "Phase 1, Phase 2..." messages
- Useful for understanding test flow even without debugger

### 9. Test Configuration (Advanced)

If you need to customize test execution:

1. **Run → Edit Configurations...**
2. Find or create a JUnit configuration
3. Modify settings like:
   - **VM options**: e.g., `-Xmx2g` for more memory
   - **Environment variables**: if needed
   - **Working directory**: should be the project root

### 10. Troubleshooting

#### Tests Still Fail to Load Native Library

If you still see the native library error after following steps:

1. Verify RocksDB version in `pom.xml` is `8.11.3`
2. Delete `.m2/repository/org/rocksdb/rocksdbjni/`
3. Run `mvn clean install` from command line
4. Restart IntelliJ

#### No Test Results Shown

1. Make sure **JUnit 5** is being used (not JUnit 4)
2. Check that test methods are annotated with `@Test`
3. Rebuild project: **Build → Rebuild Project**

#### Debugger Not Stopping at Breakpoints

1. Make sure you're running in **Debug** mode (not Run mode)
2. Check that breakpoints are enabled (red filled circles, not crossed out)
3. Ensure the code you're debugging actually gets executed

## Quick Test Examples

### Simple Test - Good for First Debug Session
```java
@Test
void testBasicPutAndGet() {
    // Set breakpoint here
    byte[] key = "testKey".getBytes();
    byte[] value = "testValue".getBytes();
    
    // Step through these
    Result<Void> putResult = store.put(key, value);
    Result<byte[]> getResult = store.get(key);
}
```

### Replication Test - Advanced Debugging
```java
@Test
void demonstrateCheckpointAndResume() throws Exception {
    // Set breakpoint at checkpoint save
    long checkpoint = replicationTarget.getLastAppliedSequence();
    
    // Set breakpoint at resume
    logIterator = replicationSource.tailFrom(checkpoint);
}
```

## Expected Behavior

When tests run successfully, you should see:
- ✓ Green checkmark next to passed tests
- Console output showing replication phases
- No exceptions or errors
- Databases cleaned up in `/tmp` after each test

Happy debugging! 🐛🔍
