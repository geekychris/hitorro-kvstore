# Hitorro KVStore

A high-performance, feature-rich RocksDB wrapper for Java 21+ providing multiple access patterns, replication support, and flexible configuration options.

## Features

- **Multiple Access Patterns**: Single operations, batch operations, iterators, consumers, and Java Streams
- **Prefix Queries**: Efficient scanning with prefix matching
- **Replication Support**: Built-in WAL tailing for primary-replica setups
- **Transactional & Non-Transactional Batches**: ACID guarantees when needed, performance when not
- **Configurable Compression**: Support for Snappy, LZ4, ZSTD, and more
- **Type-Safe Wrapper**: Automatic JSON serialization/deserialization via TypedKVStore
- **Result Pattern**: Clean error handling without exceptions
- **Resource Management**: Automatic cleanup with shutdown hooks and AutoCloseable
- **Multi-Database Management**: KVStoreManager for managing multiple database instances

## Installation

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.hitorro</groupId>
    <artifactId>hitorro-kvstore</artifactId>
    <version>3.0.1</version>
</dependency>
```

## Quick Start

### Basic Usage

```java
import com.hitorro.kvstore.*;
import com.hitorro.kvstore.config.*;

// Create database configuration
DatabaseConfig config = DatabaseConfig.builder("/path/to/db")
        .compressionType(CompressionType.SNAPPY)
        .storageMode(StorageMode.DISK)
        .createIfMissing(true)
        .build();

// Open the database
try (KVStore store = new RocksDBStore(config)) {
    // Put a key-value pair
    byte[] key = "myKey".getBytes();
    byte[] value = "myValue".getBytes();
    
    Result<Void> putResult = store.put(key, value);
    if (putResult.isSuccess()) {
        System.out.println("Successfully stored data");
    }
    
    // Get the value
    Result<byte[]> getResult = store.get(key);
    getResult.getValue().ifPresent(v -> {
        System.out.println("Retrieved: " + new String(v));
    });
    
    // Delete the key
    store.delete(key);
}
```

### Using TypedKVStore for Automatic Serialization

```java
import com.hitorro.kvstore.*;

// Define your data class
class User {
    public String id;
    public String name;
    public int age;
    
    // Constructors, getters, setters...
}

// Create a typed store
DatabaseConfig config = DatabaseConfig.builder("/path/to/users-db")
        .build();
        
try (KVStore rawStore = new RocksDBStore(config);
     TypedKVStore<User> store = new TypedKVStore<>(rawStore, User.class)) {
    
    // Put a user
    User user = new User();
    user.id = "user123";
    user.name = "Alice";
    user.age = 30;
    
    store.put("user:user123", user);
    
    // Get the user
    Result<User> result = store.get("user:user123");
    result.getValue().ifPresent(u -> {
        System.out.println("Retrieved user: " + u.name);
    });
}
```

## Configuration

### Compression Types

```java
DatabaseConfig config = DatabaseConfig.builder("/path/to/db")
        .compressionType(CompressionType.ZSTD)  // Best compression ratio
        // .compressionType(CompressionType.LZ4)    // Very fast
        // .compressionType(CompressionType.SNAPPY) // Balanced (default)
        // .compressionType(CompressionType.NONE)   // No compression
        .build();
```

### Memory vs Disk Storage

```java
// Disk-based (default)
DatabaseConfig config = DatabaseConfig.builder("/path/to/db")
        .storageMode(StorageMode.DISK)
        .build();

// In-memory (for caching or testing)
DatabaseConfig config = DatabaseConfig.builder("/path/to/db")
        .storageMode(StorageMode.MEMORY)
        .build();
```

### Performance Tuning

```java
DatabaseConfig config = DatabaseConfig.builder("/path/to/db")
        .blockCacheSize(64 * 1024 * 1024)      // 64MB block cache
        .writeBufferSize(128 * 1024 * 1024)    // 128MB write buffer
        .maxWriteBufferNumber(4)                // 4 write buffers
        .blockSize(8 * 1024)                    // 8KB block size
        .bitsPerKey(10.0)                       // Bloom filter bits per key
        .build();
```

### WAL Configuration

```java
DatabaseConfig config = DatabaseConfig.builder("/path/to/db")
        .enableWAL(true)
        .walDirectory("/path/to/wal")          // Separate WAL directory
        .syncWrites(false)                     // Async writes (faster)
        // .syncWrites(true)                   // Sync writes (safer)
        .build();
```

## Batch Operations

### Non-Transactional Batch (Best-Effort)

```java
// Batch put
Map<byte[], byte[]> entries = new HashMap<>();
entries.put("key1".getBytes(), "value1".getBytes());
entries.put("key2".getBytes(), "value2".getBytes());
entries.put("key3".getBytes(), "value3".getBytes());

Result<Void> result = store.batchPut(entries, false);

// Batch get
List<byte[]> keys = Arrays.asList(
    "key1".getBytes(),
    "key2".getBytes(),
    "key3".getBytes()
);

Result<List<byte[]>> values = store.batchGet(keys);
```

### Transactional Batch (All-or-Nothing)

```java
// Enable transactions in config
DatabaseConfig config = DatabaseConfig.builder("/path/to/db")
        .enableTransactions(true)
        .build();

try (KVStore store = new RocksDBStore(config)) {
    Map<byte[], byte[]> entries = new HashMap<>();
    entries.put("key1".getBytes(), "value1".getBytes());
    entries.put("key2".getBytes(), "value2".getBytes());
    
    // Transactional batch - all or nothing
    Result<Void> result = store.batchPut(entries, true);
    
    if (result.isSuccess()) {
        System.out.println("All entries committed");
    } else {
        System.out.println("Transaction rolled back: " + result.getError().orElse(""));
    }
}
```

## Streaming and Iteration

### Iterator Pattern

```java
// Scan by prefix
Iterator<byte[]> iterator = store.scanByPrefix("user:".getBytes());
while (iterator.hasNext()) {
    byte[] value = iterator.next();
    System.out.println("Value: " + new String(value));
}

// Scan with keys
Iterator<Map.Entry<byte[], byte[]>> keyValueIterator = 
    store.scanByPrefixWithKeys("user:".getBytes());
while (keyValueIterator.hasNext()) {
    Map.Entry<byte[], byte[]> entry = keyValueIterator.next();
    System.out.println("Key: " + new String(entry.getKey()) + 
                       ", Value: " + new String(entry.getValue()));
}
```

### Consumer Pattern

```java
// Process values with a consumer
store.consumePrefixValues("user:".getBytes(), value -> {
    System.out.println("Processing: " + new String(value));
});

// Process key-value pairs
store.consumePrefixEntries("user:".getBytes(), entry -> {
    String key = new String(entry.getKey());
    String value = new String(entry.getValue());
    System.out.println(key + " = " + value);
});
```

### Java Stream API

```java
// Stream values
Stream<byte[]> valueStream = store.streamByPrefix("user:".getBytes());
long count = valueStream
        .map(bytes -> new String(bytes))
        .filter(s -> s.contains("admin"))
        .count();

// Stream key-value pairs
store.streamByPrefixWithKeys("user:".getBytes())
        .map(entry -> new String(entry.getKey()) + ": " + new String(entry.getValue()))
        .forEach(System.out::println);
```

## Prefix Queries

Design your keys with prefixes for efficient range scanning:

```java
// Store data with hierarchical keys
store.put("user:1".getBytes(), "Alice".getBytes());
store.put("user:2".getBytes(), "Bob".getBytes());
store.put("user:3".getBytes(), "Charlie".getBytes());
store.put("product:1".getBytes(), "Laptop".getBytes());
store.put("product:2".getBytes(), "Mouse".getBytes());

// Scan all users
Iterator<byte[]> users = store.scanByPrefix("user:".getBytes());

// Scan all products
Iterator<byte[]> products = store.scanByPrefix("product:".getBytes());
```

### Key Design Best Practices

- Use consistent separators (`:`, `/`, `|`)
- Order keys for range queries: `year:month:day:id`
- Include entity type as prefix: `user:`, `order:`, `product:`
- Lexicographically sortable components

## Replication

The module supports WAL-based replication for primary-replica setups.

### Setting Up a Primary Database

```java
// Configure primary with WAL archival
DatabaseConfig primaryConfig = DatabaseConfig.builder("/path/to/primary")
        .enableWAL(true)
        .walTTLSeconds(3600)          // Keep WAL files for 1 hour
        .walSizeLimitMB(1024)         // Keep up to 1GB of WAL
        .build();

RocksDBStore primary = new RocksDBStore(primaryConfig);

// Create replication source
ReplicationSource replicationSource = primary.createReplicationSource();

// Enable WAL archival to prevent deletion
replicationSource.enableArchival(3600, 1024);
```

### Setting Up a Replica Database

```java
// Configure replica database
DatabaseConfig replicaConfig = DatabaseConfig.builder("/path/to/replica")
        .build();

KVStore replica = new RocksDBStore(replicaConfig);
ReplicationTarget replicationTarget = new ReplicationTarget(replica);

// Get the starting sequence number
long startSeq = replica.getLatestSequenceNumber();

// Tail the WAL from the primary
Iterator<LogEntry> logIterator = replicationSource.tailFrom(startSeq);

// Apply log entries to replica
while (logIterator.hasNext()) {
    LogEntry entry = logIterator.next();
    Result<Void> result = replicationTarget.applyLogEntry(entry);
    
    if (result.isFailure()) {
        System.err.println("Failed to apply entry: " + result.getError().orElse(""));
    }
}

// Save checkpoint for resuming replication
long lastAppliedSeq = replicationTarget.getLastAppliedSequence();
```

### Batch Replication for Better Performance

```java
List<LogEntry> batch = new ArrayList<>();
Iterator<LogEntry> logIterator = replicationSource.tailFrom(startSeq);

while (logIterator.hasNext()) {
    batch.add(logIterator.next());
    
    // Apply in batches of 100
    if (batch.size() >= 100) {
        replicationTarget.applyBatch(batch);
        batch.clear();
    }
}

// Apply remaining entries
if (!batch.isEmpty()) {
    replicationTarget.applyBatch(batch);
}
```

### Continuous Replication Loop

```java
class ReplicationService implements Runnable {
    private final ReplicationSource source;
    private final ReplicationTarget target;
    private volatile boolean running = true;
    
    public ReplicationService(ReplicationSource source, ReplicationTarget target) {
        this.source = source;
        this.target = target;
    }
    
    @Override
    public void run() {
        long currentSeq = target.getLastAppliedSequence();
        
        while (running) {
            Iterator<LogEntry> entries = source.tailFrom(currentSeq);
            
            while (entries.hasNext() && running) {
                LogEntry entry = entries.next();
                Result<Void> result = target.applyLogEntry(entry);
                
                if (result.isSuccess()) {
                    currentSeq = entry.getSequenceNumber();
                } else {
                    // Handle error, maybe retry
                    System.err.println("Replication error: " + 
                                       result.getError().orElse(""));
                }
            }
            
            // Sleep before checking for new entries
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                running = false;
            }
        }
    }
    
    public void stop() {
        running = false;
    }
}

// Usage
ReplicationService service = new ReplicationService(replicationSource, replicationTarget);
new Thread(service).start();
```

### Network Transport Considerations

The replication API provides the primitives (WAL tailing and log application), but network transport is outside the scope of this module. You can implement network transport using:

- **HTTP/REST**: Serialize LogEntry objects as JSON and send via HTTP
- **gRPC**: Define protobuf messages for LogEntry and stream
- **Kafka**: Use Kafka as a replication log
- **Custom TCP**: Implement a custom protocol for log shipping

Example with HTTP (conceptual):

```java
// Primary side - expose endpoint
@GET
@Path("/replication/entries")
public List<LogEntry> getLogEntries(@QueryParam("since") long sequenceNumber) {
    Iterator<LogEntry> iter = replicationSource.tailFrom(sequenceNumber);
    List<LogEntry> entries = new ArrayList<>();
    
    while (iter.hasNext() && entries.size() < 1000) {
        entries.add(iter.next());
    }
    
    return entries;
}

// Replica side - poll for entries
long lastSeq = replicationTarget.getLastAppliedSequence();
List<LogEntry> entries = httpClient.get("/replication/entries?since=" + lastSeq);

for (LogEntry entry : entries) {
    replicationTarget.applyLogEntry(entry);
}
```

## Advanced Features

### Key Extraction Functions

Store objects and let the library extract the key:

```java
class Product {
    String sku;
    String name;
    double price;
}

Product product = new Product();
product.sku = "LAPTOP-123";
product.name = "Gaming Laptop";
product.price = 1299.99;

// Use key extraction function
store.put(
    product,
    p -> ("product:" + p.sku).getBytes(),  // Key extractor
    p -> serializeToJson(p)                 // Value serializer
);
```

### Multiple Database Management

```java
KVStoreManager manager = new KVStoreManager();

// Open multiple databases
Result<KVStore> usersDb = manager.openDatabase("users",
    DatabaseConfig.builder("/path/to/users").build());

Result<KVStore> productsDb = manager.openDatabase("products",
    DatabaseConfig.builder("/path/to/products").build());

Result<KVStore> ordersDb = manager.openDatabase("orders",
    DatabaseConfig.builder("/path/to/orders").build());

// List all open databases
List<String> databases = manager.listDatabases();
System.out.println("Open databases: " + databases);

// Get a specific database
Result<KVStore> db = manager.getDatabase("users");

// Close a specific database
manager.closeDatabase("users");

// Close all databases
manager.closeAll();
```

### Result Pattern for Error Handling

```java
Result<byte[]> result = store.get(key);

// Check success/failure
if (result.isSuccess()) {
    byte[] value = result.getValue().get();
    // Process value
} else {
    String error = result.getError().orElse("Unknown error");
    System.err.println("Error: " + error);
}

// Functional style
result
    .ifSuccess(value -> System.out.println("Got: " + new String(value)))
    .ifFailure(error -> System.err.println("Error: " + error));

// Map and flatMap
Result<String> stringResult = result.map(bytes -> new String(bytes));

Result<Integer> lengthResult = result.flatMap(bytes -> {
    if (bytes.length > 0) {
        return Result.success(bytes.length);
    } else {
        return Result.failure("Empty value");
    }
});
```

## Testing

Run tests with Maven:

```bash
cd hitorro-kvstore
mvn test
```

## Performance Tips

1. **Use Batch Operations**: Batch puts/gets are significantly faster than individual operations
2. **Choose Appropriate Compression**: LZ4 for speed, ZSTD for compression ratio, Snappy for balance
3. **Tune Cache Sizes**: Larger block cache improves read performance
4. **Async Writes**: Set `syncWrites(false)` for better write throughput (with durability trade-off)
5. **Prefix Scanning**: Design keys with prefixes for efficient range queries
6. **Use Transactions Sparingly**: Only use transactional batches when atomicity is required
7. **Bloom Filters**: Higher `bitsPerKey` reduces false positives in read queries

## Dependencies

- RocksDB 10.4.2 (latest stable)
- Jackson 2.18.2 (JSON serialization)
- Log4j 1.2.17 (logging)
- JUnit Jupiter 5.11.4 (testing)
- AssertJ 3.27.3 (test assertions)

## License

Part of the Hitorro project.

## Contributing

Contributions are welcome! Please ensure tests pass before submitting PRs.
