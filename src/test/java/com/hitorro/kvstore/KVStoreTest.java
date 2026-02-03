package com.hitorro.kvstore;

import com.hitorro.kvstore.config.CompressionType;
import com.hitorro.kvstore.config.StorageMode;
import org.junit.jupiter.api.*;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic tests for KVStore functionality.
 */
class KVStoreTest {
    private static final String TEST_DB_PATH = "/tmp/hitorro-kvstore-test";
    private KVStore store;

    @BeforeEach
    void setUp() throws Exception {
        // Clean up any existing test database
        deleteDirectory(new File(TEST_DB_PATH));
        
        DatabaseConfig config = DatabaseConfig.builder(TEST_DB_PATH)
                .compressionType(CompressionType.SNAPPY)
                .storageMode(StorageMode.DISK)
                .createIfMissing(true)
                .build();
        
        store = new RocksDBStore(config);
    }

    @AfterEach
    void tearDown() {
        if (store != null && store.isOpen()) {
            store.close();
        }
        // Clean up test database
        deleteDirectory(new File(TEST_DB_PATH));
    }

    @Test
    void testBasicPutAndGet() {
        byte[] key = "testKey".getBytes();
        byte[] value = "testValue".getBytes();

        Result<Void> putResult = store.put(key, value);
        assertThat(putResult.isSuccess()).isTrue();

        Result<byte[]> getResult = store.get(key);
        assertThat(getResult.isSuccess()).isTrue();
        assertThat(getResult.getValue().orElse(null)).isEqualTo(value);
    }

    @Test
    void testDelete() {
        byte[] key = "deleteKey".getBytes();
        byte[] value = "deleteValue".getBytes();

        store.put(key, value);
        
        Result<Void> deleteResult = store.delete(key);
        assertThat(deleteResult.isSuccess()).isTrue();

        Result<byte[]> getResult = store.get(key);
        assertThat(getResult.isFailure()).isTrue();
    }

    @Test
    void testBatchPut() {
        Map<byte[], byte[]> entries = new HashMap<>();
        entries.put("key1".getBytes(), "value1".getBytes());
        entries.put("key2".getBytes(), "value2".getBytes());
        entries.put("key3".getBytes(), "value3".getBytes());

        Result<Void> result = store.batchPut(entries, false);
        assertThat(result.isSuccess()).isTrue();

        Result<byte[]> getResult = store.get("key2".getBytes());
        assertThat(getResult.isSuccess()).isTrue();
        assertThat(new String(getResult.getValue().orElse(new byte[0]))).isEqualTo("value2");
    }

    @Test
    void testBatchGet() {
        // Put some data
        store.put("key1".getBytes(), "value1".getBytes());
        store.put("key2".getBytes(), "value2".getBytes());
        store.put("key3".getBytes(), "value3".getBytes());

        List<byte[]> keys = Arrays.asList(
                "key1".getBytes(),
                "key2".getBytes(),
                "key3".getBytes()
        );

        Result<List<byte[]>> result = store.batchGet(keys);
        assertThat(result.isSuccess()).isTrue();
        
        List<byte[]> values = result.getValue().orElse(Collections.emptyList());
        assertThat(values).hasSize(3);
        assertThat(new String(values.get(0))).isEqualTo("value1");
        assertThat(new String(values.get(1))).isEqualTo("value2");
        assertThat(new String(values.get(2))).isEqualTo("value3");
    }

    @Test
    void testPrefixScan() {
        // Put data with common prefix
        store.put("user:1".getBytes(), "Alice".getBytes());
        store.put("user:2".getBytes(), "Bob".getBytes());
        store.put("user:3".getBytes(), "Charlie".getBytes());
        store.put("item:1".getBytes(), "Book".getBytes());

        List<byte[]> values = new ArrayList<>();
        Iterator<byte[]> iterator = store.scanByPrefix("user:".getBytes());
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }

        assertThat(values).hasSize(3);
    }

    @Test
    void testPrefixScanWithKeys() {
        store.put("product:1".getBytes(), "Laptop".getBytes());
        store.put("product:2".getBytes(), "Mouse".getBytes());
        store.put("product:3".getBytes(), "Keyboard".getBytes());

        List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
        Iterator<Map.Entry<byte[], byte[]>> iterator = store.scanByPrefixWithKeys("product:".getBytes());
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }

        assertThat(entries).hasSize(3);
        assertThat(new String(entries.get(0).getKey())).startsWith("product:");
    }

    @Test
    void testConsumer() {
        store.put("data:1".getBytes(), "First".getBytes());
        store.put("data:2".getBytes(), "Second".getBytes());
        store.put("data:3".getBytes(), "Third".getBytes());

        List<String> collected = new ArrayList<>();
        store.consumePrefixValues("data:".getBytes(), bytes -> {
            collected.add(new String(bytes));
        });

        assertThat(collected).hasSize(3);
        assertThat(collected).contains("First", "Second", "Third");
    }

    @Test
    void testKeyExtraction() {
        class User {
            String id;
            String name;
            
            User(String id, String name) {
                this.id = id;
                this.name = name;
            }
        }

        User user = new User("user123", "John Doe");
        
        Result<Void> result = store.put(
                user,
                u -> ("user:" + u.id).getBytes(),
                u -> u.name.getBytes()
        );

        assertThat(result.isSuccess()).isTrue();

        Result<byte[]> getResult = store.get("user:user123".getBytes());
        assertThat(getResult.isSuccess()).isTrue();
        assertThat(new String(getResult.getValue().orElse(new byte[0]))).isEqualTo("John Doe");
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
