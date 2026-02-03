package com.hitorro.kvstore;

import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;

/**
 * Helper class for loading RocksDB native libraries with better error handling.
 */
public class RocksDBLoader {
    private static final Logger logger = Logger.getLogger(RocksDBLoader.class);
    private static volatile boolean loaded = false;
    private static final Object lock = new Object();

    /**
     * Ensures RocksDB native library is loaded.
     * This method is thread-safe and will only load the library once.
     *
     * @throws RuntimeException if the library cannot be loaded
     */
    public static void loadLibrary() {
        if (loaded) {
            return;
        }

        synchronized (lock) {
            if (loaded) {
                return;
            }

            try {
                // Try to load the library
                RocksDB.loadLibrary();
                loaded = true;
                logger.info("RocksDB native library loaded successfully");
                
                // Log system information for debugging
                String osName = System.getProperty("os.name");
                String osArch = System.getProperty("os.arch");
                logger.info("Running on: " + osName + " (" + osArch + ")");
                
            } catch (Exception e) {
                String osName = System.getProperty("os.name");
                String osArch = System.getProperty("os.arch");
                String javaLibPath = System.getProperty("java.library.path");
                
                String errorMsg = String.format(
                    "Failed to load RocksDB native library.\n" +
                    "OS: %s\n" +
                    "Arch: %s\n" +
                    "Java library path: %s\n" +
                    "Error: %s\n\n" +
                    "To fix this issue:\n" +
                    "1. Ensure rocksdbjni JAR is in your classpath\n" +
                    "2. For IntelliJ IDEA: Go to File -> Invalidate Caches and restart\n" +
                    "3. Or run tests from command line: mvn test\n" +
                    "4. The rocksdbjni JAR should contain native libraries for your platform",
                    osName, osArch, javaLibPath, e.getMessage()
                );
                
                logger.error(errorMsg, e);
                throw new RuntimeException(errorMsg, e);
            }
        }
    }

    /**
     * Checks if the RocksDB library is loaded.
     *
     * @return true if loaded, false otherwise
     */
    public static boolean isLoaded() {
        return loaded;
    }
}
