package com.hitorro.kvstore;

import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper class for loading RocksDB native libraries with better error handling.
 * 
 * <p>This class handles the complexity of loading platform-specific native libraries
 * for RocksDB. Starting with RocksDB 10.4.2, the main JAR no longer includes native
 * libraries, so platform-specific JARs must be included as dependencies.</p>
 * 
 * <p>This loader attempts multiple strategies:
 * <ol>
 *   <li>Load from classpath (platform-specific JARs)</li>
 *   <li>Load from java.library.path</li>
 *   <li>Provide helpful error messages if loading fails</li>
 * </ol>
 */
public class RocksDBLoader {
    private static final Logger logger = Logger.getLogger(RocksDBLoader.class);
    private static volatile boolean loaded = false;
    private static final Object lock = new Object();
    
    // Detected platform information
    private static String detectedPlatform = null;

    /**
     * Detects the current platform and returns the classifier string.
     *
     * @return platform classifier (e.g., "osx", "linux64", "win64")
     */
    public static final String detectPlatform() {
        if (detectedPlatform != null) {
            return detectedPlatform;
        }
        
        String osName = System.getProperty("os.name").toLowerCase();
        String osArch = System.getProperty("os.arch").toLowerCase();
        
        // RocksDB 10.4.2 uses these classifier names:
        // - osx (universal for Mac - works on both ARM64 and x86_64)
        // - linux64, linux64-musl
        // - linux32, linux32-musl
        // - win64
        
        if (osName.contains("mac") || osName.contains("darwin")) {
            detectedPlatform = "osx";
        } else if (osName.contains("linux")) {
            if (osArch.contains("aarch64") || osArch.contains("arm64")) {
                detectedPlatform = "linux64";  // ARM64 uses linux64 on modern systems
            } else if (osArch.contains("amd64") || osArch.contains("x86_64")) {
                detectedPlatform = "linux64";
            } else {
                detectedPlatform = "linux32";
            }
        } else if (osName.contains("win")) {
            detectedPlatform = "win64";
        } else {
            detectedPlatform = "unknown";
        }
        
        return detectedPlatform;
    }
    
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

            String osName = System.getProperty("os.name");
            String osArch = System.getProperty("os.arch");
            String platform = detectPlatform();
            
            try {
                // Try to load the library using RocksDB's loader
                RocksDB.loadLibrary();
                loaded = true;
                logger.info("RocksDB native library loaded successfully");
                logger.info("Platform: " + osName + " (" + osArch + ") - Detected: " + platform);
                
            } catch (Exception e) {
                String javaLibPath = System.getProperty("java.library.path");
                
                // Build detailed error message
                StringBuilder errorMsg = new StringBuilder();
                errorMsg.append("Failed to load RocksDB native library.\n");
                errorMsg.append("\nSystem Information:\n");
                errorMsg.append("  OS: ").append(osName).append("\n");
                errorMsg.append("  Arch: ").append(osArch).append("\n");
                errorMsg.append("  Platform: ").append(platform).append("\n");
                errorMsg.append("  Java library path: ").append(javaLibPath).append("\n");
                errorMsg.append("\nError: ").append(e.getMessage()).append("\n");
                
                errorMsg.append("\nPossible Solutions:\n");
                errorMsg.append("\n1. For Maven projects:\n");
                errorMsg.append("   Add platform-specific dependency to pom.xml:\n");
                errorMsg.append("   <dependency>\n");
                errorMsg.append("     <groupId>org.rocksdb</groupId>\n");
                errorMsg.append("     <artifactId>rocksdbjni</artifactId>\n");
                errorMsg.append("     <version>10.4.2</version>\n");
                errorMsg.append("     <classifier>").append(platform).append("</classifier>\n");
                errorMsg.append("   </dependency>\n");
                
                errorMsg.append("\n2. For IntelliJ IDEA:\n");
                errorMsg.append("   a) File -> Invalidate Caches -> Invalidate and Restart\n");
                errorMsg.append("   b) Maven -> Reload All Maven Projects\n");
                
                errorMsg.append("\n3. For command line:\n");
                errorMsg.append("   mvn clean install\n");
                errorMsg.append("   mvn test\n");
                
                errorMsg.append("\n4. Check that rocksdbjni-").append(platform).append(".jar is in classpath\n");
                
                String msg = errorMsg.toString();
                logger.error(msg, e);
                throw new RuntimeException(msg, e);
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
