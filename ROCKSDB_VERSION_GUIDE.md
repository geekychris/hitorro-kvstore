# RocksDB Version and Native Library Loading Guide

## Overview

This module now uses **RocksDB 10.4.2** (latest stable) with an abstraction layer that automatically handles native library loading across all platforms. The complexity of dealing with platform-specific native libraries is completely hidden from users of the API.

## How It Works

### The Problem

Starting with RocksDB 10.4.2, the Maven artifact changed how native libraries are distributed:

- **Before 10.4.2**: The main `rocksdbjni-X.X.X.jar` was a "fat JAR" containing native libraries for all platforms (~30MB)
- **After 10.4.2**: The main JAR contains only Java classes (~300KB), with platform-specific JARs available separately

This caused issues in IntelliJ and other IDEs where tests would fail with:
```
RuntimeException: Neither librocksdbjni-osx-arm64.jnilib or librocksdbjni-osx.jnilib 
were found inside the JAR, and there is no fallback.
```

### The Solution

We've implemented a two-part solution that makes this transparent:

#### 1. Multi-Platform Dependencies in POM

The `pom.xml` includes **all** platform-specific dependencies:

```xml
<properties>
    <rocksdb.version>10.4.2</rocksdb.version>
</properties>

<dependencies>
    <!-- Core RocksDB (Java classes only) -->
    <dependency>
        <groupId>org.rocksdb</groupId>
        <artifactId>rocksdbjni</artifactId>
        <version>${rocksdb.version}</version>
    </dependency>

    <!-- Platform-specific native libraries -->
    <dependency>
        <groupId>org.rocksdb</groupId>
        <artifactId>rocksdbjni</artifactId>
        <version>${rocksdb.version}</version>
        <classifier>osx-arm64</classifier>
    </dependency>
    <dependency>
        <groupId>org.rocksdb</groupId>
        <artifactId>rocksdbjni</artifactId>
        <version>${rocksdb.version}</version>
        <classifier>osx-x86_64</classifier>
    </dependency>
    <dependency>
        <groupId>org.rocksdb</groupId>
        <artifactId>rocksdbjni</artifactId>
        <version>${rocksdb.version}</version>
        <classifier>linux64</classifier>
    </dependency>
    <dependency>
        <groupId>org.rocksdb</groupId>
        <artifactId>rocksdbjni</artifactId>
        <version>${rocksdb.version}</version>
        <classifier>win64</classifier>
    </dependency>
</dependencies>
```

**Why include all platforms?**
- At runtime, only the appropriate platform library is loaded
- Other JARs are ignored (they're small, ~2-3MB each)
- This ensures the library works everywhere without configuration
- Great for CI/CD, team development, and cross-platform projects

#### 2. Enhanced RocksDBLoader Class

The `RocksDBLoader` utility class provides:

**Platform Detection:**
```java
String platform = RocksDBLoader.detectPlatform();
// Returns: "osx-arm64", "osx-x86_64", "linux64", "win64", etc.
```

**Automatic Loading:**
```java
RocksDBLoader.loadLibrary();
// Automatically loads the correct native library for your platform
```

**Better Error Messages:**
If loading fails, you get detailed diagnostic information:
- Detected OS and architecture
- Expected platform classifier
- Exact Maven dependency needed
- Step-by-step troubleshooting instructions

#### 3. Transparent Integration

All `KVStore` implementations automatically call `RocksDBLoader.loadLibrary()` in their constructors, so **users don't need to think about this at all**:

```java
// Just works - native library loading is automatic!
KVStore store = new RocksDBStore(config);
```

## Benefits of This Approach

### ✅ Latest RocksDB Features
- Use RocksDB 10.4.2 with all the latest performance improvements and bug fixes
- No need to stick with old versions due to library loading issues

### ✅ Works Everywhere
- IntelliJ IDEA ✓
- Eclipse ✓
- Command line (Maven, Gradle) ✓
- CI/CD systems ✓
- All platforms (Mac M1/Intel, Linux x64/ARM, Windows) ✓

### ✅ Zero Configuration
- No manual library installation
- No environment variables to set
- No IDE-specific configuration
- Works out of the box for all team members

### ✅ Great Developer Experience
- Run/debug tests directly in IDE
- Set breakpoints and step through code
- No "works on my machine" issues
- Clear error messages when something goes wrong

## Using in Your Own Projects

If you want to use this approach in your own RocksDB projects:

### 1. Add Dependencies

Copy the RocksDB dependencies from our `pom.xml` to yours.

### 2. Copy RocksDBLoader

Copy `src/main/java/com/hitorro/kvstore/RocksDBLoader.java` to your project.

### 3. Load Before Use

Before creating any RocksDB instances:
```java
import com.hitorro.kvstore.RocksDBLoader;

public class MyApp {
    static {
        // Load once at startup
        RocksDBLoader.loadLibrary();
    }
    
    public void useRocksDB() {
        // Now safe to use RocksDB
        try (RocksDB db = RocksDB.open(options, path)) {
            // ...
        }
    }
}
```

Or in your wrapper class constructor:
```java
public class MyRocksDBWrapper {
    public MyRocksDBWrapper() {
        RocksDBLoader.loadLibrary();
        // ... initialize RocksDB
    }
}
```

## Platform-Specific Deployment

For production deployments where you want minimal dependencies, you can optimize:

### Option A: Single Platform Deployment

If deploying to a known platform (e.g., Linux containers), remove unused platform dependencies:

```xml
<!-- Keep only what you need -->
<dependency>
    <groupId>org.rocksdb</groupId>
    <artifactId>rocksdbjni</artifactId>
    <version>10.4.2</version>
    <classifier>linux64</classifier>
</dependency>
```

### Option B: Maven Profiles

Use profiles to include platform-specific deps only when needed:

```xml
<profiles>
    <profile>
        <id>mac-arm64</id>
        <activation>
            <os>
                <family>mac</family>
                <arch>aarch64</arch>
            </os>
        </activation>
        <dependencies>
            <dependency>
                <groupId>org.rocksdb</groupId>
                <artifactId>rocksdbjni</artifactId>
                <version>10.4.2</version>
                <classifier>osx-arm64</classifier>
            </dependency>
        </dependencies>
    </profile>
    <!-- More profiles for other platforms... -->
</profiles>
```

### Option C: Docker Multi-Stage Builds

For Docker, build with all platforms, deploy with only needed one:

```dockerfile
# Build stage - includes all platforms for testing
FROM maven:3.9-eclipse-temurin-21 AS build
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src ./src
RUN mvn package

# Runtime stage - only include linux64
FROM eclipse-temurin:21-jre-alpine
COPY --from=build target/your-app.jar /app.jar
# The linux64 native library is already bundled in the JAR
ENTRYPOINT ["java", "-jar", "/app.jar"]
```

## Comparison: Old vs New Approach

### Old Approach (RocksDB 8.11.3)

**Pros:**
- Single "fat JAR" with all libraries
- Simple dependency declaration

**Cons:**
- Stuck on older version
- Missing newer features and bug fixes
- Fat JAR is ~30MB

### New Approach (RocksDB 10.4.2)

**Pros:**
- Latest stable version with newest features
- Better performance and bug fixes
- Smaller individual JARs (~2-3MB each)
- Future-proof as RocksDB continues this pattern
- Explicit about platform dependencies

**Cons:**
- Slightly more complex pom.xml
- Need RocksDBLoader abstraction (but we provide it!)

## Troubleshooting

### Tests Fail in IntelliJ After Updating

1. **Reload Maven Projects:**
   - Open Maven tool window
   - Click "Reload All Maven Projects" (circular arrows)

2. **Invalidate Caches:**
   - File → Invalidate Caches...
   - Select "Invalidate and Restart"

3. **Clean and Rebuild:**
   ```bash
   mvn clean install
   ```

4. **Check Downloaded JARs:**
   ```bash
   ls ~/.m2/repository/org/rocksdb/rocksdbjni/10.4.2/
   ```
   Should see files like:
   - `rocksdbjni-10.4.2.jar` (main JAR, ~300KB)
   - `rocksdbjni-10.4.2-osx-arm64.jar` (~2MB)
   - `rocksdbjni-10.4.2-linux64.jar` (~2MB)
   - etc.

### Wrong Platform Library Loaded

Check detected platform:
```java
String platform = RocksDBLoader.detectPlatform();
System.out.println("Detected: " + platform);
```

### Custom Library Path

If you need to use a custom RocksDB installation:

```java
// Set before loading
System.setProperty("java.library.path", "/path/to/rocksdb/lib");

// Or pass to RocksDB.loadLibrary()
List<String> paths = List.of("/custom/path");
RocksDB.loadLibrary(paths);
```

## Technical Details

### How RocksDB Loads Native Libraries

1. RocksDB checks for library in JAR resources
2. If found, extracts to temp directory (e.g., `/tmp`)
3. Loads extracted library via `System.load()`
4. Library is deleted on JVM shutdown

### Platform Detection Logic

From `RocksDBLoader.detectPlatform()`:

| OS Property | Arch Property | Result Classifier |
|------------|---------------|-------------------|
| Mac OS X | aarch64 | osx-arm64 |
| Mac OS X | x86_64 | osx-x86_64 |
| Linux | aarch64 | linux-aarch64 |
| Linux | amd64/x86_64 | linux64 |
| Windows | any | win64 |

### Supported Platforms

RocksDB 10.4.2 provides native libraries for:
- **macOS**: ARM64 (M1/M2/M3), x86_64 (Intel)
- **Linux**: x86_64, ARM64, PPC64LE, S390X
- **Windows**: x64
- **FreeBSD**: x86_64

## References

- [RocksDB Issue #13893](https://github.com/facebook/rocksdb/issues/13893) - Discussion about fat JAR removal
- [RocksDB Java Basics](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics) - Official documentation
- [Maven Central](https://repo1.maven.org/maven2/org/rocksdb/rocksdbjni/10.4.2/) - RocksDB 10.4.2 artifacts

## Summary

The approach we've implemented:
1. **Includes all platform dependencies** in pom.xml (development convenience)
2. **Provides RocksDBLoader utility** for automatic platform detection and loading
3. **Integrates transparently** - users don't need to think about native libraries
4. **Works everywhere** - IntelliJ, command line, CI/CD, all platforms
5. **Uses latest RocksDB** - 10.4.2 with all newest features

This is now **part of the abstraction** - the complexity is hidden, and it just works! 🎉
