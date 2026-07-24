# Docker (Jib) & GraalVM native image

Both are opt-in on the `io.stoatflow` Gradle plugin / Maven parent — no Dockerfile needed.

## Docker (Jib)

**Gradle** — opt in, then build to the local daemon:
```kotlin
stoatflow {
    mainClass.set("com.example.MainKt")
    docker {
        enabled.set(true)                               // default: false
        imageName.set("my-app")                         // default: project.name
        port.set(8080)                                  // default: 8080
        user.set("1000:1000")                           // default: 1000:1000
        baseImage.set("eclipse-temurin:25-jre-noble")   // default (JVM/Jib only)
        stateDir.set("/data/state")                     // default: /data/state
        rocksdbMb.set(256)                              // default: auto-detect
        reproducibleBuild.set(true)                     // default: true (git-commit timestamp)
        jvmFlags.set(listOf("-XX:+UseG1GC"))            // overridable; preview/native-access always added
    }
}
```
```bash
./gradlew jibDockerBuild        # → my-app image in the local docker daemon
```

**Maven** — profile-activated (a `<properties>` flag does NOT turn it on):
```bash
mvn -Pstoatflow-docker package  # build + jib image to the local daemon, one command
```

Reproducible build, default base `eclipse-temurin:25-jre-noble`, port 8080, user `1000:1000`, entrypoint
`/app/stoatflow-entrypoint.sh` (container-aware heap).

## GraalVM native image

**Gradle** — opt in:
```kotlin
stoatflow {
    nativeImage {
        enabled.set(true)                   // default: false
        gc.set("G1")                        // default: absent (Serial GC)
        runtimeTarget.set("runtime")        // default: runtime; or "runtime-lean"
        // additionalInitializeAtRunTime.add("com.example.MyClass")
        // removeInitializeAtRunTime.add("org.rocksdb.RocksDB")
    }
}
```
```bash
./gradlew nativeDockerBuild     # → <imageName>:native + <imageName>:<version>-native
./gradlew nativeCompile         # host GraalVM smoke-compile, no Docker
```

**Maven:**
```bash
mvn package                     # build the fat JAR first
mvn stoatflow:native-docker-build   # native image via staged docker build
mvn -Pstoatflow-native package  # host GraalVM smoke-compile, no Docker
```

**Notes:**
- Use **Oracle GraalVM + G1** for any stateful (RocksDB) workload — Community Edition is Serial-GC only.
  Budget 8 GB+ RAM and ~15–20 min; a containerized build is recommended.
- The framework ships its own reflection / JNI / FFM native-image metadata — you only add metadata for
  **your own serde value classes** (see the native-image guide at
  <https://stoatflow.io/docs/runtime/native-image>).
