---
name: stoatflow-project-setup
description: Set up a project that uses StoatFlow — Gradle or Maven build, the dependency coordinates, the PRIVATE StoatFlow Maven repository (maven.stoatflow.io) and its credentials, the license key, JDK 25 with preview flags, the io.stoatflow Gradle plugin / Maven parent+BOM+plugin, Docker (Jib), and GraalVM native image. Use for build/dependency/repo wiring; for application config keys use stoatflow-configure.
license: Apache-2.0
---

# Set up a StoatFlow project

Read `references/stoatflow-primer.md` first. **The single most important fact:** StoatFlow is **NOT on
Maven Central** — it is a commercial product served only from the private repository
**`https://maven.stoatflow.io/releases`** with your customer credentials. Never resolve `io.stoatflow:*`
(or any `org.apache.kafka:kafka-streams`) from Maven Central.

## Coordinates

Group `io.stoatflow`. Four published modules:

- `io.stoatflow:stoatflow-runtime` — batteries-included runtime (HTTP, metrics, health); transitively
  pulls `-core`. The usual single main dependency.
- `io.stoatflow:stoatflow-core` — DSL + engine only.
- `io.stoatflow:stoatflow-test-utils` (test scope) — broker-free `TopologyTestDriver` (pairs with `-core`).
- `io.stoatflow:stoatflow-test-runtime` (test scope) — loads `application.yaml` (pairs with `-runtime`).

`mavenCentral()` stays in your repository list — but only to resolve ordinary third-party deps
(kafka-clients, rocksdbjni). The one Apache Kafka type you touch directly is
`org.apache.kafka.common.serialization.Serdes` (transitive). There is **no** `kafka-streams` dependency.

## Gradle (full snippets in references/build-setup.md)

Two pieces: the **dependency + plugin repositories** (in `settings.gradle.kts`, with credentials from
`~/.gradle/gradle.properties`) and the dependency + plugin application (in `build.gradle.kts`).

```kotlin
// ~/.gradle/gradle.properties  (HOME dir, never committed)
stoatflowRepoUser=customer-your-slug
stoatflowRepoToken=PASTE_YOUR_MAVEN_TOKEN_FROM_THE_ONBOARDING_EMAIL
```
```kotlin
// build.gradle.kts
plugins {
    kotlin("jvm") version "2.4.0"          // omit for a Java-only project
    id("io.stoatflow") version "<stoatflow-version>"
}
stoatflow { mainClass.set("com.example.MainKt") }
dependencies {
    implementation("io.stoatflow:stoatflow-runtime:<stoatflow-version>")
    testImplementation("io.stoatflow:stoatflow-test-utils:<stoatflow-version>")
}
```

The `io.stoatflow` convention plugin applies `application` + `shadow` (fat JAR), the JDK-25 toolchain,
`--enable-preview`, and the `integrationTest` task — so you don't wire JVM flags by hand. Resolving the
plugin needs the private repo in **`pluginManagement`** too (see `references/build-setup.md`).

## Maven (full snippets in references/build-setup.md)

Extend the `io.stoatflow:stoatflow-parent` POM (imports `stoatflow-bom` → omit versions on `io.stoatflow:*`
deps), credentials in `~/.m2/settings.xml` (`<server id="stoatflow-releases">`), the repo in `pom.xml`
(matching `<id>stoatflow-releases</id>`). The `<parent>` version must be a **literal** (Maven can't resolve
a property there). Config knobs go in `<properties>` (`stoatflow.mainClass`, `stoatflow.docker.*`,
`stoatflow.nativeImage.*`); profiles activate with `-P`, not `<properties>`.

## License key

Set **`STOATFLOW_LICENSE_KEY`** (value starts `key/…`, from your onboarding email). Precedence: env var >
`-D` system property > `application.yaml` (`stoatflow.license.key`). In `application.yaml`, interpolate to
keep the literal out of VCS: `key: ${STOATFLOW_LICENSE_KEY}`. **Production requires
`STOATFLOW_LICENSE_ENVIRONMENT`** explicitly (local dev auto-derives `<user>-<host>`; GitHub Actions
auto-derives `cicd-<run id>`). Deferred validation means short-lived test/CI instances consume zero seats.

## JDK & flags

**JDK 25** at build and runtime. Two flags everywhere (compile, test, run): **`--enable-preview`** and
**`--enable-native-access=ALL-UNNAMED`** (RocksDB uses the FFM API); `-XX:+UseG1GC` is on by default. The
`io.stoatflow` Gradle plugin / Maven parent set these for you.

## Build & run

```bash
# Gradle
./gradlew run              # run locally with the required JVM flags
./gradlew shadowJar        # fat JAR → build/libs/<name>-<version>-all.jar
./gradlew jibDockerBuild   # container image (docker.enabled)  — see references/native-and-docker.md
./gradlew nativeDockerBuild# native image  (nativeImage.enabled)
# Maven: mvn stoatflow:run · mvn package · mvn -Pstoatflow-docker package · mvn stoatflow:native-docker-build
```

## See also

- `references/build-setup.md` — the complete Gradle + Maven repository/credentials/plugin snippets
- `references/native-and-docker.md` — Docker (Jib) + GraalVM native image
- `references/stoatflow-primer.md`
- Website: <https://stoatflow.io/docs/getting-started/installation>
