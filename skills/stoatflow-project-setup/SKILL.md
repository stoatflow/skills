---
name: stoatflow-project-setup
description: Set up a project that uses StoatFlow — Gradle or Maven build, the dependency coordinates, the PRIVATE StoatFlow Maven repository (maven.stoatflow.io) and its credentials, the licence, JDK 25 or newer and the JVM flags, the io.stoatflow Gradle plugin / Maven parent+BOM+plugin, Docker (Jib), and GraalVM native image. Use for build/dependency/repo wiring; for application config keys use stoatflow-configure.
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
`--enable-native-access=ALL-UNNAMED`, and the `integrationTest` task — so you don't wire JVM flags by hand. Resolving the
plugin needs the private repo in **`pluginManagement`** too (see `references/build-setup.md`).

## Maven (full snippets in references/build-setup.md)

Extend the `io.stoatflow:stoatflow-parent` POM (imports `stoatflow-bom` → omit versions on `io.stoatflow:*`
deps), credentials in `~/.m2/settings.xml` (`<server id="stoatflow-releases">`), the repo in `pom.xml`
(matching `<id>stoatflow-releases</id>`). The `<parent>` version must be a **literal** (Maven can't resolve
a property there). Config knobs go in `<properties>` (`stoatflow.mainClass`, `stoatflow.docker.*`,
`stoatflow.nativeImage.*`); profiles activate with `-P`, not `<properties>`.

## Licence

StoatFlow verifies a signed licence **offline** at `start()` — no licence server, no network access. Set
**`STOATFLOW_LICENCE_KEY`** (value starts `sfl1.`, from your onboarding email; a free 30-day trial starts at
`https://stoatflow.io/trial` — the licence and the Maven token arrive together by email, nothing is shown on
screen — and a commercial licence comes from `sales@stoatflow.io`). Precedence: env var >
`-D` system property > `application.yaml` (`stoatflow.licence.key`); else a file (`STOATFLOW_LICENCE_FILE`,
default `~/.stoatflow/licence.key`, chmod 600; only a root-owned mount, such as a Kubernetes Secret with `fsGroup` and `defaultMode: 0440`, may also be group-readable). An explicit key always beats a file. In `application.yaml`,
interpolate to keep the literal out of VCS: `key: ${STOATFLOW_LICENCE_KEY}`. Note the British spelling:
`STOATFLOW_LICENSE_*` is not read.

Each deployment declares an **environment class** (`STOATFLOW_LICENCE_ENVIRONMENT_CLASS`): `production` (the
default), `staging`, `development` or `other`. The licence prices the classes it lists; unlisted classes are
free, and a developer licence refuses `production`. **Production, enterprise and trial licences require
`STOATFLOW_LICENCE_ENVIRONMENT`** explicitly — e.g. `prd-eu`, `stg`; in Kubernetes the auto-generated hostname is
the pod name, which changes every redeploy (developer and internal licences default it to `<user>-<host>`, or
`cicd-<run id>` on GitHub Actions). Local development: developer licence plus
`STOATFLOW_LICENCE_ENVIRONMENT_CLASS=development`; CI: any licence plus `STOATFLOW_LICENCE_ENVIRONMENT_CLASS=other` (and a stable `STOATFLOW_LICENCE_ENVIRONMENT`, e.g. `ci`, when the licence lists `other` or `*`, which counts the class).
Refusals throw `io.stoatflow.core.exception.LicenceValidationException` from `start()` with a `code`. Details:
https://stoatflow.io/docs/getting-started/licence-configuration. The licence tool — verify a licence offline, apply a
renewal to a cluster without a restart, usage reports — is the fat jar
`io.stoatflow:stoatflow-licence-tool:<version>:all` from the same repository (JDK 25+; how-to in stoatflow-operate).

## JDK & flags

**JDK 25 or newer.** CI tests JDK 25 and the newest JDK release (27 today). One flag at test and run time:
**`--enable-native-access=ALL-UNNAMED`** (RocksDB uses the FFM API); `-XX:+UseG1GC` is on by default. No preview
flag is needed. The `io.stoatflow` Gradle plugin / Maven parent set these for you.

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
