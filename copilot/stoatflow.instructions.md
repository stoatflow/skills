---
applyTo: "**/*.{kt,java}"
---

# StoatFlow — rules for AI coding assistants

> **Targets StoatFlow 1.0.0-rc.6 (Kafka Streams 4.4 API surface).** Pre-GA — pin the matching
> release of this file if your StoatFlow version differs.

StoatFlow is a JVM stream-processing library that is **source-compatible with Kafka Streams 4.4 via an
import swap** — but it is **not** Kafka Streams. Your Kafka-Streams training is *confidently wrong* on the
points below. These rules override it. (This file is the cross-tool distillate of the StoatFlow skills
pack; the full skills carry depth.)

## Identity — the non-negotiables

- **NEVER** import `org.apache.kafka.streams.*` — always **`io.stoatflow.core.*`** (see the swap table).
  `org.apache.kafka.common.*` / `org.apache.kafka.clients.*` (serdes, `Headers`, `ConsumerRecord`) are
  unchanged.
- **NEVER** add a `kafka-streams` dependency — StoatFlow depends only on **`kafka-clients`**. The artifact
  is `io.stoatflow:stoatflow-core` (+ `-runtime`, `-test-utils`, `-test-runtime`).
- **NEVER** scale with `replicas: N` — StoatFlow runs as **exactly one active instance**. Scale with
  **lanes** (`withNumberOfLanes`, config) and vertically; use **HA active/standby** (`ha.mode`) for
  availability. A second active instance corrupts state.
- **StoatFlow is not on Maven Central** — resolve from **`maven.stoatflow.io`** with customer credentials.

## The import swap

| Kafka Streams prefix | StoatFlow prefix |
|---|---|
| `org.apache.kafka.streams.kstream.` | `io.stoatflow.core.topology.` |
| `org.apache.kafka.streams.state.` | `io.stoatflow.core.state.` |
| `org.apache.kafka.streams.processor.api.` | `io.stoatflow.core.processor.` |
| `org.apache.kafka.streams.processor.` | `io.stoatflow.core.processor.` |
| `org.apache.kafka.streams.errors.` | `io.stoatflow.core.exception.` |
| `org.apache.kafka.streams.{StreamsBuilder,Topology,KeyValue,CloseOptions}` | `io.stoatflow.core.{topology.StreamsBuilder,topology.Topology,state.KeyValue,CloseOptions}` |
| `org.apache.kafka.streams.TopologyConfig` | `io.stoatflow.core.config.TopologyConfig` |
| `org.apache.kafka.streams.KafkaStreams.State` | `io.stoatflow.core.KafkaStreamsState` |

Mis-routed by the blind swap (fix by hand / the OpenRewrite recipe fixes them): `StreamPartitioner` →
`topology`, `StateStore`/`StateStoreContext`/`StateRestoreListener` → `state`,
`TimestampExtractor`/`TopicNameExtractor`/`RecordContext` → `topology`, `TaskId` → `config`,
`RocksDBConfigSetter` → `state.rocksdb`.

## Top divergences (a ported/new app gets these wrong)

1. **Default `processing.guarantee` is `EXACTLY_ONCE`** (KS: `at_least_once`). A ported app that relied on
   the KS default silently runs under EOS. Set `at_least_once` explicitly if you want ALO. *(KSC-84)*
2. **`currentStreamTimeMs()` returns the event-time watermark** (global min − out-of-orderness), not KS's
   max-observed stream time — generally lower and global. `currentWatermarkMs()` is the native name.
   *(KSC-85)*
3. **`TaskId` is always `0_0`** (`TaskId.SINGLE`) — single-instance; don't branch on it. *(D6)*
4. **Scale with lanes, not replicas** — `withNumberOfLanes` (Consumed/Grouped/Repartitioned) + config;
   HA standby for availability.
5. **Application entry point:** `StoatFlow.fromBuilder(new StreamsConfig(props), builder)` — pass the
   **builder**, not the topology. `@JvmStatic`. *(KSC-86)*
6. **Exception handlers:** `ProcessingExceptionHandler.handle(...)` takes a `processor.Record<*,*>` (not
   separate key/value); production serialization errors fold into `handle(...)` (branch on
   `(context as ProductionContext).failedOn`). *(KSC-01)*
7. **`StreamPartitioner`** lives in `io.stoatflow.core.topology`; multicast on `repartition()` **throws**
   (single-instance) — multicast only on sinks. KS 4.4 shape: override the 5-arg `partitions(..., headers, n)` in a
   class to route by headers; keys are serialized with the record's headers, as KS does at sinks (KIP-1321). *(KSC-34, KSC-103)*
8. **`suppress(untilTimeLimit)` releases on the watermark**, not the task's stream time — every update is buffered,
   a key leaves `limit` after its first one; `Duration.ZERO` emits on the next watermark advance. *(KSC-119)*
9. **`TopologyTestDriver` enforces serdes** at the source/sink boundary and **commits per `pipeInput`**
   (caching aggregations emit every intermediate) — configure serdes; expect per-pipe emissions.
   *(KSC-62/63/71)*
10. **`Stores.*` require-guard:** `Materialized.as` / `StreamJoined` accept only `Stores.*` suppliers — every
    one, timestamped and headers-aware included (hand-rolled `*BytesStoreSupplier` throws); a supplier of another store
    kind than the operator's is refused at topology build, and a windowed aggregation refuses `retainDuplicates = true`.
    And `VersionedRecord.validTo()` returns `Optional<Long>`. *(KSC-21, KSC-152, KSC-04)*
11. **Output topics must exist at start:** a missing sink or dead-letter topic refuses `start()`
    (`MissingOutputTopicException`, naming it) — KS lets the first send create it. Create them, or set
    `validation.validate-output-topics=false` for a cluster that relies on auto-creation. A handler wrapping a built-in
    DLQ handler returns its delegate's `deadLetterTopics()`. *(KSC-148)*
12. **A punctuator runs on its own instance** — every Processor API node has a dedicated punctuator instance, so a
    punctuator never sees fields `process()` sets. A buffer filled in a field and flushed by a punctuator flushes
    nothing; a `Cancellable` cancelled from `process()` cancels nothing — keep shared state in a store. Its store and
    timer work runs alone at the next commit barrier, pausing the sub-topology: keep it short; long or per-key
    time-driven work belongs in timers. *(KSC-160)*

## Config quick rules

- Config is `application.yaml` (`stoatflow.*` + `runtime.*`) or the typed `StreamsConfig.Builder`; a KS
  `Properties`/`Map` maps in via `StreamsConfig.fromProperties(props).build()` / `fromMap(map).build()`
  (79-key KS overlap).
- **Do not invent config keys** — validate against the schema:
  `https://stoatflow.io/schemas/stoatflow-config-schema.json`.
- Processing guarantee: `stoatflow.processing-guarantee: exactly_once | at_least_once` (default
  `exactly_once`). Lanes: `stoatflow.lanes.count` (default `max(2, CPU cores / 4)` per sub-topology, on
  `PLATFORM` threads), or `withNumberOfLanes(n)` for one sub-topology; many lanes plus `lanes.thread-type: VIRTUAL`
  for blocking I/O. Output is zstd-compressed by default.

## Dependencies & repository (private)

```kotlin
// settings.gradle.kts — StoatFlow is on maven.stoatflow.io, NOT Maven Central
dependencyResolutionManagement {
  repositories {
    mavenCentral()   // third-party deps only (kafka-clients, rocksdb…) — NEVER io.stoatflow:* or kafka-streams
    maven {
      url = uri("https://maven.stoatflow.io/releases")
      credentials {   // stored in ~/.gradle/gradle.properties, never committed
        username = providers.gradleProperty("stoatflowRepoUser").get()   // "customer-<your-slug>"
        password = providers.gradleProperty("stoatflowRepoToken").get()
      }
    }
  }
}
// build.gradle.kts
dependencies {
  implementation("io.stoatflow:stoatflow-runtime:1.0.0-rc.6")   // batteries-included (pulls -core)
  // implementation("io.stoatflow:stoatflow-core:1.0.0-rc.6")   // DSL/engine only
  // testImplementation("io.stoatflow:stoatflow-test-utils:1.0.0-rc.6")
}
```

Runtime needs a licence, verified offline (no egress): set `STOATFLOW_LICENCE_KEY` (value starts `sfl1.`, from your onboarding email), and `STOATFLOW_LICENCE_ENVIRONMENT_CLASS` (`staging`, `development`, `other`) outside production.
Requires **JDK 25 or newer** (CI tests JDK 25 and the newest JDK release, 27 today) with
`--enable-native-access=ALL-UNNAMED` (the `io.stoatflow` Gradle plugin / Maven parent add it). Never inline a realistic token — credentials come from the customer portal.

## Links

- Docs: <https://stoatflow.io/docs> · AI assistants: <https://stoatflow.io/docs/getting-started/ai-assistants>
- Compatibility matrix: <https://stoatflow.io/docs/reference/ks-compatibility-matrix>
- Machine-readable docs (fetch when you need a detail this file omits):
  <https://stoatflow.io/llms.txt> (index) · <https://stoatflow.io/llms-full.txt> (full bundle) ·
  any indexed page as raw markdown at `/raw/<path>.md`
- Skills repo (full skills + this file): <https://github.com/stoatflow/skills>
- Carrying KS state across a port → the separate `stoatflow-migration-tool` CLI (see the port-from-ks skill;
  teach only its documented `plan`/`translate`/`seed-offsets`/`verify` commands, never invent flags).
