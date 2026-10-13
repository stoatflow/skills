---
name: stoatflow-test
description: Write or debug tests for StoatFlow topologies. TopologyTestDriver unit tests (pipe input, read output, advance watermarks, commitBarrier, assert state stores), the serde round-trip and per-pipe commit behaviour, StoatFlowTestDriver config-driven tests, and real-broker integration tests. Use for testing StoatFlow topologies; for writing the topology itself use stoatflow-build-topology.
license: Apache-2.0
---

# Test a StoatFlow topology

Read `references/stoatflow-primer.md` first. Testing is where StoatFlow's Kafka-Streams source-compat is
most seductive and most divergent — the `TopologyTestDriver` API matches KS, but its *timing* semantics
(serde enforcement, per-pipe commit, caching that never consolidates, watermark model) differ in ways a KS-trained assistant gets wrong.

Two levels:

1. **`TopologyTestDriver`** — in-memory, synchronous, no broker. The default for topology logic.
2. **Real-broker integration testing** — the same app against a Testcontainers Kafka broker, for what the
   TTD structurally *cannot* see (EOS transactions, changelog/restoration, cross-lane ordering). This is a
   **pattern**, not a shipped harness (see `references/integration-testing.md`).

## Dependencies

```kotlin
testImplementation("io.stoatflow:stoatflow-test-utils")     // TopologyTestDriver
testImplementation("io.stoatflow:stoatflow-test-runtime")   // StoatFlowTestDriver (YAML config → TTD)
```

Resolved from `maven.stoatflow.io` — see `stoatflow-project-setup`.

## TopologyTestDriver — the idiom

```kotlin
val builder = StreamsBuilder()
builder.stream<String, String>("input")
    .mapValues { v -> v.uppercase() }
    .to("output")

// Recommended factory — captures the processor + state-store registries.
val driver = TopologyTestDriver.fromBuilder(builder, TopologyTestDriver.STRING_CONFIG)
// KS-shaped ctors also work: TopologyTestDriver(topology, props) / (topology, props, Instant)

val input  = driver.createInputTopic("input",  Serdes.String(), Serdes.String())
val output = driver.createOutputTopic("output", Serdes.String(), Serdes.String())

input.pipeInput("k", "hello")                 // synchronous — output is on the queue when this returns
assertThat(output.readValue()).isEqualTo("HELLO")

driver.close()
```

- **Factories:** `fromBuilder(builder[, config][, initialWallClockTime])` (DSL) /
  `fromTopology(topology, …)` (Processor API). Bundled configs `STRING_CONFIG` and `DEFAULT_CONFIG`
  (ByteArray serdes).
- **Pipe:** `pipeInput(k, v)` / `(k, v, timestamp)` / `(k, v, timestamp, headers)` / value-only / `Instant`
  overloads; bulk `pipeKeyValueList` / `pipeValueList` / `pipeRecordList`.
- **Read:** `readRecord()` / `readKeyValue()` / `readValue()` / `readKeyValuesToList()` /
  `readKeyValuesToMap()` (last value per key) / `getQueueSize()` / `isEmpty()`.
- **Time:** `advanceWatermark(long)` (event time — fires ET timers, closes windows, flushes suppress),
  `advanceWallClockTime(Duration)` (processing time — fires WC punctuators/scheduled sources, then
  commits; it is also what `currentSystemTimeMs()` / `currentProcessingTime()` return in your code), `commitBarrier()` (commit the epoch + flush barrier-gated suppress), `triggerScheduledSource(name)`.
- **State:** `getKeyValueStore(name)` (unwraps timestamped and headers-aware → raw `V`), `getWindowStore`,
  `getSessionStore`, `getTimestampedKeyValueStore`, `getVersionedKeyValueStore`, the `*WithHeaders`
  variants, generic `getStateStore(name)`, `getAllStateStores()`, `producedTopicNames()`.
- `TestRecord(key, value, headers, timestamp)` — KS positional order; the timestamp is unset by default, and
  `pipeInput(record)` then takes the input topic's current time (KS parity).

`TopologyTestDriver` is **not thread-safe** — one instance per test (`@BeforeEach` create, `@AfterEach`
close).

## The testing divergences (full detail in references/testing-divergences.md)

- **Serdes are enforced** at the source/sink boundary (KSC-62) — the `createInputTopic`/`createOutputTopic`
  serdes are applied, matching the real engine. Wrong/asymmetric serdes throw (they used to be silently
  ignored). A correctly-typed KS test is unaffected.
- **Caching never consolidates** (KSC-63, KSC-88) — every update is forwarded when it happens, caching on or
  off, exactly as the engine does. Piping `(k,1),(k,2),(k,3)` into `count()` emits `1,2,3`, never a deduped
  `(k,3)` — even with `test.driver.commit-per-pipe=false`. `pipeInput` commits per pipe (stores, changelogs).
  One emission per key per commit = `suppress(Suppressed.untilBarrier())` or `emit.mode: on-barrier`.
- **Per-pipe stream-time advance** (KSC-71) — `pipeInput` advances stream-time to the record's event-time,
  so stream-stream unmatched results, `OnWindowClose`, and `suppress(untilWindowCloses)` flush on a
  later-timestamped pipe, **without** an explicit `advanceWatermark`.
- **Opt-out:** `test.driver.commit-per-pipe=false` (test-harness only) reverts to legacy manual control
  (you drive `commitBarrier()` / `advanceWatermark()` yourself).
- **Headers-aware stores:** a write through the plain accessors (`put`, `compute`, `merge`) stores empty headers,
  as in KS; `test.driver.keep-existing-headers=true` (test-harness only) keeps an existing entry's headers instead
  (not on a duplicate-retaining window store, where a `put` appends — the driver WARNs).
- **`advanceWatermark`** is lenient by default (a backward request is a silent no-op); strict (forward-only
  throw) under manual mode.

## TTD vs production — the caveat that bites

The TTD is a **fidelity approximation**, not the engine:

- **Watermark model differs.** TTD stream-time = `max(observed event-time)` with **no lag**. Production
  keeps `BoundedOutOfOrderness(10s)` lag + a `min`-across-partitions global watermark. **A window-close /
  join test that passes in the TTD emits *later* in production** under default config. To make production
  match, use `forMonotonousTimestamps()` / `maxOutOfOrderness=0` and mind per-partition idleness.
- **Chained windows close later in production.** A window fed by another window's finals across a re-key runs
  ~2 commit intervals behind per step on a broker; the TTD (one sub-topology) has no such lag. A graceful shutdown
  emits every window the watermark has closed, in every step; one still open is emitted after the next start.
- **Pipe tables in event time too.** The TTD watermark is the max over **every** piped topic and it decides
  lateness (ADR-149), so a KTable record piped with no timestamp (wall clock) makes older stream records late
  — they are dropped. Pass the same event-time timestamps to the table's `pipeInput`.
- **In-memory only.** All stores (even RocksDB-`Materialized`) run as in-memory twins; **no changelog** is
  written. Changelog/restoration, **EOS transactional commits**, and cross-key/lane ordering are
  **structurally invisible** to the TTD — test those with a real broker.
- **…but the exactly-once store checks judge the DECLARED store** (KSC-126). A table a join reads from another lane
  declared `Stores.persistentVersionedKeyValueStore` passes (the driver runs its in-memory twin); one declared
  `Stores.inMemoryVersionedKeyValueStore` or `Stores.lruMap` is refused under `exactly_once` (the default) — at driver
  construction, exactly as the engine refuses it. A PK or GlobalKTable join test over a versioned table that fails to
  build: declare the RocksDB versioned store (or run the test `at_least_once`).
- **Boundary key serialization IS reproduced** (record path AND window-close/suppress/punctuator
  emissions): a key crossing a sub-topology boundary — where a re-keyed record reaches an aggregation,
  a join, `toTable()`, or an explicit `repartition()` (ADR-138; a re-key alone opens no boundary, and
  since 1.0.0 `process()`/`processValues()` no longer open one either — `topology.processor-api-key-affinity`
  defaults to `off`) — is
  serialized with the same resolved boundary serde as production, so an unresolvable re-keyed boundary
  raises `LaneKeySerializationException` in the test instead of only in production. If a previously-green
  test starts failing with it, the topology genuinely declares no serde for that re-keyed key — declare
  one (`Repartitioned`/`Grouped`/`Produced`/`Materialized`) or set the default key serde the app itself
  uses. Note the boundary now **inherits** the nearest declaration from before the re-key as a last
  resort (only when `default.key.serde` was never configured), so this reaches the exception less often
  than it used to — it still fires when the re-key changes the key TYPE, since the inherited serde
  cannot encode the new key.
- **`topology.validation.<rule-id>` failures surface here too.** The rules are raised at
  topology-compile time, so a rule at `error` fails `TopologyTestDriver` construction with no
  broker — the aggregate message names every offending site. Useful as a deliberate guard in a test
  suite; also the first thing to check when a driver suddenly refuses to build. At `warn` the same
  line is simply **logged while the driver builds** — a WARN that used to appear only at
  engine start, so a previously-silent suite can start printing it without anything having changed in the
  topology.
  **Defaults are per rule, and one of them is `error`:** `unresolved-boundary-key-serde`,
  `inherited-boundary-key-serde` and `papi-key-affinity-presumed-chain` default to `warn`, but
  **`papi-key-affinity-presumed` defaults to `error`** — a re-key feeding a Processor API node that
  connects a writable store fails the driver. That is deliberate (it is silent data loss at runtime);
  the remedies are `repartition()`, `topology.processor-api-key-affinity: presumed`, or re-keying the
  store. Never tell a user a `topology.validation.*` key cannot fail their build.
- **The driver inherits the builder's topology-build settings.** `TopologyTestDriver.fromBuilder(builder)`
  with no explicit config picks up `subTopologySplit` / `processorApiKeyAffinity` from the
  `StreamsBuilder(TopologyConfig(cfg))` it was given, so the driver compiles the shape production runs.
  Pass a config explicitly and that config wins.

## Config-driven tests (StoatFlowTestDriver)

`StoatFlowTestDriver.fromConfig(topologyBuilder, overrides?)` (from `:stoatflow-test-runtime`) loads the
same `application.yaml` your runtime uses (via Hoplite + `ConfigMapper`) and returns a `TopologyTestDriver`
— so a test exercises the real merged config, default serdes, and DSL store format. Java overloads take a
`BiConsumer`/`Consumer`.

## Integration testing (a pattern — references/integration-testing.md)

For full fidelity, run the real `StoatFlowRuntime` against a **Testcontainers** Kafka broker; the
convention is to pair a TTD `…AppTest` with a broker-backed `…IntegrationTest`. **There is no shipped
`integration-test-utils` artifact** — do **not** reference `io.stoatflow:integration-test-utils`; use the
general Testcontainers pattern.

## Hard rules

- **Configure serdes explicitly** — the TTD enforces them like the engine (KSC-62).
- **Don't expect caching to dedupe** — in the TTD or in production (KSC-88). Expect every update, assert the
  store's final value, or add `untilBarrier()` / `emit.mode: on-barrier` and drive `commitBarrier()`.
- **Don't assume TTD timing == production timing** — the watermark lag differs; test EOS/restoration with a
  real broker.
- **Don't invent an `integration-test-utils` dependency** — it doesn't exist yet.

## See also

- `references/topology-test-driver.md` — the full TTD/`TestInputTopic`/`TestOutputTopic`/`TestRecord` API
- `references/testing-divergences.md` — KSC-62/63/71 + the commit-per-pipe flag, worked
- `references/integration-testing.md` — the real-broker pattern (Testcontainers)
- Website: <https://stoatflow.io/docs/building/testing>
