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

**Licence:** a `TopologyTestDriver` test needs none; a test that calls a real `StoatFlow.start()` needs a licence
(`STOATFLOW_LICENCE_KEY`). Declare `STOATFLOW_LICENCE_ENVIRONMENT_CLASS=other` (locally and in CI, plus a stable
`STOATFLOW_LICENCE_ENVIRONMENT`, e.g. `ci`, if the licence lists `other` or `*`): against a Testcontainers broker the
licence registry lives in that throwaway cluster, so test runs count against nothing real.

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
  A `WALL_CLOCK_TIME` scheduled source fires on the **first** `advanceWallClockTime` (it is due at start —
  unlike a WC punctuator, which waits one interval), a `STREAM_TIME` one at the first stream-time advance — the
  first `pipeInput` under the default `test.driver.commit-per-pipe`, else the first `advanceWatermark` — unless
  it starts at `ScheduledStart.NEXT_INTERVAL` (per source, or `dsl.scheduled-source-start`). An advance spanning
  many intervals or cron occurrences fires a source **once**, not once per interval. While `pause()`d, advances
  fire nothing; `unpause()` fires what they skipped, once per clock.
- **State:** `getKeyValueStore(name)` (unwraps timestamped and headers-aware → raw `V`), `getWindowStore`,
  `getSessionStore`, `getTimestampedKeyValueStore`, `getVersionedKeyValueStore`, the `*WithHeaders`
  variants, generic `getStateStore(name)`, `getAllStateStores()`, `producedTopicNames()`.
  The getters read the latest writes whatever `interactive-query-isolation-level` says; call
  `readOnly(IsolationLevel.READ_COMMITTED)` on the returned store to read what the last commit (`commitBarrier()`,
  or each `pipeInput` under the default per-pipe commit) made durable. Every store commits, window and session stores
  included, so their retention expires old windows and sessions as in production. Versioned stores run in memory in the driver,
  so `READ_COMMITTED` on one reads live (with a WARN) — a test cannot show it for a versioned store.
- `TestRecord(key, value, headers, timestamp)` — KS positional order; the timestamp is unset by default, and
  `pipeInput(record)` then takes the input topic's current time (KS parity). Any timestamp but `Long.MIN_VALUE` is
  accepted, negative included (KS's `TestRecord` rejects negatives); `MIN` throws `IllegalArgumentException`.

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
- **Explicit naming:** the driver runs the application's explicit-naming check only when its config **sets**
  `ensure.explicit.internal.resource.naming` (`Properties`, `ensureExplicitNaming(true)`, or the
  `StoatFlowTestDriver` YAML key `stoatflow.validation.ensure-explicit-naming`) — then a generated node or store name
  throws the same `TopologyValidationException` the application throws at start. A builder created with a
  `TopologyConfig` that sets the key checks in `build()`, as KS's does, so a driver over it refuses too. Unset, the
  driver does not check (the application does by default). Set it to `true` in a test to catch a topology that would
  refuse to start.
- **`advanceWatermark`** is lenient by default (a backward request leaves the watermark where it is, and fires only
  the event-time timers already due); strict (forward-only throw) under manual mode.
- **Negative event times are data** (KSC-138), as in production: windows near 0 are floor-aligned, so a KS-shaped
  test with timestamps below a window size sees extra windows starting before 0 (and one sliding window per record
  where KS combines them into `[0, D]`). A record piped with timestamp `-1` on a source with no extractor/assigner is
  Kafka's "no timestamp": skipped and counted, no output. A negative-time record reaching a sink is stamped with the
  driver's wall clock. An extractor's exception fails the `pipeInput` (so `FailOnInvalidTimestamp` fails the test).

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
- **A punctuator runs on its own instance, in the driver too** (KSC-160). The constructor initialises a node's
  punctuator instance; the first piped record initialises the instance `process()` runs on. Expect two `init()` calls
  for a node with input; a punctuator never sees a field `process()` set (a field-buffer-then-flush test flushes
  nothing), and a `Cancellable` cancelled from `process()` cancels nothing. Fix the processor (state in a store),
  not the test — `references/testing-divergences.md`. Punctuators run synchronously in the advance, so the
  application's exclusive-work pauses are invisible here.
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
- **The output-topic check is not run by the driver.** `StoatFlow.start()` refuses when a sink or dead-letter topic
  is missing (`MissingOutputTopicException`, KSC-148); the `TopologyTestDriver` never starts, so a driver test
  passes against a topic name the broker does not have. A broker test that starts the app must create them.
- **The driver inherits the builder's topology-build settings.** Every construction path given no config —
  `TopologyTestDriver.fromBuilder(builder)`, `fromTopology(topology)`, the KS-shaped `TopologyTestDriver(topology)`
  and `(topology, Instant)`, and the `Properties` constructors for a setting the `Properties` do not name — picks up
  `subTopologySplit` / `processorApiKeyAffinity` from the `StreamsBuilder(TopologyConfig(cfg))` the topology was built
  with, so the driver compiles the shape production runs. Pass a config (or name the setting) and that wins. These are the **built** settings: an earlier driver or
  application compiled under another config does not change what a later config-less driver compiles.
- **Several drivers may share one builder or `Topology`** — a shared fixture, two drivers compared in one
  test, a driver over a running application's builder: each builds its own processors and state stores, as
  in Kafka Streams. Two things stay shared because the topology holds them as user objects: a scheduled
  source's emitter (every run invokes the same object — keep it stateless, or give each driver its own
  builder) and a `Printed.toFile` stream.

## Config-driven tests (StoatFlowTestDriver)

`StoatFlowTestDriver.fromConfig(topologyBuilder, overrides?)` (from `:stoatflow-test-runtime`) loads the
same `application.yaml` your runtime uses (via Hoplite + `ConfigMapper`) and returns a `TopologyTestDriver`
— so a test exercises the real merged config, default serdes, DSL store format and store type
(`stoatflow.dsl.store-suppliers-class`). A plain `TopologyTestDriver` applies `dsl.store.suppliers.class` /
`default.dsl.store` from its `StreamsConfig` the same way: every store the topology leaves to the default takes it,
then the driver keeps every store in memory (an LRU map a custom store type returns stays an LRU map). Java overloads take a
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
