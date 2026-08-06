---
name: stoatflow-test
description: Write or debug tests for StoatFlow topologies. TopologyTestDriver unit tests (pipe input, read output, advance watermarks, commitBarrier, assert state stores), the serde round-trip and per-pipe flush behaviour, StoatFlowTestDriver config-driven tests, and real-broker integration tests. Use for testing StoatFlow topologies; for writing the topology itself use stoatflow-build-topology.
license: Apache-2.0
---

# Test a StoatFlow topology

Read `references/stoatflow-primer.md` first. Testing is where StoatFlow's Kafka-Streams source-compat is
most seductive and most divergent — the `TopologyTestDriver` API matches KS, but its *timing* semantics
(serde enforcement, per-pipe flush, watermark model) differ in ways a KS-trained assistant gets wrong.

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
  commits), `commitBarrier()` (flush caching-store emissions), `triggerScheduledSource(name)`.
- **State:** `getKeyValueStore(name)` (unwraps timestamped → raw `V`), `getWindowStore`,
  `getSessionStore`, `getTimestampedKeyValueStore`, `getVersionedKeyValueStore`, the `*WithHeaders`
  variants, generic `getStateStore(name)`, `getAllStateStores()`, `producedTopicNames()`.
- `TestRecord(key, value, headers, timestamp)` — KS positional order.

`TopologyTestDriver` is **not thread-safe** — one instance per test (`@BeforeEach` create, `@AfterEach`
close).

## The testing divergences (full detail in references/testing-divergences.md)

- **Serdes are enforced** at the source/sink boundary (KSC-62) — the `createInputTopic`/`createOutputTopic`
  serdes are applied, matching the real engine. Wrong/asymmetric serdes throw (they used to be silently
  ignored). A correctly-typed KS test is unaffected.
- **Per-pipe cache flush** (KSC-63) — `pipeInput` commits per pipe, so a caching `count()`/`aggregate()`
  emits **every intermediate update** with no manual `commitBarrier()`. Piping `(k,1),(k,2),(k,3)` emits
  `1,2,3`, not a single deduped `(k,3)`.
- **Per-pipe stream-time advance** (KSC-71) — `pipeInput` advances stream-time to the record's event-time,
  so stream-stream unmatched results, `OnWindowClose`, and `suppress(untilWindowCloses)` flush on a
  later-timestamped pipe, **without** an explicit `advanceWatermark`.
- **Opt-out:** `test.driver.commit-per-pipe=false` (test-harness only) reverts to legacy manual control
  (you drive `commitBarrier()` / `advanceWatermark()` yourself).
- **`advanceWatermark`** is lenient by default (a backward request is a silent no-op); strict (forward-only
  throw) under manual mode.

## TTD vs production — the caveat that bites

The TTD is a **fidelity approximation**, not the engine:

- **Watermark model differs.** TTD stream-time = `max(observed event-time)` with **no lag**. Production
  keeps `BoundedOutOfOrderness(10s)` lag + a `min`-across-partitions global watermark. **A window-close /
  join test that passes in the TTD emits *later* in production** under default config. To make production
  match, use `forMonotonousTimestamps()` / `maxOutOfOrderness=0` and mind per-partition idleness.
- **In-memory only.** All stores (even RocksDB-`Materialized`) run as in-memory twins; **no changelog** is
  written. Changelog/restoration, **EOS transactional commits**, and cross-key/lane ordering are
  **structurally invisible** to the TTD — test those with a real broker.
- **Boundary key serialization IS reproduced** (record path AND window-close/suppress/punctuator
  emissions): a key crossing a sub-topology boundary — where a re-keyed record reaches an aggregation,
  a join, `toTable()`, `process()`/`processValues()`, or an explicit `repartition()` (ADR-138; a re-key
  alone opens no boundary) — is
  serialized with the same resolved boundary serde as production, so an unresolvable re-keyed boundary
  raises `LaneKeySerializationException` in the test instead of only in production. If a previously-green
  test starts failing with it, the topology genuinely declares no serde for that re-keyed key — declare
  one (`Repartitioned`/`Grouped`/`Produced`/`Materialized`) or set the default key serde the app itself
  uses.

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
- **Don't expect a caching aggregation to dedupe in the TTD** — expect per-pipe emissions (KSC-63), or
  assert the store's final value.
- **Don't assume TTD timing == production timing** — the watermark lag differs; test EOS/restoration with a
  real broker.
- **Don't invent an `integration-test-utils` dependency** — it doesn't exist yet.

## See also

- `references/topology-test-driver.md` — the full TTD/`TestInputTopic`/`TestOutputTopic`/`TestRecord` API
- `references/testing-divergences.md` — KSC-62/63/71 + the commit-per-pipe flag, worked
- `references/integration-testing.md` — the real-broker pattern (Testcontainers)
- Website: <https://stoatflow.io/docs/building/testing>
