# TopologyTestDriver divergences from Kafka Streams, worked

The TTD API matches KS; its *timing/enforcement* semantics differ. These are the ones a Kafka-Streams
test suite trips on. All are documented in the compatibility matrix (`Test Utilities` section) and the
porting register.

## KSC-62 — serdes are enforced at the source/sink boundary

The TTD **applies** the `createInputTopic`/`createOutputTopic` serdes and round-trips records through them,
exactly as the real engine deserializes at the source and serializes at the sink — and, since bugfix
20260803/20260804, also serializes keys crossing **sub-topology boundaries** — which, since ADR-138, open
only where a re-keyed record reaches an operator needing key affinity (aggregation, join, `toTable()`)
or an explicit `repartition()`, not at every re-key, and since 1.0.0 **not** at `process()` /
`processValues()` either (`topology.processor-api-key-affinity` defaults to `off`) — with the same
resolved boundary serde on the record path AND the window-close/suppress/punctuator emission path, so an
unresolvable re-keyed boundary raises `LaneKeySerializationException` in the test as in production. (It
used to run in
"object mode" — passing typed objects straight through, ignoring the serdes.) Since the boundary serde
gained a last-resort step — inheriting the nearest declaration from before the re-key, when
`default.key.serde` was never configured — that exception is reached less often; it still fires when the
re-key changes the key *type*, because the inherited serde then cannot encode the new key.

- **`topology.validation.<rule-id>` is `TopologyTestDriver`-visible.** The rules are raised at
  topology-compile time, not at engine start, so a rule set to `error` fails driver construction with no
  broker — naming every offending site in one message. A rule at `warn` reaches the driver too: the
  line is logged as the driver builds the topology, so a suite that was silent about a
  boundary can start warning about it with no topology change.
  **Severities are per rule and one defaults to `error`** — `papi-key-affinity-presumed` (a re-key
  feeding a Processor API node with a writable store). Its sibling `papi-key-affinity-presumed-chain`
  (the feeder is itself a Processor API node, so the re-key is only presumed) and the two boundary-serde
  rules default to `warn`. A previously-green KS-ported suite CAN start failing at driver construction
  because of this; the fix is `repartition()`, `topology.processor-api-key-affinity: presumed`, or
  re-keying the store — not silencing the rule without reading it.

- **The exactly-once store checks judge the declared store, not the in-memory stand-in** (KSC-126). Every store
  runs in memory in the driver, yet a RocksDB versioned (`persistentVersionedKeyValueStore`) table that a join reads
  from another lane passes, while an `inMemoryVersionedKeyValueStore` or `lruMap` one is refused under `exactly_once`
  (the default) at driver construction — as in production. A StoatFlow-native test of a PK or GlobalKTable join over
  a versioned table therefore declares the RocksDB versioned store. A KS-ported test can only hit this through an
  `lruMap` join input (KS has no in-memory versioned store) — which production refuses too.

- **A correctly-typed KS test needs no change** — it already declares serdes via `Consumed.with` /
  `Produced.with` / `Materialized` or KS `Properties` default serdes (`DEFAULT_{KEY,VALUE}_SERDE_CLASS_CONFIG`).
- **Wrong or asymmetric serdes now throw** (they used to be a false pass). Serde failures surface from
  `pipeInput` / `commitBarrier`, **not** from `readRecord`.
- Two readers with different deserializers over one sink decode independently.
- If a test relied on object-mode (no serdes configured, default `ByteArray` serde meeting typed data), it
  now fails with `ClassCastException` at the boundary — the same way it would against the real engine.
  Configure the intended serdes.

## KSC-63 — every update is emitted; caching never consolidates

`pipeInput` commits per pipe (stores, changelogs, barrier-gated suppress). Output never waits for a commit:
caching batches writes but does not change emissions — in the driver exactly as in the engine (KSC-88; the
driver's old KS-style record-cache emulation was removed). A `count`/`reduce`/`aggregate` emits **every
intermediate update**:

```kotlin
input.pipeInput("k", 1); input.pipeInput("k", 1); input.pipeInput("k", 1)
// counts emitted: 1, 2, 3   (NOT a single deduped 3)
assertThat(output.readValuesToList()).containsExactly(1L, 2L, 3L)
```

If a test asserted a single deduped emission after a manual `commitBarrier()`, update it to expect every
update — or assert the state store's final value, or consolidate explicitly with `untilBarrier()` /
`emit.mode: on-barrier` under `test.driver.commit-per-pipe=false`, closing each epoch with `commitBarrier()`.
This is a divergence from Kafka Streams, whose record cache does consolidate downstream emissions.

## KSC-71 — stream-time advances per pipe

`pipeInput` advances stream-time to the record's event-time (before the per-pipe commit). So window closes,
stream-stream unmatched (LEFT/OUTER) results, `OnWindowClose` aggregations, and
`suppress(untilWindowCloses)` flush when a **later-timestamped** record is piped — with **no explicit
`advanceWatermark`**:

```kotlin
input.pipeInput("k", "a", windowStart)                 // in-window
input.pipeInput("k", "b", windowStart + size + grace)  // past close → the window's result flushes here
```

## The unified opt-out: `test.driver.commit-per-pipe`

A test-harness-only boolean (default `true`; the engine never reads it). It governs **both** the KSC-63
commit and the KSC-71 advance. Set it `false` for legacy manual control — pipes then neither commit the
epoch nor advance stream-time (output is still emitted per record), and you drive both yourself via `commitBarrier()` / `advanceWatermark()`.
Under manual mode, `advanceWatermark` is forward-only (a backward request throws); under the default it is
lenient (a backward request is a silent no-op, matching KS stream-time).

## Writes through the plain accessors over a headers-aware store: `test.driver.keep-existing-headers`

`getKeyValueStore` / `getWindowStore` / `getSessionStore` (and the timestamped / versioned accessors) over a
`withRecordHeaders()` store drop the headers on read. A write through them has no record context, so by default
every write — `put`, `compute`, `merge` — stores **empty headers** (the KS rule). Set the test-harness-only boolean
`test.driver.keep-existing-headers=true` (default `false`; the engine never reads it) to have each write to an
existing entry keep that entry's headers; a new entry still gets empty headers. Exception: a window store that
retains duplicates (a join buffer, or `retainDuplicates = true`) appends on `put` instead of updating, so the flag has
no effect there — the driver logs a WARN naming the store. Use the `*WithHeaders` accessors to set headers explicitly.

## No-timestamp default

`TestInputTopic` seeds an omitted-timestamp pipe from the **driver wall-clock** (KS-faithful), not
`System.currentTimeMillis()` — `pipeInput(k, v)` and a `TestRecord` whose timestamp is unset (its default) alike.
The topic's time is read from the driver clock when the topic is created and then moves only through the topic
(auto-advance, `withTimestamp`, `advanceTime`, `reset()`); a later `advanceWallClockTime` does not move it (also KS). Pin `initialWallClockTime` on the driver for deterministic
timestamps.

## One driver per builder

The processors belong to the `StreamsBuilder` / `Topology`, and every run over it reuses them. Build a fresh builder
per driver: two drivers open over one builder — or a driver over the builder of a running application — share
processor instances and wiring and interfere (tracked as a fix).

## Wall clock in user code (KSC-133)

Under the driver, `currentSystemTimeMs()` on every context (processor, fixed-key, timer, punctuator, global/read-only
store update processor, KIP-1112 wrapper decorator, scheduled-source emitter) and `TimerService.currentProcessingTime()`
return the **driver wall clock** — the one `advanceWallClockTime` moves — as Kafka Streams' driver reports its
`MockTime`. So wall-clock logic in `process()` ("is it due yet?", TTLs) is testable with `advanceWallClockTime`, and a
timer registered at `currentProcessingTime() + d` fires on `advanceWallClockTime(d)`. In production both read the
system clock. Never call `System.currentTimeMillis()` directly in a processor you want to test this way.

## The production-fidelity caveat (retained divergence)

TTD stream-time = `max(observed event-time)` with **no lag**. Production uses `BoundedOutOfOrderness(10s)`
+ a `min`-across-partitions global watermark. So a window-close / join test that passes in the TTD emits
**later** in production. To make production match the TTD, configure `forMonotonousTimestamps()` /
`maxOutOfOrderness = 0` and account for per-partition idleness. The TTD has no partition buffers or head
ordering either: a record stamped in the future is processed as soon as it is piped, while production holds it behind
older records (event time follows the other partitions meanwhile). Anything depending on changelog,
restoration, EOS transactions, or cross-lane ordering is invisible to the TTD — cover it with a real
broker (`integration-testing.md`).
