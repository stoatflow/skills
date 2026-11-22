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

**Eager suppress buffers:** the driver is one lane, so `maxRecords(n)` / `maxBytes(n)` with `emitEarlyWhenFull()`
gives it the whole bound and it emits its oldest record on overflow (a single KS task). In the application each writer —
each lane of the suppress's sub-topology — holds bound ÷ writer count (KSC-154), so a small bound emits early more often
there: assert final results, not the number of early emissions. Nor assert the order of different keys' releases
across lanes: in the application each key leaves from its own lane, so only one key's results (and one lane's keys)
keep buffer order — the driver, one lane, emits all of them in buffer order.

## KSC-160 — a punctuator runs on its own instance, under the driver too

Every Processor API node (`process`, `processValues`, `transformValues`, `addProcessor`) has a dedicated punctuator
instance, in the driver as in the application. The driver's constructor initialises it (its `init()` registers the
punctuators), and the first piped record initialises a second instance that runs `process()`. A Kafka Streams test
can therefore break:

- **`init()` counts:** a node with input runs `init()` (and `close()`) **twice** — expect two, not one.
- **A punctuator reading a field `process()` set** sees its own instance's field — still empty. A test that pipes
  records and then asserts what a punctuator flushed from a field-held buffer gets nothing. Fix the processor, not the
  test: keep the buffer in a key-value store (put in `process()`, iterate and delete in the punctuator).
- **A `Cancellable` kept in a field and cancelled from `process()`** cancels nothing — `schedule()` on the record
  instance returns a do-nothing `Cancellable`. The punctuator keeps firing on `advanceWallClockTime` /
  `advanceWatermark`. Cancel from inside the punctuator, or keep the decision in a store.

The driver runs punctuators synchronously inside the advance, on one thread: it shows none of the application's
exclusive-work pauses, parallel punctuators of different processors, or write ordering at a barrier. Cover those with a
broker test (`integration-testing.md`).

## The unified opt-out: `test.driver.commit-per-pipe`

A test-harness-only boolean (default `true`; the engine never reads it). It governs **both** the KSC-63
commit and the KSC-71 advance. Set it `false` for legacy manual control — pipes then neither commit the
epoch nor advance stream-time (output is still emitted per record), and you drive both yourself via `commitBarrier()` / `advanceWatermark()`.
Under manual mode, `advanceWatermark` is forward-only (a backward request throws); under the default it is
lenient (a backward request is a silent no-op, matching KS stream-time).

## The explicit-naming check runs only when the key is set (KSC-157)

Kafka Streams checks `ensure.explicit.internal.resource.naming` in `StreamsBuilder.build()`, so a driver over a topology
built with it on fails as the application would; the key is off by default. StoatFlow's application checks by default
(node names too), and its driver runs the same check only when the driver's own config **sets** the key —
`Properties`, `StreamsConfig.builder(...).ensureExplicitNaming(true)`, or the `StoatFlowTestDriver` YAML key
`stoatflow.validation.ensure-explicit-naming`. Unset, a test of a topology with generated names keeps running; set it
`true` in a test to catch a topology that would refuse to start. As in Kafka Streams, a `StreamsBuilder` created with a
`TopologyConfig` that sets the key `true` checks in `build()`, so a driver over that builder refuses too.

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

## KSC-138 — negative event times are data

The driver applies the engine's event-time rules: every `Long` but `Long.MIN_VALUE` is a time.

- **Windows near 0 follow Flink, not KS.** Windows are floor-aligned with no clamp at 0, so a KS-shaped test with
  timestamps below a window size sees more windows than on KS: a record at 5 s in 60 s windows advancing by 10 s
  joins six windows (starting −50 s … 0), KS one (`[0, 60 s)`); a sliding record at `0 ≤ t < D` gets its own window
  `[t − D, t]`, where KS combines them into `[0, D]`. Results above those times are identical — shift the test's
  timestamps above the window size, or assert the extra windows.
- **Negative timestamps pipe** (KS's `TestRecord` rejects them); `Long.MIN_VALUE` throws `IllegalArgumentException`.
- **No timestamp is skipped.** A record piped with `-1` (Kafka's "no timestamp") on a source with no extractor or
  assigner, or one whose extractor returns `Long.MIN_VALUE` (`LogAndSkipOnInvalidTimestamp` on a negative timestamp),
  produces nothing and does not move stream time — counted as `dropped.records{reason=no_event_time}`, as in production.
- **An extractor's exception fails the pipe**, as it fails the application: `FailOnInvalidTimestamp` throws out of
  `pipeInput` on a negative timestamp. Extractors get the partition time the driver keeps per topic, and the piped
  record — its topic, headers, key bytes and timestamp — as in production; its partition and offset are always 0 (KS's
  driver counts offsets per topic).
- **A sink stamps a negative event time with the driver's wall clock** (production writes no timestamp and the
  producer or broker stamps the wall clock), and counts it in `stoatflow.sink.records.negative.timestamp.total`.

## Exception handlers in the driver

- **The deserialization handler is asked** about a piped (or intermediate-topic) record the source cannot
  deserialize, as in production and in KS's driver: CONTINUE skips it (its offset used up, nothing processed, stream
  time unmoved) and routes its dead letters to an output topic you read with `createOutputTopic`; the default handler
  lets the deserializer's own exception out of `pipeInput`.
- **The production handler is asked** about a sink's key/value serialization or `StreamPartitioner` failure — never
  the processing handler: CONTINUE drops the record and routes its dead letters; FAIL (the default for a
  serialization error) rethrows the original exception, which a processing-handler CONTINUE cannot swallow.
- **Every CONTINUE is counted** on the configured `InstrumentationCallback` — `onDroppedRecord("deserialization" |
  "processing" | "production")` and `onDeadLettersSent(source, count)` — as in production, so a test can assert a
  poison record was counted. Deserialization and production skips WARN once per topic under the driver (a running
  application WARNs at most once a minute per partition for deserialization, per output topic for production).

## Wall clock in user code (KSC-133)

Under the driver, `currentSystemTimeMs()` on every context (processor, fixed-key, timer, punctuator, global/read-only
store update processor, KIP-1112 wrapper decorator, scheduled-source emitter) and `TimerService.currentProcessingTime()`
return the **driver wall clock** — the one `advanceWallClockTime` moves — as Kafka Streams' driver reports its
`MockTime`. So wall-clock logic in `process()` ("is it due yet?", TTLs) is testable with `advanceWallClockTime`, and a
timer registered at `currentProcessingTime() + d` fires on `advanceWallClockTime(d)`. In production both read the
system clock. Never call `System.currentTimeMillis()` directly in a processor you want to test this way.

## `Properties` without `application.id` (KSC-155)

`TopologyTestDriver(topology, props)` and `(topology, props, Instant)` fill in a missing `application.id` and
`bootstrap.servers` on a copy of `props` (your object is unchanged), as Kafka Streams' driver does — so a ported test
that passes only its serdes, or an empty `Properties`, builds as it did on Kafka Streams. The default id is `test-app`,
the driver's default on every construction path; Kafka Streams' driver picks a random
`dummy-topology-test-driver-app-id-<n>` to keep drivers' state directories apart, which StoatFlow's driver gets from a
temporary directory per instance. The id is what `context.applicationId()` reports and what app-scoped internal names
(and a mock schema registry's subjects) start with — set `application.id` when a test needs its own, and in tests that
share a `mock://` scope without dropping it, or their subjects meet. A `StreamsConfig` built from `Properties` still
requires one, as in Kafka Streams. A split setting (`topology.sub-topology-split`, `topology.processor-api-key-affinity`)
the `Properties` do not name is the one the topology was built with (`TopologyConfig`), as for a constructor given no
config; a `StreamsConfig` passed to the driver sets both itself.

## The production-fidelity caveat (retained divergence)

TTD stream-time = `max(observed event-time)` with **no lag**. Production uses `BoundedOutOfOrderness(10s)`
+ a `min`-across-partitions global watermark. So a window-close / join test that passes in the TTD emits
**later** in production. To make production match the TTD, configure `forMonotonousTimestamps()` /
`maxOutOfOrderness = 0` and account for per-partition idleness. The TTD has no partition buffers or head
ordering either: a record stamped in the future is processed as soon as it is piped, while production holds it behind
older records (event time follows the other partitions meanwhile). For the same reason the TTD does **not model
watermark alignment** — `withWatermarkAlignment(...)` holds nothing in a test, so a hold that keeps production waiting
never shows; the two alignment build rules (`alignment-idleness-off`, `alignment-mixed-watermark-bounds`) do run. Anything depending on changelog,
restoration, EOS transactions, or cross-lane ordering is invisible to the TTD — cover it with a real
broker (`integration-testing.md`).
