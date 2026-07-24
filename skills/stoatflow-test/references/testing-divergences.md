# TopologyTestDriver divergences from Kafka Streams, worked

The TTD API matches KS; its *timing/enforcement* semantics differ. These are the ones a Kafka-Streams
test suite trips on. All are documented in the compatibility matrix (`Test Utilities` section) and the
porting register.

## KSC-62 — serdes are enforced at the source/sink boundary

The TTD **applies** the `createInputTopic`/`createOutputTopic` serdes and round-trips records through them,
exactly as the real engine deserializes at the source and serializes at the sink. (It used to run in
"object mode" — passing typed objects straight through, ignoring the serdes.)

- **A correctly-typed KS test needs no change** — it already declares serdes via `Consumed.with` /
  `Produced.with` / `Materialized` or KS `Properties` default serdes (`DEFAULT_{KEY,VALUE}_SERDE_CLASS_CONFIG`).
- **Wrong or asymmetric serdes now throw** (they used to be a false pass). Serde failures surface from
  `pipeInput` / `commitBarrier`, **not** from `readRecord`.
- Two readers with different deserializers over one sink decode independently.
- If a test relied on object-mode (no serdes configured, default `ByteArray` serde meeting typed data), it
  now fails with `ClassCastException` at the boundary — the same way it would against the real engine.
  Configure the intended serdes.

## KSC-63 — cache flush per pipe

`pipeInput` commits per pipe. A caching aggregation (`count`/`reduce`/`aggregate`) therefore emits **every
intermediate update**, no manual commit required:

```kotlin
input.pipeInput("k", 1); input.pipeInput("k", 1); input.pipeInput("k", 1)
// counts emitted: 1, 2, 3   (NOT a single deduped 3)
assertThat(output.readValuesToList()).containsExactly(1L, 2L, 3L)
```

If a StoatFlow-specific test asserted a single deduped emission after a manual `commitBarrier()`, update it
to expect per-pipe emissions — or assert the state store's final value instead.

## KSC-71 — stream-time advances per pipe

`pipeInput` advances stream-time to the record's event-time (before the KSC-63 commit). So window closes,
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
cache nor advance stream-time, and you drive both yourself via `commitBarrier()` / `advanceWatermark()`.
Under manual mode, `advanceWatermark` is forward-only (a backward request throws); under the default it is
lenient (a backward request is a silent no-op, matching KS stream-time).

## No-timestamp default

`TestInputTopic` seeds an omitted-timestamp pipe from the **driver wall-clock** (KS-faithful), not
`System.currentTimeMillis()`. Pin `initialWallClockTime` on the driver for deterministic timestamps.

## The production-fidelity caveat (retained divergence)

TTD stream-time = `max(observed event-time)` with **no lag**. Production uses `BoundedOutOfOrderness(10s)`
+ a `min`-across-partitions global watermark. So a window-close / join test that passes in the TTD emits
**later** in production. To make production match the TTD, configure `forMonotonousTimestamps()` /
`maxOutOfOrderness = 0` and account for per-partition idleness. Anything depending on changelog,
restoration, EOS transactions, or cross-lane ordering is invisible to the TTD — cover it with a real
broker (`integration-testing.md`).
