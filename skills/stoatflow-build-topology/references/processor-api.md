# Processor API, timers, punctuators, scheduled sources

Interfaces are `io.stoatflow.core.processor.*`.

## Processors

- `Processor<KIn,VIn,KOut,VOut>` — attach via `KStream.process(supplier, *storeNames)`; can change key.
- `FixedKeyProcessor<KIn,VIn,VOut>` — attach via `KStream.processValues(...)`; key fixed.
- Base classes `ContextualProcessor` / `ContextualFixedKeyProcessor` expose `context()` after `init`.
- Records: `Record<K,V>` (`.key`/`.value`/`.timestamp`; `withTimestamp(...)`, `withHeaders(...)`) and
  `FixedKeyRecord<K,V>`.
- Suppliers: `ProcessorSupplier` / `FixedKeyProcessorSupplier` — `get()` **must return a fresh instance
  every call** (the engine creates one per key-affinity lane); `stores()` (default empty) declares
  `Set<StateStoreBuilder<*>>`.

### Key affinity after a re-key (read before advising on per-key state)

Since 1.0.0 a Processor API node does **not**, by itself, require key affinity
(`topology.processor-api-key-affinity: off`) — matching Kafka Streams, which never repartitions before
`process()` / `processValues()` even with connected stores.

- **Injective (1:1) re-key, or no re-key**: nothing changes. StoatFlow lanes by the *old* key, so
  affinity is preserved for free.
- **Many-to-one re-key** (several old keys collapsing onto one new key): records reach the node on the
  lane of their *old* key, so two records with the same new key run concurrently on different lanes.
  Per-key state is then **fragmented across lanes** — instance fields stay race-free (one instance per
  lane, as in a KS task) but a given key's state lives in several of them, and a store
  read-modify-write can lose an update across a commit barrier. Neither `compute()` nor a
  `KeyLockManager` lock closes that window.
- A node that **declares a timer** (`declaredTimerTypes()` non-empty) keeps its boundary either way —
  the timer service hashes the *new* key while records lane by the *old* one.

StoatFlow refuses to compile the provable case — a **proven** re-key (`selectKey`/`map`/`groupBy`/…)
feeding a Processor API node that connects a **writable** store — via the `papi-key-affinity-presumed`
validation rule, default `error`. Read-only (KIP-813) stores are excluded: they cannot be written, so
there is no update to lose.

Where the feeder is itself a Processor API node the re-key is only **presumed** — `process()` and
`Topology.addProcessor()` stamp `isKeyChanging = true` on every node, since the compiler cannot see
inside a user `Processor`. That reports through `papi-key-affinity-presumed-chain`, default `warn`, and
does **not** fail the build: a plain `process(parse).process(count, "store")` chain is exactly what a
KSML-generated topology is made of, and Kafka Streams compiles and runs it. If the upstream node really
does re-key, apply a remedy; if it does not, set that rule to `off`. A real many-to-one re-key there spreads one key's
writes over several lanes of one sub-topology, which cross commit barriers at different times — commit ordering only
protects a key that stays on one lane, so this WARN is the **only** guard for that shape.

What the check cannot see: per-lane instance fields (no declaration can express them), and a **store key
that is not the record key** (`store.put(record.value().customerId, …)`), which reaches the same
cross-barrier window with no re-key anywhere — it is computed at runtime. See
`references/serdes-state-stores.md` § Multi-key atomicity.

Remedies, in order: `repartition()` before the node (**in-memory in StoatFlow, not a Kafka topic**),
`topology.processor-api-key-affinity: presumed`, or don't key store state by the post-re-key key.
Two further consequences of `off`: a node whose boundary is elided joins the **upstream** sub-topology
and inherits its lane count, and elided boundaries at storeless nodes are logged once at `INFO`.

```kotlin
class UppercaseProcessor : ContextualFixedKeyProcessor<String, String, String>() {
    override fun process(record: FixedKeyRecord<String, String>) {
        val v = record.value
        if (v.isNotBlank()) context().forward(record.withValue(v.uppercase()))
    }
}
```

**Context:** `forward(record)` / `forward(key, value[, timestamp])` / `forward(record, childName)` (fixed-key:
`forward(value[, timestamp])`, `key()` reads the fixed key). Metadata/time: `timestamp()`, `headers()`,
`topic()`/`partition()`/`offset()`, `applicationId()`, `currentWatermarkMs()`, `currentStreamTimeMs()` (KS
alias for the watermark), `currentSystemTimeMs()` (system clock; the driver's wall clock under the
`TopologyTestDriver`), `getStateStore(name)`, `isLate()` (in `process`).

**Several parents (KS parity):** a child connected to one parent twice (`addProcessor("x", s, "p", "p")`)
gets a plain `forward` once **per connection** and a named `forward(record, "x")` **once**. A node with
several parents (`addProcessor("x", s, "a", "b")`) in different sub-topologies is handed each record on its own
lanes by key, hashed with the key serde its parents agree on; parents declaring **different** key serdes keep their own (different key
types are fine) and the build WARNs naming the node — the same key could then land on two lanes, so advise
one key serde for every input when the parents carry the same keys.

**Store attachment — a processor may only reach a store CONNECTED to its node** (Kafka Streams rule;
an undeclared `getStateStore(name)` throws `StoreNotConnectedException`, which is fatal and cannot be
routed to `processing.exception.handler`). Two ways to connect one, and they union:

1. override `stores()` on the supplier with `Stores.keyValueStoreBuilder(...)` — connects automatically;
2. `builder.addStateStore(builder)` **and** name it at the call site:
   `process(supplier, Named.as("node"), "my-store")` / `processValues(supplier, "my-store")`.
   Registering without naming it does **not** connect it.

Processor-API route: `topology.addStateStore(builder, "node")` or
`topology.connectProcessorAndStateStores("node", "my-store")` — either order relative to `addProcessor`.

Exempt, no declaration needed: **global** stores (`addGlobalStore`, `globalTable`) — also returned
**read-only** to an ordinary processor, for every store family (plain, timestamped, versioned,
headers-aware) and without changing the store's declared type — and **read-only** stores
(`addReadOnlyStateStore`). Declaring a global store opts into write access (a StoatFlow extension).
The processor-less `addGlobalStore(builder, topic, consumed)` / `addReadOnlyStateStore(builder, topic, consumed)`
(StoatFlow conveniences) write in the store's shape: a timestamped builder holds `ValueAndTimestamp(value, record ts)`.

A lookup in `init` fails at startup (`start()` throws; under the `TopologyTestDriver`, its constructor), as in
Kafka Streams; a lookup in `process()` on the record that makes it. Build store builders via `Stores`
(`io.stoatflow.core.state`).

**Sharing a store — placement follows the WRITER (KS parity).** A node connected to a store that another part of
the topology writes (a `table()` store, another processor's store) is placed with that store's **writer**, as KS puts
processors sharing a store into one subtopology — with no re-key between them a key hits the same lane in both. A node
that only reads the store (a join) is not moved. Processor API nodes that connect one store from **different
sub-topologies** (a re-key between them) can lose an update under either guarantee — their lanes cross barriers at
different times. StoatFlow places nodes by key (KS forces them together, KAFKA-20464), so the build WARNs:
`topology.validation.presumed-cross-sub-topology-write` (default `warn`). Only one of them writes (the rest read)?
Nothing is lost — set it `off`. Otherwise connect the store from one sub-topology, or let one processor own it and
forward what the others need. The same shape among DSL writers is refused.

**Cross-lane reads of `lruMap` / in-memory versioned stores.** Every other built-in store is read as of the reading
lane's commit under `exactly_once`; these two write directly and their reads are live. A Processor API node that
**may** read one across lanes — from a punctuator (or a node downstream of one in its sub-topology) while record lanes
write it, or from another sub-topology than its writer — gets `topology.validation.presumed-cross-lane-read`
(default `warn`); a global or read-only such store counts as read by every lane. Fix: a default store supplier or
`persistentVersionedKeyValueStore`, or `at_least_once`; `off` if the processor never reads it that way. A DSL join
doing the same is refused (`KSC-126`, `references/windowing-joins.md`), as is a DSL node reading one on the punctuation
lane (behind a LEFT/OUTER stream-stream join) while record lanes write it; a store only the punctuation lane writes and
reads (the table an `OnWindowClose` aggregation feeds) is fine.

## Event time on a Processor API source (🆕 `addSource(WatermarkStrategy, …)`)

`topology.addSource(timestampExtractor, name, topics…)` wraps the extractor in an adapter that pairs it
with a monotonous (Kafka Streams stream-time) generator. That is a sensible default but it is *only* a
default — it cannot express a bounded-out-of-orderness generator, a per-source idleness timeout, or
watermark alignment.

Use the watermark-strategy overload when the source needs any of those:

```kotlin
val strategy = WatermarkStrategy
    .forBoundedOutOfOrderness<String, Order>(Duration.ofSeconds(30))
    .withTimestampAssigner { _, v, _ -> v!!.eventTime }
    .withIdleness(Duration.ofMinutes(2))

topology.addSource(strategy, "orders-src", "orders")
// or with an offset-reset policy:
topology.addSource(AutoOffsetReset.Earliest, strategy, "orders-src", "orders")
```

`withIdleness` on a source overrides the application-wide
`stoatflow.watermark.idleness-timeout-ms` for that source's partitions; a source that sets none inherits
it. This is a StoatFlow extension — Kafka Streams has no `addSource(WatermarkStrategy, …)`.

**Implementing `WatermarkStrategy` yourself?** If its timestamp assigner reads the event time from the key, the
value or the headers, `hasCustomTimestampAssigner()` must return `true` (the built-ins set it when you call
`withTimestampAssigner`). StoatFlow trusts the flag: it orders partitions by event time with it, and counts a
partition waiting behind older records by that event time. Left `false`, such a partition counts with its records'
Kafka timestamps — if the extracted times are earlier, the watermark passes them and the records are dropped as late.

## Threading — the rule that bites

**One processor instance per lane** (the KS-task analog). Instance fields are per-lane and safe without
synchronization — but state shared across lanes, or read from a punctuator, **must live in a state store**,
not an instance field. A punctuator runs on a dedicated **punctuation lane** concurrently with record
lanes, so its read-modify-write is **not** serialized against a key's record processing **within a commit**. For a
per-key time-triggered mutation, prefer a **timer** (`onTimer` runs in the same per-key serialized context as
`process`).

- **Across commits, writes are ordered (both guarantees):** after each barrier a punctuation lane writes state only
  once every record lane of its sub-topology has crossed it, so a punctuator's write and a record's write of the same
  key never land in the wrong commit order. If the record lanes miss the barrier timeout the app stops (open epoch
  aborted, replay from the last commit) rather than write out of order.
- **Known issue — a punctuator's write to a key `process()` read-modify-writes can be lost (both guarantees):** a
  punctuator that fires just before a barrier, or is still running when the next one passes, can run after a record
  lane crossed it; if that lane already read and rewrote the key (e.g. incremented a counter), the punctuator's
  decrement / reset / delete of it is lost. Fix under investigation. Until then, do such time-triggered per-key changes
  from a **timer** (`onTimer` is serialized with the key's records); punctuators that only read, or write keys
  `process()` never updates, are unaffected.
- **Reads are as of the lane's commit under `exactly_once`:** `process`, `onTimer` and punctuators never see another
  lane's uncommitted next-commit write (except from `lruMap` / in-memory versioned stores, above). IQ and
  `at_least_once` read the latest state.
- **Threads you fork yourself are unsupported under `exactly_once`:** not a processing lane — their reads see the
  latest state, not the lane's commit, and their writes are not attributed to the lane's commit.

## Punctuators

`context.schedule(interval, TimeNotion, PunctuatorMode?, callback)` → `Cancellable`. `TimeNotion.STREAM_TIME`
(alias `EVENT_TIME`) / `WALL_CLOCK_TIME` (alias `PROCESSING_TIME`); Java `PunctuationType`/`TimeDomain`
resolve to the same constants. `PunctuatorMode.BLOCKING` (default) / `NON_BLOCKING`. Anchored overload takes
`startTime: Instant` (KIP-1146). Locked-run: an overlapping fire is skipped.

**Fire times (Kafka Streams' `PunctuationSchedule`):** `STREAM_TIME` is aligned to 0 — it fires on the first data (the
first watermark ≥ 0), then at every multiple of the interval; `WALL_CLOCK_TIME` first fires one interval after it was
scheduled, then on that grid; anchored fires at `startTime`, or at the first grid point after now when `startTime` is
already behind. A jump past several grid points fires once (with the current time), then the next grid point — no drift.
Inside the callback `context.timestamp()` is the punctuation time and `headers()` is empty (Kafka Streams parity) —
not the last record's.

**Start timing (Kafka Streams parity):** `init()` runs when the application starts — not with the first record —
so a punctuator scheduled there (catch-up, heartbeat, TTL cleanup) runs even if no input arrives. One instance per
node is initialised at start; the first lane to receive a record adopts it, the other lanes initialise theirs on
their first record. In HA active/standby, `init()` runs when a pod becomes active, never on a standby. In `init()`,
`context.timestamp()` is the wall clock.

## Timers (🆕 Flink-style)

Override `declaredTimerTypes(): Set<TimeNotion>` (registering an undeclared type throws). `context.timerService()`
→ `TimerService<K>`: `registerEventTimeTimer(key, ts)`, `registerProcessingTimeTimer(key, ts)`,
`deleteEventTimeTimer(...)`, `deleteProcessingTimeTimer(...)`, `currentWatermark()`, `currentProcessingTime()`.
Handle in `onTimer(timestamp, key, TimerContext)` — `TimerContext` gives `forward(...)`,
`getStateStore(name)` (read/write), `timerService()`, `timestamp()`, `timeDomain()`. Timers dedup by
`(key, timestamp)` and fire once — also when re-registered from the key's own lane while already due and queued on it —
and a delete from that lane cancels a queued fire (`onTimer` does not run); a write from another lane or a punctuator
follows the cross-lane rule below. Behind a re-key, a due timer's `onTimer` waits until the latest commit boundary has
reached its lane (as re-keyed records do). Re-arming the same `(key, timestamp)` from its own `onTimer` is a new timer that
fires once more. Persistent backend survives restart. A timer key must not be `null` —
`register*`/`delete*` throw `IllegalArgumentException("Timer key must not be null")`. Re-registering from `onTimer` via
`context.timerService()` works in event and processing time. An `onTimer` throw, or a key that fails to deserialize at
fire time, goes to `processing.exception.handler` (source topic `__timer`; the handler's record key is the decoded key,
or the raw key bytes if decoding failed): CONTINUE writes its dead letters and drops the timer; FAIL (or a throwing
handler, or a DLQ send that fails) stops the instance and keeps the timer, which fires again after restart on a
persistent backend (`TwoTier` default / `RocksDB`; a `Heap` timer is lost on restart). Punctuator failures are routed
the same way (`__punctuator`).
Timer key serde: the key serde declared for the processor's key — upstream (`Consumed`/`Grouped`/`Repartitioned`) or
downstream before any key change (`Repartitioned`/`Produced`) — else `default.key.serde`. With nothing declared on the
records' path, an upstream DSL store's key serde counts (`groupBy(…).count(Materialized.withKeySerde(X)).toStream()` → X,
the serde its records are laned by); a Processor API store never does (its keys are whatever the processor writes).
Over several branches (a `merge`, a multi-parent node) a re-keying branch declares nothing and disagreeing declarations
fall to the default — never decided by parent order. A Processor API node in between (`process()`/`addProcessor()`)
counts as a key change. The timer's lane is MurmurHash3 over those bytes —
except behind windowed aggregations in the same sub-topology, where a `Windowed<K>` timer is laned (and fires) by its
innermost key's bytes, the lane its records keep (a windowed aggregation is not a key change).
For a Processor API source **declare the key serde**: 🆕 `Topology.addSource(name, Consumed.with(keySerde, valueSerde),
topics)` (+ `Pattern` overload; `Consumed` maps as in `StreamsBuilder.stream()` — serdes, timestamp extractor /
watermark strategy, offset reset, lane overrides; a `Consumed` name must equal the node name). `addGlobalStore` /
`addReadOnlyStateStore` have the same `Consumed` twin (`storeBuilder, sourceName, consumed, topic, processorName,
supplier`; offset reset must be `earliest`). Behind a deserializer-only `addSource(name, keyDeserializer, …)` the
timer serde falls back to `default.key.serde`, which must reproduce the topic's key bytes; a default that cannot
serialize the key type throws a named `ClassCastException` at the first registration (`SerializationException` is
rethrown untouched).
- **Contract:** a key's timers run on its records' lane whenever the `timer-key-lane-affinity` validation rule
  (default `warn`) is silent — except under a custom `repartition()` `StreamPartitioner`. The rule fires on a
  timer-aware node behind a deserializer-only source, or entered across a boundary laned by another key serde. Fix:
  declare the key serde (`Consumed.with(Serdes.ByteArray(), …)` for a byte-array source; for a boundary, the same key
  serde on the operator that opens it — `Repartitioned`/`Grouped`); if the default already writes exactly the topic's
  key bytes, set the rule `off`.
- **Both runtime checks compare LANES:** the lane the timer key's bytes hash to vs the record's lane. Bytes that
  differ but land on the same lane are no split and never reported; every split counts
  `stoatflow.timer.lane.mismatch.total{processor}`.
- **Runtime guard:** a timer registered/deleted for the key being processed that picks another lane. Verdict =
  `timer.lane-affinity.guard` (YAML `stoatflow.processing.timer-lane-affinity-guard`): `error` (default — logs at
  ERROR and throws `TimerLaneAffinityException`, `io.stoatflow.core.exception`; bypasses `processing.exception.handler`,
  epoch aborts, offsets held; the timer is not registered, and one caught by user code — around the timer call or
  upstream around `context.forward` — is rethrown once that processor returns and again at the record's end) | `warn` (WARN once per processor, timer registers) | `off`.
- **Sampled check:** first 64 records per processor and lane, then 1 in 4,096, re-serialize the key before `process`.
  Verdict = `timer.lane-affinity.sampled-check` (YAML `stoatflow.processing.timer-lane-affinity-sampled-check`):
  `warn` (default — WARN once per processor, every split counted, app keeps running) | `error` (fatal) | `off`.
  Warn by default because it also judges processors that never time the record's own key, and some encodings
  legitimately differ from a re-serialization (evolved Schema Registry id, JSON from another producer). Both checks
  skip single-lane sub-topologies, records laned by a `repartition()` `StreamPartitioner`, and registrations from
  `onTimer`/punctuators (only the record being processed is judged). `TopologyTestDriver` checks every piped record
  over the processor's sub-topology lane count (`numberOfLanes`, else `num.stream.threads`, else a fixed reference
  count of 16 — never the host's cores) and applies a `repartition()` partitioner per record, as the engine does.
- **Format risk:** the timer key serde is part of the timer store's format — changing it between deployments (e.g.
  adding `addSource(…, Consumed…)` to an app that ran on the default) leaves stored timers with old bytes (decode
  failure at fire, never deleted, or — worse — decoded as a DIFFERENT key, so `onTimer` acts on the wrong key's
  state).
- **Any key:** a timer may be registered for any key the timer serde can serialize, from `process`, `onTimer` or a
  punctuator (`init` included); a timer for `y` armed while processing `x` fires on `y`'s lane, serially with `y`'s
  records (beyond Flink's current-key scoping; KS has no keyed timers). Cross-lane order is NOT guaranteed: a
  register/delete for `y` from another lane or a punctuator races `y`'s own lane. The set stays consistent: a write from
  a later commit wins over one from an earlier commit, even when the earlier one was made later in time, and this holds
  after a restart too. So a cross-lane re-registration from a later commit than a fire waiting or running on `y`'s lane
  fires the timer once more, after it, and a cross-lane delete from a later commit than a waiting fire does NOT cancel
  it (the fire runs in its own commit; a delete from the same commit or an earlier one cancels) — persistent backends;
  `TimerBackend.Heap` has no commits and applies a cross-lane write when it happens: one fire, and a delete cancels. A
  replay after a failure can place a cross-lane mutation in a different commit (one extra fire, one fewer, or a delete
  that cancels a fire it came after the first time, or the reverse). When order matters use the Flink-style pattern — forward a record
  re-keyed to `y` and arm the timer while processing it (one writer per key).

## Processor wrapping (KIP-1112 `processor.wrapper.class`)

One configured `ProcessorWrapper` decorates **every** node of the topology — DSL operators *and* Processor-API
nodes — at topology-compile time. The surface is the KS one: `wrapProcessorSupplier(name, supplier)` /
`wrapFixedKeyProcessorSupplier(name, supplier)`, the markers `WrappedProcessorSupplier` /
`WrappedFixedKeyProcessorSupplier`, the statics `ProcessorWrapper.asWrapped(...)` / `.asWrappedFixedKey(...)`,
and a `configure(Map)` default invoked once at instantiation. Default `NoOpProcessorWrapper` (pass-through).

Wire it the KS way (portable) — `new StreamsBuilder(new TopologyConfig(config))` / `new Topology(topologyConfig)`
— or just set `processor.wrapper.class` on the runtime config: **StoatFlow honours that too** (KS silently
ignores it as "too late"). `TopologyConfig` wins on conflict, with a WARN.

What a wrapper can do, by node kind:

| Node | Capability |
|---|---|
| Your PAPI nodes (`process`/`processValues`/`addProcessor`) | decorate, **replace**, add stores via the wrapped supplier's `stores()` |
| DSL operator nodes | **record path only** — observe / decorate / short-circuit; a decorator's record rewrite (`record.withTimestamp(...)` / `withHeaders(...)`) flows through to the operator, downstream nodes, and the sink. Replacement is rejected at build time (`TopologyValidationException` naming the node), as is declaring `stores()` for one |
| `init`/watermark/suppress-flush/restore callbacks on DSL nodes | not routed through the wrapper; a DSL-node decorator's context also cannot `schedule()` / `timerService()` (both throw). `onTimer` is NOT bypassed — keyed timers exist only on Processor-API nodes, which Layer 1 wraps in full |
| `addGlobalStore` / `addReadOnlyStateStore` state-update suppliers, engine-synthetic nodes | not wrapped (KS parity) |

Two gotchas: the returned supplier's **`get()` runs once per lane** (not once per task), so decorator instance
fields are lane-scoped — put cross-lane aggregation on the thread-safe wrapper object; and **node names are
StoatFlow's** (`mapValues-0`, `process-0`, your `Named`), not KS's `KSTREAM-…-0000000001`, so a ported
name-matching wrapper compiles and matches nothing. Zero overhead when the key is unset.

## Scheduled sources (🆕 no KS equivalent)

`StreamsBuilder.scheduled(...)` → `KStream<K,V>` — periodically generates records without consuming from
Kafka. Two overloads:

- Interval: `scheduled(named, interval: Duration, type: PunctuationType, keySerde?, emitter)` —
  `WALL_CLOCK_TIME` or `STREAM_TIME`; interval must be positive.
- Cron: `scheduled(named, cron: CronExpression, keySerde?, emitter)` — always wall-clock.

`CronExpression` (factories `@JvmStatic`): `CronExpression.unix(...)` (5 fields, Sun=0), `.quartz(...)`
(6–7 fields, Sun=1), `.spring(...)` (6 fields); unparseable → `IllegalArgumentException`.

`ScheduledEmitter<K,V>` receives `ScheduledEmitterContext`: `forward(key, value[, timestamp])` /
`forward(record)`, `getStateStore(name)` (**read-only** — `put`/`delete` throw `UnsupportedOperationException`
for every store family: plain KV/window/session as well as timestamped, versioned and headers-aware, incl.
the timestamped store a DSL `Materialized` aggregation produces. The view keeps the store's declared type,
so a `getStateStore<TimestampedKeyValueStore<..>>` cast still succeeds), `currentWallClockTime()` and
`currentSystemTimeMs()` (both the system clock, or the driver's wall clock under the `TopologyTestDriver`),
`currentWatermarkMs()`/`currentStreamTimeMs()` (`Long.MIN_VALUE` until first event).

```kotlin
builder.scheduled<String, String>(
    interval = Duration.ofSeconds(30),
    type = PunctuationType.WALL_CLOCK_TIME,
    emitter = { ctx -> ctx.forward("heartbeat", "alive-${ctx.currentWallClockTime()}") },
).to("heartbeats", Produced.with(Serdes.String(), Serdes.String()))
```

Auto-named `scheduled-source-0`, `-1`, … `STREAM_TIME` is data-driven (won't fire without records);
cron/`WALL_CLOCK_TIME` fire independent of throughput. Emitted records participate in the exactly-once
commit boundary. A topology with ONLY scheduled sources (no `stream()`/`table()`) is valid under both guarantees —
no dummy input topic needed. Test with `driver.triggerScheduledSource(name)` / `advanceWallClockTime` /
`advanceWatermark`.
