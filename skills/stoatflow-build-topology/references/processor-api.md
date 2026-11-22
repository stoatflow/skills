# Processor API, timers, punctuators, scheduled sources

Interfaces are `io.stoatflow.core.processor.*`.

## Processors

- `Processor<KIn,VIn,KOut,VOut>` — attach via `KStream.process(supplier, *storeNames)`; can change key.
- `FixedKeyProcessor<KIn,VIn,VOut>` — attach via `KStream.processValues(...)`; key fixed.
- Base classes `ContextualProcessor` / `ContextualFixedKeyProcessor` expose `context()` after `init`.
- Records: `Record<K,V>` (`.key`/`.value`/`.timestamp`; `withTimestamp(...)`, `withHeaders(...)`) and
  `FixedKeyRecord<K,V>`.
- Suppliers: `ProcessorSupplier` / `FixedKeyProcessorSupplier` — `get()` **must return a fresh instance
  every call** (the engine creates one per key-affinity lane, plus one **punctuator instance** per node — see
  *Punctuators*); `stores()` (default empty) declares `Set<StateStoreBuilder<*>>`.

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
`forward(value[, timestamp])`, `key()` reads the fixed key — the record's in `process()`, the timer's in `onTimer`, a
kept context's too; in `init()`, `close()` and a punctuator both throw `IllegalStateException`: forward a
`FixedKeyRecord` there. Neither exists on Kafka Streams' `FixedKeyProcessorContext`). Metadata/time: `timestamp()`, `headers()`,
`topic()`/`partition()`/`offset()`, `applicationId()`, `currentWatermarkMs()`, `currentStreamTimeMs()` (KS
alias for the watermark), `currentSystemTimeMs()` (system clock; the driver's wall clock under the
`TopologyTestDriver`), `getStateStore(name)`, `isLate()`. `headers()` is the record being processed
— the same instance as `record.headers()`, headers set upstream with `forward(record.withHeaders(...))` included —
in `onTimer` the timer's headers — those of the record that armed it (*Timer headers* below) — and empty in punctuators
and `init()`. All five record accessors — `timestamp()`,
`headers()`, `topic()`, `partition()`, `offset()` — follow the callback they are called in, so a context kept from
`init()` always reports one record: in `process()` the record and its source coordinates (past an in-memory
`repartition()` or re-key boundary, the source record's — Kafka Streams reports its repartition topic's); in `onTimer`
the timer's time, in a punctuator the punctuation's, with `""` / `-1` / `-1` for topic, partition and offset — also for
what those callbacks (or a scheduled source, or a window's final result emitted on a watermark advance) forward; in a
`suppress` result the record it was buffered with (as in Kafka Streams); in `init()` and `close()` the wall clock when
read and `""` / `-1` / `-1`. They describe the calling thread's record: read them in the callback — a thread you start
(an executor task, a `CompletableFuture`) sees no record. A kept context's `forward(...)` works in `onTimer` like the
`TimerContext`'s; a `forward(...)` in `init()`, `close()` or a thread you start has nothing to forward downstream of and
is dropped (one WARN per processor).

`isLate()` is true only for an **arrival** — a record a topic delivered, or one forwarded from it (an aggregation's or a
join's update included, judged by the timestamp it is forwarded with), whose timestamp is below the watermark. What a
timer, a punctuator or a scheduled source produces is never late, nor a window's final result emitted on a watermark
advance or a `suppress(untilWindowCloses | untilTimeLimit)` result, nor what they forward: below the watermark by
construction. An `untilBarrier()` flush is judged against the watermark its update arrived under (`emit.mode =
on-barrier` leaves `isLate()` unchanged, a downstream re-stamp included). False in timer and punctuator callbacks.

**Timestamps: any `Long` but `Long.MIN_VALUE`.** Negative times are valid event times (windows, joins, timers,
punctuators handle them). `Long.MIN_VALUE` is StoatFlow's "no event time", and passing it as a time throws
`IllegalArgumentException`: a `Record` / `FixedKeyRecord` (constructor, `withTimestamp`, …), every
`forward(key, value, timestamp)` (a punctuator's too), timer registration in both domains, an anchored punctuator's
`startTime`, a scheduled source's `forward`. `currentWatermarkMs()` / `currentStreamTimeMs()` return `Long.MIN_VALUE`
before the first watermark, so `record.withTimestamp(context.currentStreamTimeMs())` throws there (Kafka Streams'
`Record` likewise rejects its initial stream time, `-1`). A negative-time record reaching a sink is written without a
timestamp — the producer or broker stamps the wall clock, counted as `stoatflow.sink.records.negative.timestamp.total`.

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
Nothing is lost — set it `off`. A punctuator counts as a writer: its exclusive work (below) pauses only its own
sub-topology, so the other one's lanes keep writing meanwhile. Otherwise connect the store from one sub-topology, or let
one processor own it and forward what the others need. The same shape among DSL writers is refused.

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

**Extractors and missing times (DSL sources too).** A `TimestampExtractor`'s `partitionTime` argument is the
partition's real partition time — the highest event time extracted from that partition before the record, restored
from the committed offsets after a restart (`-1` while unknown, as in Kafka Streams). An extractor or assigner that
returns `Long.MIN_VALUE` skips the record, and so does Kafka's `-1` ("no timestamp") on a source with neither; the
skip is counted as `stoatflow.dropped.records.total{reason=no_event_time}` (WARN at most once a minute per partition)
and its offset committed. A `-1` an extractor reads from the payload is a time. `Long.MAX_VALUE` is a valid time too.
Kafka Streams' built-in extractors ship in `io.stoatflow.core.processor`, behaving as in KS — each of the first three
treats every negative Kafka timestamp as invalid: `FailOnInvalidTimestamp` (fails the application, not restartable),
`LogAndSkipOnInvalidTimestamp` (skips and counts the record), `UsePartitionTimeOnInvalidTimestamp` (uses the partition
time; throws while it is unknown), and `WallclockTimestampExtractor` (processing time). Use one per source
(`Consumed.withTimestampExtractor(...)`, `addSource(extractor, …)`) or globally (`default.timestamp.extractor`) to
refuse negative times as Kafka Streams does; an extractor's exception fails the application.

**Implementing `WatermarkStrategy` yourself?** If its timestamp assigner reads the event time from the key, the
value or the headers, `hasCustomTimestampAssigner()` must return `true` (the built-ins set it when you call
`withTimestampAssigner`). StoatFlow trusts the flag: it orders partitions by event time with it, and counts a
partition waiting behind older records by that event time. Left `false`, such a partition counts with its records'
Kafka timestamps — if the extracted times are earlier, the watermark passes them and the records are dropped as late.

## Threading — the rule that bites

**One processor instance per lane** (the KS-task analog), **plus one punctuator instance per node**. Instance fields
are per-lane and safe without synchronization — but state shared across lanes, or between `process()` and a
punctuator, **must live in a state store**, not an instance field (KSC-160, *Punctuators* below).

**One key, one thread at a time, within a sub-topology.** A key's records, its timers, a LEFT/OUTER stream-stream
join's unmatched results and an eager/time-limited `suppress()`'s releases and evictions all run on the key's lane, so
keyed state behind them (a `count`, an aggregation) is never read-modify-written by two threads, and a key's emissions
keep their order. A punctuator works on shared state only alone, at a commit barrier (below).

**How a punctuator runs.** Each Processor API node's punctuators run on a thread of their own: one processor's
punctuators one after another, **different processors' in parallel**, concurrently with the record lanes. A run's
**free part** — everything before its first access to a connected store, its timer service or a `KeyLockManager`
lock (external calls, computing, logging, global/read-only store reads, forwards) — pauses nothing. From that first
access the run's **shared part** waits for the next commit barrier (one is created at once if none is due) and runs
there as **exclusive work**: the sub-topology's record lanes stop at the barrier, the run continues alone, its forwards
go downstream, then the lanes continue. A run that touched no state but forwards into its own sub-topology (anything
before the next boundary, a sink included) takes a short exclusive window for the hand-off; forwards that only cross a
boundary take none. A store of the user's own `StateStore` type (built by their own `StoreBuilder`, not `Stores`) is
handed out as it is — `init()` may cast it to its class, as in Kafka Streams — but cannot report its first access: once
the context has handed one out, every run of that processor's punctuators is a shared part from its start, the lanes
stopped for the whole run.
Advice to give:

- **Keep the shared part short; do slow I/O first, touch stores last.** While it runs the sub-topology takes no input,
  and under load the whole application's input stops (full lane queues stop the dispatcher for every source).
  `stoatflow.subtopology.exclusive.work{reason,phase}` measures it; a window past 5 s logs a WARN naming the
  punctuators; past `stoatflow.commit-stall.threshold-ms` (45 s) the app stops. A run with a shared part lasts at least
  until its barrier, so a schedule shorter than that wait skips fires (locked-run: an overlapping fire is skipped).
- **Writes follow processing order, not timestamps:** they land after every record dispatched before the barrier. A
  read-then-write inside the shared part builds on the newest value; a blind write computed from earlier data (a field,
  an external call, a free-part global read) replaces what a lane wrote earlier in the commit. To keep a newer value,
  compare the stored `ValueAndTimestamp`, use a versioned store, or use a timer.
- **Global and read-only stores never pause it**; in the free part they read committed data under `exactly_once` (up
  to one commit behind a lane), in the shared part as the lanes do.
- **Locks:** `KeyLockManager.withLock(s)` in a punctuator waits for the exclusive work first, then locks (uncontended).
  For any **other** lock, semaphore or pooled resource shared with `process()`: never hold it across the punctuator's
  first store, timer or `KeyLockManager` access — a lane blocked on it never reaches the barrier, and the commit-stall
  watchdog stops the app.
- **Graceful close** serves the runs in flight within the final commit's time limit; a run cut off commits nothing and
  fires again after the restart.
- **Long or per-key time-driven work belongs in a timer** (`onTimer` fires on the key's lane, timers of different keys
  run in parallel, nothing pauses); record generation belongs in a scheduled source (`builder.scheduled()`). A
  punctuator that scans and rewrites a large store pauses its sub-topology for the whole scan.

- **After a crash under `at_least_once`, no state change is durable before its output** — a punctuator's, an
  `onTimer` call's, a record's, a downstream sub-topology's: each commit waits for the epoch's output to be
  acknowledged before the cached stores send their changelog records and the timer, versioned and `lruMap` stores
  release theirs, held until that commit. Output and state are kept or lost together; lost, the work runs again from the
  committed state — at most a duplicate (Kafka Streams can lose a punctuator's output here). Cost: those three stores'
  changelogs and an HA standby trail by up to one commit; held writes count toward the uncommitted-state memory limit.
  Not covered: an output whose send fails.
- **Reads are as of the lane's commit under `exactly_once`:** `process`, `onTimer` and punctuators never see another
  lane's uncommitted next-commit write (except from `lruMap` / in-memory versioned stores, above). IQ and
  `at_least_once` read the latest state.
- **Threads you fork yourself are unsupported under `exactly_once`:** not a processing lane — their reads see the
  latest state, not the lane's commit, and their writes are not attributed to the lane's commit.

## Punctuators

`context.schedule(interval, TimeNotion, punctuator)` → `Cancellable` — exactly Kafka Streams'
`schedule(Duration, PunctuationType, Punctuator)`; `Punctuator` is a fun interface, so a Kotlin or Java lambda or a
method reference binds. `TimeNotion.STREAM_TIME` (alias `EVENT_TIME`) / `WALL_CLOCK_TIME` (alias `PROCESSING_TIME`);
Java `PunctuationType`/`TimeDomain` resolve to the same constants. The anchored overload (KIP-1146) takes the start
time first, as Kafka Streams does: `schedule(startTime: Instant, interval, type, punctuator)`. There is no execution
mode: every punctuator runs as described under *Threading*.
Locked-run: an overlapping fire is skipped.

**A punctuator runs on its own instance (KSC-160, divergence).** Every Processor API node (`process`,
`processValues`, `transformValues`, `addProcessor`) has a dedicated punctuator instance created at start;
`schedule()` registers punctuators only there and they always run on it — `process()` never does. On a lane instance
`schedule()` returns a `Cancellable` that does nothing. Same under the `TopologyTestDriver`. Two Kafka Streams
patterns compile and then break **silently** — always rewrite them:

- Records buffered in an instance field in `process()` and flushed from a punctuator → the punctuator's field is
  empty, nothing flushes. Use a key-value store: `put` in `process()`, iterate and delete in the punctuator.
- A `Cancellable` kept in a field and cancelled from `process()` → cancels nothing. Cancel from inside the punctuator
  (it runs on the instance that scheduled it), or keep the decision in a store the punctuator reads.

```kotlin
// "batch-buffer" connected at the call site: process({ BatchingProcessor() }, named, "batch-buffer")
class BatchingProcessor : ContextualProcessor<String, Event, String, Event>() {
    private lateinit var buffer: KeyValueStore<String, Event>

    override fun init(context: ProcessorContext<String, Event>) {
        super.init(context)
        buffer = context.getStateStore("batch-buffer")
        context.schedule(Duration.ofSeconds(30), TimeNotion.WALL_CLOCK_TIME) { ts ->   // on the punctuator instance
            val flushed = mutableListOf<String>()
            buffer.all().use { it.forEach { e -> context.forward(Record(e.key, e.value, ts)); flushed += e.key } }
            flushed.forEach { buffer.delete(it) }
        }
    }

    override fun process(record: Record<String, Event>) {
        buffer.put(record.key, record.value)   // a store, never a field: the punctuator runs on another instance
    }
}
```

**Fire times (Kafka Streams' `PunctuationSchedule`):** `STREAM_TIME` fires on the first data — the first watermark,
negative or not — then on the grid aligned to 0 (every multiple of the interval); for a first watermark ≥ 0 that is
exactly Kafka Streams' schedule, and a negative one fires with a negative punctuation time. `WALL_CLOCK_TIME` first
fires one interval after it was scheduled, then on that grid; anchored fires at `startTime`, or at the first grid point after now when `startTime` is
already behind. A jump past several grid points fires once (with the current time), then the next grid point — no drift.
Inside the callback `context.timestamp()` is the punctuation time and `headers()` is empty (Kafka Streams parity) —
not the last record's; it is one instance for the punctuation, so a header added there rides its `forward(key, value)`.

**Start timing (Kafka Streams parity):** `init()` runs when the application starts — not with the first record —
so a punctuator scheduled there (catch-up, heartbeat, TTL cleanup) runs even if no input arrives. The punctuator
instance is initialised at start; each lane initialises its own instance on its first record (no lane adopts the
punctuator instance), so a node with input runs `init()` / `close()` once more than on Kafka Streams. In HA active/standby, `init()` runs when a pod becomes active, never on a standby. In `init()`,
`context.timestamp()` is the wall clock.

## Timers (🆕 Flink-style)

Override `declaredTimerTypes(): Set<TimeNotion>` (registering an undeclared type throws). Declaring `EVENT_TIME` makes
the topology need a watermark: refused (`TopologyValidationException`) under `timestampCoordinationMode = DISABLED`,
beside a `noWatermarks()` source, or with no Kafka topic — by the TopologyTestDriver too. The timer store and its
`<app>-timer-store-changelog` topic exist only when some processor declares a timer type, on every timer backend (the
`MEMORY` backend keeps the store in memory); once none does, the local `timer-store` directory is cleaned up as stale and its timers stop
firing (they come back from the changelog if timers are declared again — lost instead when the timer backend keeps no
changelog, ran without one at some point, or state restoration is off, which a start-up WARN names; a
`processor.wrapper.class` decorator that does not forward `declaredTimerTypes()` drops the declaration too).
`context.timerService()`
→ `TimerService<K>`: `registerEventTimeTimer(key, ts)`, `registerProcessingTimeTimer(key, ts)`,
`deleteEventTimeTimer(...)`, `deleteProcessingTimeTimer(...)`, `currentWatermark()`, `currentProcessingTime()`,
plus the 🆕 named members below and the 🆕 `headers` overloads (*Timer headers*).
An event-time timer fires when the watermark **reaches** its time (watermark ≥ ts), a processing-time one when the wall
clock does (≥). A timer registered already due — at or before the current watermark / wall clock — never fires inside
the registering call: the next collection pass fires it (every watermark advance, and the wall-clock tick,
`processing.wall-clock-advance-interval-ms`, 10 ms default), milliseconds later, with no new input needed.
Handle in `onTimer(timestamp, key, TimerContext)` — `TimerContext` gives `forward(...)`,
`getStateStore(name)` (read/write), `timerService()`, `timestamp()`, `timeDomain()`, `headers()`, `timerName()` (`null`
for an unnamed timer; `FixedKeyTimerContext` has both). Unnamed timers dedup by
`(key, timestamp)` and fire once — also when re-registered from the key's own lane while already due and queued on it —
and a delete from that lane cancels a queued fire (`onTimer` does not run); a write from another lane or a punctuator
follows the cross-lane rule below. Behind a re-key, a due timer's `onTimer` waits until the latest commit boundary has
reached its lane (as re-keyed records do). Re-arming the same `(key, timestamp)` from its own `onTimer` is a new, already-due timer that
fires once more, at the next collection pass. Timers survive a restart on every backend: `MEMORY` restores them from the timer changelog (on by
default), the on-disk backends also keep them locally. A timer key must not be `null` —
`register*`/`delete*` throw `IllegalArgumentException("Timer key must not be null")`; a timer timestamp may be any
`Long` but `Long.MIN_VALUE`, negative included (`register*` throws `IllegalArgumentException` on `MIN`).
Re-registering from `onTimer` via `context.timerService()` works in event and processing time. An `onTimer` throw, or a key that fails to deserialize at
fire time, goes to `processing.exception.handler` (source topic `__timer`; the handler's record key is the decoded key,
or the raw key bytes if decoding failed, and its headers are the timer's — with raw key bytes, plus what the key
serializer wrote, so a header-mode key stays decodable): CONTINUE writes its dead letters and drops the timer; FAIL (or a throwing
handler, or a DLQ send that fails) stops the instance and keeps the timer, which fires again after restart — on every
timer backend with its changelog on (default), and on `HYBRID` / `PERSISTENT` from local state with it off. Punctuator failures are routed
the same way (`__punctuator`, no headers).
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
  it (the fire runs in its own commit; a delete from the same commit or an earlier one cancels) — every timer backend;
  only the TopologyTestDriver has no commits and applies a cross-lane write when it happens: one fire, and a delete cancels. A
  replay after a failure can place a cross-lane mutation in a different commit (one extra fire, one fewer, or a delete
  that cancels a fire it came after the first time, or the reverse). When order matters use the Flink-style pattern — forward a record
  re-keyed to `y` and arm the timer while processing it (one writer per key).

### Timers and event time — the bounded timer cache

Every timer backend holds at most `maxCachedTimers` timers **per time domain** in its timer cache, loaded in timestamp
order (YAML `timer-backend.max-cached-timers`). Constructors lead with `changelogEnabled`:
`TimerBackend.TwoTier(changelogEnabled, cacheHorizon, maxCachedTimers = 100_000)` (YAML `HYBRID`, the default) and
`TimerBackend.RocksDB(changelogEnabled, maxCachedTimers)` (`PERSISTENT`) keep every timer in RocksDB;
`TimerBackend.InMemory(changelogEnabled, cacheHorizon, maxCachedTimers)` (`MEMORY`, formerly `HEAP` — `type: HEAP` now
fails the start) keeps every timer in memory with no bound on the total, loads no RocksDB, and replays the whole timer
changelog at every start and in-place restart (slow start-up for many timers; with the changelog or state restoration
off its timers are lost at every restart). `loadIntervalMs` / `load-interval-ms` no longer exist. Pick it in YAML
(`stoatflow.timer-backend.type` + its keys) or in code: `StreamsConfig.builder(appId, servers).timerBackend(TimerBackend.InMemory())`
— no Properties/Map key, so after `StreamsConfig.fromProperties(props)` chain `.timerBackend(...)`; under the runtime,
`streamsConfigOverrides { timerBackend(...) }` replaces the whole YAML `timer-backend` block. From Java the
`changelogEnabled` argument comes first: `new TimerBackend.InMemory(true, Duration.ofSeconds(30))`.

- **Event time waits for an unloaded timer.** When more event-time timers are due than fit, the effective watermark
  stops at the first one not yet loaded; the loaded ones fire, the next load, it moves on. Records that would move event
  time past it wait in their partition buffers. No timer fires late; no window closes, no record is judged late and no
  stream-time punctuator fires past an unfired timer — results equal an all-in-memory run.
- `currentWatermarkMs()` follows the held watermark (in `process()` and `onTimer`).
- Catch-up is stepwise (~`backlog / maxCachedTimers` steps); a slow `onTimer` holds every event-time effect in the app.
  Advise raising `maxCachedTimers` when `stoatflow.watermark.timer.held` toggles often.
- Processing-time timers have their own budget — a processing-time backlog never holds event time.
- A **custom `WatermarkGenerator`** is never held record by record (no preview): only the watermark waits, so which late
  records a window drops can differ from an all-in-memory run.
- A timer registered for a time already loaded joins even a full cache (registrations within `cacheHorizon`; 1 s for
  `RocksDB`). The `TopologyTestDriver` keeps every timer in memory and never holds the watermark.

### Named timers (🆕, no KS equivalent)

One timer per `(key, name)` across BOTH domains — the tool for a deadline that moves (session timeout, cart
abandonment, debounce). Prefer it over keeping a `lastSeen` value in a store plus an exact
`delete*Timer(key, oldTs)`.

- `registerEventTimeTimer(key, name, ts)` / `registerProcessingTimeTimer(key, name, ts)` create or **replace** the
  name's pending timer — at another time, or in the other domain (a processing-time registration moves an event-time
  timer of that name). Same time + value again: no-op.
- `deleteTimer(key, name)` — whatever the domain; also cancels a due fire waiting on the key's lane.
  `deleteAllNamedTimers(key)` — every name of the key, both domains; unnamed timers stay.
- `namedTimer(key, name): NamedTimer?` — `timeNotion()` (`TimeNotion.EVENT_TIME` / `PROCESSING_TIME`) and
  `timestamp()` from Java; `null` when none.
- In `onTimer`: `context.timerName()`. The fired timer is removed when `onTimer` returns normally (a failure: see the CONTINUE/FAIL rule below); inside that call
  `namedTimer(key, name)` already returns `null` for it (through either `timerService()`) until the call registers it
  again — `if (timers.namedTimer(key, name) == null) timers.registerEventTimeTimer(key, name, next)` re-arms.
- Same rules as unnamed: domain gated by `declaredTimerTypes()` (`IllegalStateException`; deletes and the query are
  not gated), non-null key, any timestamp but `Long.MIN_VALUE`. Name: non-empty, ≤ 255 UTF-8 bytes, else
  `IllegalArgumentException`; a `null` name from Java throws `NullPointerException` (Kotlin's parameter
  check — Kafka Streams' own APIs' convention for a `null` argument; Kafka Streams has no named timers). CONTINUE removes a failed named timer; FAIL keeps it (name and time) for the restart, as an
  unnamed timer.
- Named and unnamed timers coexist: same key + timestamp + domain → two timers, both fire.
- **Key first:** with `K = String`, `registerEventTimeTimer("abandon", cartId, ts)` compiles and arms a timer for KEY
  `"abandon"`. Check the argument order.
- **Moves:** exact from the key's own lane (its `process()` / `onTimer`), also when the old deadline's fire is already
  waiting. From another lane or a punctuator they follow the commit order above: within about one commit interval
  around the old deadline the old deadline may fire too (the new one still fires later); two near-simultaneous moves
  from different lanes are ordered by commit. A deadline moved long before it is due is replaced exactly. For
  exactness in that window, forward a record re-keyed to the key and move its timers while processing it.

```kotlin
// one timeout per session: every event moves it, so only the latest deadline fires
timers.registerEventTimeTimer(record.key, "timeout", session.lastActivity + SESSION_TIMEOUT_MS)

override fun onTimer(timestamp: Long, key: String, context: TimerContext<String, Session>) {
    val store = context.getStateStore<KeyValueStore<String, Session>>("sessions")
    val session = store.get(key) ?: return // no "is this still the current deadline?" check needed
    context.forward(key, session.complete())
    store.put(key, null)
}
```


### Timer headers (🆕)

A timer carries the **headers of the record that armed it**: every registration, unnamed or named, reads the calling
thread's record context at call time — the record in `process()`, the firing timer's own headers in `onTimer` (a re-arm
keeps them), none in `init()` or a punctuator. `registerEventTimeTimer(key, ts, headers)`,
`registerProcessingTimeTimer(key, ts, headers)` and the named `(key, name, ts, headers)` forms **upsert** theirs by key
(each key they hold replaces the captured ones of that key, then they are appended) — a retry counter re-armed from
`onTimer` stays one header. Nothing is mutated; the timer stores a copy. A timer keeps **one header per key** (the last
of a repeated key): a header-mode serializer appends its schema id to the headers it is handed — a forward from
`onTimer` hands it the timer's own — so a timer that forwards then re-arms stays bounded. The engine's `stoatflow.epoch`
stamp is never stored, from the record or the passed headers. At fire, `TimerContext.headers()` / `FixedKeyTimerContext.headers()` (and a kept context's `headers()`) return
a fresh mutable copy. **`forward(key, value)`** (with or without timestamp / child name) **and fixed-key
`forward(value)` carry them** (they build their record on the call's record context); once a forward reaches a sink the
producer makes that instance read-only (as for `process()`'s headers, and in KS) — change a copy for a later forward.
`forward(record)` sends the record's own headers, as in `process()` and KS — a new `Record(k, v, ts)` has none; build it with `context.headers()`
(or `record.withHeaders(context.headers())`) to carry them. Headers never reach the timer key serde: a key keeps
one byte form, so deletes, `deleteTimer`, `deleteAllNamedTimers` and `namedTimer` from any context find the timer and
take no headers; what the key serializer writes (Confluent header schema-id mode's key schema id) is stored with the
timer and decodes its key at fire — so a **header-mode key serde can key timers**; a key serde whose bytes depend on
header **contents** cannot (lane guard). A named re-arm at the same time is a no-op only with equal headers; a move takes
the moving call's headers. Cost: headers are stored per timer (row, changelog) and in memory with each cached timer when
≤ 1 KiB (larger ones are read back at fire); > 1 KiB WARNs (once a minute per processor); re-arming
the same timer from every record writes it each time the records' headers differ (a per-record trace id). To opt out, arm from a context with no
headers or forward to the timer node with `record.withHeaders(RecordHeaders())`.
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

Three gotchas: the returned supplier's **`get()` runs once per lane** (not once per task; plus once for a PAPI node's
punctuator instance), so decorator instance fields are lane-scoped — put cross-lane aggregation on the thread-safe
wrapper object; **node names are StoatFlow's** (`mapValues-0`, `process-0`, your `Named`), not KS's
`KSTREAM-…-0000000001`, so a ported name-matching wrapper compiles and matches nothing; and **a decorator must never
forward from `process()` of an emit-final node** — an `OnWindowClose` windowed / sliding / session / cogroup
aggregation, an `untilWindowCloses` suppress, a strict `untilTimeLimit` suppress. Those nodes emit only when a window
closes or a suppression ends; the first such forward fails with an error naming the wrapper and the node, never offered
to the processing handler, and the app stops (KSC-161; Kafka Streams allows it). Decorating, observing and timing them
is fine. Zero overhead when the key is unset.

## Scheduled sources (🆕 no KS equivalent)

`StreamsBuilder.scheduled(...)` → `KStream<K,V>` — periodically generates records without consuming from
Kafka. Two overloads:

- Interval: `scheduled(named?, interval: Duration, type: PunctuationType, keySerde?, options?, emitter)` —
  `WALL_CLOCK_TIME` or `STREAM_TIME`; interval must be positive. Java: named and unnamed forms, each with and
  without `ScheduledSourceOptions`.
- Cron: `scheduled(named, cron: CronExpression, keySerde?, emitter)` — always wall-clock.

`CronExpression` (factories `@JvmStatic`): `CronExpression.unix(...)` (5 fields, Sun=0), `.quartz(...)`
(6–7 fields, Sun=1), `.spring(...)` (6 fields); unparseable → `IllegalArgumentException`.

**Fire times.** `WALL_CLOCK_TIME`: at start-up, then every interval. `STREAM_TIME`: at the first watermark, then
each time the watermark passes a multiple of the interval — counted from event time 0, so the boundaries survive a
restart. A jump over several intervals or cron occurrences (backlog, data gap, far-future timestamp, long pause)
fires a source **once**, then it continues on its grid / at its next occurrence — not once per interval. "Start" is
when processing begins — with HA, the promotion (a standby fires nothing). Every start fires an interval source again
at once — in-place engine restarts too (a `REPLACE_THREAD` fault restart, a poison replay, a deleted pattern topic, an
HA promotion). To wait for the first grid point instead (one interval after start, or the first multiple at or after
the first watermark — at once when the first watermark is one), pass
`options = ScheduledSourceOptions.start(ScheduledStart.NEXT_INTERVAL)` or set
`stoatflow.dsl.scheduled-source-start: next-interval` (the default for sources that do not set it; `immediately` by
default). A fire due while the previous run is still going waits for it, then fires once (late, coalesced). A
`STREAM_TIME` source fires at the first watermark even when it is negative (small test event times); a Kafka sink sends
a negative timestamp as `null`, so the broker stamps the wall clock (A2.16). A `STREAM_TIME` source needs a
watermark: refused under `timestampCoordinationMode = DISABLED`, beside a `noWatermarks()` source, or in a topology
that reads no Kafka topic — at start, and by the TopologyTestDriver when it is built.

`ScheduledEmitter<K,V>` receives `ScheduledEmitterContext`: `forward(key, value[, timestamp])` (no timestamp ⇒ the
fire time: the watermark that fired a `STREAM_TIME` source, the wall-clock time otherwise) /
`forward(record)` (a timestamp of `Long.MIN_VALUE` throws `IllegalArgumentException`), `getStateStore(name)` (**read-only** — `put`/`delete` throw `UnsupportedOperationException`
for every store family: plain KV/window/session as well as timestamped, versioned and headers-aware, incl.
the timestamped store a DSL `Materialized` aggregation produces. The view keeps the store's declared type,
so a `getStateStore<TimestampedKeyValueStore<..>>` cast still succeeds), `currentWallClockTime()` and
`currentSystemTimeMs()` (both the system clock, or the driver's wall clock under the `TopologyTestDriver`),
`currentWatermarkMs()`/`currentStreamTimeMs()` (`Long.MIN_VALUE` until first event).

```kotlin
builder.scheduled<String, String>(
    interval = Duration.ofSeconds(30),
    type = PunctuationType.WALL_CLOCK_TIME,
    keySerde = Serdes.String(),
    emitter = { ctx -> ctx.forward("heartbeat", "alive-${ctx.currentWallClockTime()}") },
).to("heartbeats", Produced.with(Serdes.String(), Serdes.String()))
```

Each forwarded (non-null) key is serialized with the source's key serde (`keySerde`, else `default.key.serde`) to pick
its lane — so the serde must serialize the keys you forward; a `String` key with neither set fails the fire, in the
`TopologyTestDriver` as in production.

Auto-named `scheduled-source-0`, `-1`, … `STREAM_TIME` is data-driven (won't fire without records);
cron/`WALL_CLOCK_TIME` fire independent of throughput. Emitted records participate in the exactly-once
commit boundary. A topology with ONLY scheduled sources (no `stream()`/`table()`) is valid under both guarantees —
no dummy input topic needed. Test with `driver.triggerScheduledSource(name)` / `advanceWallClockTime` /
`advanceWatermark`.
