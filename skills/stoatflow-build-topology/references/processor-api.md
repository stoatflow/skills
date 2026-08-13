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
does re-key, apply a remedy; if it does not, set that rule to `off`.

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
alias for the watermark), `currentSystemTimeMs()`, `getStateStore(name)`, `isLate()` (in `process`).

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

The failure surfaces on the first record reaching each lane, not at startup (instances are created
per-lane on demand, including `init`). Build store builders via `Stores` (`io.stoatflow.core.state`).

## Threading — the rule that bites

**One processor instance per lane** (the KS-task analog). Instance fields are per-lane and safe without
synchronization — but state shared across lanes, or read from a punctuator, **must live in a state store**,
not an instance field. A punctuator runs on a dedicated **punctuation lane** concurrently with record
lanes, so its read-modify-write is **not** serialized against a key's record processing. For a per-key
time-triggered mutation, prefer a **timer** (`onTimer` runs in the same per-key serialized context as
`process`).

## Punctuators

`context.schedule(interval, TimeNotion, PunctuatorMode?, callback)` → `Cancellable`. `TimeNotion.STREAM_TIME`
(alias `EVENT_TIME`) / `WALL_CLOCK_TIME` (alias `PROCESSING_TIME`); Java `PunctuationType`/`TimeDomain`
resolve to the same constants. `PunctuatorMode.BLOCKING` (default) / `NON_BLOCKING`. Anchored overload takes
`startTime: Instant` (KIP-1146). Locked-run: an overlapping fire is skipped.

## Timers (🆕 Flink-style)

Override `declaredTimerTypes(): Set<TimeNotion>` (registering an undeclared type throws). `context.timerService()`
→ `TimerService<K>`: `registerEventTimeTimer(key, ts)`, `registerProcessingTimeTimer(key, ts)`,
`deleteEventTimeTimer(...)`, `deleteProcessingTimeTimer(...)`, `currentWatermark()`, `currentProcessingTime()`.
Handle in `onTimer(timestamp, key, TimerContext)` — `TimerContext` gives `forward(...)`,
`getStateStore(name)` (read/write), `timerService()`, `timestamp()`, `timeDomain()`. Timers dedup by
`(key, timestamp)` and fire exactly once; persistent backend survives restart.

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
so a `getStateStore<TimestampedKeyValueStore<..>>` cast still succeeds), `currentWallClockTime()`,
`currentSystemTimeMs()`, `currentWatermarkMs()`/`currentStreamTimeMs()` (`Long.MIN_VALUE` until first event).

```kotlin
builder.scheduled<String, String>(
    interval = Duration.ofSeconds(30),
    type = PunctuationType.WALL_CLOCK_TIME,
    emitter = { ctx -> ctx.forward("heartbeat", "alive-${ctx.currentWallClockTime()}") },
).to("heartbeats", Produced.with(Serdes.String(), Serdes.String()))
```

Auto-named `scheduled-source-0`, `-1`, … `STREAM_TIME` is data-driven (won't fire without records);
cron/`WALL_CLOCK_TIME` fire independent of throughput. Emitted records participate in the exactly-once
commit boundary. Test with `driver.triggerScheduledSource(name)` / `advanceWallClockTime` /
`advanceWatermark`.
