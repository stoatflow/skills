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

**Store attachment:** either override `stores()` with `Stores.keyValueStoreBuilder(...)`, or
`builder.addStateStore(...)` then look it up by name. Build store builders via `Stores`
(`io.stoatflow.core.state`).

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

## Scheduled sources (🆕 no KS equivalent)

`StreamsBuilder.scheduled(...)` → `KStream<K,V>` — periodically generates records without consuming from
Kafka. Two overloads:

- Interval: `scheduled(named, interval: Duration, type: PunctuationType, keySerde?, emitter)` —
  `WALL_CLOCK_TIME` or `STREAM_TIME`; interval must be positive.
- Cron: `scheduled(named, cron: CronExpression, keySerde?, emitter)` — always wall-clock.

`CronExpression` (factories `@JvmStatic`): `CronExpression.unix(...)` (5 fields, Sun=0), `.quartz(...)`
(6–7 fields, Sun=1), `.spring(...)` (6 fields); unparseable → `IllegalArgumentException`.

`ScheduledEmitter<K,V>` receives `ScheduledEmitterContext`: `forward(key, value[, timestamp])` /
`forward(record)`, `getStateStore(name)` (**read-only** — `put`/`delete` throw), `currentWallClockTime()`,
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
