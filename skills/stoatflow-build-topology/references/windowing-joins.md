# Windowing & joins

All types `io.stoatflow.core.topology.*`. Windows close on the **event-time watermark**, not wall clock.

## Windowing

On `KGroupedStream`: `.windowedBy(TimeWindows|SlidingWindows)` → `TimeWindowedKStream<K,V>`;
`.windowedBy(SessionWindows)` → `SessionWindowedKStream<K,V>`. Result: `KTable<Windowed<K>, V>`.

| Window | Factory |
|---|---|
| Tumbling | `TimeWindows.ofSizeWithNoGrace(size)` / `ofSizeAndGrace(size, grace)` |
| Hopping | `TimeWindows.ofSizeWithNoGrace(size).advanceBy(advance)` (advance ≤ size) |
| Sliding (KIP-450) | `SlidingWindows.ofTimeDifferenceWithNoGrace(diff)` / `ofTimeDifferenceAndGrace(diff, grace)` → `TimeWindowedKStream` |
| Session | `SessionWindows.ofInactivityGapWithNoGrace(gap)` / `ofInactivityGapAndGrace(gap, grace)` |

`Windowed<K>`: `windowStart()`/`windowEnd()` (millis), `windowStartTime()`/`windowEndTime()` (`Instant`),
`key`/`window` (Kotlin) / `getKey()`/`getWindow()` (Java). Bounds by window kind, as in Kafka Streams: tumbling and
hopping `[start, end)`; sliding `[start, start + D]` (`windowEnd()` = `start + D`, a record's left window ends at the
record; it closes at `windowEnd + 1 + grace`); session `[start, end]` (its first and last record's times).
`window.contains(t)` (StoatFlow's) follows them — a sliding window's too, wherever StoatFlow builds it (output, store,
IQ, a `suppress` behind it); one rebuilt from serialized bytes reads its end as excluded. `window.overlap(other)` is
KS's: half-open for every `TimeWindow`, sliding included. A `suppress(untilWindowCloses)` behind a sliding aggregation
(or a `merge` including one) releases a window when the aggregation closes it, one millisecond after KS's (KSC-164).

**Any event time but `Long.MIN_VALUE`, negative included — windows near 0 differ from Kafka Streams.** Windows are
floor-aligned for every time, with no clamp at 0: a record joins every window that contains it, including windows that
start before 0. Tumbling and hopping windows differ from KS for a time below `size − advance`: a record at 5 s in 60 s
windows advancing by 10 s joins six windows (starting −50 s … 0), where KS gives one, `[0, 60 s)`. A sliding record at
`0 ≤ t < D` gets its own left window `[t − D, t]`, where KS combines those records into one `[0, D]` window. Results
above those times are identical. Joins probe across 0 and emit LEFT/OUTER null sides across 0 as at any other time;
`OnWindowClose` emits windows and sessions that end below 0. A Kafka Streams consumer of the windowed output fails on a
negative window start (its `TimeWindowedDeserializer` builds a `Window`, which rejects one). At the other end, no window
holds `Long.MAX_VALUE`: a record within one window size of it (a sliding record at exactly `MAX`) joins no window and
is dropped as late, `stoatflow.dropped.records.total{reason=late_window}` (KS fails on it — its window end wraps).

Session `aggregate(...)` **requires an explicit session merger** `(key, agg1, agg2) -> merged` as a third
function; `count()`/`reduce()` supply it implicitly.

```kotlin
val tumbling: KTable<Windowed<String>, Long> =
    pageViews.groupByKey(Grouped.with(Serdes.String(), pageViewSerde))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
        .count()
```

**Late records:** a window closes when the **watermark** passes its end + grace (session: last event + gap + grace);
a later record for it is dropped, never added — counted in `stoatflow.dropped.records.total{reason=late_window|late_session}`;
a record within the gap of a closed session starts a new session instead of re-opening it. A final result carries its
window's event time (max record time; a session's end), so it can be windowed again; across a re-key that downstream
window closes ~2 commit intervals later per step, and must close no earlier than the upstream window (end + grace):
into larger aligned windows the upstream grace suffices; into smaller or hopping windows add the size difference
(1 h → 1 min: upstream grace + 59 min), because a final can carry a time early in its window.
`suppress(untilWindowCloses)` inherits the upstream grace through the graph (refused with no windowed upstream).
A partition whose next record is stamped far in the future waits behind older records but, with the built-in
strategies, counts towards the watermark as if that record had been processed — so it does not hold event time back; a
record behind it that is in order is never late, one far out of order is (as once the future record is processed).
The watermark trails the newest event time by the source's max out-of-orderness (10 s default) and survives restarts,
so a closed window stays closed; a graceful close emits every window the final watermark closes, in every step of a
chain. Kafka Streams judges lateness per aggregation instead (KSC-132): out-of-order or filtered input can count
different records. A topology with a windowed aggregation, `suppress` or stream-stream join is refused at startup if any
source uses `WatermarkStrategy.noWatermarks()` (it pins the watermark for the whole app) or event-time tracking is off.

**Emit control:** `.emitStrategy(EmitStrategy.onWindowClose())` vs default `onWindowUpdate()` (on
`TimeWindowedKStream`/`SessionWindowedKStream`). `onWindowClose()` costs latency = grace period.

**Suppression:** `KTable.suppress(Suppressed.untilWindowCloses(bufferConfig))` (windowed) /
`Suppressed.untilTimeLimit(duration, bufferConfig)`. `Suppressed.BufferConfig`: `unbounded()`,
`maxRecords(n)`, `maxBytes(n)` / `.withMaxBytes(n)` (serialized size, headers included — KS estimates memory),
`.emitEarlyWhenFull()`, `.shutDownWhenFull()`, `.withLoggingDisabled()`, `.withChangelogNumPartitions(n)` (StoatFlow
extension). The bound covers the whole application, not one task (KSC-154: size a KS bound × the input partitions, at
least one record per writer). Each `suppress()` has its own buffer; an eager one gives each writer an even share of the
bound (bound ÷ the writer count, rounded down — a record larger than a writer's byte share leaves at once). The writers
are the lanes of its sub-topology; only a buffer no lane writes (behind an `OnWindowClose` aggregation) has one writer,
its punctuation lane, with the whole bound. Every buffered key belongs to the lane it runs on, and **only that lane
takes it out** — time-limit releases and evictions alike (KSC-154, amended): a writer over its share emits its own
oldest keys (earliest buffer time, then key bytes); with skewed keys a busy lane emits early at its share while other
shares sit unused; a key's values leave in buffered order and every writer keeps suppressing. After a restart or an HA
promotion — a different lane count included — each restored key belongs to the lane it now runs on, which evicts its
oldest within its share on its next write. A key with no lane (a sub-topology whose lanes a `repartition()` partitioner
picks, or one a punctuator's forward first buffered) is taken out at the next commit barrier while the lanes wait. A
`processor.wrapper.class` changes no writer count (KSC-161). Over a bound after a put
(`records > maxRecords || bytes > maxBytes`, KS's check) a strict one throws `BufferFullException` to the processing
exception handler (the default shuts down). Every emission carries the latest buffered update's **headers, timestamp and source coordinates** (`topic()` / `partition()` / `offset()` downstream, as in KS) and is never `isLate()`;
records released together leave in (buffer time, key) order **per key and per lane** — keys on different lanes have
no fixed order relative to each other (each key is emitted by its own lane; KS orders them within a task), and a buffer
only the punctuation lane writes leaves wholly in that order; `untilWindowCloses` drops a `null` final result (KS).
**`untilTimeLimit` (KS semantics):** every update is buffered — the first too — and a key leaves `duration` after its
**first** buffered update with its latest value, once the watermark reaches that update's timestamp + `duration`; it is
measured on the **watermark**, not KS's task stream time (KSC-119: `Duration.ZERO` emits on the next watermark
advance). A join or `groupBy` reads an `untilWindowCloses` / `untilTimeLimit` table through its value getter — a
buffered key reads its value from before it was buffered — and gets that value as the old value of each emission
(KSC-127: KS uses the latest update's); its `queryableStoreName()` is `null`. An `untilBarrier()` table keeps its
source's store and reads the source's current value. The buffer's changelog **follows the global `changelog.num-partitions` by default**, like
every other store — no surface carries its own default — so set
`.withChangelogNumPartitions(n)` only on a measurement. See `references/serdes-state-stores.md`.
`untilWindowCloses` needs a windowed ancestor — on a non-windowed table it throws `TopologyValidationException`
("Window close time is only defined for windowed computations", KS parity). The close time (size + grace; session:
gap + grace) is found through any intermediate operators (`filter`, `mapValues`, `toStream()`/`toTable()`…); across
a `merge` the largest grace wins.

**Windowed-key serdes:** `WindowedSerdes.timeWindowedSerdeFrom(inner)` /
`sessionWindowedSerdeFrom(inner)`. StoatFlow takes a `Serde<T>` inner (KS takes `Class<T>`) and no
window-size arg. Windowed `Materialized` takes the **plain** inner key serde (StoatFlow wraps it); the
windowed serde goes on `Produced`. To window an already-windowed key again, declare its serde on the grouping —
`groupByKey(Grouped.with(WindowedSerdes.timeWindowedSerdeFrom(inner), valueSerde))` — or on the aggregation's
`Materialized` (`count(Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(inner), Serdes.Long()))`); with neither,
the nested aggregation is rejected at build time (`TopologyValidationException`). `groupByKey(Grouped)` with no
serdes (e.g. `Grouped.as(name)`) keeps the stream's declared key and value serdes, like `groupByKey()` (KS: the
defaults). A stream-stream join without `StreamJoined` serdes stores each side with its stream's value serde and the key
both sides agree on (KS: the defaults).
A windowed aggregation keys (and values) by `Materialized`'s serde first, then `Grouped`'s / the stream's, then the
default — KS parity. **Merging windowed streams** never throws at the `merge`: both sides carry the same key type, so
an undeclared side takes the declared one, equal serdes carry through, and a full `timeWindowedSerdeFrom(inner)` counts
as the windowed key over `inner`; only two DIFFERENT declared serdes (or window kinds) leave the merged key undeclared —
then declare the windowed key serde downstream (sink `Produced`, `Repartitioned`, `Grouped`, `Materialized`); a
windowed key reaching an undeclared site fails the build naming the site.

**Windows and a timer backlog:** with event-time timers (any timer backend), the watermark waits while a
backlog of more due timers than `timer-backend.max-cached-timers` drains — windows close (and finals emit) only after
the timers before them fired, never before; the records that would move event time on wait in their buffers, so the
verdicts are unchanged. With a custom `WatermarkGenerator` only the watermark waits and late-drop decisions can differ
from an all-in-memory run. Nothing expires during a hold (window/session segments stay on disk).

## Watermark alignment (🚀 Flink FLIP-217, not in Kafka Streams)

`WatermarkStrategy.withWatermarkAlignment(group, maxAllowedWatermarkDrift[, updateInterval])` — Flink's signature —
on each source of an alignment group. Semantics (not Flink's fetch pause):

- A partition more than the drift ahead of a **lagging** partition of its group (one that delivered records and is
  not idle) is **held**. It keeps being fetched; its records go only in timestamp order (records are never reordered).
- When the held partition's next record is the oldest buffered one and a lagging partition has **nothing buffered**,
  the **whole application waits** — every source, every group — until that partition delivers or is declared idle.
  When every lagging partition has data, the record is dispatched (it *yields*).
- Drift contract: the drift bound holds against lagging partitions with nothing buffered; against ones with data the
  guarantee is timestamp order. Use **one out-of-orderness bound across the group** — differing bounds keep the
  smaller-bound source held for good (build-time warning `alignment-mixed-watermark-bounds`).
- **Sizing rule** for a sparse source with mean gap `G`: `maxAllowedWatermarkDrift ≥ G + b(sparse) − b(held)`, or the
  application stops for most of each gap; a shorter `withIdleness(...)` on the sparse source bounds each wait instead,
  at the risk of its later records being late.
- **Idleness is required**: an aligned source with no idleness timeout (global `watermark.idleness-timeout-ms: 0` and
  no `withIdleness`, a custom `idlenessTimeout()` of zero, `withIdleness(FOREVER)`) fails the build —
  `alignment-idleness-off`, `error` by default (`topology.validation.alignment-idleness-off: warn|off` to override).
- `updateInterval` (1 s default; the smallest across sources wins) bounds how far a newly-ahead partition overshoots.
- The `TopologyTestDriver` does not model alignment; the two rules do run there (compile time).

```kotlin
val aligned = WatermarkStrategy.forBoundedOutOfOrderness<String, Order>(Duration.ofSeconds(5))
    .withIdleness(Duration.ofMinutes(1))
    .withWatermarkAlignment("orders-and-payments", Duration.ofSeconds(30))
```

## Joins

Joiners: `ValueJoiner<V1,V2,VR> = (left,right)->result`, `ValueJoinerWithKey<K,V1,V2,VR> = (k,left,right)->result`;
GlobalKTable joins only: `ValueJoinerWithStreamAndMappedKey<K,GK,V,GV,VR> = (streamKey,mappedKey,left,right)->result`.

- **Stream-stream:** `left.join(right, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(d), StreamJoined?)`
  — `join`/`leftJoin`/`outerJoin`. Self-joins work (`s.join(s, …)`, `s.join(s.mapValues{…}, …)`): every
  record is matched as left and right, as in KS. `JoinWindows`: `ofTimeDifferenceWithNoGrace(d)` /
  `ofTimeDifferenceAndGrace(d, grace)` / `.before(d)` / `.after(d)`. `StreamJoined`: `.as(storeName)` (the
  stores' base name, as KS — not the processor's), `.withName(name)` (the processor),
  `.with(keySerde, leftValueSerde, rightValueSerde)`, `.withStoreName(base)` (→ `{base}-left-store` /
  `{base}-right-store`), `.withChangelogNumPartitions(n)` (StoatFlow extension — **multiplied**: it sets
  the count on both buffers, plus `-outer-store` on LEFT/OUTER, so `(6)` is 12 or 18 partitions),
  `.withThisStoreSupplier`/`.withOtherStoreSupplier` (must retain duplicates —
  `Stores.persistentWindowStore(..., retainDuplicates = true)` — and keep records for at least before + after + grace,
  or the build throws `KSC-99`, as Kafka Streams' does; a timestamped or headers-aware `Stores` supplier is accepted too and builds the same plain buffer, since the
  join keeps its serializers' headers in the stored values; a hand-written supplier whose builder is timestamped,
  headers-aware or versioned is refused),
  `.withDslStoreSuppliers(Materialized.StoreType.IN_MEMORY)` (both stores retain duplicates; a left/outer join's outer store takes its backend too), `.withLoggingDisabled()`. **Unmatched left/outer results
  emit when the window closes** (watermark-driven), not on left-record arrival — **on the key's own lane**, so an
  aggregation behind the join never sees one key on two threads. Their order relative to other keys' results is not
  fixed, and a null side may follow a later match of the same key with another partner (KS, which throttles these
  emissions, can do the same); a late match against the buffered record itself never also yields its null side. A record whose window has **already
  closed** when it arrives (`ts + after` for a left record / `ts + before` for a right one, `+ grace`, at or behind the
  watermark) is never buffered: if unmatched, its null-side result is emitted at once under its own timestamp — Kafka
  Streams' late branch; only the clock differs (watermark vs KS stream time).
- **Stream-table:** `stream.join(table, joiner, Joined?)` / `leftJoin` — point-in-time lookup (table
  updates do **not** re-trigger); **no `outerJoin`**; key preserved; any readable table (below). With no re-key it
  reads the table on its own lane; after a re-key it reads it from another lane (see *Exactly-once reads* below). `Joined`
  settings other than `.as(name)` are **advisory** (serdes resolve from upstream `Consumed`/`Grouped`).
- **Stream-GlobalKTable:** `stream.join(globalTable, keySelector, joiner, Named?)` / `leftJoin` — the
  `keySelector` `(k, v) -> lookupKey` derives the table key; `null` = no lookup (inner drops, left calls the
  joiner with a null table value); output keeps the stream key. **Prefer
  `ValueJoinerWithStreamAndMappedKey`** (KIP-1340, KS 4.4) when the result needs a key — Kotlin:
  `stream.join(gt, { _, v -> v.ref }) { streamKey, mappedKey, v, gv -> ... }` (name it with
  ``stream.join(gt, { _, v -> v.ref }, Named.`as`("x")) { ... }``). A Kotlin left-join joiner types the mapped key and
  table value nullable. Null-keyed stream records are joined too (by their mapped key): if the topic can carry null
  keys, type the stream `stream<String?, V>` so the joiner's stream key is nullable. The `ValueJoinerWithKey`
  global overloads are **deprecated** (as in KS 4.4): their key is the *stream* key, not the lookup key. Under
  `exactly_once` the global table is read as of the stream record's commit — more consistent than KS (its global thread
  has no cut against the stream tasks); same API.
- **Table-table:** `left.join(right, joiner, Named?, Materialized?)` / `leftJoin` / `outerJoin` — re-emits on either
  side's update; tombstone-aware. Every join reads a table through its **value getter**, as KS does: a source or
  materialized table, a `filter` / `mapValues` / `transformValues` without `Materialized` (KSC-143), a suppressed table,
  a windowed aggregation (KSC-142). Only a join result built without `Materialized`, and a table derived from one,
  cannot be read — materialize it. A `transformValues` table read this way runs `transform()` on every lookup, in a
  second transformer instance — one per reading thread, created and initialised once and reused — whose context
  reports the looked-up key and value timestamp; a `schedule()` in its `init()` is ignored (WARN), and `transform()`
  should not write stores, since every lookup writes again.
- **Foreign-key:** `left.join(right, foreignKeyExtractor, joiner, TableJoined.as(name), Materialized?)` —
  extractor `(V)->KO?` or `(K,V)->KO?` (null FK ⇒ inner: no result, left: `joiner(left, null)`, KIP-962); a
  delete / FK change retracts with `(key, null)` materialized or not, as in KS (possibly for a key that never joined);
  **inner + left only (no outer)**; result
  keeps the left key. Java overloads take `Function`/`BiFunction` + `ValueJoiner`. `TableJoined.as(name)` is
  its only effective setting. Keys match by their **serialized bytes** under the right table's key serde, as in KS:
  the FK is filed after the round trip KS's subscription topic gives it (`ser(deser(ser(fk)))`), so the extractor
  may return any class that serde serializes (a wrapper, a `byte[]`, a record of another schema version), and the
  right table — a `filter`/`mapValues` over it included — is read with the FK as that serde deserializes it. An FK
  the serde writes no bytes for (a null object, KSML's `DataNull`), or whose bytes it reads back as `null`, is no FK, like `null`; records are counted as
  dropped (`stoatflow.dropped.records.total{reason=join_null_key}`) wherever KS counts them in `dropped-records`
  (either join type, a right record with no key). A right key with no bytes fails the record, as in KS. One left
  row's failing extractor, serde or joiner in a right-side update goes to the processing handler after the other
  rows' results, never instead of them (a fatal error or a `FAIL` verdict stops at once); a left update that failed
  before its subscription was written is caught up by a later update of the row — the first whose old and new values
  extract and serialize (KS files the subscription on every update).
- **Cogroup:** `KGroupedStream.cogroup((k,v,agg)->…)` → `CogroupedKStream`; chain `.cogroup(other){…}`;
  terminal `.aggregate(Initializer, Named?, Materialized?)`; windowable via `.windowedBy(...)` before
  `aggregate`. Legs may share a source (each record folds through every leg's aggregator); cogrouping the
  **same** `KGroupedStream` again **replaces** its aggregator (KS parity), it does not add a second one. Key serde
  (plain, windowed, session): `Materialized` key serde, else the key serde the legs agree on (their `Grouped` /
  upstream declarations), else `default.key.serde` (KS reads only the `Materialized`, then the default).

**Exactly-once reads (`KSC-126`):** under `exactly_once` (the default) a join reads the other table **as of its own
lane's commit**, never another lane's uncommitted next-commit value; under `at_least_once` it reads the latest value.
That needs a store that can answer as of a commit: the default stores and `Stores.persistentVersionedKeyValueStore` can;
`Stores.lruMap` and `Stores.inMemoryVersionedKeyValueStore` cannot. Such a table, read by a join **from another lane**,
is refused at build (`TopologyValidationException` naming the join and the store; in `TopologyTestDriver` too): either
side of a table-table (PK) or FK join, a stream-table join after a re-key, a stream-GlobalKTable join, a join reading
another join's result table. A stream-table join with no re-key is not refused; nor is a join's own result store until
another join reads it. Fix: a default store supplier or `persistentVersionedKeyValueStore`, or `at_least_once`. A custom
persistent versioned `StateStoreBuilder` must override `toInMemoryBuilder()` (the default — itself — reads as
in-memory). KS accepts all of these.

```kotlin
val matched = orders.join(
    payments,
    { order, payment -> OrderPayment(order, payment) },
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
)
```

**StoatFlow note:** join sides do **not** need to be co-partitioned (no shared partition count) — FK
re-keying happens in-memory between lanes, not via a repartition topic.
