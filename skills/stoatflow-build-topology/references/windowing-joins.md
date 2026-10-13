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
`key`/`window` (Kotlin) / `getKey()`/`getWindow()` (Java). Half-open `[start, end)`.

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
`maxRecords(n)`, `.emitEarlyWhenFull()`, `.shutDownWhenFull()`, `.withLoggingDisabled()`,
`.withChangelogNumPartitions(n)` (StoatFlow extension). **`maxBytes(...)`
throws `UnsupportedOperationException`** (StoatFlow buffers in-memory objects, not bytes) — use
`maxRecords`. The buffer's changelog **follows the global `changelog.num-partitions` by default**, like
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

## Joins

Joiners: `ValueJoiner<V1,V2,VR> = (left,right)->result`, `ValueJoinerWithKey<K,V1,V2,VR> = (k,left,right)->result`;
GlobalKTable joins only: `ValueJoinerWithStreamAndMappedKey<K,GK,V,GV,VR> = (streamKey,mappedKey,left,right)->result`.

- **Stream-stream:** `left.join(right, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(d), StreamJoined?)`
  — `join`/`leftJoin`/`outerJoin`. Self-joins work (`s.join(s, …)`, `s.join(s.mapValues{…}, …)`): every
  record is matched as left and right, as in KS. `JoinWindows`: `ofTimeDifferenceWithNoGrace(d)` /
  `ofTimeDifferenceAndGrace(d, grace)` / `.before(d)` / `.after(d)`. `StreamJoined`: `.as(name)`,
  `.with(keySerde, leftValueSerde, rightValueSerde)`, `.withStoreName(base)` (→ `{base}-left-store` /
  `{base}-right-store`), `.withChangelogNumPartitions(n)` (StoatFlow extension — **multiplied**: it sets
  the count on both buffers, plus `-outer-store` on LEFT/OUTER, so `(6)` is 12 or 18 partitions),
  `.withThisStoreSupplier`/`.withOtherStoreSupplier` (must retain duplicates —
  `Stores.persistentWindowStore(..., retainDuplicates = true)` — or the build throws `KSC-99`),
  `.withDslStoreSuppliers(StoreType.IN_MEMORY)` (both stores retain duplicates), `.withLoggingDisabled()`. **Unmatched left/outer results
  emit when the window closes** (watermark-driven), not on left-record arrival. A record whose window has **already
  closed** when it arrives (`ts + after` for a left record / `ts + before` for a right one, `+ grace`, at or behind the
  watermark) is never buffered: if unmatched, its null-side result is emitted at once under its own timestamp — Kafka
  Streams' late branch; only the clock differs (watermark vs KS stream time).
- **Stream-table:** `stream.join(table, joiner, Joined?)` / `leftJoin` — point-in-time lookup (table
  updates do **not** re-trigger); **no `outerJoin`**; key preserved; table must be materialized. With no re-key it
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
- **Table-table:** `left.join(right, joiner, Named?, Materialized?)` / `leftJoin` / `outerJoin` — both
  materialized; re-emits on either side's update; tombstone-aware.
- **Foreign-key:** `left.join(right, foreignKeyExtractor, joiner, TableJoined.as(name), Materialized?)` —
  extractor `(V)->KO?` or `(K,V)->KO?` (null FK ⇒ inner: no result, left: `joiner(left, null)`, KIP-962); a
  delete / FK change retracts with `(key, null)` materialized or not, as in KS (possibly for a key that never joined);
  **inner + left only (no outer)**; result
  keeps the left key. Java overloads take `Function`/`BiFunction` + `ValueJoiner`. `TableJoined.as(name)` is
  its only effective setting.
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
