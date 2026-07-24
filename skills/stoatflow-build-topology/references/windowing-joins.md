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

**Emit control:** `.emitStrategy(EmitStrategy.onWindowClose())` vs default `onWindowUpdate()` (on
`TimeWindowedKStream`/`SessionWindowedKStream`). `onWindowClose()` costs latency = grace period.

**Suppression:** `KTable.suppress(Suppressed.untilWindowCloses(bufferConfig))` (windowed) /
`Suppressed.untilTimeLimit(duration, bufferConfig)`. `Suppressed.BufferConfig`: `unbounded()`,
`maxRecords(n)`, `.emitEarlyWhenFull()`, `.shutDownWhenFull()`, `.withLoggingDisabled()`. **`maxBytes(...)`
throws `UnsupportedOperationException`** (StoatFlow buffers in-memory objects, not bytes) — use
`maxRecords`.

**Windowed-key serdes:** `WindowedSerdes.timeWindowedSerdeFrom(inner)` /
`sessionWindowedSerdeFrom(inner)`. StoatFlow takes a `Serde<T>` inner (KS takes `Class<T>`) and no
window-size arg. Windowed `Materialized` takes the **plain** inner key serde (StoatFlow wraps it); the
windowed serde goes on `Produced`.

## Joins

Joiners: `ValueJoiner<V1,V2,VR> = (left,right)->result`, `ValueJoinerWithKey<K,V1,V2,VR> = (k,left,right)->result`.

- **Stream-stream:** `left.join(right, joiner, JoinWindows.ofTimeDifferenceWithNoGrace(d), StreamJoined?)`
  — `join`/`leftJoin`/`outerJoin`. `JoinWindows`: `ofTimeDifferenceWithNoGrace(d)` /
  `ofTimeDifferenceAndGrace(d, grace)` / `.before(d)` / `.after(d)`. `StreamJoined`: `.as(name)`,
  `.with(keySerde, leftValueSerde, rightValueSerde)`, `.withStoreName(base)` (→ `{base}-left-store` /
  `{base}-right-store`), `.withThisStoreSupplier`/`.withOtherStoreSupplier`,
  `.withDslStoreSuppliers(StoreType.IN_MEMORY)`, `.withLoggingDisabled()`. **Unmatched left/outer results
  emit when the window closes** (watermark-driven), not on left-record arrival.
- **Stream-table:** `stream.join(table, joiner, Joined?)` / `leftJoin` — point-in-time lookup (table
  updates do **not** re-trigger); **no `outerJoin`**; key preserved; table must be materialized. `Joined`
  settings other than `.as(name)` are **advisory** (serdes resolve from upstream `Consumed`/`Grouped`).
- **Table-table:** `left.join(right, joiner, Named?, Materialized?)` / `leftJoin` / `outerJoin` — both
  materialized; re-emits on either side's update; tombstone-aware.
- **Foreign-key:** `left.join(right, foreignKeyExtractor, joiner, TableJoined.as(name), Materialized?)` —
  extractor `(V)->KO?` or `(K,V)->KO?` (null FK ⇒ record ignored); **inner + left only (no outer)**; result
  keeps the left key. Java overloads take `Function`/`BiFunction` + `ValueJoiner`. `TableJoined.as(name)` is
  its only effective setting.
- **Cogroup:** `KGroupedStream.cogroup((k,v,agg)->…)` → `CogroupedKStream`; chain `.cogroup(other){…}`;
  terminal `.aggregate(Initializer, Named?, Materialized?)`; windowable via `.windowedBy(...)` before
  `aggregate`.

```kotlin
val matched = orders.join(
    payments,
    { order, payment -> OrderPayment(order, payment) },
    JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
)
```

**StoatFlow note:** join sides do **not** need to be co-partitioned (no shared partition count) — FK
re-keying happens in-memory between lanes, not via a repartition topic.
