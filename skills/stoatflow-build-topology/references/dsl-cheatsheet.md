# DSL cheat sheet — operators, grouping, aggregations, config objects

All DSL types are `io.stoatflow.core.topology.*`; `KeyValue` is `io.stoatflow.core.state.KeyValue`; serdes
are Kafka `org.apache.kafka.common.serialization.Serde`/`Serdes`. Every operator takes an optional trailing
`Named`.

## Sources & sinks

- `builder.stream<K,V>(topic | Collection<String> | Pattern, Consumed?)` → `KStream`. A `Pattern` source: whole-name `matches()`; new matching topics picked up within `metadata.max.age.ms` (30 s default for pattern topologies) as an incremental assignment; several patterns are fine but a topic matched by two is rejected at assignment; a literal topic a pattern also matches is rejected at build; matching nothing = WARN + wait; **no own-topic filtering** — keep `<appId>-*-changelog`, the HA coordination topic and DLQ topics out of the pattern (ADR-143). **A matched topic being DELETED is survived** — the engine restarts in place (~0.5-1 s, no pod restart) and converges on the remaining topics; a deleted *literal* source topic is still fatal, matching KS
- `builder.table(topic, Consumed?, Materialized?)` → `KTable` (always materialized; store `{topic}-store`)
- `builder.globalTable(...)` → `GlobalKTable` (single-instance: functionally identical to `table`; KS compat; always reads from the earliest offset, whatever `Consumed` or `auto.offset.reset` say, as KS's GlobalKTable does; when its committed offset left the topic it re-reads the topic over its kept store, where KS's global thread wipes and reloads — KSC-156)
- `stream.to(topic, Produced?)` / `stream.to(TopicNameExtractor, Produced?)` (per-record routing) · `forEach` · `print`

## Stateless KStream transforms

| Operator | Signature | Key-changing? |
|---|---|---|
| `mapValues` | `(V)->VR` or `(K,V)->VR` | no (same lane) |
| `map` | `(K,V)->KeyValue<KR,VR>` | **yes** |
| `filter` / `filterNot` | `(K,V)->Boolean` | no |
| `flatMapValues` | `(V)->Iterable<VR>` | no |
| `flatMap` | `(K,V)->Iterable<KeyValue<KR,VR>>` | **yes** |
| `selectKey` | `(K,V)->KR` | **yes** |
| `peek` | `(K,V)->Unit` | no |
| `merge` | `(KStream)` (same `StreamsBuilder`) | no |
| `split` | `(Named)` → `BranchedKStream` | no |

**`merge` lanes:** when one side comes straight from a source and the other is processed first
(`orders.filter{…}.merge(returns)`), the source side runs on its **own** lanes and hands records to the merge
(exact commit boundaries under EOS): one extra in-memory hop + one extra lane set. Tune that source with
`Consumed.withNumberOfLanes(n)` if it does not need the default count. Lane declarations feeding one
sub-topology (`Consumed`/`Grouped`/`Repartitioned`) must agree, or the build fails naming both.

**Branching:** `stream.split(Named.as("router")).branch({k,v->…}, Branched.as("-high")).defaultBranch(Branched.as("-low"))`
→ `Map<String, KStream<K,V>>` keyed `router-high`, `router-low`. First-match (each record to exactly one
branch). Inline: `Branched.withFunction(s->…, "-suffix")` / `Branched.withConsumer(s->…, "-suffix")`.

Java SAM types: `ValueMapper`, `ValueMapperWithKey`, `KeyValueMapper`, `Predicate`, `ForeachAction`.

## KTable

`mapValues` (value-only + key-aware), `filter`/`filterNot`, `toStream()` / `toStream(keyMapper, Named)`
(key-changing), `KStream.toTable(Materialized?)`. Without `Materialized`, `filter`/`mapValues` are
**non-materialized derived views**; materialized `filter` writes a tombstone when an entry stops matching.

## Grouping → aggregation

- `KStream.groupByKey(Grouped?)` → `KGroupedStream` (no re-key)
- `KStream.groupBy(selector, Grouped?)` → `KGroupedStream` (key-changing; selector `(K,V)->KR`)
- `KTable.groupBy(selector)` → `KGroupedTable` (selector `(K,V)->KeyValue<KR,VR>`; any readable table — materialized,
  derived `filter`/`mapValues`/`transformValues`, suppressed, windowed; only an unmaterialized join result fails the build)

**`KGroupedStream` folds → all return `KTable`:**
- `count()` (+ `(Named)`, `(Materialized)`, `(Named, Materialized)`) → `KTable<K, Long>`
- `reduce(Reducer<V>)` (+ `Named`/`Materialized`) → `KTable<K, V>`
- `aggregate(Initializer<VR>, Aggregator<K,V,VR>)` (+ `Named`/`Materialized`) → `KTable<K, VR>`
- A reducer/aggregator returning `null` **deletes the key** (emits a tombstone; the next record starts fresh) — as in
  Kafka Streams, windowed aggregations included; a session reduce/aggregate returning `null` removes the session.
  Windowed/session: the tombstone is emitted under `onWindowUpdate()` (default); under `onWindowClose()` the removed
  window/session is simply never emitted

**`KGroupedTable` folds** (adder + subtractor for changelog correctness):
- `count()`
- `reduce(adder: Reducer<V>, subtractor: Reducer<V>)`
- `aggregate(Initializer, adder: Aggregator<K,V,VR>, subtractor: Aggregator<K,V,VR>)` — KS uses `Aggregator`
  for **both** roles; there is no separate `Subtractor` type.

Functional interfaces: `Initializer<VA> = () -> VA`, `Aggregator<K,V,VA> = (k,v,agg) -> agg`,
`Reducer<V> = (agg, v) -> agg`. **Aggregators must be pure** — return a new aggregate, never mutate.
Null (tombstone) values are ignored (don't increment `count`); a `KGroupedTable.count` that returns to zero stays
stored as `0` (KS parity) — one row per group ever counted. A decrement for a group with no count stores nothing
(tombstone forward); one for a group at `0` gives `-1` (KS) — only with a non-pure or changed `groupBy` selector.

```kotlin
val counts: KTable<String, Long> =
    words.groupBy({ _, word -> word }, Grouped.`as`("group-by-word"))
        .count(Named.`as`("count"), Materialized.`as`<String, Long, StateStore>("word-counts"))
```

## Config objects

- `Consumed.with(keySerde, valueSerde)` · `.withKeySerde`/`.withValueSerde`/`.withName`/`.withWatermarkStrategy`/`.withTimestampExtractor` (KS's built-in extractors are in `io.stoatflow.core.processor`)/`.withOffsetResetPolicy` (a partition with no committed offset, or one the topic no longer holds — KS's rule; else `auto.offset.reset`; `none` stops the app naming the partition)/`.withMaterializeFromSourceTopic(bool?)` (tables)
- `Produced.with(keySerde, valueSerde)` · `.withKeySerde`/`.withValueSerde`/`.withStreamPartitioner`/`.withName`
- `Grouped.with([name,] keySerde, valueSerde)` · `Grouped.as(name)`
- `Repartitioned.with(keySerde, valueSerde)` (for `repartition()`; multicast partitioner **throws** here)
- `Materialized` → see `references/serdes-state-stores.md`
- `AutoOffsetReset.earliest()/.latest()/.none()/.byDuration(d)` (KIP-1106)
- `StreamPartitioner<K,V>` (KIP-837): returns `Optional<Set<Integer>>` — empty = default, single = route, empty
  set = drop (counted as `dropped.records{reason=empty_partition_set}`, WARN once a minute per topic; sink and
  `repartition()`, every dispatch mode), multiple = multicast **on sinks only**. Lives in `topology/`. KS 4.4 shape (KIP-1321): StoatFlow calls
  the default `partitions(topic,key,value,headers,numPartitions)` with the record's headers; override it in a
  **class** to route by headers. The 4-arg `(topic,key,value,numPartitions)` stays the single abstract method (a
  lambda still works, never sees headers, and gets a deprecated-for-removal warning). On `repartition()`
  `numPartitions` = the lane count; route each (key, headers) to one lane consistently before a stateful operator.
