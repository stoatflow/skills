# DSL cheat sheet — operators, grouping, aggregations, config objects

All DSL types are `io.stoatflow.core.topology.*`; `KeyValue` is `io.stoatflow.core.state.KeyValue`; serdes
are Kafka `org.apache.kafka.common.serialization.Serde`/`Serdes`. Every operator takes an optional trailing
`Named`.

## Sources & sinks

- `builder.stream<K,V>(topic | Collection<String> | Pattern, Consumed?)` → `KStream`
- `builder.table(topic, Consumed?, Materialized?)` → `KTable` (always materialized; store `{topic}-store`)
- `builder.globalTable(...)` → `GlobalKTable` (single-instance: functionally identical to `table`; KS compat)
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
- `KTable.groupBy(selector)` → `KGroupedTable` (selector `(K,V)->KeyValue<KR,VR>`; **source table must be
  materialized** or build fails)

**`KGroupedStream` folds → all return `KTable`:**
- `count()` (+ `(Named)`, `(Materialized)`, `(Named, Materialized)`) → `KTable<K, Long>`
- `reduce(Reducer<V>)` (+ `Named`/`Materialized`) → `KTable<K, V>`
- `aggregate(Initializer<VR>, Aggregator<K,V,VR>)` (+ `Named`/`Materialized`) → `KTable<K, VR>`

**`KGroupedTable` folds** (adder + subtractor for changelog correctness):
- `count()`
- `reduce(adder: Reducer<V>, subtractor: Reducer<V>)`
- `aggregate(Initializer, adder: Aggregator<K,V,VR>, subtractor: Aggregator<K,V,VR>)` — KS uses `Aggregator`
  for **both** roles; there is no separate `Subtractor` type.

Functional interfaces: `Initializer<VA> = () -> VA`, `Aggregator<K,V,VA> = (k,v,agg) -> agg`,
`Reducer<V> = (agg, v) -> agg`. **Aggregators must be pure** — return a new aggregate, never mutate.
Null (tombstone) values are ignored (don't increment `count`); `KGroupedTable.count` removes a key when its
count reaches zero.

```kotlin
val counts: KTable<String, Long> =
    words.groupBy({ _, word -> word }, Grouped.`as`("group-by-word"))
        .count(Named.`as`("count"), Materialized.`as`<String, Long, StateStore>("word-counts"))
```

## Config objects

- `Consumed.with(keySerde, valueSerde)` · `.withKeySerde`/`.withValueSerde`/`.withName`/`.withWatermarkStrategy`/`.withOffsetResetPolicy`/`.withMaterializeFromSourceTopic(bool?)` (tables)
- `Produced.with(keySerde, valueSerde)` · `.withKeySerde`/`.withValueSerde`/`.withStreamPartitioner`/`.withName`
- `Grouped.with([name,] keySerde, valueSerde)` · `Grouped.as(name)`
- `Repartitioned.with(keySerde, valueSerde)` (for `repartition()`; multicast partitioner **throws** here)
- `Materialized` → see `references/serdes-state-stores.md`
- `AutoOffsetReset.earliest()/.latest()/.none()/.byDuration(d)` (KIP-1106)
- `StreamPartitioner<K,V>` (KIP-837): `(topic,key,value,numPartitions) -> Optional<Set<Integer>>` — empty =
  default, single = route, empty set = drop, multiple = multicast **on sinks only**. Lives in `topology/`.
