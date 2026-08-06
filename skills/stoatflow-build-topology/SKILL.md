---
name: stoatflow-build-topology
description: Write or modify StoatFlow stream-processing code — topologies, KStream/KTable, aggregations, windows, joins, the Processor API, serdes, state stores, DLQ/error handling, and scheduled sources. Use for StoatFlow application code; for writing tests use stoatflow-test, and for migrating existing Kafka Streams code use stoatflow-port-from-ks.
license: Apache-2.0
---

# Build a StoatFlow topology

Read `references/stoatflow-primer.md` first — the identity rules (import root `io.stoatflow.core.*`,
`kafka-clients` only, single-instance, `EXACTLY_ONCE` default) apply to every line you write.

The DSL is **Kafka Streams 4.3-shaped** under `io.stoatflow.core.*`: `StreamsBuilder`, `KStream`, `KTable`,
`GlobalKTable`, `Materialized`, `Stores`, `Consumed`/`Produced`/`Grouped`, the `Processor` API — same names,
same shapes. Write it as you'd write Kafka Streams, then apply the StoatFlow-specific rules below.

## The shape of a topology

```kotlin
// Package the topology as a builder function; the runtime calls it.
fun buildTopology(builder: StreamsBuilder) {
    builder.stream<String, String>("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
        .mapValues { v -> v.uppercase() }
        .filter { _, v -> v.length > 5 }
        .to("output-topic", Produced.with(Serdes.String(), Serdes.String()))
}

// Run it (batteries-included runtime):
StoatFlowRuntime.fromConfig(topologyBuilder = { buildTopology(it) }).start()
```

Sources: `builder.stream(topic|collection|Pattern, Consumed?)` → `KStream`; `builder.table(...)` → `KTable`
(always materialized, store `{topic}-store`); `builder.globalTable(...)` → `GlobalKTable`;
`builder.scheduled(...)` → `KStream` (🆕 no KS equivalent — see `references/processor-api.md`). Sinks:
`stream.to(topic|TopicNameExtractor, Produced?)`, `forEach`, `print`.

## The one rule that isn't Kafka Streams: fan-out by REUSING the reference

To send one source down multiple branches, **reuse the same `KStream`/`KTable` reference**. Calling
`builder.stream()` (or `table`/`globalTable`) **twice on the same topic is rejected at `build()`** with
`TopologyValidationException` ("Multiple source nodes consuming the same topic are not supported").

```kotlin
val stream = builder.stream<String, String>("input")
stream.filter { _, v -> v.isNotEmpty() }.to("output-a")   // branch 1
stream.mapValues { v -> v.length }.to("output-b")          // branch 2
```

(`scheduled()` is not topic-backed, so repeated calls are fine.)

## What each area covers (depth in references/)

- **Operators + aggregations** → `references/dsl-cheatsheet.md`: stateless `KStream` transforms
  (`map`/`mapValues`/`filter`/`flatMap`/`selectKey`/`peek`/`merge`/`branch` via `split`), `KTable`
  (`mapValues`/`filter`/`toStream`/`toTable`), grouping (`groupByKey`/`groupBy`), and folds
  (`count`/`reduce`/`aggregate`, stream and table), with the `Consumed`/`Produced`/`Grouped`/`Materialized`
  config objects.
- **Windowing + joins** → `references/windowing-joins.md`: `TimeWindows`/`SlidingWindows`/`SessionWindows`,
  `emitStrategy`, `suppress`, and stream-stream / stream-table / table-table / **foreign-key** / cogroup
  joins.
- **Processor API + scheduled sources** → `references/processor-api.md`: `process`/`processValues`,
  `Processor`/`FixedKeyProcessor`, `ProcessorContext`, punctuators, **timers** (🆕 Flink-style
  `onTimer`), and `builder.scheduled()`/`CronExpression`.
- **Serdes + state stores + DLQ** → `references/serdes-state-stores.md`: `Serdes`/`Serde`, custom + Avro,
  the `Stores` factory + `Materialized`, interactive queries, and the three exception handlers (deser /
  processing / production) + DLQ.

## StoatFlow-specific rules (get these right)

- **Default guarantee is `EXACTLY_ONCE`** — output, changelog, and offsets commit atomically on the commit
  barrier. Don't assume at-least-once behavior. (Set `at_least_once` in config if you need it.)
- **A re-key does NOT create a sub-topology by itself** (ADR-138, default
  `topology.sub-topology-split: lazy`). StoatFlow opens an in-memory repartition boundary only where a
  re-keyed record reaches an operator that needs the new key's lane affinity — a grouped aggregation, any
  join, `toTable()`, `process()`/`processValues()` — or at an explicit `repartition()`. This matches
  Kafka Streams' rule for materialising a repartition topic, with one deliberate difference: KS never
  repartitions before a Processor API node, StoatFlow does. **Consequence for advice you give:** if a user
  relies on a re-key to spread work across lanes (skewed or low-cardinality source keys feeding an
  expensive operator), tell them to insert an explicit `repartition()` — the same idiom as in KS. Genuinely
  null keys are round-robined across lanes, so they are unaffected; an empty non-null key is not.
- **Scale with lanes, not partitions/replicas** — parallelism is `stoatflow.lanes.count` (≈ CPU × 4), a
  virtual-partition count independent of the topic's Kafka partitions. Never suggest `replicas: N`.
- **Handle `null` values (tombstones)** — mappers/predicates/foreach receive `null` values on KTable
  changelog streams; StoatFlow does not filter them. A serde must round-trip `null` cleanly.
- **Aggregators must be pure** — build and return a **new** aggregate; never mutate the incoming one in
  place (it breaks changelog recovery).
- **Kotlin null-key lambdas:** over a topic that may carry null keys, declare the key param nullable
  (`map { _: K?, v -> … }`) — the compiler's `checkNotNullParameter` otherwise crashes on a non-null key
  param (this fires on Kafka Streams too). Aggregations and the stream–table INNER join already drop
  null-key records.
- **`Named` naming:** `Named.as("x")` (Kotlin: `` Named.`as`("x") ``, `as` is a soft keyword). Explicit
  naming is encouraged/validated (`stoatflow.validation.ensure-explicit-naming`).

## See also

- `references/dsl-cheatsheet.md`, `references/windowing-joins.md`, `references/processor-api.md`,
  `references/serdes-state-stores.md`, `references/stoatflow-primer.md`
- Website: <https://stoatflow.io/docs/building> · Compat matrix:
  <https://stoatflow.io/docs/reference/ks-compatibility-matrix>
