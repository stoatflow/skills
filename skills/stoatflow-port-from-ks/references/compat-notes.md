# What you do NOT change (§3) + compatibility pointers

These are source-compatible — a KS/KSML port leaves them alone. If an assistant "fixes" one of these it is
regressing working code.

- **The DSL itself** — `stream()`/`table()`/`map`/`filter`/`groupBy`/`aggregate`/`join`/… are
  source-compatible.
- **Functional-interface lambdas *and* pre-typed functional objects** — `KeyValueMapper`, `ValueJoiner`,
  `Aggregator`, `ValueMapper`, `Predicate`, `ForeachAction`, etc. match KS (ADR-047), so Java lambdas
  written for KS compile unchanged. Since KSC-38 they carry KS-matching bounded-wildcard variance, so a
  pre-typed supertype/subtype functional object compiles too. `Reducer`/`Initializer` stay invariant.
- **Windowed `Materialized`** — windowed/session/sliding aggregations take a **plain-key**
  `Materialized<K, V, *>` (KSC-36), exactly like KS (and, as in KS, its serdes win over `Grouped`'s); pass a plain `Serde<K>` — StoatFlow wraps it to
  `Serde<Windowed<K>>` internally. Do not retype the key to `Windowed<K>`.
- **Sliding windows** — `windowedBy(SlidingWindows)` returns `TimeWindowedKStream` /
  `TimeWindowedCogroupedKStream` (KSC-37). There is no `SlidingWindowedKStream` type.
- **Processor-API record headers** — inside `process()`/`processValues()`, `context.headers()` and
  `record.headers()` carry the source record's headers, and `context.forward(record)` carries the record's
  headers through to the sink, as in KS. Punctuator callbacks see empty headers (no record in scope); a 🆕 timer's
  `onTimer` sees the timer's headers — those of the record that armed it (StoatFlow extension, no KS equivalent).
- **State-store types** — `KeyValueStore`, `WindowStore`, `SessionStore`, the `ReadOnly*`/`Timestamped*`/
  versioned variants, `KeyValueIterator`/`WindowStoreIterator`, `ValueAndTimestamp`, `Windowed`,
  `VersionedRecord`, and the `Stores` factory + `*BytesStoreSupplier`/`StoreBuilder` family are all present
  and survive obfuscation in the published jar.
- **`Materialized.as(supplier).withStoreType(...)`** (KSC-100) — resolves as in KS: the explicit supplier
  wins, `withStoreType` / `withDslStoreSuppliers` fills in only where no explicit supplier is set, and no
  setter clears the other (same per side on `StreamJoined`). Before 2026-09-09 StoatFlow had this inverted
  — the store type outranked *and erased* the supplier — so a ported topology that set both silently
  changed backend. Nothing to change; both set → one WARN per store naming the ignored store type.
- **`Stores.lruMap`** (KSC-116) — same factory and store shape; like KS's `MemoryLRUCache` it has no transaction
  buffer, so its reads are always live (an interactive query at `READ_COMMITTED` reads it live, with a WARN),
  and a query `get()` changes the next eviction victim. Still correct
  under `exactly_once` (an aborted commit ends in an engine rebuild from the changelog — empty if logging is
  disabled). Nothing to change — unless a
  join reads it across lanes under `exactly_once` (a PK, FK, re-keyed stream–table or stream–GlobalKTable join), which
  is refused (KSC-126).
- **KIP-892 (KS 4.4) keys and reads** — `statestore.uncommitted.max.bytes` sets the uncommitted-state budget as given
  (default stays 256 MiB vs KS 64 MiB, application-wide, under exactly-once also counting the output held back during a
  commit; a non-positive value — KS's `-1`/disable, or `0` — is refused at startup — KSC-105);
  `default.interactive.query.isolation.level` sets the IQ level; `enable.transactional.statestores` is accepted and
  ignored (`false` WARNs, KSC-104). `readOnly(IsolationLevel)` / `approximateNumUncommittedBytes()` are the KS 4.4
  interface defaults; `READ_COMMITTED` reads the last commit barrier's state (iterators over in-memory and segmented
  window/session stores weakly consistent), an in-memory versioned store or `lruMap` reads live with a WARN (KSC-106/116),
  the byte count includes the committed epoch still flushing (KSC-117) and leaves out a custom builder's own store (no
  commit drains it), the `uncommitted-bytes` gauge covers every store family (KSC-118). Nothing to
  change — but with `READ_COMMITTED` configured, `store()` reads **global** stores committed too, where KS's IQv1 reads
  them uncommitted (KSC-144).
- **`KeyValueStore.delete()`** — the same read-then-write as in KS: it returns the previous value, so it reads
  (and deserialises) it first. Nothing to port — but when the value is not needed, write `put(key, null)`
  instead: the same tombstone without the read, in both engines (both StoatFlow's DSL and Kafka Streams
  tombstone that way). https://stoatflow.io/docs/building/state-stores#deleting-a-key
- **`Named`** is subclassable (KSML name-validation subclasses work); `Stores.*` returns the KS
  `*BytesStoreSupplier` / `StoreBuilder` types.
- **Kafka Streams' built-in timestamp extractors** — `FailOnInvalidTimestamp`, `LogAndSkipOnInvalidTimestamp`,
  `UsePartitionTimeOnInvalidTimestamp`, `WallclockTimestampExtractor` live in `io.stoatflow.core.processor` (where the
  `processor.` swap row sends them) and behave as in KS, so a ported `default.timestamp.extractor` or
  `Consumed.withTimestampExtractor(...)` keeps its meaning; every extractor receives the real partition time. A
  class-name string may keep its KS name: `org.apache.kafka.streams.processor.*` loads StoatFlow's class, so a ported
  `Properties` needs no change. Not the KS default, though: StoatFlow
  sets no default extractor and treats negative times as valid — see KSC-138 in `divergences.md`.
- **Interactive Queries v2** (`org.apache.kafka.streams.query`) — the whole package ports with the import swap
  (`io.stoatflow.core.query`), and `streams.query(request)` compiles against `StoatFlow`. Leave the query code alone;
  four behaviours differ (one store partition `0`, a per-commit `Position`, the gate and exception shapes, final window
  query classes — KSC-107…110 in `divergences.md`). The four KS 4.4 `*WithHeadersQuery` types (KIP-1356) port too,
  with `ReadOnlyRecord` (`processor.api` → `io.stoatflow.core.processor`) and `ReadOnlyRecordIterator` (`state`), KS
  semantics; `skipCache()` does nothing (KSC-111), a negative stored timestamp is returned (KSC-138), and a headers
  builder over a non-headers supplier is refused at build (KSC-145).
- **Interactive-query metadata** (KSC-47) — `StoatFlow.queryMetadataForKey`/`streamsMetadataForStore`/
  `metadataForAllStreamsClients` and the `StreamsMetadata`/`KeyQueryMetadata`/`HostInfo` types exist and
  resolve to the single local host. KS "find the host, then query" IQ code compiles **and runs** (always
  local); `partition()` is faithful (Kafka `murmur2`). The KS 4.4 `Headers` and `StreamPartitioner`
  overloads of `queryMetadataForKey` exist too (KIP-1321, KSC-103). Fix the `StreamsMetadata`/`KeyQueryMetadata`
  imports to `io.stoatflow.core.*` by hand (`HostInfo` comes via the `state.` codemod row).
- **Test-utils** (KSC-48/49/50/62) — `TopologyTestDriver`/`TestInputTopic`/`TestOutputTopic` match KS;
  `readKeyValue()`/`readKeyValuesToList()` return `io.stoatflow.core.state.KeyValue`; serdes are now
  enforced at the source/sink boundary like the real engine (a correctly-typed KS test is unaffected). See
  the `stoatflow-test` skill for the testing divergences in depth.

## Compatibility matrix

The per-method compatibility status (✅ / ⚠️ / ❌ / 🆕 StoatFlow-extension) lives at
<https://stoatflow.io/docs/reference/ks-compatibility-matrix>. Use it to confirm whether a specific DSL
method is implemented, partial, or a StoatFlow extension before assuming KS behavior. The **divergence
register** (`references/divergences.md`) explains *why* each intentional difference exists.
