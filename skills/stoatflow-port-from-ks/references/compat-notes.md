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
  headers through to the sink, as in KS. Timer/punctuator callbacks see empty headers (no record in scope).
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
  buffer, so its reads are always live (once KIP-892 isolation-level reads land it will not honour `READ_COMMITTED`),
  and a query `get()` changes the next eviction victim. Still correct
  under `exactly_once` (an aborted commit ends in an engine rebuild from the changelog — empty if logging is
  disabled). Nothing to change — unless a
  join reads it across lanes under `exactly_once` (a PK, FK, re-keyed stream–table or stream–GlobalKTable join), which
  is refused (KSC-126).
- **`KeyValueStore.delete()`** — the same read-then-write as in KS: it returns the previous value, so it reads
  (and deserialises) it first. Nothing to port — but when the value is not needed, write `put(key, null)`
  instead: the same tombstone without the read, in both engines (both StoatFlow's DSL and Kafka Streams
  tombstone that way). https://stoatflow.io/docs/building/state-stores#deleting-a-key
- **`Named`** is subclassable (KSML name-validation subclasses work); `Stores.*` returns the KS
  `*BytesStoreSupplier` / `StoreBuilder` types.
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
