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
  `Materialized<K, V, *>` (KSC-36), exactly like KS; pass a plain `Serde<K>` — StoatFlow wraps it to
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
- **`Named`** is subclassable (KSML name-validation subclasses work); `Stores.*` returns the KS
  `*BytesStoreSupplier` / `StoreBuilder` types.
- **Interactive-query metadata** (KSC-47) — `StoatFlow.queryMetadataForKey`/`streamsMetadataForStore`/
  `metadataForAllStreamsClients` and the `StreamsMetadata`/`KeyQueryMetadata`/`HostInfo` types exist and
  resolve to the single local host. KS "find the host, then query" IQ code compiles **and runs** (always
  local); `partition()` is faithful (Kafka `murmur2`). Fix the `StreamsMetadata`/`KeyQueryMetadata` imports
  to `io.stoatflow.core.*` by hand (`HostInfo` comes via the `state.` codemod row).
- **Test-utils** (KSC-48/49/50/62) — `TopologyTestDriver`/`TestInputTopic`/`TestOutputTopic` match KS;
  `readKeyValue()`/`readKeyValuesToList()` return `io.stoatflow.core.state.KeyValue`; serdes are now
  enforced at the source/sink boundary like the real engine (a correctly-typed KS test is unaffected). See
  the `stoatflow-test` skill for the testing divergences in depth.

## Compatibility matrix

The per-method compatibility status (✅ / ⚠️ / ❌ / 🆕 StoatFlow-extension) lives at
<https://stoatflow.io/docs/reference/ks-compatibility-matrix>. Use it to confirm whether a specific DSL
method is implemented, partial, or a StoatFlow extension before assuming KS behavior. The **divergence
register** (`references/divergences.md`) explains *why* each intentional difference exists.
