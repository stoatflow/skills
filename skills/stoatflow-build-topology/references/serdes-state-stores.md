# Serdes, state stores, interactive queries, DLQ

## Serdes

StoatFlow uses standard Kafka `org.apache.kafka.common.serialization.Serde<T>` / `Serdes` — **not** a
StoatFlow type. Resolution: explicit serde on the operator config (`Consumed`/`Produced`/`Grouped`/
`Materialized`) wins; else the runtime default; else startup fails.

- **Boundary key serdes** (StoatFlow-specific, no KS equivalent): where a re-keyed record reaches an
  operator that needs the new key's lane affinity — a grouped aggregation, any join, `toTable()`,
  `process()`/`processValues()`, or an explicit `repartition()` — the new key is serialized in-memory for
  lane-assignment hashing at that sub-topology boundary. A re-key alone does **not** open one
  (`selectKey → mapValues → to()` is a single sub-topology; ADR-138, `topology.sub-topology-split: lazy`
  is the default), so such a topology needs no boundary key serde at all. The serde resolves from any declaration for THAT key — upstream
  first (`Consumed`, a `Repartitioned` flowed through), then downstream (`Repartitioned`, `Grouped`, a
  sink's `Produced`, an aggregation's `Materialized.withKeySerde`), else the configured default. A
  declaration only counts for the key it describes: adjacent re-keys resolve to the default, a `Grouped`
  serde never answers for the key ARRIVING at its `groupBy`, and two conflicting declarations fall back
  to the default. Unresolved boundaries WARN at startup naming the `'parent' → 'child'` edge and fail on
  the first record with `LaneKeySerializationException` if keys are not raw bytes.

- Defaults in code: `streamsConfigOverrides { defaultKeySerde(...); defaultValueSerde(...) }`; or YAML
  `stoatflow.default-key-serde` / `default-value-serde` (FQ class name, no-arg ctor only). Code wins.
- Built-ins: `Serdes.String()/Long()/Integer()/Short()/Float()/Double()/ByteArray()/ByteBuffer()/Bytes()/UUID()/Void()`.
- Custom: implement `Serde<T>`, or `Serdes.serdeFrom(serializer, deserializer)`.
- **A serde must round-trip `null` cleanly** (return `null` for `null`) — tombstones depend on it.
- **Avro:** no StoatFlow serde — use Confluent `SpecificAvroSerde`. `stoatflow.schema-registry-url` in YAML
  propagates only to *default* serdes; per-operator Avro serdes must be `configure(mapOf("schema.registry.url" to url), isKey)`
  by hand. Pin Apache `kafka-clients` (Confluent pulls `-ccs`). Tests use `mock://…`.

## State stores — the `Stores` factory (`io.stoatflow.core.state`)

| Store | Persistent (RocksDB, default) | In-memory |
|---|---|---|
| Key-value | `persistentKeyValueStore(name)` | `inMemoryKeyValueStore(name)` |
| LRU | — | `lruMap(name, maxCacheSize)` |
| Window | `persistentWindowStore(name, retention, windowSize)` | `inMemoryWindowStore(...)` |
| Session | `persistentSessionStore(name, retention, inactivityGap)` | `inMemorySessionStore(...)` |
| Timestamped KV (KIP-258) | `persistentTimestampedKeyValueStore(name)` | `inMemoryTimestampedKeyValueStore(name)` |
| Timestamped window | `persistentTimestampedWindowStore(...)` | `inMemoryTimestampedWindowStore(...)` |
| Versioned KV (KIP-889) | `persistentVersionedKeyValueStore(name, historyRetention)` | `inMemoryVersionedKeyValueStore(...)` 🆕 |

Durations are `java.time.Duration`; window/session retention ≥ window size; names match `[a-zA-Z0-9._-]`.
Store builders: `Stores.keyValueStoreBuilder(supplier, keySerde, valueSerde)` (+ `windowStoreBuilder`/
`sessionStoreBuilder`/`versionedKeyValueStoreBuilder`).

**`Materialized`** (`io.stoatflow.core.topology`): `Materialized.as<K,V,StateStore>("name")`,
`Materialized.as(StoreType.ROCKS_DB)`, `Materialized.as(supplier)`, `Materialized.with(k,v)`. Options:
`.withKeySerde`/`.withValueSerde`, `.withStoreType(StoreType.IN_MEMORY | ROCKS_DB)`, `.withRetention(d)`,
`.withCachingEnabled([config])`/`.withCachingDisabled()`, `.withLoggingEnabled([topicConfig])`/
`.withLoggingDisabled()`, `.withRecordHeaders()` (KIP-1271). Store-type precedence (ADR-029): `withStoreType`
> explicit supplier > RocksDB default.

**`.withRecordHeaders()` is close to a one-way door.** Turning it *on* is free — a lazy in-place upgrade,
no pause, no rewrite. Turning it *off* again over a store that has already migrated data is a **declared
format change**: startup refuses with a `StoreFormatDowngradeException` until the operator sets
`stoatflow.state.format-downgrade: wipe-and-restore`, which deletes that store's local state and rebuilds
it from the changelog. It is refused **unconditionally** — no acknowledgement available — when the store
has no recovery source (`withLoggingDisabled()`, or the global changelog off). Only the case where nothing
was ever written in headers mode is free. And the round trip is lossy: a downgrade sheds persisted
headers, and re-enabling converts the legacy values back with *empty* headers. Kafka Streams refuses the
same downgrade; the acknowledgement key is StoatFlow's addition. See `stoatflow-operate` for the
operational side.

Store handles: `KeyValueStore<K,V>` (`get`/`put`/`delete`, atomic `compute(key, fn)` / `merge(key, value, fn)`);
read-only `ReadOnlyKeyValueStore` (`get`, `containsKey`, `all`/`reverseAll`, `range`/`reverseRange`,
`prefixScan(prefix, serializer)`, `approximateNumEntries`); `KeyValueIterator` is `Closeable`. Range bounds
are KS-exact (KIP-763): a null bound is open-ended (both null ≡ `all()`), `reverseRange` takes ascending
`(low, high)` bounds like `range`, and `from > to` (on the serialized key bytes) yields an empty iterator +
WARN — the same rules apply to the window/session key-range queries (`fetch(keyFrom, keyTo, …)`,
`findSessions(keyFrom, keyTo, …)`).

**Notes:** RocksDB is default/recommended; in-memory still writes a changelog (durable via replay) unless
`withLoggingDisabled()` (then unrecoverable after a crash). Caching only affects downstream *emissions*
(the changelog compacts to the final value per barrier); `withCachingDisabled()` emits every intermediate.
Versioned `historyRetention` is retention **and** grace (older out-of-order records rejected).

## Interactive queries

`stoatflow.store(StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.keyValueStore()))`.
`QueryableStoreTypes`: `keyValueStore()`, `windowStore()`, `sessionStore()`, `timestampedKeyValueStore()`,
`timestampedWindowStore()`. `stoatflow.storeNames()` lists stores. Single-instance ⇒ **all state is global,
no partition routing** (`StoreQueryParameters` has no `withPartition`); `enableStaleStores()` to read during
restore. `store(...)` throws unless the app is `RUNNING` (or `RESTORING`/`VALIDATING_STATE` with stale reads).
Note: `StoatFlowRuntime.fromConfig` (`:runtime`) exposes **no** public query accessor today — typed
in-process IQ needs the `:core` `StoatFlow` engine handle directly.

## Error handling & DLQ

Three handlers (config props / `streamsConfigOverrides {}` / YAML FQCN): `deserializationExceptionHandler`,
`processingExceptionHandler`, `productionExceptionHandler`. Decisions: `CONTINUE`, `FAIL`, `RETRY` (production
only). Built-ins (`io.stoatflow.core.exception`):

- Deser: `LogAndFailDeserializationExceptionHandler` (default), `LogAndContinue…`, `DeadLetterQueue…`
- Processing: `LogAndFailProcessingExceptionHandler` (default), `LogAndContinue…`, `DeadLetterQueue…`
- Production: `DefaultProductionExceptionHandler` (retry-vs-fail + optional DLQ when `dlqTopic` is set)

DLQ handlers need `dlqTopic`, so configure them in **code** (YAML FQCN needs a no-arg ctor):

```kotlin
deserializationExceptionHandler(
    DeadLetterQueueDeserializationExceptionHandler(dlqTopic = "my-app.deserialization-errors.dlq"))
productionExceptionHandler(
    DefaultProductionExceptionHandler(dlqTopic = "my-app.production-errors.dlq"))
```

Deser + processing defaults are **fail-fast** (no silent drops unless opted in). DLQ records ride the
**same transactional producer** (atomic on the barrier). Error headers use the `__stoatflow.errors.*`
namespace. For business-rule rejections, use ordinary `filterNot(...).to("…dlq")` routing, not the exception
handlers. Custom handlers implement `handle(ErrorHandlerContext, record, exception)` returning a response
(e.g. `ProcessingHandlerResponse.continueProcessing()` / `.fail()`).
