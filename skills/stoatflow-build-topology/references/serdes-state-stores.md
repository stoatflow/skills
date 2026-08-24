# Serdes, state stores, interactive queries, DLQ

## Serdes

StoatFlow uses standard Kafka `org.apache.kafka.common.serialization.Serde<T>` / `Serdes` — **not** a
StoatFlow type. Resolution: explicit serde on the operator config (`Consumed`/`Produced`/`Grouped`/
`Materialized`) wins; else the runtime default; else startup fails.

- **Boundary key serdes** (StoatFlow-specific, no KS equivalent): where a re-keyed record reaches an
  operator that needs the new key's lane affinity — a grouped aggregation, any join, `toTable()`, or an
  explicit `repartition()` — the new key is serialized in-memory for lane-assignment hashing at that
  sub-topology boundary. A re-key alone does **not** open one (`selectKey → mapValues → to()` is a single
  sub-topology; ADR-138, `topology.sub-topology-split: lazy` is the default), and since 1.0.0 neither
  does `process()`/`processValues()` (`topology.processor-api-key-affinity: off`, matching Kafka
  Streams) — so such a topology needs no boundary key serde at all. The serde resolves from any declaration for THAT key — upstream
  first (`Consumed`, a `Repartitioned` flowed through), then downstream (`Repartitioned`, `Grouped`, a
  sink's `Produced`, an aggregation's `Materialized.withKeySerde`). A declaration only counts for the key
  it describes: adjacent re-keys find nothing, a `Grouped` serde never answers for the key ARRIVING at
  its `groupBy`, and two conflicting declarations are declined rather than guessed between.
  **Last resort:** when that finds nothing AND the user never configured `default.key.serde`, the
  boundary inherits the nearest declaration from *before* the re-key. Sound because these bytes are
  lane-hash-only — never stored, never sent to Kafka, never deserialized back (which is exactly why the
  same leniency is NOT applied to keyed timers, whose key bytes ARE persisted and read back). It works
  when the re-key preserves the declared key type; when the type changes, the boundary still fails on the
  first record with `LaneKeySerializationException`, naming the inherited serde.
  **Advice to give:** a user who genuinely keys a boundary with `byte[]` should set `default.key.serde`
  to `Serdes.ByteArray()` **explicitly** — that closes inheritance. StoatFlow cannot tell a deliberate
  byte-array key from an unconfigured default, so "was it configured at all" is the signal.
  Every unresolved-or-inherited boundary WARNs naming the `'parent' → 'child'` edge, raised at
  **topology-compile** time — so the WARN fires under `TopologyTestDriver` too, with no broker. Promote
  either to a build failure with `topology.validation.unresolved-boundary-key-serde` /
  `topology.validation.inherited-boundary-key-serde` = `error` (default `warn`); all three severities are
  decided at that same point, and `unresolved-…` reports nothing at ANY severity once a default key serde
  is configured (the default is then the answer).

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

### Multi-key atomicity: `KeyLockManager`, and the one thing it cannot do

`io.stoatflow.core.state.KeyLockManager` is a striped lock for the rare case where one record must
update **several keys** atomically — e.g. a transfer debiting one account and crediting another, whose
keys live on different lanes. Construct one per processor class (`KeyLockManager()`, or
`KeyLockManager(stripes, timeout)`) and call `withLock(key) { … }` / `withLocks(a, b) { … }`. Nesting
`withLock` throws; `withLocks` takes at most 8 keys and orders them internally, so it cannot deadlock
against itself. From Java the vararg form compiles to an array:
`locks.withLocks(new Object[]{ a, b }, () -> { …; return null; })`.

**You almost never need it.** Records with the same key run on the same lane in arrival order, so
per-key read-modify-write is already serial, and `compute(key, fn)` / `merge(key, value, fn)` are atomic
for a single key.

**The boundary of what any lock can do:** `compute()`/`merge()` are atomic **within a commit epoch**.
Across a barrier rotation neither `compute()` nor a `KeyLockManager` lock is sufficient — a lane still
draining epoch N is structurally forbidden from reading a value another lane wrote into epoch N+1 (that
would import uncommitted state into a committed transaction), so the two writes can derive from the same
base and one is durably lost. **A lock cannot create the safe ordering, only make it deterministic when
it already exists.** It is unreachable while the key you *store under* is the key the record is *laned
by*. Two shapes break that:

1. A many-to-one re-key into a Processor API node under `topology.processor-api-key-affinity: off`
   (the default). Remedy: `repartition()` or `presumed`, never a lock. The provable case — a proven
   re-key into a node with a **writable** store — fails the build (`papi-key-affinity-presumed`,
   `error`); a Processor-API feeder, whose re-key is only presumed, warns
   (`papi-key-affinity-presumed-chain`, `warn`). See `references/processor-api.md`.
2. **Writing a store key that is not the record key** — which is exactly what `withLocks` is for
   (the transfer above; or many `orderId`s updating one `customerId` aggregate). Computed inside the
   processor at runtime, so no compile-time check can see it. The lock still does real work *within*
   an epoch; it simply does not extend across a rotation.

When advising on `withLocks`, say both halves: it serialises the keys within an epoch, and the
rotation boundary is outside its reach. Do not claim locks make cross-key updates unconditionally safe.

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
**same transactional producer** (atomic on the barrier) — and only a `CONTINUE` verdict writes them:
a **`FAIL` aborts the failing epoch** (offsets held, outputs discarded) and writes **no** dead letter
on the processing/sync-production paths, because the record is preserved and replays on operator
restart. A deserialization `FAIL` keeps the graceful stop instead — the failed record never enters the
offset tracker, so the shutdown commits the pre-poison outputs and stops *before* the poison — and it
writes no dead letter either. The processing handler renders its verdict **once** per failure (a FAIL
is not re-offered at ancestor DAG levels). Error headers use the `__stoatflow.errors.*` namespace.

**A size-rejected dead letter carries no value.** When the broker refuses an output record with
`RecordTooLargeException`, the dead letter is emitted with `value = null` plus
`__stoatflow.errors.value.omitted` / `.value.size` / `.value.omitted.reason`, and a capped stack trace.
Carrying the refused value would make the dead letter *larger* than the record the broker just
rejected, so a DLQ topic sized like the output topic would reject it too. Recover the payload from the
**source** topic at the recorded `(topic, partition, offset)` — check that topic's `retention.ms`
against your response time. Size a DLQ topic for the ~600-byte metadata header set, not just the
payload.

**One exception to per-record semantics.** When the *broker* rejects an already-sent output record
(classically one exceeding the topic's `max.message.bytes`), the rejection poisons the in-flight
transaction, so the runtime aborts the **whole epoch** — but it does **not** advance past it. It commits a
secondary transaction carrying the dead letter alone, quarantines the poison's source coordinates,
restarts the engine in place, and **replays** the epoch with exactly that record skipped; the epoch's
other records are reprocessed and commit normally. `RETRY` there means the same replay **without**
quarantining (the record is fine, the send wasn't); under `AT_LEAST_ONCE` it is treated as `FAIL`.
`FAIL` holds the offsets and stops, so an operator restart replays the epoch — on **every** path: the
synchronous routes (processing, sync production, punctuator/timer/scheduled-source) abort the epoch the
same way, differing only in that they write no dead letter (the async path's rides a separate post-abort
transaction). The replay covers the epoch's consumer records via the held offsets; a punctuator tick or
scheduled-source firing does not itself re-fire retroactively (a cron occurrence crossed by the restart
is skipped; interval schedules resume fresh).

Loss is bounded to the poison record's **own** outputs across every sink it fans out to — not zero — and a
poison derived from **accumulated state** never converges (the next record on that key reproduces it).
Each replay spends `stoatflow.commit-barrier.max-poison-replays`, an **in-memory** budget, so `CONTINUE`
is bounded per process rather than end to end. Alert on `stoatflow.dlq.poison.replays.total`; prevent the
class by sizing the destination topic's `max.message.bytes`. A poison emitted by a punctuator, window
close, suppression flush, timer or scheduled source cannot be replayed at all — the runtime names the
plane and stops.

For business-rule rejections, use ordinary `filterNot(...).to("…dlq")` routing, not the exception
handlers. Custom handlers implement `handle(ErrorHandlerContext, record, exception)` returning a response
(e.g. `ProcessingHandlerResponse.continueProcessing()` / `.fail()`).
