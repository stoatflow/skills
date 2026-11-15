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
- Custom: implement `Serde<T>`, or `Serdes.serdeFrom(serializer, deserializer)`. A (de)serializer
  *instance* passed to the Processor API (`addSource`/`addSink`) is called concurrently by the dispatcher, parallel
  deser and every lane, so it must be thread-safe (KS shares it across tasks too, but its default single stream thread
  never calls it concurrently); a serde (declared or default) is asked for `deserializer()`/`serializer()` per call.
- **A serde must round-trip `null` cleanly** (return `null` for `null`) — tombstones depend on it.
- **Headers reach the serde in both directions** (KS parity): sources call `deserialize(topic, headers, data)`;
  the sink key + value, the boundary key bytes above and `queryMetadataForKey` call `serialize(topic, headers, data)`
  (KIP-1321) — exactly as Kafka Streams serialises sinks, so a port places records as on KS. Header-agnostic serdes are
  byte-identical, and Confluent's schema id in headers (`HeaderSchemaIdSerializer`, which needs non-null headers)
  works. Headers are the record's own, shared by its sinks as in KS, so a header-writing serializer's additions are
  visible to a sibling sink and to later processors. A record crossing an in-memory sub-topology boundary (`repartition()`,
  or a re-key followed by an aggregation, join or `toTable()`) gets its own copy of the headers there, as a KS
  repartition topic gives each record its own — what is added after the boundary is not visible before it. Exception,
  as in KS: the boundary KEY is serialized with the record's own headers before the copy, so a key serializer's header
  (Confluent header mode) is visible upstream too. At a `repartition()` boundary advise against a key serde whose bytes depend on header **contents** (one key
  would spread over several lanes of one global store, and away from its timers) — the user's responsibility; serdes
  that only add a header keep one lane per key; a Confluent header-mode **key** serde can key a Processor API timer —
  what it writes is stored with the timer and decodes the key at fire, never the timer's own (arming-record) headers;
  a key serde whose bytes depend on header contents cannot (its timers would pick another lane: lane guard). A timer
  key that cannot be decoded at fire (key serde changed between deployments) goes to the processing handler with the
  raw key bytes — CONTINUE + DLQ drops it, FAIL stops and re-fires it every start: advise CONTINUE + a DLQ for a
  timer-owning processor whose key serde may change; keep StoatFlow's key serde in the input topic's schema-id mode (at
  the first sub-topology records lane by the consumed bytes, timers by StoatFlow's serialization). Switching a **key** serializer from Confluent's prefix to header mode changes the
  key bytes, hence partitions, in any client (Confluent's warning) — keep keys in prefix mode or treat it as a re-key.
- **Store serdes and headers (B5):** a store that persists no headers — every plain store — calls its serdes with fresh **empty** headers on every write, read, bound and iterator
  step, so it decodes the same in `process()`, punctuators, window-close emissions, IQ and restore; header-agnostic and
  Confluent prefix-mode serdes write unchanged bytes. A **header-mode** serde (schema id in headers) cannot work on
  such a store — the id is discarded and reads fail (rarely: decode against the wrong schema, when the payload starts
  with the magic byte). That includes a serde that switches to header mode whenever it gets headers (Apicurio with
  `apicurio.registry.headers.enabled`): the store passes empty headers, never `null`. StoatFlow WARNs once per store,
  naming the headers and the fix: `Materialized.withRecordHeaders()`, `stoatflow.dsl.store-format: HEADERS`, a
  `Stores.…WithHeadersBuilder` for a Processor API store, or the schema id in the payload. Any header triggers it; one
  the deserializer does not need (a tracing id, a JSON type id) is harmless. Kafka Streams 4.3+ passes the current
  record's headers instead (a divergence — KSC-129). A **headers store** (`withRecordHeaders()`, `HEADERS`,
  `Stores.*WithHeaders`) follows Kafka Streams 4.4: a write's key + value serialize with the stored headers (the
  serializers' additions are stored), lookups/bounds/`prefixScan` with the current record's, iterators with each
  entry's stored headers — so header-mode keys and values work there. A header a store serializer appends replaces an
  older one of the same name, so carrying an entry's stored headers forward keeps one schema id per name (KS keeps every
  copy). **Pitfall (KS-equal):** a headers store stores the record's headers, so a header-mode input's
  `__value_schema_id` is kept with the value; a **prefix-mode** store serde replaces nothing, and its header-first
  deserializer then decodes against the input topic's schema and fails. Keep the store's serdes in the input's mode (both
  header mode), or drop the schema-id headers before the stateful step. The **FK-join subscription** store keeps the FK's and the PK's
  serializer headers separately and decodes with them (restore, standby) — header-mode key serdes work there without
  configuration (KS decodes the FK subscription with the triggering record's headers). **Stream-stream joins** (inner,
  left, outer, self-joins) keep their serializers' headers too: each side buffer stores what the key and value
  serializers added with the value and decodes with it (an IQ on a buffer decodes keys the same way), so header-mode
  serdes work there with no configuration — every store supplier, after restart/restore, on a standby, whatever
  `stoatflow.dsl.store-format` is. A buffer never stores the record's own headers, so header-mode input with prefix-mode
  join serdes decodes too. The key and value serdes must write different header names (Confluent's and Apicurio's do).
  KS 4.3+ side buffers follow `dsl.store.format`: `HEADERS` or in-memory keeps the record's headers plus the
  serializers' (works), the default drops them (a header-mode serde fails). An explicit `StreamJoined` store supplier
  must build a **plain** window store (timestamped / headers / versioned → refused at `join()`). Store serializers always work on a **copy**: the
  record's headers (and outputs) never gain a store's schema id (KS appends it to the record). The wrapper serdes
  (`WindowedSerdes`, the timestamped value serde) forward headers to the inner serde; their 2-arg forms pass fresh
  empty headers, as Kafka Streams' do. A suppressed table read by a join looks keys up with the **reading** record's
  headers (KS parity): a header-dependent key serde misses when they differ from the buffered record's.
- **Avro:** no StoatFlow serde — use Confluent `SpecificAvroSerde`. `stoatflow.schema-registry-url` in YAML
  propagates only to *default* serdes; per-operator Avro serdes must be `configure(mapOf("schema.registry.url" to url), isKey)`
  by hand. Pin Apache `kafka-clients` (Confluent pulls `-ccs`). Tests use `mock://…`.
- **Schema-registry subjects StoatFlow registers** (`TopicNameStrategy`: `<topic>-key` / `-value`). Every
  serialization without a real topic of its own uses an **app-scoped** topic name, one type per subject:
  - **store** → KS `changelogFor`: its backing source topic (`globalTable()`, `addGlobalStore()`, a read-only store,
    a `table()` reusing its source — the default `auto-reuse-ktable-source-topics`), else `<app>-<store>-changelog`,
    logged or not;
  - **in-memory boundary** → `<app>-<name>-repartition`, one per **key stream and input**: `<name>` is KS's
    repartition-topic name where the user named it and KS creates it — after a re-key (the `Repartitioned` name of the `repartition()` the key comes
    from; `Grouped` on `groupBy` **and** `groupByKey`, else the aggregation's `Materialized` store name — per leg for a
    cogroup; `Joined`; `StreamJoined` as `-left` / `-right`; `toTable(Named)`), else — into a merge, or an unnamed or un-re-keyed
    input of a join/cogroup — the node's own name, shared by those inputs, else the name of the node the key comes from (StoatFlow
    generated, never KS's; `<cogroup>-windowed` out of a windowed or session cogroup, whose key is `Windowed<K>`; also
    each input of a multi-parent Processor API node, which is untyped). One `Grouped`-named grouping feeding several aggregations shares one subject (store-named aggregations get their own); unrelated key streams on one
    name are refused at compile, all in one error, as KS refuses a duplicate internal topic;
  - **timer key** → `<app>-<processor>-timer`; **scheduled source key** → `<app>-<source>-scheduled`;
  - **FK join** → `<app>-<join>-subscription-registration-topic-fk` / `-pk`, results cross under
    `<app>-<join>-subscription-response-topic`;
  - **LEFT/OUTER stream-stream join** buffer → key + left values under the outer store's topic, right values under
    `<app>-<outer store>-changelog-right` (KS shares one subject for both sides, which a `BACKWARD` registry refuses
    for two value types).

  **Advice to give:** with `auto.register.schemas=false`, pre-register these like KS internal subjects; name
  repartitions (`ensure.explicit.internal.resource.naming`) so a port keeps its KS repartition and FK subjects — a
  stream–stream join's store subjects follow StoatFlow's store names (`-left-store` / `-right-store` / `-outer-store`,
  KS `-this-join-store` / `-other-join-store` / `-outer-shared-join-store`). Lane hashing across two
  subjects (a timer vs its records' boundary key, the `-left` / `-right` sides of a named join) needs the registry to
  give one schema one id in every subject — Confluent and Apicurio's default content-id mode do (KS co-partitioning
  relies on the same); otherwise the timer lane-affinity guard refuses the timer. A source-backed store serializes under
  the source topic's subject, so restored and written key bytes agree on any registry. Under the EAGER split a
  boundary is named at the re-key, ignoring a `Grouped` / `Joined` name behind an intervening operator.

## State stores — the `Stores` factory (`io.stoatflow.core.state`)

| Store | Persistent (RocksDB, default) | In-memory |
|---|---|---|
| Key-value | `persistentKeyValueStore(name)` | `inMemoryKeyValueStore(name)` |
| LRU | — | `lruMap(name, maxCacheSize)` — no commit buffer (see Notes); under `exactly_once` not a table a join reads from another lane (`KSC-126`) |
| Window | `persistentWindowStore(name, retention, windowSize)` | `inMemoryWindowStore(...)` |
| Session | `persistentSessionStore(name, retention)` | `inMemorySessionStore(name, retention[, expiryIndex])` 🆕 third param |
| Timestamped KV (KIP-258) | `persistentTimestampedKeyValueStore(name)` | `inMemoryTimestampedKeyValueStore(name)` |
| Timestamped window | `persistentTimestampedWindowStore(...)` | `inMemoryTimestampedWindowStore(...)` |
| Versioned KV (KIP-889) | `persistentVersionedKeyValueStore(name, historyRetention)` | `inMemoryVersionedKeyValueStore(...)` 🆕 |

Durations are `java.time.Duration`; window/session retention ≥ window size; names match `[a-zA-Z0-9._-]`.
Store builders: `Stores.keyValueStoreBuilder(supplier, keySerde, valueSerde)` (+ `windowStoreBuilder`/
`sessionStoreBuilder`/`versionedKeyValueStoreBuilder`). The plain key-value and window builders take any `Stores.*`
supplier of their kind — a timestamped or headers-aware one builds the plain store of its backend, as in Kafka Streams —
and the timestamped, session and versioned builders build a `Stores.*WithHeaders` supplier's store without record
headers (use the `…WithHeadersBuilder` factories to keep them — for a versioned store StoatFlow's
`Stores.versionedKeyValueStoreWithHeadersBuilder`, which Kafka Streams does not have).
**Do not switch an existing Processor API store between the
plain and timestamped builders, to another store type, or to another window size or `retainDuplicates`, under the
same name:** a store with a changelog refuses to start (`StoreLayoutMismatchException`, the store left untouched) —
its changelog holds the old layout (a window changelog's keys carry the window start only), where Kafka Streams
would upgrade plain→timestamped in place (KSC-150). Give the new layout a new store name — a resized DSL windowed
aggregation too (`Materialized.as`). DSL stores are timestamped whatever supplier `Materialized` gets, so swapping
one there changes nothing, and a stream-stream join's `JoinWindows` change is adopted (a join reads its buffers by
record timestamp). A hand-written `StateStoreBuilder` must report
`isTimestamped = true` exactly when its `build()` returns a timestamped store (`TimestampedKeyValueStore`/
`TimestampedWindowStore` or a headers twin), and a custom supplier behind `Materialized.as(...)` on a timestamped
aggregation (`count()`, `aggregate()`, …) must build a timestamped store — otherwise startup fails with
`TopologyValidationException`, before any store opens. `Stores.*` builders always comply. A hand-written
`StateStoreBuilder` must also not return a persistent supplier's `get()` (`Stores.persistentKeyValueStore(..).get()`,
or the timestamped/window/session equivalent): that byte-level RocksDB store has no epoch lifecycle wired, so startup
refuses it with `TopologyValidationException` (KSC-124). Pass the supplier to `Stores.*StoreBuilder(supplier, …)` or
`Materialized.as(supplier)`. In-memory and LRU `get()` stores are fine. A hand-written **versioned** builder that builds
a persistent store must override `toInMemoryBuilder()` to return its in-memory twin: the default returns the builder
itself, which StoatFlow reads as an in-memory store — refused under `exactly_once` as a table a join reads from another
lane (`KSC-126`).

**In-memory session store expiry (StoatFlow-only `expiryIndex`).** The in-memory session store's map is
key-first, so its default expiry pass walks every live session on every watermark advance (~50 ns each in the
reference measurement, 4–23 ns on a densely packed heap: 25 ms per barrier at 500 k sessions; past ~3 M sessions
per store it exceeds the 150 ms barrier floor) on the punctuation lane the commit waits for. It logs one WARN
per store the first time a pass exceeds 15 ms on a store of ≥ 100 k live sessions.
Opt in per store with the third argument on **both** factories — `Stores.inMemorySessionStore(name, retention,
expiryIndex = true)` / `Stores.inMemorySessionStoreWithHeaders(name, retention, expiryIndex = true)` — via
`Materialized.as(supplier)` (the supplier wins over `withStoreType`, KSC-100) or `Stores.sessionStoreBuilder(supplier, k, v)`.
The trade: expiry cost moves from *how many sessions are alive* (the per-pass walk, gone — ~1 µs, flat) to
*how many expire* (≈ 4× dearer per removal, end-time-order removal), plus ≈ 0.3 µs per put and 60 B heap per
live session. Wins in any steady state where sessions outlive a minute or two; loses on a pass that expires a
large fraction of the store at once. Both arms warn once per store over 15 ms (≥ 100 k live sessions).
**Do not opt in blindly — evaluate on the real workload** (index off vs on: `stoatflow.barrier.commit.latency`,
`stoatflow.store.num.keys`, heap, the warning). Starting point: off below ~100 k live sessions; usually on from
~100 k, confirmed against the session churn; past a few million, or when most of the store expires in one pass
as a steady state, prefer `persistentSessionStore`. Nothing is
persisted — enabling it is a restart. Not a config key; no KS equivalent (KS's in-memory session store is
end-time-first instead).

**`Materialized`** (`io.stoatflow.core.topology`): `Materialized.as<K,V,StateStore>("name")`,
`Materialized.as(StoreType.ROCKS_DB)`, `Materialized.as(supplier)` (any `Stores.*` supplier, timestamped and
headers-aware included — `Materialized.as(Stores.persistentTimestampedKeyValueStore("counts"))` ports as is; the
supplier overloads take two type arguments, `Materialized.as<K,V>(supplier)`, and type the store as Kafka Streams does,
`KeyValueStore<Bytes, ByteArray>` / `WindowStore<…>` / `SessionStore<…>`), `Materialized.with(k,v)`. An operator refuses a
supplier of another store kind at topology build (a key-value one in a windowed aggregation, say; KSC-21), and a windowed
aggregation refuses a `retainDuplicates = true` window supplier (KSC-152 — Kafka Streams accepts it and emits once per
update) and a window or session supplier it cannot use (KSC-153): a retention below its minimum — window size + grace,
2 × time difference + grace (sliding), gap + grace (session) — or another window size (a time window's size; for sliding
windows the time difference `D` or `D + 1`). Kafka Streams checks only `withRetention`. Options:
`.withKeySerde`/`.withValueSerde`, `.withStoreType(StoreType.IN_MEMORY | ROCKS_DB)`, `.withRetention(d)`,
`.withCachingEnabled([config])`/`.withCachingDisabled()`, `.withLoggingEnabled([topicConfig])`/
`.withLoggingDisabled()`, `.withRecordHeaders()` (KIP-1271), `.withChangelogNumPartitions(n)` (StoatFlow
extension — no KS equivalent). Store-type precedence (Kafka Streams' order,
KSC-100): an explicit `Materialized.as(supplier)` > `withStoreType` > RocksDB default; neither setter clears
the other, and with both set the supplier wins with one WARN. Same per side on `StreamJoined`. A
`Stores.*WithHeaders` supplier keeps its headers under `.withoutRecordHeaders()` (the supplier wins, one WARN).

**`.withChangelogNumPartitions(n)` — per-store changelog partition count** (also on `StreamJoined`, the
Processor-API `StoreBuilder`, and `Suppressed.BufferConfig`; the unmaterialized timer store uses the
`stoatflow.changelog.timer-num-partitions` config key). Defaults to the global
`stoatflow.changelog.num-partitions` (**3**). No KS equivalent: KS co-partitioning forces the changelog
partition count to equal the source topic's, so it is never a decision there.

The count sets a changelog's **write-throughput ceiling** and how the store's write load **shards across
brokers**; faster cold-start restore is secondary and sub-linear (one restore task per partition, but one
store restores into one RocksDB whose writes serialise, and all stores share one worker pool). The cost is
paid on **every commit**: changelog records ride the same transaction as sink records, so each partition a
store's epoch touches widens that commit.

Guidance: **prefer small counts; raise a store only on a measurement.** `restorationPoolSize` is a ceiling
beyond which restore parallelism cannot help, never a target. The override is **bidirectional** — with the
default at 3, the everyday use is pinning a small store (lookup table, config table)
back to `1`. On `StreamJoined` the number is **multiplied**: one call sets it on 2 stores (INNER) or 3
(LEFT/OUTER), so `(6)` means 12 or 18 partitions.

**It is create-time immutable.** Changing it later is a no-op plus a startup WARN — StoatFlow never
re-partitions a live changelog. To change it, wipe local state AND delete/recreate the changelog topic.
Never increase a compacted changelog's partition count by hand: keys re-map while stale values survive in
the old partitions, and the restore reconstructs corrupted state. Unrelated to
`Repartitioned.numberOfPartitions`, which sizes a repartition/lane fan-out.

A custom Kafka `partitioner.class` is **refused at startup** — it would break the changelog-key ->
partition pinning this rests on. Customise sink partitioning with `Produced.withStreamPartitioner(...)` or
`Repartitioned.streamPartitioner(...)`.

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

**What a headers store does not keep (KSC-149).** A headers-aware store (`withRecordHeaders()`,
`dsl.store.format=HEADERS` / `stoatflow.dsl.store-format: HEADERS`, the `Stores.*WithHeaders*` builders) keeps every
header except StoatFlow's own: `__stoatflow.v` and every name starting with `__stoatflow.v.` are **reserved** for
StoatFlow's changelog format version and are dropped from the stored headers (from the record and from what a
serializer appends) with one WARN per store naming the header; the engine stamp `stoatflow.epoch` (`emit.mode =
on-barrier`) is dropped silently. Everything else stays — `__stoatflow.vendor` (not reserved: the rule is the exact
name or the `__stoatflow.v.` prefix) and the dead-letter `__stoatflow.errors.*` headers included, so dead letters can be
reprocessed into a headers-aware table. The restore drops the same names (a table restored from its source topic too),
so a live store and a restored one hold the same headers; IQ and forwards see the stored headers. Kafka Streams keeps
every header. Never tell a user to put data in a `__stoatflow.v` or `__stoatflow.v.*` header; to keep one, rename it before the stateful
step.

Store handles: `KeyValueStore<K,V>` (`get`/`put`/`delete`, atomic `compute(key, fn)` / `merge(key, value, fn)`);
**to delete a key, write `put(key, null)` unless you need the previous value** — `delete()` returns it, so it
reads (and deserialises) it first; `put(key, null)` is the same tombstone without the read, and it is what
both StoatFlow's DSL and Kafka Streams do internally (https://stoatflow.io/docs/building/state-stores#deleting-a-key).
On a timestamped store a **null value is an absent entry** (Kafka Streams parity): a `ValueAndTimestamp` whose value is
`null` is written as a delete, a value your deserializer maps to `null` reads back as `null`, and
`ValueAndTimestamp.make(null, ts)` returns `null` (`makeAllowNullable` returns an instance); `merge` rejects a null-valued
input (`IllegalArgumentException`, like `Map.merge`) — delete with `put(key, null)` — and a `null` remapping
result deletes; `putIfAbsent(key, null)` writes nothing; window `put(key, null, windowStart)` deletes the window;
session `put(session, null)` removes the session (`put`/`putIfAbsent` take `null` from Kotlin and Java, as in KS;
`merge` needs a value, like `Map.merge`);
read-only `ReadOnlyKeyValueStore` (`get`, `containsKey`, `all`/`reverseAll`, `range`/`reverseRange`,
`prefixScan(prefix, serializer)`, `approximateNumEntries`); `KeyValueIterator` is `Closeable`. Range bounds
are KS-exact (KIP-763): a null bound is open-ended (both null ≡ `all()`), `reverseRange` takes ascending
`(low, high)` bounds like `range`, and `from > to` (on the serialized key bytes) yields an empty iterator +
WARN — the same rules apply to the window/session key-range queries (`fetch(keyFrom, keyTo, …)`,
`findSessions(keyFrom, keyTo, …)`).

**Notes:** RocksDB is default/recommended; in-memory still writes a changelog (durable via replay) unless
`withLoggingDisabled()` (then unrecoverable after a crash). Caching is always on (every non-LRU store batches its
writes per commit — EOS epoch buffering, changelog compaction) and never changes emissions — every update is forwarded
(KSC-88). `withCachingEnabled(EmissionSuppression…)` sets a store's cache caps (past them: an early commit);
`withCachingDisabled()` is accepted and changes nothing (default caps). There is no global caching switch.
Consolidate with `Suppressed.untilBarrier()` / `emit.mode: on-barrier`.
Versioned `historyRetention` is retention **and** grace (older out-of-order records rejected).
A versioned supplier in `Materialized.as(...)` makes any non-windowed key-value materialisation versioned
(under the supplier's name): `table`/`toTable`, `KTable` `filter`/`mapValues`/`transformValues`, join results,
non-windowed cogroup, `count`/`reduce`/`aggregate`. A rejected (too old) write forwards nothing; downstream
aggregations ignore out-of-order history writes per source key (KS `isLatest`). Windowed/session aggregations
refuse a versioned supplier at build time; `globalTable` accepts one (KS refuses — `KSC-125`). A versioned table
accepts only `suppress(untilBarrier())` (or `emit.mode: on-barrier`): latest changes consolidated per commit, history
writes pass through in order (the key's buffered latest first); other suppress kinds are refused (KIP-914), also
through a `filter` / `mapValues` / `transformValues` of it without its own `Materialized`. Under
`exactly_once` every lane reads a RocksDB versioned store as of its own commit, so it may feed any join (FK included);
an **in-memory** versioned table that a join reads from another lane is refused at build time (`KSC-126`, below).
**No commit buffer — `lruMap` and the in-memory versioned store.** Both write directly instead of buffering each
commit (an LRU store decides its own evictions, so a buffer above it is circular). Still exactly-once: an aborted
commit always ends in an engine rebuild of the store from its changelog, so its writes never survive (with
`withLoggingDisabled()` the rebuild is empty: every in-place ABORT restart, poison replays included, drops all its
state). But neither can
be read as of the last commit. **Exactly-once reads (`KSC-126`):** under `exactly_once` every lane reads every
other built-in store (the default KV/window/session stores, the RocksDB versioned store) as of its own commit — joins,
Processor API reads and punctuators never see another lane's uncommitted next-commit write; IQ and `at_least_once` read
the latest state. So an `lruMap` / in-memory versioned table that a join reads from another lane is refused at build
(`TopologyValidationException` naming the join and the store): either side of a PK or FK join, a stream-table join
after a re-key, a stream-GlobalKTable join, a join reading another join's result table. A stream-table join with no
re-key reads its own lane and is fine. Fix: a default store supplier or `persistentVersionedKeyValueStore`, or
`at_least_once`. A Processor API node that *may* read one across lanes gets the `presumed-cross-lane-read` WARN instead
(`references/processor-api.md`). Both stores' reads are always live, so neither honours an interactive query's
`READ_COMMITTED` level: they are read live, with a one-shot WARN (`KSC-116` lruMap, `KSC-106` in-memory versioned). LRU extras: a `get()` (an interactive query too) promotes the key and so changes the next eviction; a
restart does not bring back the pre-restart key set — with more than `maxCacheSize` keys in the changelog, which survive
depends on the parallel partition restore order and can differ between restarts, so a table backed by it can answer
differently after a restart or HA failover. Evictions are local only by default; 🆕 `Stores.lruMap(name, maxCacheSize,
tombstoneOnEviction = true)` also writes a changelog delete per eviction (needs logging). The `TopologyTestDriver` runs
every versioned store on the
in-memory backend, but the exactly-once checks judge the **declared** store: a RocksDB versioned join input passes, an
in-memory versioned or `lruMap` one is refused — as in the engine.
A `table()` store — or an `addReadOnlyStateStore` store, with or without an update processor — restored from its
**source topic** takes each record's Kafka record timestamp (as in KS), not a custom extractor's — derived
join/aggregation timestamps can differ after a restart until the key updates. Likewise a record skipped live for having
no event time (Kafka's `-1`) is present after such a restore, as in KS. Global stores restore from their
changelog and keep the stored timestamps.

**A restore never classifies a store on metadata it could not read.** A changelog or source topic's partition count of 0
can mean three things: the topic is missing, it was just created and a broker has not seen it yet, or the describe
failed. That count, and a failed end- or begin-offset read, are retried for up to `state.restoration-metadata-timeout-ms`
(30 s). If the read still fails, the start fails with an error naming the store, and the store is never deleted or
restored from nothing. The local check that follows is read-only and opens no data file — RocksDB's manifest, every data
file present at its recorded size, the stored offsets — each store's check within
`state.restoration-validation-timeout-ms` (30 s) of its own start, at most `state.restoration-pool-size` at once; a
store that fails it (a missing or truncated file) is restored in full,
while a read failing for another reason (too many open files, an I/O error) fails the start rather than delete the store.
Every other startup probe of a store directory is bounded by the same setting, each on its own, so a stalled disk fails
the start, naming the store, instead of hanging it.
- **KTable source-topic reuse** (`ktable.auto-reuse-source-topics`, on by default) reads the source topic's
  `cleanup.policy`, which needs `DescribeConfigs`. An unreadable answer fails the start, and an authorization or
  authentication error does so at once. The fix is to grant the ACL, or pin the table to the source it already restores
  from — the error names it (`withMaterializeFromSourceTopic(true)` for a table on its source topic, `(false)` for one on
  its changelog).
- **A table restored from its source topic is restored up to the committed offset** of the consumer group
  (`application.id`), never to the topic's end, and processing resumes from there, as in Kafka Streams. So a replayed
  update sees its real old value, which `groupBy` aggregations and FK joins depend on. With no committed offset (a first
  start, or a deleted or reset group) nothing is restored and processing rebuilds the table from the offset reset
  position: a `table()` under `latest` starts empty, while a `globalTable()` always reads from the earliest offset. Local
  state ahead of the committed offset (an at-least-once crash) is kept, and the records in between are processed again;
  so is a partition with nothing to restore whose topic start retention has since moved past it. A committed offset no
  longer in the topic resets by the source's policy, else `auto.offset.reset`, as in Kafka Streams. A restore whose
  position falls out of the topic mid-way fails the start and marks the store: the next start restores it in full; one
  cut short by a crash resumes where its data stops (a window/session store restoring a partition from nothing: in full).
  The read needs `Describe` on the group (implied by `Read`); an unreadable answer fails the start like any restore
  metadata. Not yet with `ha.mode: active-standby`: there the table is still restored to the topic's end.
- **A table never switches its restoration source under its state.** A table whose local state holds source-topic
  offsets, and which a start resolves to a changelog — reuse off, pinned `(false)`, the topic no longer compacted —
  fails that start, before anything is created or deleted. To switch on purpose: stop, reset the group on the source
  topic to the earliest offset (the input is reprocessed), delete the store's local state, start. A table without such
  state (in-memory, a fresh disk, after `cleanUp()`) gets a **WARN** instead, when its changelog topic does not exist
  while the consumer group is past the source's start: the start goes on, since a stream turned into a table or a
  renamed store looks the same; treat the WARN as the same rebuild. The warning's reads are tried once within the metadata
  window (`Describe` on the group), skipped once it has passed; a failed read means no warning, never a failed start. A logging-disabled table is left as it is. Kafka
  Streams switches silently.
- **A versioned table always restores from its changelog** — a compacted topic keeps one version per key. Pinning one
  to its source topic, or a versioned store in `addReadOnlyStateStore`, fails at build time.
- **One open per store directory.** A store directory another open still holds (a second instance on the same state
  dir, a store a previous engine left open across an in-place restart) fails the start, naming the holder; nothing is
  deleted.
- **Turning the timer changelog on** (`timer-backend.changelog-enabled`) after running with it off starts the timer
  store empty, as with any store whose logging is turned on.

**Versioned-store retention is a minimum bound, and its read contract matches Kafka Streams.** A
point-in-time read below `observedStreamTime - historyRetention` (inclusive) is outside the window: it
returns the **latest** version when that version is old enough to satisfy the bound, and `null`
otherwise — so a key written once and never updated keeps answering forever, while superseded history
ages out. Physical reclamation lags: expired versions stop being readable immediately but are removed by
a background sweep that runs a slice per commit, targeting one `stoatflow.state.versioned.sweep-period-ms`
(default 15 min) for a full pass. Two operational consequences: on-disk size is bounded rather than
exact, and a hot-standby replica does not sweep at all (it has no commit barriers) — but it now *applies*
the active's reclamation tombstones through the same restore seam, so its copy tracks the active within
replication lag rather than growing until promotion. A standby that stops shedding is a real signal. Kafka Streams reclaims by dropping
whole segments instead, and does not persist its observed stream time — StoatFlow does, so the grace
floor survives a restart rather than being rebuilt from the first record after it. Its **changelog** is `compact` with unlimited retention (as KS's is, and for the same reason): one row per version means compaction cannot collapse them, and a delete policy would eventually remove the only row of a key written once and never updated. The topic is bounded by **explicit reclamation** instead — the sweep writes a tombstone for each version it removes locally, which is what gives compaction something to act on. Three clocks, only the first immediate: the read window (immediate), the sweep (within one `sweep-period-ms`), then the broker's compaction cadence. That third one matters because the log cleaner never touches the *active* segment, so StoatFlow sets `segment.ms` + `max.compaction.lag.ms` to the sweep period — floored at **five minutes**, below which the broker bound stops tracking the configured period — and `min.cleanable.dirty.ratio` to 0.1 when it creates the changelog; Kafka's own `segment.ms` default is **7 days**, and a changelog created before this shipped keeps it (reconciliation only adjusts `cleanup.policy`/`retention.ms`). The topic grows before it shrinks, since a version costs one extra key-sized record on its way out. `stoatflow.store.versioned.sweep.tombstones.total` lagging `…sweep.reclaimed.total` persistently means the local store is shedding but the topic is not.

**Window/session retention is a minimum bound on a persistent store.** Expiry drops whole segments, so the one containing `watermark - retention` is retained in full: effective retention is **up to** `retention + segmentInterval`. Segments are sized `max(retention / 3, 60 s, windowSize)` on every persistent DSL or `Stores`-built window store (the stream-stream join buffers included) and `max(retention / 3, 60 s)` on every persistent session store (no window size); in-memory stores are not segmented at all; a hand-built non-segmented store — nothing the DSL or `Stores` builds — omits the window-size term. Where the divisor governs that is **up to 4/3x** what you configure; where `windowSize` governs — segmented only, and the default for `TimeWindows.ofSizeWithNoGrace`, which makes `retention == windowSize` — it is **up to 2x**. Below about three minutes of retention the 60-second floor governs **provided `windowSize <= 60 s`** — a larger window still wins (at `retention = 2 min, windowSize = 90 s` the interval is 90 s, for an effective 3 min 30 s). Being on the floor also says nothing about the segment *count*. Segments are fixed-width and epoch-aligned, so the count is `retention / segmentInterval` whole intervals plus the boundary one when the watermark lands on a segment edge, and `ceil(retention / segmentInterval) + 1` in the worst phase — the same number when the retention divides evenly, one higher when it does not — size against the maximum. 2 min of retention on a 60 s interval keeps three; 120 s over 90 s segments keeps two or three depending on phase. A single segment is never guaranteed. That assumes one uniform layout: after a retention or interval change, existing segments keep the span they were recorded with (ADR-141), so until they age out the live set is a mix of widths and the formula does not apply — size against `retention + the widest existing span` through that transition. That floor regime is also a *large* multiple rather than a small one: `retention + 60 s` is **3x** at 30 s retention, and it applies to StoatFlow as much as to KS (ADR-141), so it is not a KS-only footnote. Under `EmitStrategy.onWindowClose()` expiry is gated on the durable emit frontier instead of the committed watermark, adding a further `(watermark - emit frontier)` term that the retention contract does not bound. The in-memory stores drop per record, so they retain exactly **1x** — switching `StoreType` changes observable retention. Kafka Streams sizes segments `max(retention / 2, 60_000)` on both backends, so it keeps up to 3/2x: a ported app retains somewhat **less** here on join buffers and session stores, and **more** on a default windowed aggregation. Not configurable (ADR-030, ADR-141).

**Changing retention on an existing segmented store is safe** (ADR-141): each segment records its own time range on disk, so the change remaps rather than reinterpreting what is already there — no restore, unlike Kafka Streams. (Join buffers remap too since 2026-09-03; only a hand-built non-segmented store keeps the interval in every key, where a retention change that moves it needs a restore.) Changing the **window size**, switching **plain↔timestamped**, changing the store type, or flipping `retainDuplicates` cannot remap, and the changelog holds the old layout too (a window changelog key carries the window start only), so none is rebuilt from it: each is detected at startup and a store whose changelog is its recovery source refuses to start, naming the field that moved and both values (KSC-150). A store with no changelog refuses as well, naming every affected store — `stoatflow.state.layout-mismatch: wipe-and-restore` accepts the loss and starts those stores empty. A stream-stream join's `JoinWindows` change is the exception: a join reads its buffers by record timestamp, so it is adopted with no restore.

After a retention *narrowing*, existing segments keep the span they were written with, so disk frees gradually and effective retention stays at `retention + the widest existing span` until they age out. One `INFO` line at startup says so.

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

A third shape needs no store-key mismatch: **Processor API nodes in different sub-topologies connecting one store**
(a re-key between them splits them). Their lanes cross barriers at different times, so one key's writes can land in
the wrong commit order — under either guarantee. The build warns (`presumed-cross-sub-topology-write`, `warn`); see
`references/processor-api.md`.

## Interactive queries

`stoatflow.store(StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.keyValueStore()))`.
`QueryableStoreTypes`: `keyValueStore()`, `windowStore()`, `sessionStore()`, `timestampedKeyValueStore()`,
`timestampedWindowStore()`, `versionedKeyValueStore()`, and the headers-aware
`timestamped{KeyValue,Window}StoreWithHeaders()` / `sessionStoreWithHeaders()` /
`versionedKeyValueStoreWithHeaders()`. **Pick the factory by the value shape you want back** (KS parity): every DSL
materialisation — `table()`, `toTable()`, `globalTable()`, a `KTable` operator, a grouped or cogroup aggregation and
every windowed aggregation — builds a timestamped store (the headers twin under `withRecordHeaders()` or
`dsl.store-format: HEADERS`), and `keyValueStore()` / `windowStore()` over one — or its headers twin — answer with the plain
value (`count()` reads back as `Long`); `timestampedKeyValueStore()` /
`timestampedWindowStore()` give `ValueAndTimestamp` and also accept the headers twins; `sessionStore()` accepts a
headers session store and answers with the plain aggregate. Do not unwrap `ValueAndTimestamp` after
`keyValueStore()` — it never returns one. A Processor API read of one of these stores (`getStateStore`, e.g. a
`globalTable` store) sees `ValueAndTimestamp<V>` values: type it as `TimestampedKeyValueStore<K, V>`. Each operator
stores the timestamp it forwards (KS parity): the input's for a table/operator, `max(left, right)` for a table-table
join, `max(input, old aggregate)` for a non-windowed aggregation. **Every IQ view is read-only** (KS parity): a same-shape view's `put`/`delete`/`remove`/
`expireSessions` throw `UnsupportedOperationException`; a converting view (the pairs above) implements only the
`ReadOnly*` interface, so there is nothing to call and a cast to a writable type throws `ClassCastException`. The UOE holds — including through `sessionStoreWithHeaders()` and
`versionedKeyValueStoreWithHeaders()`, whose static type shows the write methods. Never write state through
IQ; write from a processor connected to the store. **Isolation level (KIP-892):** IQ reads the **latest** state by
default, uncommitted writes included (`READ_UNCOMMITTED`, KS's default). `stoatflow.state.interactive-query-isolation-level:
READ_COMMITTED` (KS key `default.interactive.query.isolation.level`) makes every `store()` handle read the state of the
last commit barrier — every store with a commit buffer, global stores included (KSC-144, KS reads globals uncommitted) — and
`readOnly(IsolationLevel)` (`org.apache.kafka.common.IsolationLevel`) picks the level per handle: on
`ReadOnlyKeyValueStore`, `ReadOnlyWindowStore`, `ReadOnlySessionStore`, `VersionedKeyValueStore`,
`VersionedKeyValueStoreWithHeaders`; the same level returns the handle itself, a `store()` handle's copy still
re-resolves per read. `READ_COMMITTED` lags processing by ≥ one commit interval plus the previous epoch's RocksDB flush,
never shows a write that could still roll back; point reads, and iterators over RocksDB KV / non-segmented window/session
stores, are one commit (a scan racing a commit retries); iterators over **in-memory** stores and the default
**segmented** window/session stores are weakly consistent (never an uncommitted row, but an iterator held open can see
later commits). Versioned committed reads answer as
of the last commit, `validTo` included. `approximateNumEntries()` ignores the level. `StateStore.
approximateNumUncommittedBytes()` (KIP-892) counts the in-memory bytes of epochs not yet in RocksDB, the committed one
still being written included — memory, not visibility (KSC-117). A custom `QueryableStoreType` reads the configured level
from `storeProvider.defaultIsolationLevel()` and applies it with `readOnly(level)` per read. A handle re-resolves the store on every read, so it can be
kept: across an in-place engine restart (`REPLACE_THREAD`, HA demote) a read throws the retriable
`InvalidStateStoreException` until the app is `RUNNING` again (after an HA demote: until this pod is promoted back —
query the new active); a held handle also serves while `PAUSED`. With
stale reads, a `store()` call while stores restore throws `InvalidStateStoreException` — retry. A custom `QueryableStoreType` implements KS's
`create(StateStoreProvider, String)` and should call `storeProvider.stores(name, this)` per read (and apply
`storeProvider.defaultIsolationLevel()`, above). `stoatflow.storeNames()` lists stores. Single-instance ⇒ **all state is global,
no partition routing** (`StoreQueryParameters` has no `withPartition`); `enableStaleStores()` to read during
restore. `store(...)` throws unless the app is `RUNNING` (or `RESTORING`/`VALIDATING_STATE` with stale reads).
Key metadata: `queryMetadataForKey(store, key, keySerializer)` (+ a `(store, key, headers, keySerializer)`
overload) and `queryMetadataForKey(store, key, [headers,] partitioner)` (KS 4.4, KIP-1321) return `KeyQueryMetadata`
— the local host, no standbys, the key's partition in the changelog (or source) topic; `-1` for a store no topic
backs (logging disabled, no source topic — the partitioner is not called). Informational only. Pass the headers when the serializer or
partitioner reads them. With a partitioner, anything but a single in-range partition throws
`IllegalArgumentException` — including `Optional.empty()` (KS returns an unusable metadata object instead). The
partitioner is called with `value = null` (as KS does), so a Kotlin partitioner used here needs a nullable `V` (a
non-null `V` fails with Kotlin's own `NullPointerException`).
Note: `StoatFlowRuntime.fromConfig` (`:runtime`) exposes **no** public query accessor today — typed
in-process IQ (`store()` and `query()`) needs the `:core` `StoatFlow` engine handle directly.

### Interactive Queries v2 — `query(StateQueryRequest)`

Kafka Streams' IQv2 (KIP-796), name for name in `io.stoatflow.core.query` (`StateQueryRequest`, `StateQueryResult`,
`QueryResult`, `Position`, `PositionBound`, `FailureReason`, the eight pre-4.4 query types and the four KIP-1356
headers-aware ones) — on `StoatFlow`,
`StreamProcessingApp` and the `TopologyTestDriver` (KS's driver has none). Read the answer with
`result.getPartitionResults()[StateQueryResult.SINGLE_PARTITION]` (one store partition, `0`; `getGlobalResult()` is
always `null`); check `isSuccess()` before `getResult()`. A `RangeQuery`/window/`MultiVersionedKeyQuery` result is an
iterator — close it (`use {}` / try-with-resources). Value shapes follow KS per family: `KeyQuery`/`RangeQuery` give the
bare value from timestamped and headers KV stores (`count()` → `Long`); `Timestamped*Query` gives `ValueAndTimestamp`
(`UNKNOWN_QUERY_TYPE` on a plain store); a `WindowKeyQuery` over a DSL windowed aggregation gives `ValueAndTimestamp`
elements — declare `WindowKeyQuery<String, ValueAndTimestamp<Long>>`; `WindowRangeQuery.withKey` on a session store the
aggregate; `VersionedKeyQuery` a `VersionedRecord`; `MultiVersionedKeyQuery` (`fromTime`/`toTime`, newest first unless
`withAscendingTimestamps()`) a `VersionedRecordIterator` — the same scan is public as
`ReadOnlyVersionedKeyValueStore.get(key, fromTime, toTime, order)` (🆕); on a RocksDB store both read one consistent
snapshot lazily (an open iterator pins old versions — close it). Headers stores answer those types without headers;
the **headers-aware types** (KIP-1356) return them: `TimestampedKeyWithHeadersQuery.withKey(k)` → a `ReadOnlyRecord<K, V>`
(`io.stoatflow.core.processor`; key, value, timestamp, headers — `null` for a missing key) and
`TimestampedRangeWithHeadersQuery` (`withRange`/`withLowerBound`/`withUpperBound`/`withNoBounds`, `withDescendingKeys()`)
→ a `ReadOnlyRecordIterator<K, V>` (`io.stoatflow.core.state`) from a headers KV store;
`TimestampedWindowKeyWithHeadersQuery.withKeyAndWindowStartRange(k, from, to)` and
`TimestampedWindowRangeWithHeadersQuery.withWindowStartRange(from, to)` → `ReadOnlyRecordIterator<Windowed<K>, V>` from a
headers window store (`Windowed` spans `[start, start + size)`, timestamp = the stored event time);
`TimestampedWindowRangeWithHeadersQuery.withKey(k)` → the key's sessions from a headers session store (timestamp = the
session's end). The headers are a frozen shallow copy (`add`/`remove` throw `IllegalStateException`; each query decodes its
own, so a value-bytes change stays on that record); empty, never `null`, for a record written without. Served only by their headers family — any other store,
versioned included, → `UNKNOWN_QUERY_TYPE`; the wrong `TimestampedWindowRangeWithHeadersQuery` form on a window/session
store → `UNKNOWN_QUERY_TYPE`. A negative stored timestamp is returned (KS fails it); an iterator's `next()` throws
`StreamsException` at a `Long.MIN_VALUE` timestamp (a session's end) or a KV/window value the serde reads as `null` and
stays open — close it in `use {}`. The store must be built headers-aware: `withRecordHeaders()` / `dsl.store-format: HEADERS`, or a
`Stores.*WithHeaders(...)` supplier — `Stores.*WithHeadersBuilder` refuses a plain supplier at build (KSC-145).
`KeyQuery`/`RangeQuery` on a versioned store throws
`UnsupportedOperationException`; a `suppress` buffer answers `UNKNOWN_QUERY_TYPE`. `withIsolationLevel(...)` overrides
the configured level per request; `enableExecutionInfo()` reports the store and the level applied. **`Position`** = the
last commit's source-topic offsets, the same for every store, moving once per commit (a lower bound under both levels) —
except a store no restore rebuilds (logging disabled and no source topic, or state restoration off), which reports only
this instance's commits since it started
(empty after a restart or HA failover, so bounds on it fail until its next commit);
`withPositionBound(PositionBound.at(p))` answers `NOT_UP_TO_BOUND` until the committed position reaches `p` — never
waits; a bound just taken from a consumed record fails until the next commit. The position survives an in-place engine
restart; after a process restart it refills from the committed offsets within seconds (bounds fail until then). **When it answers** (KS's exclusion
list, not `store()`'s): unknown store → `UnknownStateStoreException` first, in every state; not started / stopping /
stopped / failed → `IllegalStateException`; HA standby → throws like `store()`; starting, restoring, in-place restart →
`NOT_ACTIVE` (retry), as is a read whose store closes under it or a `READ_COMMITTED` read that commits kept overtaking;
`RUNNING`/`PAUSED`/`DRAINING` → answers, `requireActive()` included (as in KS, whose `pause()` keeps threads running).
So a `PAUSED` app answers `query()` where `store()` throws. Another partition than `0` in `withPartitions` →
`DOES_NOT_EXIST` (KS returns `NOT_PRESENT` — KSC-107); `skipCache()` does nothing on any key query (KSC-111 — use the
isolation level). Kotlin: type a key query's value
nullable, `KeyQuery.withKey<String, Long?>(key)`.

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

**Create the dead-letter topic before the start.** Every topic a handler declares in `deadLetterTopics()` — the
built-in DLQ handlers declare their `dlqTopic`; a deserialization handler's only when the topology reads a Kafka
topic, a production handler's only when it has a sink — is checked at `start()` with the static sink topics; a missing
one refuses the start with `MissingOutputTopicException` (KSC-148). A handler class written to set a built-in DLQ
handler from YAML should override `deadLetterTopics()` to return its delegate's. Opt-out (clusters relying on broker
auto-creation only): `validation.validate-output-topics: false`.

Deser + processing defaults are **fail-fast** (no silent drops unless opted in). DLQ records ride the
**same transactional producer** (atomic on the barrier) — and only a `CONTINUE` verdict writes them:
a **`FAIL` aborts the failing epoch** (offsets held, outputs discarded) and writes **no** dead letter
on the processing/sync-production paths, because the record is preserved and replays on operator
restart. A deserialization `FAIL` keeps the graceful stop instead — the failed record never enters the
offset tracker, so the shutdown commits the pre-poison outputs and stops *before* the poison — and it
writes no dead letter either. The processing handler renders its verdict **once** per failure (a FAIL
is not re-offered at ancestor DAG levels). Its `context.headers()` and `record.headers()` are one copy of the failing
record's headers — the record the processor was handed; a failed timer's are the timer's (its arming record's), a
punctuator's or scheduled source's none — and the DLQ handler copies them onto the dead letter first, except a
`__key_schema_id` / `__value_schema_id` for a key or value it writes as `toString()` text (kept beside a timer's raw,
undecodable key bytes, where it is the key serializer's own). Error headers use the `__stoatflow.errors.*` namespace.

**With a timestamp assigner (`withTimestampAssigner` / a `TimestampExtractor`), a deserialization skip goes
first.** It has no event time, so it is dispatched ahead of every other partition's records; a partition
receiving undeserializable records faster than they are skipped (a backlog during catch-up) holds the
others back until it drains. Kafka Streams slows the others instead. Treat a sustained DLQ flood as an
upstream fault.

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
quarantining (the record is fine, the send wasn't) — on any exception, where Kafka Streams fails a `RETRY` on a
non-retriable one; under `AT_LEAST_ONCE` it is treated as `FAIL` — the app stops and its restart (by a supervisor)
replays the epoch, where Kafka Streams revives the task in place (KSC-151).
`DefaultProductionExceptionHandler` is asked about an asynchronous failure only when it has a `dlqTopic`;
without one such a failure stops the app under both guarantees.
Under `AT_LEAST_ONCE` there is no transaction to poison: `CONTINUE` sends the dead letter with the next
commit and commits past the record (no replay); `FAIL` stops that commit before it writes anything.
`FAIL` holds the offsets and stops, so an operator restart replays the epoch — on **every** path, under
both guarantees: the
synchronous routes (processing, sync production, punctuator/timer/scheduled-source) abort the epoch the
same way, differing only in that they write no dead letter (the async path's rides a separate post-abort
transaction). The replay covers the epoch's consumer records via the held offsets; a punctuator tick or
scheduled-source firing does not itself re-fire retroactively (a cron occurrence crossed by the restart
is skipped; interval schedules resume fresh).

**A failed send no handler is asked about is a `FAIL`** under both guarantees: an output record when the
handler does not route asynchronous failures (`requiresAsyncRouting()` false — `DefaultProductionExceptionHandler`
without a `dlqTopic`, the default), a changelog record, a dead letter (KSC-147: Kafka Streams would ask the handler
about those two), a fatal producer error. **A handler that throws, or returns `null` or a `null` dead-letter record (Java), is a `FAIL` too**, on every
path — synchronous serialization/send/partitioner failures included, never re-offered to the processing handler (with a
`REPLACE_THREAD` uncaught-exception handler a synchronous one restarts in place first, as any synchronous `FAIL`); a
processing or deserialization handler that throws, or returns `null` or a `null` dead-letter record, is a `FAIL` as well, and so is a synchronous `CONTINUE` whose dead
letters cannot be sent. The commit stops, the offsets are held, the restart sends again; a send failure is never committed past
without a handler's `CONTINUE`. A record too large for its topic therefore crash-loops (as in Kafka Streams)
until the topic's `max.message.bytes` is raised or a DLQ handler answers `CONTINUE`.

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
