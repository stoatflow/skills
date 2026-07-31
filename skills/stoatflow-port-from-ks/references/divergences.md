# The divergence register (full §2 table) + the manual sweep

Every StoatFlow ↔ Kafka Streams difference a port touches, tagged with its register id (`KSC-NN` / `D-N`).
The OpenRewrite recipe (SKILL.md §1) mechanizes the imports and flags the rest; when porting by hand, work
this table against your compile errors. Sourced from the StoatFlow porting guide — the authoritative
upstream is the register (ADR-118) + the compatibility matrix.

## The manual import sweep (fallback for §1)

```bash
# Order matters: rewrite the longer/more-specific prefixes first.
find src -name '*.java' -o -name '*.kt' | xargs sed -i '' \
  -e 's#org\.apache\.kafka\.streams\.kstream\.#io.stoatflow.core.topology.#g' \
  -e 's#org\.apache\.kafka\.streams\.state\.#io.stoatflow.core.state.#g' \
  -e 's#org\.apache\.kafka\.streams\.processor\.api\.#io.stoatflow.core.processor.#g' \
  -e 's#org\.apache\.kafka\.streams\.processor\.#io.stoatflow.core.processor.#g' \
  -e 's#org\.apache\.kafka\.streams\.errors\.#io.stoatflow.core.exception.#g' \
  -e 's#org\.apache\.kafka\.streams\.StreamsBuilder#io.stoatflow.core.topology.StreamsBuilder#g' \
  -e 's#org\.apache\.kafka\.streams\.TopologyConfig#io.stoatflow.core.config.TopologyConfig#g' \
  -e 's#org\.apache\.kafka\.streams\.Topology#io.stoatflow.core.topology.Topology#g' \
  -e 's#org\.apache\.kafka\.streams\.KeyValue#io.stoatflow.core.state.KeyValue#g' \
  -e 's#org\.apache\.kafka\.streams\.CloseOptions#io.stoatflow.core.CloseOptions#g'
```

A raw FQN left pointing at `org.apache.kafka.streams.*` after the sweep is **expected** for the handful of
types StoatFlow renames by class (below) — fix those by hand.

## §2 — Residual manual fixes (the intentional divergences)

| Area | Kafka Streams | StoatFlow | Fix |
|---|---|---|---|
| **Application entry point** (KSC-86) | `new KafkaStreams(topology, props)` | `StoatFlow.fromBuilder(new StreamsConfig(props), builder)` — the ctor captures the processor + state-store registries the `StreamsBuilder` accumulated; a bare `Topology` can't supply them | Pass the **builder**, not the topology. `StoatFlow.fromBuilder` is `@JvmStatic` (Java calls it on `StoatFlow`). The recipe rewrites it when the builder is in the same method |
| **`StreamsConfig` / `Properties`** (KSC-42 → KSC-55 → ADR-124) | `Properties` keyed by `StreamsConfig.*_CONFIG`; serdes by class name | `StreamsConfig.fromProperties(props).build()` / `fromMap(map).build()` cover the full KS-4.3.0 overlap (74-key map); the typed `Builder` stays primary | **Usually no change:** a KS-keyed `Properties` carries over; `new TopologyTestDriver(topology, props)` works; **`new StreamsConfig(props)` / `(map)` also compile (KSC-79)**. Recognised-but-inapplicable KS keys WARN and are ignored; unknown keys WARN (throw under `stoatflow.config.strict`). For engine knobs use the typed `Builder` or YAML |
| **`TopologyConfig` / `processor.wrapper.class`** (KSC-77, ADR-118 Batch-14) | `new StreamsBuilder(new TopologyConfig(config))`; `processor.wrapper.class` (KIP-1112) | `TopologyConfig` → `io.stoatflow.core.config.TopologyConfig` (projects the build-time defaults **and the `ProcessorWrapper`**; not a Kafka `AbstractConfig`). `processor.api.{ProcessorWrapper,WrappedProcessorSupplier,WrappedFixedKeyProcessorSupplier}` → `io.stoatflow.core.processor.*` via the §1 sweep | **Usually no change** — the wrapper compiles and runs. Check: internal-node **replacement** is rejected at build time (decorate/observe/short-circuit instead); `init`/watermark/suppress-flush/restore callbacks on DSL nodes bypass the wrapper and a DSL-node decorator's context cannot `schedule()`/`timerService()` (keyed timers are Processor-API-only, so `onTimer` is never bypassed); `get()` runs once per **lane** (not per task) so decorator fields are lane-scoped; a replacing supplier must forward `declaredTimerTypes()`; node names are StoatFlow's, so name matching finds nothing. 🆕 Honoured even when set only on the runtime config (KS ignores that case) |
| **`processing.guarantee` default** ⚠️ (KSC-84) | absent ⇒ **`at_least_once`** | absent ⇒ **`EXACTLY_ONCE`** — `fromMap`/`fromProperties` do **not** inject the KS default | **A ported app that relied on the KS default silently runs under EOS.** Stronger, but it changes transactional-id usage, broker requirements, throughput. For ALO set `processing.guarantee=at_least_once` explicitly. `exactly_once`/`exactly_once_beta` are lenient aliases of `exactly_once_v2` |
| **Exception-handler signature** (KSC-01) | `handle(ErrorHandlerContext, record, Exception)` | Same order — but `ProcessingExceptionHandler` takes a `processor.Record<*,*>`, not separate key/value | Read `record.key()`/`record.value()`; downcast the context to `Deserialization`/`Processing`/`ProductionContext` for `failedOn` if needed |
| **Production serialization errors** (KSC-01) | `handleSerializationException(...)` (separate method) | Folded into `handle(...)` | Branch on `(context as ProductionContext).failedOn` for `KEY_SERIALIZATION`/`VALUE_SERIALIZATION` |
| **Transformer `init`** (KSC-15) | `ValueTransformerWithKey.init(ProcessorContext)` | `init(FixedKeyProcessorContext)` | Change the param type; `context.getStateStore(name)` is identical |
| **`VersionedRecord.validTo()`** (KSC-04) | `long validTo` getter | `validTo(): Optional<Long>` | Use `Optional` (`.orElse(...)` / `.isPresent()`) |
| **`Joined` serdes + grace** (D5) | honored | **advisory** — resolved from `Consumed`/`Grouped` upstream | No code change; just be aware |
| **`null` on optional builder args** (KSC-59) | `null` accepted (= "use default") | now accepts `null` too (serdes, grace, partitioner, offset-reset, name) | No change. (`Named.withName`/`as` still require non-null) |
| **`Consumed.withTimestampExtractor`** (KSC-61) | source-level `TimestampExtractor` | **supported** — adapted to a per-source watermark strategy; the extractor gets the real `ConsumerRecord` (CR3-CORE3-005) | No change. For bounded-out-of-orderness/idleness use `Consumed.withWatermarkStrategy(...)` |
| **`taskId()`** (D6 / KSC-02) | real task id | always `TaskId.SINGLE` (`0_0`) | Don't branch on task id — StoatFlow is single-instance |
| **`currentStreamTimeMs()`** (KSC-85) | max observed record timestamp | the event-time **watermark** (global min − out-of-orderness; `currentWatermarkMs()` is the native name) | No change; be aware it is generally **lower** and **global**. `currentSystemTimeMs()` / `isLate()` unchanged |
| **`KStream.transform*`** (KSC-15) | removed in KS 4.x | absent | Use `process` / `processValues`, or `KTable.transformValues` |
| **`JoinWindows.beforeMs`/`afterMs`** (KSC-27) | public final **fields** | Kotlin `val` → `getBeforeMs()`/`getAfterMs()` | Java: replace field access with the getter. Kotlin callers unaffected |
| **`persistentVersionedKeyValueStore(name, retention, segmentInterval)`** (KSC-20) | 3rd arg sizes segments | accepted but **ignored** + WARN | No change; drop the arg to silence the WARN |
| **`StreamsUncaughtExceptionHandler` responses** (KSC-22) | `REPLACE_THREAD` swaps the failed thread | `REPLACE_THREAD` ⇒ bounded **in-place engine restart** (ADR-127); budget-limited (`maxEngineRestarts`/`engineRestartWindowMs`, default 5/5 min), exhaustion ⇒ `SHUTDOWN_APPLICATION`; under HA, faults fail over. `SHUTDOWN_CLIENT`/`SHUTDOWN_APPLICATION` ⇒ graceful instance shutdown | No change; the handler still runs. Watch `stoatflow.engine.restart.total{trigger="replace_thread"}` |
| **Custom `*BytesStoreSupplier` into `Materialized.as`/`StreamJoined`** (KSC-21) | any KS supplier accepted | only `Stores.*` suppliers accepted (require-guard) | Build via `Stores.*` factories; hand-rolled `*BytesStoreSupplier` impls throw `IllegalArgumentException` |
| **`Stores.*WithHeadersBuilder(supplier, …)`** (KSC-26) | plain byte-store supplier | requires a `Stores.*WithHeaders(...)` supplier | Pass a header-aware supplier, e.g. `Stores.persistentSessionStoreWithHeaders(...)` |
| **`StreamJoined.withThisStoreSupplier`/`withOtherStoreSupplier`** (KSC-18) | caller's responsibility | must be `retainDuplicates` for correct multi-record-per-`(key,ts)` matching | Pass `Stores.persistentWindowStore(name, retention, windowSize, retainDuplicates = true)`; the default join store already is |
| **IQ range scans over a `retainDuplicates` (join) window store** | `fetchAll`/`all`/`fetch(from,to)` return every duplicate | only **single-key** `fetch` returns duplicates; range/`all`/`fetchAll` collapse same-`(key,ts)` | Don't rely on range/`all`/`fetchAll` for duplicate counts; iterate per key. Joins themselves unaffected |
| **`StreamPartitioner` package** (KSC-34) | `…streams.processor.StreamPartitioner` | `io.stoatflow.core.topology.StreamPartitioner` (in `topology/`) | The blind swap mis-routes it — rewrite by hand (the recipe does it). `partitions(): Optional<Set<Integer>>` unchanged |
| **Other prefix-sweep mis-routes** (KSC-87) | `…processor.{StateStore, StateStoreContext, StateRestoreListener, TimestampExtractor, TopicNameExtractor, RecordContext, TaskId}`, `…state.RocksDBConfigSetter` | `io.stoatflow.core.state.{StateStore, StateStoreContext, StateRestoreListener}`, `io.stoatflow.core.topology.{TimestampExtractor, TopicNameExtractor, RecordContext}`, `io.stoatflow.core.config.TaskId`, `io.stoatflow.core.state.rocksdb.RocksDBConfigSetter` | The §1 sweep sends these to a package that doesn't exist. The recipe rewrites all of them; by hand, fix the eight imports. `StateStoreContext` has no `appConfigs()`/`register()`; `RecordContext` accessors are Kotlin properties (nullable for internally generated records) |
| **`processor.api.ProcessingContext` / `RecordMetadata`** (KSC-87) | KIP-820 context supertype; `recordMetadata()` yields topic/partition/offset | **absent** — no `recordMetadata()`, no public `RecordMetadata` | Source topic/partition/offset aren't reachable from a processor. **Trap:** `io.stoatflow.core.exception.ProcessingContext` exists but is an unrelated `ErrorHandlerContext` — do not import it here |
| **`StreamPartitioner` repartition multicast** (KSC-34) | >1 partition honoured on `repartition()` | **throws** `UnsupportedOperationException` on the in-memory repartition/lane path. Sink `to()`/`addSink()` multicast is honoured | Don't multicast on `repartition()`; multicast only on sinks |
| **`Topology.describe()` repoint** (KSC-31) | one model | `describe()` returns the **KS** `TopologyDescription`; the native model moved to `describeStoatFlow()` | KS/KSML code is unaffected; native callers switch to `describeStoatFlow()` |
| **`Suppressed.BufferConfig` nesting** (KSC-35) | nested config types | same — but `maxBytes`/`withMaxBytes` **throw** (in-memory) | No change for `maxRecords`/`unbounded`; avoid `maxBytes` |
| **IQ on an unknown store** (KSC-69) | throws `UnknownStateStoreException` | **same now** | No change (KS parity). `catch (UnknownStateStoreException)` or base `InvalidStateStoreException` |
| **`TopologyTestDriver` + caching** (KSC-63) | commits per `pipeInput` | **same now** | See `stoatflow-test` — if a test asserted a single deduped emission after a manual commit, expect per-pipe emissions |
| **Null `range`/`reverseRange` bounds (KIP-763)** (KSC-89) | null = open-ended (both null ≡ `all()`); `from > to` → empty + WARN | **same now** — was NPE / IAE | No change (KS parity). A non-null bound whose serializer returns null bytes also coerces to open-ended + WARN (KS `Bytes.wrap(null)` parity) |
| **`reverseRange` bounds order** (KSC-90) | ascending (low, high), like `range` | **same now** — was (high, low) | Ported KS code: no change. StoatFlow-native code written before 2026-07-31 must flip its `reverseRange` arguments to (low, high) — the old order now returns empty + WARN |
| **Null window/session key-range bounds (KIP-763)** (KSC-91) | null `keyFrom`/`keyTo` = open-ended on window `fetch`/`backwardFetch` + session `findSessions`/`fetch`/`backward*` | **same now** — was NPE | No change (KS parity). `keyFrom > keyTo` → empty + WARN; backward variants were always (low, high) |
| **Swapping KafkaStreams ↔ StoatFlow** (KSC-67) | n/a | `StoatFlow` implements `io.stoatflow.core.StreamProcessingApp` (StoatFlow-typed) | Program against `StreamProcessingApp`; for a KS backend write a ~10-line adapter mapping `KafkaStreams.State → StoatFlow.State`. The interface also declares `close(Duration)` / `close(io.stoatflow.core.CloseOptions)` (KSC-83) — the adapter delegates both to `KafkaStreams.close(...)` |
| **`KafkaStreams.State`** (KSC-66/81) | 7-state enum | `StoatFlow.State` has a **richer** set; map via `io.stoatflow.core.KafkaStreamsState` (`toKafkaStreamsState()` / `StoatFlow.kafkaStreamsState()`) | For KS-verbatim state logic, map once at the listener boundary. `DRAINING→RUNNING`, `PAUSED→REBALANCING` |
| **`setStateListener`** (KSC-82) | `StateListener` | single `setStateListener(StateListener)` (the `BiConsumer` overload was removed) | A method-ref/lambda resolves with no cast; a `BiConsumer<State,State>` migrates via `StateListener.of(biConsumer)` |
| **Custom `Processor` state read from a punctuator** (CR2-CORE5-002) | one task instance per partition; `process` + punctuator share fields | one instance **per key-affinity lane**; a punctuator fires once over global state on a different instance and **cannot see** `process()` fields | Move cross-record state into a **state store** (punctuators have full read/write). For a scheduled write to one key, prefer a **timer** (`onTimer`) over a punctuator |
| **Kotlin lambda over a null-key topic** (CR2-CORE3-001) | (Kotlin-language artifact — fires on KS too) | a Kotlin `map { k, v -> }` / `filter` / `selectKey` over a null-key topic crashes on the inserted `checkNotNullParameter` | Declare the key param **nullable**: `map { _: K?, v -> … }`. Aggregations (`groupBy(...).count/reduce/aggregate`, KSC-72) + the stream–table INNER join already drop null-key records (no change there) |

## §3 — See `compat-notes.md`

for the do-not-change list (the DSL, functional interfaces, windowed `Materialized`, sliding windows,
state-store types, IQ metadata, test-utils) and the compat-matrix pointers.
