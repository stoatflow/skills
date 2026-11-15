# Kafka Streams config → StoatFlow

StoatFlow keeps a **typed** config model (`StreamsConfig` data class + `Builder`) as canonical, plus a
**complete Kafka Streams adapter skin** so a KS-keyed `Properties`/`Map` carries over (ADR-124).

## The adapter

- **Entry points:** `StreamsConfig.fromProperties(props).build()` / `StreamsConfig.fromMap(map).build()`.
  The KS-faithful ctors `new StreamsConfig(props)` / `new StreamsConfig(map)` (KSC-79) delegate to
  `fromMap(...).build()`. A missing `application.id` throws `IllegalArgumentException` (KS throws
  `ConfigException`). `application.server` is validated at construction (`host:port`, `protocol://host:port`,
  `[ipv6]:port` or blank) and a malformed value throws `IllegalArgumentException` — Kafka Streams 4.4 does
  the same (with `ConfigException`). `StreamsConfig.toProperties()` emits the resolved, masked config back as KS-keyed
  `Properties` (for `/config` / diffing).
- **Coverage — the 79-key KS-4.4.0 overlap:** typed mappings, class-name pluggables (serdes, the three
  exception handlers, `default.timestamp.extractor`, `default.client.supplier`, `rocksdb.config.setter`), and client prefixes
  (`consumer.` / `main.consumer.` / `restore.consumer.` / `producer.` / `admin.`). The KS `*_CONFIG` symbol
  constants + prefix helpers (`consumerPrefix(...)` … `adminClientPrefix(...)`) are shipped for symbol-level
  source compatibility (KSC-80).
- **Unprefixed (bare) client keys route like KS (KSC-92).** A bare key reaches every client whose own
  `configNames()` contains it — the validity filter KS uses, not a blanket copy. So a top-level
  `security.protocol` / `ssl.*` / `sasl.*` reaches the consumer, producer **and** admin; a top-level `acks`
  reaches only the producer; `max.poll.records` only the consumer. Precedence is KS's:
  `main.consumer.X` > `consumer.X` > `X` (`producer.X` > `X`, `admin.X` > `X` likewise), and StoatFlow's
  forced overrides still win over all of them. **Do not tell a user to add a prefix to make security work
  — the bare spelling is supported.** Note `main.consumer.` is a **scope**, not an alias for `consumer.`:
  it reaches the main consumer only, and is not inherited by the restoration consumer or by the
  producer/admin security passthrough. **Eight** client keys stay deliberate no-ops and are *not* routed:
  `client.id` (StoatFlow derives client ids from `application.id` — use `consumer.`/`producer.`/`admin.`
  `client.id`), `group.protocol` (membership is managed internally), `config.providers`, and the Kafka-client
  metrics family `metric.reporters` / `metrics.recording.level` / `metrics.sample.window.ms` /
  `metrics.num.samples` / `enable.metrics.push` (StoatFlow reports through Micrometer — a prefixed spelling
  routes normally if the user wants the clients' own metrics or KIP-714 telemetry push).
- **Unknown-key policy (3-tier):** *mapped* / *recognised-no-op* (single-instance / Micrometer-metrics keys
  WARN and are ignored) / *unknown* — a key that is not a Kafka client config either is forwarded to every
  client as a custom property (KS `getClientCustomProps` parity). StoatFlow lists them in one startup WARN;
  each client additionally reports what it does not recognise as a single aggregate **INFO** line (that is
  kafka-clients' `logUnused`, and it does not run at all under `TopologyTestDriver`). Opt-in
  **`stoatflow.config.strict`** makes those truly-unknown keys **throw** instead — the documented opt-out
  from custom-prop forwarding. Recognised KS keys and client keys never throw. Default is lenient.
- **`topology.description.push.enabled`** (KS 4.4, KIP-1331) is a recognised no-op: the push needs the streams
  group protocol and StoatFlow is a classic consumer group. It WARNs once and is dropped (never forwarded, never
  throws under strict). The same topology description is a pull from `GET /topology/ks` (KSC-113).
- **The three KIP-892 keys** (KS 4.4): `statestore.uncommitted.max.bytes` maps to `stateStoreUncommittedMaxBytes`
  (YAML `stoatflow.state.uncommitted-max-bytes`), honoured as given — the default stays 256 MiB (KS: 64 MiB), the
  budget is application-wide, under exactly-once it also counts the output held back during a commit, and `-1` (KS
  "disable") is refused because the budget also caps the epoch size (KSC-105).
  `default.interactive.query.isolation.level` maps to `interactiveQueryIsolationLevel` (YAML
  `stoatflow.state.interactive-query-isolation-level`; `READ_UNCOMMITTED` default or `READ_COMMITTED`, case-insensitive;
  anything else throws) — `store()` applies it to every store, global stores included (KSC-144).
  `enable.transactional.statestores` is a recognised no-op: `true` is silent, `false` WARNs that it cannot be honoured
  — StoatFlow's DSL-selectable stores are always transactional (KSC-104). Constants on `StreamsConfig`:
  `TRANSACTIONAL_STATE_STORES_CONFIG`, `STATESTORE_UNCOMMITTED_MAX_BYTES_CONFIG`,
  `DEFAULT_INTERACTIVE_QUERY_ISOLATION_LEVEL_CONFIG`.
  `metadata.cluster.check.enable` (KS 4.4) is a client key and routes to all three clients like any bare key.

- **`processor.wrapper.class` (KIP-1112)** is supported (ADR-118 Batch-14): the named
  `io.stoatflow.core.processor.ProcessorWrapper` is instantiated at ingestion and handed the raw config map
  via `configure(...)`, then wraps **every** topology node at compile time. Either no-op FQCN — KS's
  `org.apache.kafka.streams.processor.internals.NoOpProcessorWrapper` or StoatFlow's
  `io.stoatflow.core.processor.NoOpProcessorWrapper` — counts as "unset". 🆕 Unlike KS, it is honoured even
  when set only on the runtime config (KS requires a `TopologyConfig` and silently ignores it otherwise); a
  `TopologyConfig`-provided wrapper wins on conflict, with a WARN when the classes differ. Typed equivalent:
  `Builder.processorWrapper(wrapper)` or `Builder.processorWrapperClassName(className)` (the latter calls
  `configure(emptyMap())`; it is a separately named method so the Java call `processorWrapper(null)` stays
  unambiguous). Caveats live in the `stoatflow-build-topology` processor-API reference.
- **`default.timestamp.extractor`** is supported: a global fallback `TimestampExtractor` for sources whose
  `Consumed` carries no watermark strategy (a `Class` or a class name; KS's built-ins are
  `io.stoatflow.core.processor.{FailOnInvalidTimestamp, LogAndSkipOnInvalidTimestamp,
  UsePartitionTimeOnInvalidTimestamp, WallclockTimestampExtractor}`, and their KS names
  `org.apache.kafka.streams.processor.*` — string or `Class` — load those). **Unset by default** — the Kafka record
  timestamp is used and a negative one is a valid event time, where KS defaults to `FailOnInvalidTimestamp` and
  refuses it (KSC-138). Set it to `FailOnInvalidTimestamp` or `LogAndSkipOnInvalidTimestamp` to keep KS's refusal.
  Typed equivalent: `Builder.defaultTimestampExtractor(extractor | className)`; no YAML key.
- **`num.stream.threads`** maps to the lane count (`numLanes`; default `max(2, CPU cores / 4)` per sub-topology).
  The lanes run on `PLATFORM` threads by default; the StoatFlow-only key **`lanes.thread-type`** (`PLATFORM` |
  `VIRTUAL`, case-insensitive; YAML `stoatflow.lanes.thread-type`, `Builder.laneThreadType(...)`) selects
  `VIRTUAL` for many lanes doing blocking I/O. Kafka Streams has no such key — its stream threads are OS threads.

So a ported KS `Properties` usually needs **no change**; for StoatFlow-only engine knobs use the typed
`Builder` or `application.yaml`.

## Kafka client config — the layered merge

`stoatflow.kafka.consumer` is **Layer 3** of the consumer's 5-layer merge (each layer overrides the
previous):

1. Kafka client defaults → 2. StoatFlow framework defaults → 3. **your** `stoatflow.kafka.consumer`
   → 4. `main.consumer.` overrides (KS-`Properties` only) → 5. forced overrides (cannot be overridden).

The **producer** is a 5-layer variant: 1. Kafka client defaults → 2. StoatFlow framework defaults →
3. the consumer's `security.*` / `ssl.*` / `sasl.*` subset → 4. **your** `stoatflow.kafka.producer` →
5. forced overrides. Layer 4 overrides Layer 3 **per key**, so a producer block that shadows a security
family only partially inherits the rest and can fail producer construction — shadow a family completely or
not at all. The admin and restoration-consumer blocks inherit the same way; StoatFlow warns when it sees a
partial shadow in any of the three.

The **main consumer** is likewise 5 layers: the `main.consumer.` overrides sit between
`stoatflow.kafka.consumer` and the forced layer, and are scoped to that client alone.

The **restoration consumer** is also 5 layers (inherits `stoatflow.kafka.consumer` as a baseline, then
restoration framework defaults, then `stoatflow.kafka.restoration-consumer`, then forced overrides).
**Caveat:** its Layer-3 defaults sit above the inherited baseline, so `max.poll.records`,
`fetch.max.bytes` and `max.partition.fetch.bytes` set on the processing consumer do **not** reach it —
set those under `restoration-consumer` too. Security and everything else inherit normally.

The **admin client** merges `bootstrap.servers` → per-site defaults (timeouts) → the consumer's security
subset → `stoatflow.kafka.admin` (which wins).

```yaml
stoatflow:
  kafka:
    consumer:
      max.poll.records: 1000
      fetch.min.bytes: 65536
    producer:
      batch.size: 524288
      linger.ms: 50
      compression.type: lz4
    restoration-consumer:
      max.poll.records: 5000
    admin:
      # only when the admin principal differs from the consumer's
      request.timeout.ms: 30000
```

**StoatFlow framework defaults (overridable — Layer 2):**

- Consumer: `max.poll.records` 500, `fetch.max.bytes` 50 MiB, `max.partition.fetch.bytes` 10 MiB (Kafka 1
  MiB), `auto.offset.reset` `earliest` (matches KS; while a source has its own `Consumed` reset policy — a
  `globalTable` always — the consumer runs `none` and StoatFlow resets a partition it cannot position, no committed
  offset or one out of range, by the source's policy, else this value, as KS's `StreamThread` does; a log truncated by
  an unclean leader election resumes at the divergence point, never reset),
  `max.poll.interval.ms` 10 min (Kafka 5 min),
  `partition.assignment.strategy` `CooperativeStickyAssignor` (Kafka's default list runs eager Range; under it
  a topic newly matching a pattern source or a partition-count increase terminated the single instance —
  ADR-143; overriding to an eager assignor logs a startup WARN). Conditional: a topology with a pattern
  source — `stream(Pattern)` or Processor-API `addSource(…, Pattern)` — sets `metadata.max.age.ms` 30 s (Kafka
  5 min) unless you set it; `/config` shows the effective value.
- Producer: `batch.size` 256 KiB, `linger.ms` 20, `retry.backoff.ms` 50, `buffer.memory` 256 MiB,
  `enable.idempotence` true, `acks` all, `max.in.flight.requests.per.connection` 5, `compression.type` zstd
  (KS sets no codec; outputs, changelogs and dead letters all go through this producer).

**Forced overrides (the last layer — you cannot change these):**

- Consumer: `bootstrap.servers`, `group.id` = applicationId, `group.instance.id` = applicationId (KIP-345;
  **omitted under HA** `assign()`), `enable.auto.commit=false`, `allow.auto.create.topics=false`, `bootstrap.resolve.timeout.ms=0`; **EOS only:** `isolation.level=read_committed`
  (KSC-93 — an EOS app must not ingest a transactional upstream's aborted batches; under ALO the key is
  user-settable).
- Producer (always): `bootstrap.servers`, `retries=Integer.MAX_VALUE`, `max.block.ms` = barrier timeout,
  `delivery.timeout.ms` = barrier timeout, `bootstrap.resolve.timeout.ms=0`.
- Admin client: `bootstrap.resolve.timeout.ms=0`. On every client this pins synchronous bootstrap DNS
  resolution (Kafka Streams 4.4 forces the same, KAFKA-20939): an asynchronously resolving client whose
  resolution fails stays broken until rebuilt.
- Producer (**EOS only**): `transactional.id` = `{applicationId}-producer`, `transaction.timeout.ms` =
  barrier timeout, `enable.idempotence` true, `acks` all, `max.in.flight.requests.per.connection` 5. (Under
  ALO these last three revert to the overridable framework defaults.)
- Restoration consumer: `group.id` = `{applicationId}-restoration-{timestamp}`, `enable.auto.commit=false`, `allow.auto.create.topics=false`,
  `bootstrap.resolve.timeout.ms=0`, `auto.offset.reset=none` (as Kafka Streams' restore consumer), `isolation.level` =
  `read_committed` under EOS only (under ALO the `consumer.` / `restore.consumer.` value, default `read_uncommitted`;
  a `main.consumer.` key does not reach it — Kafka Streams parity).

**Security: set it once, on the consumer.** `security.*` / `ssl.*` / `sasl.*` under
`stoatflow.kafka.consumer` propagate to **every** other client StoatFlow builds — the producer (KSC-92),
the internal admin clients, the restoration consumers and the HA coordination-topic clients. Only repeat them
on another client's own map when that client authenticates as a different principal; that map always wins. The
HA coordination-topic clients follow the same maps (producer → `producer`, admin → `admin`, consumer →
`restoration-consumer`, as it reads the topic like a restoration consumer), each with its counterpart's
`client.dns.lookup` too.

A ported KS `Properties` that spells security **unprefixed** works too — see the bare-key routing bullet
above. YAML has no unprefixed form (the nesting *is* the prefix), which is exactly why the producer
inherits the consumer's subset.
