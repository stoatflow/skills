# Kafka Streams config → StoatFlow

StoatFlow keeps a **typed** config model (`StreamsConfig` data class + `Builder`) as canonical, plus a
**complete Kafka Streams adapter skin** so a KS-keyed `Properties`/`Map` carries over (ADR-124).

## The adapter

- **Entry points:** `StreamsConfig.fromProperties(props).build()` / `StreamsConfig.fromMap(map).build()`.
  The KS-faithful ctors `new StreamsConfig(props)` / `new StreamsConfig(map)` (KSC-79) delegate to
  `fromMap(...).build()`. A missing `application.id` throws `IllegalArgumentException` (KS throws
  `ConfigException`). `StreamsConfig.toProperties()` emits the resolved, masked config back as KS-keyed
  `Properties` (for `/config` / diffing).
- **Coverage — the ~74-key KS-4.3.0 overlap:** typed mappings, class-name pluggables (serdes, the three
  exception handlers, `default.client.supplier`, `rocksdb.config.setter`), and client prefixes
  (`consumer.` / `main.consumer.` / `restore.consumer.` / `producer.` / `admin.`). The KS `*_CONFIG` symbol
  constants + prefix helpers (`consumerPrefix(...)` … `adminClientPrefix(...)`) are shipped for symbol-level
  source compatibility (KSC-80).
- **Unknown-key policy (3-tier):** *mapped* / *recognised-no-op* (single-instance / Micrometer-metrics keys
  WARN and are ignored) / *unknown* (WARN). Opt-in **`stoatflow.config.strict`** throws only on
  truly-unknown keys. Default is lenient — unknown keys warn, don't fail.

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

So a ported KS `Properties` usually needs **no change**; for StoatFlow-only engine knobs use the typed
`Builder` or `application.yaml`.

## Kafka client config — the 4-layer merge

`stoatflow.kafka.consumer` / `.producer` are **Layer 3** of a 4-layer merge (each layer overrides the
previous):

1. Kafka client defaults → 2. StoatFlow framework defaults → 3. **your** `stoatflow.kafka.{consumer,producer}`
   → 4. forced overrides (cannot be overridden).

The **restoration consumer** is a 5-layer variant (inherits `stoatflow.kafka.consumer` as a baseline, then
restoration framework defaults, then `stoatflow.kafka.restoration-consumer`, then forced overrides).

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
```

**StoatFlow framework defaults (overridable — Layer 2):**

- Consumer: `max.poll.records` 500, `fetch.max.bytes` 50 MiB, `max.partition.fetch.bytes` 10 MiB (Kafka 1
  MiB), `auto.offset.reset` `earliest` (matches KS), `max.poll.interval.ms` 10 min (Kafka 5 min).
- Producer: `batch.size` 256 KiB, `linger.ms` 20, `retry.backoff.ms` 50, `buffer.memory` 256 MiB,
  `enable.idempotence` true, `acks` all, `max.in.flight.requests.per.connection` 5.

**Forced overrides (Layer 4 — you cannot change these):**

- Consumer: `bootstrap.servers`, `group.id` = applicationId, `group.instance.id` = applicationId (KIP-345;
  **omitted under HA** `assign()`), `enable.auto.commit=false`.
- Producer (always): `bootstrap.servers`, `retries=Integer.MAX_VALUE`, `max.block.ms` = barrier timeout,
  `delivery.timeout.ms` = barrier timeout.
- Producer (**EOS only**): `transactional.id` = `{applicationId}-producer`, `transaction.timeout.ms` =
  barrier timeout, `enable.idempotence` true, `acks` all, `max.in.flight.requests.per.connection` 5. (Under
  ALO these last three revert to the overridable framework defaults.)
- Restoration consumer: `group.id` = `{applicationId}-restoration-{timestamp}`, `enable.auto.commit=false`,
  `auto.offset.reset=earliest`, `isolation.level` = `read_committed` (EOS) / `read_uncommitted` (ALO).

Security is plain passthrough: `security.*` / `ssl.*` / `sasl.*` on the consumer also propagate to the
internal admin clients.
