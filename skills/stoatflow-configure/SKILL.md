---
name: stoatflow-configure
description: Configure a StoatFlow application — application.yaml (stoatflow.* / runtime.* keys), the typed StreamsConfig builder, or a Kafka Streams Properties/StreamsConfig adapter. Processing guarantee, lanes, commit barriers, state/memory/RocksDB, changelog, watermarks, HA, licence, the Kafka client config merge, and the runtime HTTP/metrics/health surface. Use for StoatFlow configuration; for build/dependency setup use stoatflow-project-setup.
license: Apache-2.0
---

# Configure a StoatFlow application

Read `references/stoatflow-primer.md` first. **Do not invent config keys** — every accepted key is in
`references/config-reference.md` (generated from the published JSON schema
`https://stoatflow.io/schemas/stoatflow-config-schema.json`). An unknown key WARNs, or throws under
`stoatflow.config.strict`.

## Two configuration surfaces

1. **`application.yaml`** (with `:stoatflow-runtime`) — three top-level groups: **`stoatflow:`** (engine),
   **`runtime:`** (HTTP/metrics/health wrapper), **`logging:`** (per-logger levels). `${VAR:-default}`
   interpolation is resolved at load. Only `stoatflow.application-id` is required.
2. **Typed `StreamsConfig.Builder`** (with `:core`) — `StreamsConfig.builder(applicationId, bootstrapServers)`
   + fluent setters, or `streamsConfigOverrides { … }` in the runtime builder. A Kafka Streams
   `Properties`/`Map` adapts in via `StreamsConfig.fromProperties(props).build()` / `fromMap(map).build()`
   — see `references/ks-config-mapping.md`. The timer backend (`timerBackend(TimerBackend.TwoTier | RocksDB |
   InMemory)`, YAML `stoatflow.timer-backend`) has no Properties/Map key: chain `.timerBackend(...)` after
   `fromProperties(props)`; in `streamsConfigOverrides` it replaces the whole YAML `timer-backend` block.

```yaml
stoatflow:
  application-id: order-processor          # REQUIRED
  bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}
  processing-guarantee: EXACTLY_ONCE       # EXACTLY_ONCE (default) | AT_LEAST_ONCE
  lanes:
    count: 4                               # default: max(2, CPU cores / 4) per sub-topology
  state:
    dir: /var/lib/stoatflow/state
  rocks-db:
    preset: DEFAULT                        # DEFAULT | LOW_MEMORY | HIGH_PERFORMANCE
  licence:
    key: ${STOATFLOW_LICENCE_KEY}
runtime:
  http: { enabled: true, port: ${HTTP_PORT:-8080} }
  metrics: { enabled: true }
```

## The config areas (full keys in references/config-reference.md)

- **`stoatflow.processing-guarantee`** — `EXACTLY_ONCE` (default; atomic output+changelog+offsets) or
  `AT_LEAST_ONCE` (non-transactional, lower latency, duplicates possible on crash). This is the KSC-84
  divergence — KS defaults to at-least-once.
- **`stoatflow.lanes`** — `count` (virtual-partition parallelism per sub-topology, default `max(2, CPU cores / 4)`;
  **not** Kafka partitions; raise it when a CPU-bound sub-topology lags while the CPU is idle), `queue-capacity`
  (300), `thread-type` (`PLATFORM` default | `VIRTUAL` — many lanes for blocking I/O; engine-wide, a WARN at
  startup for more `PLATFORM` lanes than cores).
- **`stoatflow.commit-barrier`** — self-tuning barrier cadence: `interval-ms` (500 seed),
  `min-interval-ms`/`max-interval-ms` (150/5000), `timeout-ms` (60000; **must be > interval-ms**; cascades
  to producer `transaction.timeout.ms`/`max.block.ms`/`delivery.timeout.ms`). Plus EMA/epoch-sizing knobs.
- **`stoatflow.state`** + **`stoatflow.rocks-db`** — `state.dir`, `state.uncommitted-max-bytes` (256 MiB; KS's
  default is 64 MiB), `state.interactive-query-isolation-level`
  (`READ_UNCOMMITTED` default / `READ_COMMITTED` — what `store()` reads, KIP-892), `rocks-db.preset` (one block
  cache shared by every store, memtables charged inside it: `LOW_MEMORY` 32 MiB / `DEFAULT` 128 MiB /
  `HIGH_PERFORMANCE` 512 MiB — a budget, not a hard limit; KS gives each store its own), `rocks-db.backend` (`AUTO`/`FFM`/`JNI`),
  `rocks-db.metrics`, `state.format-downgrade` (`refuse` default | `wipe-and-restore` — see below).
- **`stoatflow.dsl`** — `store-format` (`DEFAULT` | `HEADERS`): the global default for whether DSL-materialized
  stores (tables, `KTable` operators, table-table and FK join results, aggregations) persist the processing record's `Headers`
  (KIP-1271/1285). Per-store
  `Materialized.withRecordHeaders()` / `withoutRecordHeaders()` always wins. Turning headers **on** is a
  free lazy in-place upgrade; turning them **off** over a store that already carries them is a *declared
  format change* — startup refuses with an actionable message unless you set
  `state.format-downgrade: wipe-and-restore` (which rebuilds that store from its changelog: one full
  restore). An empty headers keyspace is dropped in place for free; a store with no recovery source
  (`withLoggingDisabled()`, or the changelog globally off) is refused regardless. A downgrade sheds
  persisted headers, and re-enabling headers does **not** bring them back.
- **`stoatflow.changelog`** — `enabled`, `replication-factor` (-1 = broker default), `num-partitions` (3;
  per-store override via `Materialized.withChangelogNumPartitions(n)` and its siblings — prefer small
  counts, and note the count is create-time immutable), `timer-num-partitions` (-1 = inherit). With
  `create-topics-if-not-exist: false`, pre-create the engine's own changelogs too — `describe()` does not list
  them: `<app>-stoatflow-watermark-changelog` (1 partition; any windowed aggregation, time-based `suppress` or
  stream-stream join), `<app>-<store>-emitfrontier-changelog` per `onWindowClose()` aggregation,
  `<app>-timer-store-changelog` when a processor declares timer types (`declaredTimerTypes()`) on a persistent timer
  backend with its changelog on.
- **`stoatflow.watermark`** — `max-out-of-orderness-ms` (10000), `idleness-timeout-ms`, `auto-interval-ms`.
  `idleness-timeout-ms` is the **default** for sources that set no `WatermarkStrategy.withIdleness(...)` of
  their own; a per-source timeout wins for that source's partitions. `0` disables idleness only for the
  sources that inherit the default, and the start-up WARN names those sources.
- **`stoatflow.ha`** — `mode` (`off` default | `active-standby`), `acceptable-recovery-lag`,
  `desired-standbys`/`max-standbys`, `failover-priority`, coordination/heartbeat knobs (see
  `stoatflow-operate`).
- **`stoatflow.kafka.{consumer,producer,restoration-consumer,admin}`** — freeform client passthrough (the
  user-override layer of each client's merge; see `references/ks-config-mapping.md` for the per-client layers); see `references/ks-config-mapping.md`.
- **`stoatflow.licence`** — `key`/`file`/`environment`/`environment-class`/`service`/`verbose-banner`, plus `source`
  (`configured` default | `registry`: adopt the one licence applied to the registry topic when the app configures
  none; env `STOATFLOW_LICENCE_SOURCE`) and `registry.topic` (default `__stoatflow_licence`, history topic
  `<topic>_history`; env `STOATFLOW_LICENCE_REGISTRY_TOPIC`) and `reporting.enabled` (daily usage report to
  `licensing.stoatflow.io`; unset = on for a trial, off otherwise; env `STOATFLOW_LICENCE_REPORTING_ENABLED`) (see
  `stoatflow-project-setup` for how the licence is supplied).
- **`runtime.*`** — `http` (port 8080, `debug`), `metrics` (`prefix` `stoatflow`, `naming`
  `stoatflow|kafka-streams|both`, `common-tags`), `endpoints.info` (per-field toggles), `health`
  (broker/schema-registry timeouts).
- **`logging.level`** — a map of logger → `TRACE|DEBUG|INFO|WARN|ERROR|OFF`.

## Rules

- **Never invent a key** — validate against the schema / `references/config-reference.md`.
- `commit-barrier.timeout-ms` **must be greater than** `commit-barrier.interval-ms`.
- Config precedence (highest→lowest): env vars (`__` separator) → `STOATFLOW_*` env → `STOATFLOW_CONFIG_FILES`
  overlays → classpath `application.yml` → `application.yaml` → data-class defaults; code
  `streamsConfigOverrides` wins last.
- The website reference page is curated (not exhaustive); the **schema is authoritative** — so is
  `references/config-reference.md`.

## See also

- `references/config-reference.md` — the complete generated key list (type / default / description)
- `references/ks-config-mapping.md` — Kafka Streams `Properties`/`StreamsConfig` → StoatFlow adapter
- `references/stoatflow-primer.md`
- Website: <https://stoatflow.io/docs/configuration> · <https://stoatflow.io/docs/reference/configuration-reference>
