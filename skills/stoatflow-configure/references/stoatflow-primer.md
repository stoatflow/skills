# StoatFlow primer

> Shared context, read first by every StoatFlow skill. Assemble copies this file into each skill's
> `references/`. Keep it lean (~100 lines) — it is the head of the AGENTS.md distillate too.

**StoatFlow is a JVM stream-processing library** that pairs the Kafka Streams DSL with Flink-style
commit-barrier reliability, powered by Project Loom virtual threads. It is **source-compatible with
Kafka Streams 4.3 via an import swap** — but it is **not** Kafka Streams, and it is a **commercial,
closed-source** product distributed only from the private repository `maven.stoatflow.io` (never Maven
Central).

Assistants trained on `org.apache.kafka.streams.*` produce code that is wrong on exactly the divergences
below. These rules override those priors.

## The hard rules (wrong → right)

- **NEVER** import `org.apache.kafka.streams.*` — always `io.stoatflow.core.*` (see the import-swap
  table in `stoatflow-port-from-ks`). `org.apache.kafka.common.*` / `org.apache.kafka.clients.*`
  (serdes, `Headers`, `ConsumerRecord`, …) are unchanged — those are `kafka-clients` types.
- **NEVER** add a `kafka-streams` dependency — StoatFlow depends only on **`kafka-clients`**. The
  artifact is `io.stoatflow:stoatflow-core` (plus `-runtime`, `-test-utils`, `-test-runtime`).
- **NEVER** scale with `replicas: N` — StoatFlow runs as **exactly one active instance**. Scale
  *vertically* (CPU/memory) and with **lanes** (`withNumberOfLanes`, config), not replicas. Running a
  second active instance corrupts state. For availability, use **HA active/standby** mode (`ha.mode`).
- Default `processing.guarantee` is **`EXACTLY_ONCE`** (Kafka Streams defaults to `at_least_once`). A
  ported KS app that relied on the KS default silently runs under EOS on StoatFlow — stronger, but it
  changes transactional-id usage, broker requirements, and throughput. Set `at_least_once` explicitly
  if you want ALO. *(KSC-84)*
- `ProcessorContext.currentStreamTimeMs()` returns the StoatFlow **event-time watermark** (the global
  minimum across partitions, minus the out-of-orderness bound), **not** Kafka Streams' max-observed
  task stream-time. It is generally **lower** and **global**, not per-task. `currentWatermarkMs()` is
  the native name. *(KSC-85)*
- **Do not invent config keys.** Validate against the published JSON schema:
  `https://stoatflow.io/schemas/stoatflow-config-schema.json`.
- **StoatFlow artifacts are not on Maven Central.** Resolve from `maven.stoatflow.io` with your
  customer/trial credentials (see `stoatflow-project-setup`).

## The architecture, in one screen

StoatFlow is taught as a side effect of every task — you rarely reason about it directly, but these
facts explain the rules above:

- **Single-instance.** One active instance owns all partitions; there is no consumer-group rebalancing
  in the normal path. This is the core design choice (it eliminates rebalancing complexity) and the
  reason `replicas: N` is forbidden.
- **Key-affinity lanes** are StoatFlow's unit of parallelism — *virtual* partitions, **not** Kafka
  partitions. Records with the same key always route to the same lane (consistent hashing) → per-key
  ordering with high parallelism across virtual threads. Lane count is configured at startup
  (`withNumberOfLanes`; recommended ≈ CPU cores × 4), independent of the topic's partition count.
- **Commit barriers** flow through the topology as a Flink-style *consistent cut* linked to a Kafka
  transaction → end-to-end **exactly-once** across state and offsets.
- **Global state.** All state is reachable from any lane/virtual thread (no partition-scoped
  isolation). State is durable via changelog topics (Kafka Streams style), on RocksDB or in-memory
  stores.

## Compatibility statement

**Targets StoatFlow 1.0.0-beta.3-SNAPSHOT (Kafka Streams 4.3 API surface).** This is a pre-GA release —
APIs may still change. If your StoatFlow version differs, prefer the matching tagged release of this
pack. The divergence register (`KSC-NN` / `D-N`) is the authoritative list of where StoatFlow
intentionally differs from Kafka Streams; the `stoatflow-port-from-ks` skill carries it in full.
