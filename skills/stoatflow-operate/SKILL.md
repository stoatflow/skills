---
name: stoatflow-operate
description: Deploy and run a StoatFlow app in production — the single-instance Kubernetes pattern (one active pod, never replicas > 1), HA active/standby mode, liveness/readiness probes, the runtime REST surface, metrics/observability, tuning (lanes, commit cadence, memory), upgrades and rollback, and the production checklist. Use for deployment/ops; for application config keys use stoatflow-configure.
license: Apache-2.0
---

# Operate a StoatFlow app

Read `references/stoatflow-primer.md` first. The one rule that dwarfs all others:

> **Never run two _active_ instances of the same application against the same source topics.** Two active
> processes writing the same state **corrupts it**. StoatFlow is single-instance by design.

You scale a StoatFlow app **vertically** (CPU/memory) and with **lanes** — never horizontally with replicas.
Availability comes from **HA active/standby** (passive standbys), not from multiple active pods.

## Kubernetes: a single-replica StatefulSet

Deploy as a **`StatefulSet` with `replicas: 1`, `updateStrategy: RollingUpdate`, and
`podManagementPolicy: OrderedReady`** — **not** a `Deployment`. Why: a `Deployment` with `replicas: 1` and
the default rolling update **starts the new pod before terminating the old** (`maxSurge: 1`) → two active
processes for a moment → corruption. A StatefulSet's rolling update **terminates the old pod and waits for
it to fully stop** before creating the replacement → at-most-one-running. (Do **not** use `Recreate`; the
StatefulSet rollout already gives the guarantee.) Full manifest in
`references/kubernetes-and-ha.md`.

- **Probes:** readiness `GET /health/ready` (503 throughout restoration — gates traffic), liveness
  `GET /health/live`, and a `startupProbe` on `/health/ready` with a generous `failureThreshold` (state
  restoration can take minutes).
- **State:** a PVC (`volumeClaimTemplates`) mounted at `stoatflow.state.dir` — durable, but **not
  authoritative** (the changelog is the source of truth). It changes restart *speed* (delta restore), not
  correctness.
- **Startup metadata:** before a restore StoatFlow describes every changelog and source topic and reads their
  offsets (needs `Describe`). For KTable source-topic reuse it also needs `DescribeConfigs` on the source topics,
  unless the source is pinned or reuse is off. A table restored from its source topic is restored up to the consumer
  group's committed offset, which needs `Describe` on the group (implied by `Read`). An unreadable answer (a topic created moments ago included) is retried for
  up to `stoatflow.state.restoration-metadata-timeout-ms` (30 s), then the start fails, naming the store (an
  authorization or authentication error at once). A store is never wiped on an answer StoatFlow could not read. The
  local state check — read-only, no data file opened, so cheap for any store size — is bounded by
  `stoatflow.state.restoration-validation-timeout-ms` (30 s per store's check, from its own start; at most
  `restoration-pool-size` at once; a stuck RocksDB open is not waited for), and every other startup probe of a store
  directory (layout and format checks, the KTable source-switch check, the lock check before a deletion) gets the same
  bound on its own. Both windows are short by design: the failed start stops the app, the pod restart recovers.
- **One instance per state directory:** a store directory whose RocksDB lock another open holds fails the start, naming
  the store and the likely holder (a second instance, or a store an earlier engine left open — a process restart
  releases it); nothing is deleted.
- **Don't flip a KTable's restoration source in place:** turning `ktable.auto-reuse-source-topics` off, or pinning a
  table that restores from its source topic to a changelog, fails the start when the table's local state holds
  source-topic offsets; the switch needs a rebuild (reset the group on the source topic to the earliest offset, delete
  the store's local state, start). An in-memory table or a fresh disk has no such state: the start goes on with a WARN
  naming the table (its changelog topic absent while the consumer group is past the source's start; read with
  `Describe` on the group, no warning without it) — act on it with the same rebuild.
- **Shutdown:** `terminationGracePeriodSeconds` ≥ `stoatflow.shutdown.timeout-ms` / 1000 (default 25 s) with
  margin (e.g. 60). SIGTERM → graceful drain.
- **Licence:** inject `STOATFLOW_LICENCE_KEY` from a `Secret` (verified offline at `start()`; only usage reporting
  — on by default for a trial — sends anything, to `licensing.stoatflow.io`);
  production, enterprise and trial licences **require** `STOATFLOW_LICENCE_ENVIRONMENT`; declare
  `STOATFLOW_LICENCE_ENVIRONMENT_CLASS` (`production` default, `staging`, `development`, `other`). Every instance
  registers in the compacted topic `__stoatflow_licence` of the app's own Kafka cluster (heartbeat 30 s;
  `__stoatflow_licence_history` is created too): grant the principal DESCRIBE, READ, WRITE (+ CREATE,
  DESCRIBE_CONFIGS) on both, or pre-create them (references/kubernetes-and-ha.md). A start past the licensed count
  is refused (`SERVICE_LIMIT`); a restart of a registered service never is — registered while an instance runs and
  for 7 days after it was last seen, so a graceful or `Recreate` restart over the cap starts too, with as many
  counted HA standbys as it ran (never an extra one) — and a running
  instance is never stopped by the count. Registry unavailable: paid licences run `UNVERIFIED` (readiness UP, count unchecked), trials refuse
  to start. **Renew without a restart:** `stoatflow-licence-tool apply licence.sfl1 --bootstrap-server …` writes the
  renewal to the registry topic and running instances of that licence id switch at once (higher sequence number
  only; per cluster). The same tool's `verify`, `status`, `report` (usage report `stoatflow-usage/1`, also
  `GET /licence/usage`) and `fleet-report` (several clusters). Counting, expiry, renewal and reports:
  references/rest-and-metrics.md.

## The runtime REST surface (details in references/rest-and-metrics.md)

The runtime serves an HTTP surface on `0.0.0.0:8080` by default (`runtime.http`). Expose only
`/health/live` + `/health/ready` publicly; keep the rest in-cluster.

- **Health:** `/health/live`, `/health/ready` · **Introspection:** `/info`, `/config` (masked),
  `/topology`(+`/ks`,`/compiled`), `/state`, `/consumer`, `/offsets`, `/watermarks`, `/licence`, `/licence/usage` ·
  **Control:** `POST /pause`, `POST /unpause` · **HA** (only when `ha.mode != off`): `/ha/status`,
  `POST /ha/{switch,promote,demote}` · **Diagnostics** (`runtime.http.debug.enabled`): `/debug/threads`,
  `/debug/barriers` · **Metrics:** `/metrics`.

## HA active/standby (details in references/kubernetes-and-ha.md)

`stoatflow.ha.mode: off` (default — single instance, fast restart) or `active-standby`. The standard shape
is a **`replicas: 2` StatefulSet** (one active + one passive standby) with readiness-gated rolling updates.
Standbys pre-warm state continuously; on failover the freshest caught-up standby promotes (fenced by the
EOS producer epoch / the promotion token). `POST /ha/switch` (drains active, promotes peer — `409` unless a
caught-up standby exists, override `?force=true`). A standby is promotion-eligible while total
committed-changelog lag ≤ `stoatflow.ha.acceptable-recovery-lag` (default 50000, a **total sum**).

A `REPLACE_THREAD` verdict works the same with HA on: a restartable fault is absorbed **in place** and the
pod keeps the active role — no failover until the restart budget is spent. The pod is `/health/ready`-DOWN
while it rebuilds, and role-change commands return **409** meanwhile (not `?force`-overridable). See the
reference for the full behaviour and the embedded-`:core` caveat.

## Tuning levers (details in references/tuning.md)

- **Lanes:** `stoatflow.lanes.count` (default `max(2, CPU cores / 4)` per sub-topology, on `PLATFORM` threads) —
  start at the default; raise toward the cores when a CPU-bound stateful sub-topology lags while the CPU is idle;
  4×–8× cores with `lanes.thread-type: VIRTUAL` for blocking I/O. Fixed at startup; decoupled from partitions.
- **Goal recipes** (low latency, high throughput, resource saver, blocking I/O, …) with measured trade-offs:
  references/tuning.md → Goal recipes.
- **Commit cadence self-tunes** between `commit-barrier.min-interval-ms` / `max-interval-ms` — you set the
  bounds, not the exact interval. `commit-barrier.timeout-ms` is a safety bound, not a cadence knob.
- **Memory:** `stoatflow.state.uncommitted-max-bytes` (256 MiB — fires an early `MEMORY_PRESSURE` commit; under
  exactly-once it also counts the output held back during a commit, `stoatflow.eos.send.buffer.bytes`);
  `stoatflow.rocks-db.preset` (the shared block cache, memtables charged inside it: `LOW_MEMORY` 32 MiB / `DEFAULT`
  128 MiB / `HIGH_PERFORMANCE` 512 MiB; the image's `STOATFLOW_ROCKSDB_MB` reservation does not follow it).

## Store-format downgrade (record headers turned off)

If a deploy removes `withRecordHeaders()` (or flips `stoatflow.dsl.store-format` back to `DEFAULT`) over a
store that already persists headers, **startup refuses** with a `StoreFormatDowngradeException`. This is
deliberate, not a bug: the on-disk keyspace exists and RocksDB cannot open the database without naming it,
so StoatFlow classifies it before the store opens and reports it instead of dying on a raw RocksDB error.
Nothing on disk is touched, so an accidental flag removal costs a restart, never data.

Three remedies, in the order most operators want them:

1. **Re-enable record headers** and restart — instant, nothing is rewritten.
2. **Acknowledge:** `stoatflow.state.format-downgrade: wipe-and-restore`. That store's local state is
   deleted and rebuilt from its changelog — one full restore, bounded by changelog size, on the startup
   critical path. Budget for it, and see `references/tuning.md` for making restoration fast.
3. **Reset the local state yourself** — `cleanUp()`, or delete the store directory.

Two cases never reach the setting: an **empty** headers keyspace (the flag was flipped on and off with
nothing written) is dropped in place for free, and a store with **no recovery source** (per-store
`withLoggingDisabled()`, or the changelog globally disabled) is refused unconditionally — remedy 2 does not
apply, because there is nothing to rebuild from.

**The round trip is lossy.** A downgrade sheds persisted headers from the local store, and re-enabling
headers later upgrades *lazily* — legacy values convert with empty headers — so they do not come back
without a forced full restore. Kafka Streams behaves the same way (it refuses outright; the acknowledgment
key is StoatFlow's addition). Watch
`stoatflow_store_format_downgrade_total{outcome="dropped-empty"|"wiped"}`. The `refused` outcome is
counted too, but startup fails immediately after, so a scrape will not catch it — detect a refusal from the
crash-loop and the log line, not from this metric.

## Upgrades and rollback: roll forward, not back

A changelog record can carry a format version in the native header `__stoatflow.v`; a record without it is
version 1. No release writes it yet (every changelog format is v1), but every restore path checks it: cold
start, in-place restart, a standby's bootstrap and streaming, promotion. Every RocksDB store (key-value,
versioned, timer, window, session) also records which local and changelog format versions it holds.

- **A normal rolling update needs no upgrade ordering.** Only the new build reads what the new build writes.
- **A build older than the data refuses, by design — unlike Kafka Streams**, whose KIP-796 changelog version
  header only WARNs. A newer changelog record fails `StoatFlow.start()` with
  `io.stoatflow.core.exception.ChangelogFormatException` (the message names store, topic, partition, offset,
  the version found and the versions this build reads); the app goes to `ERROR`, local state is kept,
  nothing from that poll is applied. A store whose local state holds newer data (a newer layout-stamp schema,
  or changelog records in a newer format) refuses at start with `StoreLayoutMismatchException` saying "this
  build is older than the data", local state kept, whatever `stoatflow.state.layout-mismatch` says, when it
  has a changelog or source topic. On Kubernetes: CrashLoopBackOff until a build that reads the data runs.
  **There is no skip or wipe option** — the changelog is the only copy. The remedy is to deploy a newer build;
  do not advise deleting state or topics.
- **The rollback bound is the newest build whose records a store holds, not the last build that ran.** A store
  remembers the highest changelog format it ever held; a rollback does not lower it. After N+1 → N, a further
  step back to N−1 is refused at start if N+1 wrote records N−1 cannot read. A store with no changelog (a
  source-topic table, logging disabled) holds no changelog records: only its local format governs it.
- **A rollback across a local-only format change works:** the older build WARNs, deletes the store's local
  state and rebuilds it from its changelog or source topic (one full restore). A store with **no recovery
  source** refuses instead, or starts **empty** under `stoatflow.state.layout-mismatch: wipe-and-restore`.
- **`layout-mismatch` scope:** still `refuse` (default) or `wipe-and-restore`, still consulted only for a store
  with no recovery source (`withLoggingDisabled()`, or the changelog globally disabled, and no source topic to restore
  from). There it now also
  covers a rollback (a newer local format or layout-stamp schema). A store holding changelog records newer
  than the build is never wiped.
- **HA pair:** a paused rollout or a rollback can put an older pod in front of a newer pod's records. An older
  standby meeting one terminates its pod (restarts, fails again); an older pod promoting releases the
  promotion token so a peer that reads the data takes over; if the newer active is gone, there is **no active**
  until the rollout moves forward.

## Metrics & alerts (details in references/rest-and-metrics.md)

`GET /metrics` (Prometheus). `runtime.metrics.naming: stoatflow | kafka-streams | both` — use `both` to
light up existing Kafka Streams dashboards during a migration. Headline alerts: barrier failures
(`rate(stoatflow_barrier_failed_total[5m]) > 0`), licence expiring (`stoatflow_licence_days_remaining < 30`) or invalid
(`stoatflow_licence_valid == 0`), licence overage (`stoatflow_licence_services_registered >
stoatflow_licence_services_cap`), registry unavailable (`stoatflow_licence_registry_available == 0`), high consumer lag,
watermark stall. **The whole application's input stops for seconds at a time, then resumes?** Look at
`stoatflow.subtopology.exclusive.work{sub_topology,reason,phase}` and its WARN: one sub-topology's punctuator store work
(or its slow punctuation lane, `phase = waiting_for_p`) runs alone at a barrier, its lanes parked, and full lane queues
stop dispatch for every source — shorten the punctuator's store part or move the work to timers
(`references/rest-and-metrics.md`).

## Production checklist (the essentials)

- [ ] **Exactly one _active_ instance** — `replicas: 1` StatefulSet (or HA active/standby with passive
      standbys); never two active against the same topics.
- [ ] Stable `application-id`; graceful shutdown on SIGTERM.
- [ ] PVC for warm restart; recovery point = the last committed barrier.
- [ ] Pre-creating topics (`changelog.create-topics-if-not-exist: false`)? Create every store's
      `<app>-<store>-changelog` **and** the engine's own, which `describe()` does not list:
      `<app>-stoatflow-watermark-changelog` (1 partition; any windowed aggregation, time-based `suppress` or
      stream-stream join), `<app>-<store>-emitfrontier-changelog` per `onWindowClose()` aggregation,
      `<app>-timer-store-changelog` when a processor declares timer types (`declaredTimerTypes()`) on a persistent
      timer backend with its changelog on. Startup
      names any that is missing.
- [ ] Mirroring changelogs to another cluster? Replicate them **with their record headers** (MirrorMaker 2
      does): the changelog format version is a native header, and a replicator that drops headers makes every
      record read as version 1.
- [ ] Upgrades roll forward; a rollback is bounded by the newest build whose records a store holds.
- [ ] Processing guarantee chosen on purpose; downstream consumers match.
- [ ] Licence injected as a secret; readiness wired to licence health; renewal before expiry; registry topic ACLs
  granted and `stoatflow_licence_registry_available` watched.
- [ ] `/health/ready` (readiness) + `/health/live` (liveness) + a generous `startupProbe`.
- [ ] `/metrics` scraped; alerts on lag / commit stalls / errors / restoration.
- [ ] Output topics created before the start — every sink topic and every DLQ topic: a missing one refuses the start
      (`MissingOutputTopicException`, naming it; `validation.validate-output-topics`, on by default). Topics a
      `TopicNameExtractor` picks per record are not checked — create those too.
- [ ] Exception + deserialization handlers chosen; DLQ monitored.
- [ ] Resources sized (CPU/memory for one process; lane count checked against the load — the default is
  2 per sub-topology up to 11 cores); `-XX:+UseG1GC`.

## See also

- `references/kubernetes-and-ha.md` — the full StatefulSet manifest, probes, and the HA deployment + `/ha/*`
- `references/rest-and-metrics.md` — the endpoint table + metric families + naming modes + alerts
- `references/tuning.md` — lanes, commit cadence, memory/RocksDB, restoration, the full checklist
- `references/stoatflow-primer.md`
- Website: <https://stoatflow.io/docs/operating> · <https://stoatflow.io/docs/runtime/rest-api>
