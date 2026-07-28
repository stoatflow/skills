---
name: stoatflow-operate
description: Deploy and run a StoatFlow app in production — the single-instance Kubernetes pattern (one active pod, never replicas > 1), HA active/standby mode, liveness/readiness probes, the runtime REST surface, metrics/observability, tuning (lanes, commit cadence, memory), and the production checklist. Use for deployment/ops; for application config keys use stoatflow-configure.
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
- **Shutdown:** `terminationGracePeriodSeconds` ≥ `stoatflow.shutdown.timeout-ms` / 1000 (default 25 s) with
  margin (e.g. 60). SIGTERM → graceful drain.
- **License:** inject `STOATFLOW_LICENSE_KEY` from a `Secret`; production **requires**
  `STOATFLOW_LICENSE_ENVIRONMENT`.

## The runtime REST surface (details in references/rest-and-metrics.md)

The runtime serves an HTTP surface on `0.0.0.0:8080` by default (`runtime.http`). Expose only
`/health/live` + `/health/ready` publicly; keep the rest in-cluster.

- **Health:** `/health/live`, `/health/ready` · **Introspection:** `/info`, `/config` (masked),
  `/topology`(+`/ks`,`/compiled`), `/state`, `/consumer`, `/offsets`, `/watermarks`, `/license` ·
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

## Tuning levers (details in references/tuning.md)

- **Lanes:** `stoatflow.lanes.count` (default `max(2, CPU cores)`) — start at cores for CPU-bound, 4×–8×
  cores for I/O-bound; fixed at startup. Decoupled from Kafka partition count.
- **Commit cadence self-tunes** between `commit-barrier.min-interval-ms` / `max-interval-ms` — you set the
  bounds, not the exact interval. `commit-barrier.timeout-ms` is a safety bound, not a cadence knob.
- **Memory:** `stoatflow.state.uncommitted-max-bytes` (256 MiB — fires an early `MEMORY_PRESSURE` commit);
  `stoatflow.rocks-db.preset` (`LOW_MEMORY` 64 MiB / `DEFAULT` 256 MiB / `HIGH_PERFORMANCE` 1 GiB).

## Metrics & alerts (details in references/rest-and-metrics.md)

`GET /metrics` (Prometheus). `runtime.metrics.naming: stoatflow | kafka-streams | both` — use `both` to
light up existing Kafka Streams dashboards during a migration. Headline alerts: barrier failures
(`rate(stoatflow_barrier_failed_total[5m]) > 0`), license invalid (`stoatflow_license_valid == 0` with
`for: 10m` — a fresh deploy reads 0 during the ~5-min license `PENDING` window), high consumer lag,
watermark stall.

## Production checklist (the essentials)

- [ ] **Exactly one _active_ instance** — `replicas: 1` StatefulSet (or HA active/standby with passive
      standbys); never two active against the same topics.
- [ ] Stable `application-id`; graceful shutdown on SIGTERM.
- [ ] PVC for warm restart; recovery point = the last committed barrier.
- [ ] Processing guarantee chosen on purpose; downstream consumers match.
- [ ] License injected as a secret; readiness wired to license health.
- [ ] `/health/ready` (readiness) + `/health/live` (liveness) + a generous `startupProbe`.
- [ ] `/metrics` scraped; alerts on lag / commit stalls / errors / restoration.
- [ ] Exception + deserialization handlers chosen; DLQ provisioned and monitored.
- [ ] Resources sized (CPU/memory for one process, lanes for cores); `-XX:+UseG1GC`.

## See also

- `references/kubernetes-and-ha.md` — the full StatefulSet manifest, probes, and the HA deployment + `/ha/*`
- `references/rest-and-metrics.md` — the endpoint table + metric families + naming modes + alerts
- `references/tuning.md` — lanes, commit cadence, memory/RocksDB, restoration, the full checklist
- `references/stoatflow-primer.md`
- Website: <https://stoatflow.io/docs/operating> · <https://stoatflow.io/docs/runtime/rest-api>
