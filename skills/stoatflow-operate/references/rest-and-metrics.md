# The runtime REST surface + metrics

The `:stoatflow-runtime` HTTP server binds `0.0.0.0:8080` by default (`runtime.http.{enabled,port,host}`).
Read endpoints are `GET` (wrong method → 405; pre-startup → 503). **Expose only `/health/live` +
`/health/ready` on public ingress; keep everything else in-cluster.**

| Endpoint | Method | Purpose |
|---|---|---|
| `/health/live` | GET | Liveness — process healthy enough to stay alive? (200 UP / 503 DOWN). Flips DOWN if the commit pipeline freezes past `stoatflow.commit-stall.threshold-ms` (45 s). License never fails liveness. |
| `/health/ready` | GET | Readiness — ready to process? 503 throughout restoration + the ~5-min license `PENDING` window. |
| `/info` | GET | App id, JVM/Kafka info, uptime, state, registered endpoints. |
| `/config` | GET | Merged effective config, secrets masked (YAML; JSON via `Accept`). |
| `/topology` `/topology/ks` `/topology/compiled` | GET | Native view · Kafka-Streams-compatible `describe()` view · compiled internal view. |
| `/state` | GET | Lifecycle state + transition history. |
| `/consumer` | GET | Consumer-group metadata, subscription, last commit time, per-partition buffer + pause state. |
| `/offsets` | GET | Per-partition committed source offsets + lag, and committed changelog offsets per store. |
| `/watermarks` | GET | Global + per-partition event-time watermarks, idle flags, last event times. |
| `/license` | GET | Runtime license-validation state. |
| `/pause` `/unpause` | POST | Pause (→ `DRAINING` → `PAUSED`, readiness DOWN, no rebalance) / resume (→ `RUNNING`). Wrong state → 400. |
| `/ha/status` | GET | HA cluster view (only when `ha.mode != off`; else 404). |
| `/ha/switch` `/ha/promote` `/ha/demote` | POST | Role commands (202 + `commandOffset`). |
| `/debug/threads` `/debug/barriers` | GET | Thread stacks (freeze diagnosis) · commit-pipeline phase snapshot. Gated by `runtime.http.debug.enabled` (default true). |
| `/metrics` | GET | Prometheus text-exposition scrape. |

`/debug/barriers` `phase`: `IDLE`, `AWAITING_LANE_ACKS`, `WAITING_FOR_COMMIT_GATE`, `QUEUED_FOR_COMMIT`,
`IN_TX_COMMIT`, `AWAITING_ASYNC_FLUSH`.

## Metrics

`GET /metrics` (Micrometer `PrometheusMeterRegistry`; dotted `stoatflow.*` names, Prometheus rewrites `.` →
`_`). Enabled by default under `runtime.metrics`.

```yaml
# prometheus.yml
scrape_configs:
  - job_name: stoatflow
    metrics_path: /metrics
    static_configs: [{ targets: ["my-app:8080"] }]
```
Or Kubernetes pod annotations `prometheus.io/scrape: "true"`, `prometheus.io/path: "/metrics"`,
`prometheus.io/port: "8080"` (or a `ServiceMonitor`).

**Sub-topology ids shifted in 1.0.0 (ADR-138).** Boundaries now open only where an operator needs a re-keyed
record's lane affinity, not at every key change — so re-keying topologies have **fewer** sub-topologies and the
ids renumber. Any dashboard, alert or recording rule pinning a `sub_topology` / `subtopology_id` value, or a
`lane_id=~"N_.*"` regex (lane ids are `{subtopology}_{lane}`), needs re-checking against a live scrape after an
upgrade. Thread names (`stoatflow-{chain}-st{N}-lane-{L}`) and `/topology*` shift the same way.
`stoatflow.topology.sub-topology-split: eager` restores the old numbering.

**Naming modes** — use `both` to light up existing Kafka Streams dashboards during a migration:

```yaml
runtime:
  metrics:
    naming: both               # stoatflow (default) | kafka-streams | both
    ks-compat:
      shape: micrometer-binder # micrometer-binder | jmx-exporter
```

Main metric families (level `info`): throughput/latency (`stoatflow.lane.records.processed.total`,
`stoatflow.lane.process.latency`, `stoatflow.e2e.latency`), barrier commits
(`stoatflow.barrier.{completed,failed}.total`, `stoatflow.barrier.commit.latency`,
`stoatflow.barrier.trigger.total{TIME,RECORD_COUNT,MEMORY_PRESSURE,CACHE_PRESSURE}`), watermarks
(`stoatflow.watermark.{current,lag.ms,late.records.total}`), changelog/restoration
(`stoatflow.restoration.{in.progress,records.restored.total}`), consumer/producer
(`stoatflow.consumer.lag.records`), client state (`stoatflow.client.state` — ordinal, `== 4` = RUNNING =
healthy-and-processing), HA (`stoatflow.ha.*`, only when on), and license
(`stoatflow.license.valid{tier}`). JVM/Kafka-client metrics via `bind-jvm-metrics` (default true).

**RocksDB (`stoatflow.rocksdb.*`) is opt-in and two-gated:** `stoatflow.rocks-db.metrics.enabled: true`
gives the property + cache gauges (`info`, cheap); adding `statistics-enabled: true` gives the ticker /
ratio / histogram meters (`debug`, ~5–10% write-path cost, applied at store open so it needs a restart).
~43 series **per store** plus 3 global. The ones worth a dashboard: `background.errors` (alert on any
non-zero), `estimate.pending.compaction.bytes` (climbing = compaction losing ground),
`write.stall.duration.{avg,total}.ms` (RocksDB backpressuring the commit path),
`block.cache.{,data.,index.,filter.}hit.ratio`, `{total,live}.sst.files.size.bytes`, `estimate.num.keys`
(the RocksDB counterpart of `store.num.keys`, which covers in-memory stores only), and the three global
`shared.block.cache.{capacity,usage,pinned.usage}.bytes` — StoatFlow runs one LRU cache across all
stores, so prefer those over the per-store duplicates that exist for KS-dashboard portability. Two
caveats: `open.files` is not published (its RocksDB ticker is gone in the bundled RocksJava), and
`*.min.ms` is JNI-only (the default FFM backend's statistics dump has no MIN token).

- `stoatflow.dlq.abort.dropped.records` — source records whose outputs were dropped when a broker-side
  production failure aborted the whole epoch under *continue*. Bounded data loss; any increase is worth
  an alert. Counts the aborted epoch's offset span (includes the poison record), not dropped outputs.
- `stoatflow.dropped.records.total{reason}` — six reasons: `null_key`, `late_window`, `late_session`,
  `join_null_key`, `join_null_value`, `table_source_null_key`. The tag *is* the semantics — always group
  by it rather than alerting on the bare total.
- Store operation latency is `stoatflow.store.{get,put,delete,range,all,flush}.latency` (all `debug`).

**Headline alerts:**

- `rate(stoatflow_barrier_failed_total[5m]) > 0` — commit failures (Critical).
- `stoatflow_license_valid == 0` **`for: 10m`** — the `for` matters: every deploy reads 0 during the ~5-min
  license `PENDING` window (Critical).
- `max(stoatflow_consumer_lag_records) > 100000` — high lag.
- watermark stall / backpressure / HA standby lag / redundancy below desired.
