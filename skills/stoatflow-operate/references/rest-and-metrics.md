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
(`stoatflow.license.valid{tier}`). JVM/Kafka-client metrics via `bind-jvm-metrics` (default true). RocksDB
metrics are opt-in (`stoatflow.rocks-db.metrics.enabled`).

**Headline alerts:**

- `rate(stoatflow_barrier_failed_total[5m]) > 0` — commit failures (Critical).
- `stoatflow_license_valid == 0` **`for: 10m`** — the `for` matters: every deploy reads 0 during the ~5-min
  license `PENDING` window (Critical).
- `max(stoatflow_consumer_lag_records) > 100000` — high lag.
- watermark stall / backpressure / HA standby lag / redundancy below desired.
