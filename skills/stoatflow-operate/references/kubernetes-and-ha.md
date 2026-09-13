# Kubernetes deployment + HA active/standby

## Single-instance StatefulSet

A `StatefulSet` with `replicas: 1` + `RollingUpdate` + `OrderedReady` — **never a `Deployment`** (its
default rolling update starts the new pod before stopping the old → two active → corruption), and **not
`Recreate`** (the StatefulSet rollout already tears the old pod down first).

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: my-stream-app
spec:
  replicas: 1                      # exactly one — never more (single active instance)
  serviceName: my-stream-app
  podManagementPolicy: OrderedReady
  updateStrategy:
    type: RollingUpdate            # stops the old pod before starting the new
  selector:
    matchLabels: { app: my-stream-app }
  template:
    metadata:
      labels: { app: my-stream-app }
    spec:
      terminationGracePeriodSeconds: 60   # > stoatflow.shutdown.timeout-ms / 1000 (default 25s), with margin
      containers:
        - name: app
          image: my-registry/my-stream-app:latest
          ports: [{ name: http, containerPort: 8080 }]
          env:
            - name: STOATFLOW_LICENSE_KEY
              valueFrom: { secretKeyRef: { name: stoatflow-license, key: license-key } }
            - name: STOATFLOW_LICENSE_ENVIRONMENT   # REQUIRED in production
              value: production
          readinessProbe: { httpGet: { path: /health/ready, port: http }, periodSeconds: 5,  timeoutSeconds: 3, failureThreshold: 3 }
          livenessProbe:  { httpGet: { path: /health/live,  port: http }, periodSeconds: 10, timeoutSeconds: 3, failureThreshold: 3 }
          startupProbe:   { httpGet: { path: /health/ready, port: http }, periodSeconds: 10, failureThreshold: 60 }  # ~10 min for restore
          resources:                # requests == limits → Guaranteed QoS
            requests: { cpu: "16", memory: 64Gi }
            limits:   { cpu: "16", memory: 64Gi }
          volumeMounts:
            - { name: state, mountPath: /var/lib/stoatflow/state }
  volumeClaimTemplates:
    - metadata: { name: state }
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: fast-ssd
        resources: { requests: { storage: 100Gi } }
```

- Point `stoatflow.state.dir` at the mounted PVC. State is durable but **not authoritative** (changelog is
  the source of truth) — the PVC changes restart *speed* (delta restore), not correctness.
- `/health/ready` returns 503 until restoration completes; verify with
  `kubectl rollout status statefulset/my-stream-app`.
- Rough sizing: Light 4 cores / 8 GiB / 20 GiB SSD · Medium 16 / 64 GiB / 100 GiB · Heavy 32 / 128 GiB /
  500 GiB NVMe. Requests == limits for Guaranteed QoS.

## HA active/standby

`stoatflow.ha.mode: off` (default) or `active-standby`. The standard supported shape is **`replicas: 2`**
(one active + one passive standby). Standbys are passive (safe to run alongside the active); they pre-warm
state continuously and promote on failover.

Key config (defaults):

| Key | Default | Purpose |
|---|---|---|
| `stoatflow.ha.mode` | `off` | `off` (single instance) or `active-standby` |
| `stoatflow.ha.pod-id` | `POD_NAME`→`HOSTNAME`→hostname | stable per-pod identity |
| `stoatflow.ha.coordination-topic` | `__stoatflow_<applicationId>_ha` | single-partition compacted coordination topic; 1 partition is the required shape, and a pre-existing topic with a different count is **warned about at startup, not rejected** (reads and writes are both pinned to partition 0, so extra partitions are unused) |
| `stoatflow.ha.acceptable-recovery-lag` | `50000` | **total** committed-changelog lag (all partitions summed) at/below which a standby is `READY_STANDBY` + promotion-eligible |
| `stoatflow.ha.desired-standbys` | `1` | warm spares the readiness redundancy-floor protects |
| `stoatflow.ha.max-standbys` | `3` | hard ceiling (total instances ≤ max-standbys + 1) |
| `stoatflow.ha.failover-priority` | `0` | tie-break within the equal-lag bucket (never overrides lag) |

Deployment: same `StatefulSet` + `RollingUpdate`, but `replicas: 2`; wire `POD_NAME` from the downward API
→ `stoatflow.ha.pod-id`; a headless `Service`. Readiness gate: the active is Ready while processing; a
standby is Ready only once caught up; a still-catching-up standby keeps liveness UP so it isn't killed.

`/ha/*` commands (write endpoints return `202` + a `commandOffset` idempotency token):

- `GET /ha/status` — role, total lag, `tailFreshnessMs`/`tailCaughtUp`, caught-up-standby count,
  promotion-token epoch/holder, peers.
- `POST /ha/switch` — active drains, peer promotes. `409` unless a caught-up (`READY_STANDBY`) peer exists;
  override `?force=true`.
- `POST /ha/promote?pod=<id>` / `POST /ha/demote?pod=<id>` — target a specific pod (same `409`/`?force`).

Correctness: `?force=true` overrides only the *readiness* check — a forced promotion still fully restores to
the committed changelog before processing. Split-brain is impossible under EOS (broker producer-epoch
fence) and bounded-duplicate under ALO (the promotion token is the fence). Pausing the active does **not**
promote a standby.

### Application faults under HA

With `ha.mode` **off** or **on**, a `StreamsUncaughtExceptionHandler` returning `REPLACE_THREAD` on a
restartable fault (processing / production failure) gets a bounded **in-place engine restart**: the engine
is rebuilt inside the live JVM and **the pod keeps the active role — no failover**. Budget
`stoatflow.commit-barrier.max-engine-restarts` = 5 within `…engine-restart-window-ms` = 5 min.

When the budget is spent the instance shuts down, and *that* is when the role moves — a graceful
`DRAINING` hand-off, so a caught-up standby promotes on the fast path. One failover after a bounded number
of local attempts, not one per fault.

Not every fault is restartable. Deserialization failures, punctuator failures and commit stalls are not:
the handler still runs (alerting/metrics fire) but a `REPLACE_THREAD` answer cannot be honoured, a WARN
records it, and the instance drains and hands off.

Operational consequences worth telling a user about:

- **`/health/ready` is DOWN while a pod rebuilds** — it is not processing, and IQ endpoints served from it
  are unavailable. An in-flight rolling update stalls; the pod leaves Service endpoints. Liveness is
  unaffected (a successful in-place restart never reaches a terminal state).
- **Role-change commands are refused mid-restart.** `/ha/switch`, `/ha/promote`, `/ha/demote` return
  **409** if a pod the command would move is restarting — and `?force=true` does NOT override it (force
  overrides the *readiness* judgement, not a structurally impossible operation). Every command moves the
  **active**, so a `promote` is refused while the active it would supersede is rebuilding, not only when
  its own `?pod=` target is. Poll `GET /ha/status` until `selfRestarting` / the peer's `restarting` is
  false, then re-issue. A peer that has gone *stale* is not believed — a pod that died mid-restart cannot
  lock the cluster out.
- **Mixed-version clusters report `UNKNOWN`.** Mid rolling upgrade, a `role`/`state` published by a newer
  build that this pod does not recognise shows as `UNKNOWN` on its `/ha/status`. The peer still counts as
  alive but takes part in no promotion decision on that pod. It means "finish the upgrade", not "something
  is broken".
- **`stoatflow.ha.restarting`** (0/1 gauge) is the dashboard signal: the pod stays `ACTIVE` throughout, so
  `stoatflow.ha.role` does not move. `stoatflow.engine.restart.total{trigger=replace_thread}` counts them.
- **A source-topic-set change restarts the active in place too** (ADR-143): a topic newly matching a
  `stream(Pattern)` source, a partition-count increase, or a source topic being deleted, is detected by a check
  every 10 s, confirmed on the next
  check, and absorbed by an automatic in-place engine restart — `stoatflow.engine.restart.total{trigger=source_topics_changed}`,
  outside the fault budget; readiness is DOWN for the swap + restore. `stoatflow.ha.restart.required` now flags
  only a *declined* restart (pod already closing/demoting). A pattern matching nothing at promotion idles until
  the first matching topic appears.
- **Deleting a source topic is the expensive direction**, and before 1.0.0-rc.3 it wedged the active outright.
  The active assigns partitions manually, so there is no revocation to react to and the deletion is only seen by
  that 10 s check; records already consumed from the deleted topic are still named in the in-flight offset
  commit, which the broker rejects retriably and the client retries without end. Symptom before the fix: the pod
  stops committing and sits in `RESTORING` with no error — the restart begins and its teardown hangs. Advise
  draining a topic rather than deleting it under load, and expect a pause of about a minute at the defaults
  even with the fix (measured ~66 s): it is dominated by the commit already in flight when the topic went
  away running out `barrier-timeout-ms`, not by the 10 s detection window.
  **This cost is HA-specific, and applies to PATTERN-matched topics.** Without HA the app uses a consumer group,
  so deleting a pattern-matched topic arrives as a revocation within ~130 ms, is classified as a gone topic (not
  a second instance), and an in-place `ABORT` restart has it committing again in ~0.5 s stateless / ~1 s
  stateful. Do not quote the ~66 s figure for a non-HA deployment. A deleted *literal* source topic stays fatal
  either way (KS-exact) — advise a pattern source for topics that are expected to come and go.
- Since 1.0.0-beta.8 a terminal failure publishes the hand-off from the state transition itself, so it no
  longer depends on anything calling `close()`. Before that, an **embedded `:core`** deployment (no
  `:runtime` wrapper, no user `close()`) left a dead engine heartbeating as a healthy active and the
  standby never promoted — advise upgrading, or ensuring a liveness probe can reach the pod.
