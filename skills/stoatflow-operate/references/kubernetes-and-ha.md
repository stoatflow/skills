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
| `stoatflow.ha.coordination-topic` | `__stoatflow_<applicationId>_ha` | single-partition compacted coordination topic |
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
