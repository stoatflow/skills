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
            - name: STOATFLOW_LICENCE_KEY
              valueFrom: { secretKeyRef: { name: stoatflow-licence, key: licence-key } }
            - name: STOATFLOW_LICENCE_ENVIRONMENT   # REQUIRED for production, enterprise and trial licences in a class they count
              value: prd-eu
            - name: STOATFLOW_LICENCE_ENVIRONMENT_CLASS   # default production
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

- The image built by the StoatFlow plugins (Jib) carries the JVM flags and the main class in its **entrypoint**:
  `args:` on the container are the application's own `main` arguments, and `command:` must stay unset — it would
  replace the entrypoint, flags and heap sizing included. Images built before 1.0.0-rc.5 had no command at all and
  exited at start (see the project-setup skill's docker reference for the workaround).
- Point `stoatflow.state.dir` at the mounted PVC. State is durable but **not authoritative** (changelog is
  the source of truth) — the PVC changes restart *speed* (delta restore), not correctness.
- `/health/ready` returns 503 until restoration completes; verify with
  `kubectl rollout status statefulset/my-stream-app`.
- Rough sizing: Light 4 cores / 8 GiB / 20 GiB SSD · Medium 16 / 64 GiB / 100 GiB · Heavy 32 / 128 GiB /
  500 GiB NVMe. Requests == limits for Guaranteed QoS.

### Licence registry topics

The licence registry is the compacted topic `__stoatflow_licence` (heartbeat every 30 s; `__stoatflow_licence_history`,
`compact,delete` with 400-day retention, is created too) in the application's own Kafka cluster; it is how the licence
counts services.

- **ACLs** on both topics: DESCRIBE, READ, WRITE; CREATE for auto-creation; DESCRIBE_CONFIGS for the start's
  timestamp-type check (without it the type is read from the topic's records; an empty topic uses the `CreateTime`
  fallback until one shows it). The clients authenticate like the app's own: producer `producer.*`, admin `admin.*`,
  consumer `restore.consumer.*` (each falling back to `consumer.*`).
- **Locked-down cluster:** pre-create both topics with the settings printed in the start's error (and on the website's
  licence page, section "the licence registry topics"), including `message.timestamp.type=LogAppendTime` on the
  registry; on `CreateTime` the registry uses host clocks, a start that finds other instances' records waits 90 s
  before counting and `stoatflow.licence.registry.broker.clock` reads 0.
- **Per-tenant registry on a shared cluster:** `stoatflow.licence.registry.topic` (env
  `STOATFLOW_LICENCE_REGISTRY_TOPIC`); the history topic follows as `<topic>_history`.
- **Cap:** a start that would exceed the environment class's licensed count (times 1 + burst) throws
  `LicenceValidationException` `SERVICE_LIMIT`; the pods crash-loop, the running fleet is unaffected. A service
  registered — an instance running, or last seen within 7 days — always restarts, even while its class is over the cap.
- **HA standbys** count per the licence's standby term: all free by default, every standby counted, or the first *n*
  free. `/licence` shows `standbysRunning` and `standbysCounted` per class. A registered service brings back as many
  counted standbys as it ran in those 7 days, even over the cap; a standby beyond that is refused like a new service.
- **Registry unavailable** (cannot be ensured, read or written): paid licences run `UNVERIFIED` (readiness UP, count
  unchecked, background retry that reads the topic again in full); trials refuse to start (`REGISTRY_UNAVAILABLE`).

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

Coordination-topic credentials: its three clients connect and authenticate exactly like their processing
counterparts, taking their `security.*`/`ssl.*`/`sasl.*` keys and `client.dns.lookup` — the producer from
`stoatflow.kafka.producer` over the consumer's, the admin client (creates the topic) from
`stoatflow.kafka.admin`, the consumer (reads it like a restoration consumer) from
`stoatflow.kafka.restoration-consumer`. One principal: set the credentials once under
`consumer`. A principal per client: the producer's needs `Write` on the topic, the consumer's `Read`, the
admin's `Describe` (+ `Create` when relying on auto-creation, + `DescribeConfigs` for the startup
compaction check, skipped without it). A failed topic creation's error lists these permissions.

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
fence) and bounded-duplicate under ALO (the promotion token is the fence). Under ALO the active writes each epoch's
state to the changelogs at that epoch's commit, after its output is acknowledged, so a standby trails it by up to one
commit. Pausing the active does **not** promote a standby.

Tables restored from their source topic (a `table()` over a compacted topic, `globalTable()`, a read-only store): a
standby applies the source topic only up to the consumer group's committed offset — reading it, with `Describe` on the
group, every `stoatflow.ha.heartbeat-ms` while it holds a partition there, with the topic's end offsets (a caught-up
standby, idle or busy: two small requests per heartbeat) — never the records the active has read but not committed;
their lag counts what the standby may still apply (from the topic's end while the offsets cannot be read; unknown —
the largest long, never caught up — while the end cannot be read either). A table
standing past its topic's end — the topic re-created or truncated under the standby — is rebuilt from the topic; a
promotion that finds one fails once, and the next start restores it in full. Retention (`compact,delete`) or
`deleteRecords` removing records the application has not committed (the topic's first offset past where the standby
holds the table): the standby rebuilds the table from what the topic holds, as Kafka Streams does, with an ERROR — keys
only the removed records carried are gone from it. A promotion, a cold start and an in-place restart bring each such
table exactly to the offset processing resumes from, so a `groupBy` over it stays exact across a failover (GA A2.37);
a cold start and an in-place restart bring every other store to its changelog's end read after the producer fence,
so a commit the previous run was completing when it crashed is never missed (GA A2.38).

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
- **An older pod in front of a newer pod's data refuses.** A paused rollout or a rollback across a changelog
  format change: an older standby that meets a newer-format changelog record terminates its pod
  (`ChangelogFormatException` in the log; it restarts and fails again); an older pod promoting releases the
  promotion token so a peer that reads the data takes over; if the newer active is gone there is no active
  until the rollout moves forward. Advise rolling forward, never deleting state — the changelog is the only
  copy. A normal rolling update needs no upgrade ordering. Full rules: SKILL.md → Upgrades and rollback.
- **`stoatflow.ha.restarting`** (0/1 gauge) is the dashboard signal: the pod stays `ACTIVE` throughout, so
  `stoatflow.ha.role` does not move. `stoatflow.engine.restart.total{trigger=replace_thread}` counts them.
- **A thread blocked in user code holds the restart.** The rebuild reuses the processors and state stores, so
  before the old engine closes them it waits for a processing thread still in a user call (a `process()`
  stuck on a network call or a lock): a WARN `In-place restart: N lane thread(s) are still in user code after
  the teardown halted them…` names each thread with its stack, every 10 s. A call that returns finishes its
  record against the old engine (nothing of it is kept) and the restart goes ahead. One more
  `stoatflow.shutdown.timeout-ms` (25 s) later the restart is refused (`In-place restart refused…`, ERROR) and
  the instance shuts down — a pod restart, and with HA a failover (the HA demote included), that replays the
  records once. Point the user at the stack in that WARN: the fix is a timeout on that call.
- **A source-topic-set change restarts the active in place too** (ADR-143): a topic newly matching a
  `stream(Pattern)` source, a partition-count increase, or a source topic being deleted, is detected by a check
  every 10 s, confirmed on the next
  check, and absorbed by an automatic in-place engine restart — `stoatflow.engine.restart.total{trigger=source_topics_changed}`,
  outside the fault budget; readiness is DOWN for the swap + restore. `stoatflow.ha.restart.required` now flags
  only a *declined* restart (pod already closing/demoting). A pattern matching nothing at promotion idles until
  the first matching topic appears. A **standby** whose table's source topic gains partitions notices it the same
  way (while the active is healthy) and re-initialises in place —
  `stoatflow.engine.restart.total{target=standby_replicate, trigger=source_topics_changed}`; it reports
  `REPLICATING` until the new partitions are restored to the committed offset, so a later failover activates at once
  (GA A2.40; before, that failover's first activation failed and the pod restarted). During the re-init it takes no part
  in failover: a PROMOTE sent to it is refused with a WARN (re-issue once `READY_STANDBY`). Every standby runs the same
  check and re-initialises in the same window, so an active that fails meanwhile is failed over once the first re-init
  ends.
- **Deleting a source topic is the expensive direction**, and before 1.0.0-rc.3 it wedged the active outright.
  The active assigns partitions manually, so there is no revocation to react to and the deletion is only seen by
  that 10 s check; records already consumed from the deleted topic are still named in the in-flight offset
  commit, which the broker rejects retriably and the client retries without end. Symptom before the fix: the pod
  stops committing and sits in `RESTORING` with no error — the restart begins and its teardown hangs. Advise
  draining a topic and deleting it once idle, rather than deleting it under load. A topic deleted while records
  from it are still being consumed costs a pause of about a minute at the defaults even with the fix (measured
  ~66 s): it is dominated by the commit already in flight when the topic went away running out
  `barrier-timeout-ms`, not by the 10 s detection window. An idle topic is not named in the commits outside its
  keep-alive (a commit names only partitions that moved), so its deletion costs the detection window plus the restart.
  **This cost is HA-specific, and applies to PATTERN-matched topics.** Without HA the app uses a consumer group,
  so deleting a pattern-matched topic arrives as a revocation within ~130 ms, is classified as a gone topic (not
  a second instance), and an in-place `ABORT` restart has it committing again in ~0.5 s stateless / ~1 s
  stateful. Do not quote the ~66 s figure for a non-HA deployment. A deleted *literal* source topic stays fatal
  either way (KS-exact) — advise a pattern source for topics that are expected to come and go.
- **Under HA, the brokers' `offsets.retention.minutes` must be at least a day** (default 7 days). The active's
  `assign()` group has no members, so Kafka expires each partition's committed offset that long after its last
  commit; StoatFlow re-commits every source partition's offset about 15 minutes after a pod becomes active and every
  12 hours after, records or not. Below a day, an idle partition's offset can expire and the next active resumes it
  from `auto.offset.reset`. The same re-commit keeps the exactly-once `transactional.id` alive (any mode) — keep
  `transactional.id.expiration.ms` at a day or more too.
- **Under exactly-once, advise `auto.create.topics.enable=false` on the brokers before deleting a pattern-matched
  topic** (HA or not). With the Kafka 4.4 client the producer keeps asking, with
  auto-creation allowed, for the topics it committed offsets for in roughly the last 10 minutes, so on a permissive
  broker it can re-create a deleted topic, empty, before StoatFlow drops it — the pattern then matches it again.
  StoatFlow commits a partition's offset when it moves, about 15 minutes after the application starts and every 12
  hours, so a topic is exposed for about 10 minutes after each of those and not otherwise. Kafka Streams 4.4 has the same exposure.
- Since 1.0.0-beta.8 a terminal failure publishes the hand-off from the state transition itself, so it no
  longer depends on anything calling `close()`. Before that, an **embedded `:core`** deployment (no
  `:runtime` wrapper, no user `close()`) left a dead engine heartbeating as a healthy active and the
  standby never promoted — advise upgrading, or ensuring a liveness probe can reach the pod.
