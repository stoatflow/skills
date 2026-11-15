# The runtime REST surface + metrics

The `:stoatflow-runtime` HTTP server binds `0.0.0.0:8080` by default (`runtime.http.{enabled,port,host}`).
Read endpoints are `GET` (wrong method → 405; pre-startup → 503). **Expose only `/health/live` +
`/health/ready` on public ingress; keep everything else in-cluster.**

| Endpoint | Method | Purpose |
|---|---|---|
| `/health/live` | GET | Liveness — process healthy enough to stay alive? (200 UP / 503 DOWN). Flips DOWN if the commit pipeline freezes past `stoatflow.commit-stall.threshold-ms` (45 s). License never fails liveness. |
| `/health/ready` | GET | Readiness — ready to process? 503 throughout restoration. The ~5-min license `PENDING` window after start is UP (the pod takes traffic). License `EXPIRED`/`REVOKED`/`INVALID`/no key → 503; the instance-limit state is `INVALID` with `reason: instance_limit` while the pod keeps processing. |
| `/info` | GET | App id, effective lane count (`app.lanes`) and record-lane thread type (`app.laneThreadType`; under DIRECT dispatch, `topology.dispatchMode`, no record lanes run), JVM/Kafka info, uptime, state, registered endpoints. |
| `/config` | GET | Merged effective config, secrets masked (YAML; JSON via `Accept`). |
| `/topology` `/topology/ks` `/topology/compiled` | GET | Native view · Kafka-Streams-compatible `describe()` view · compiled internal view. `/topology/ks` (JSON with `Accept: application/json`) is the pull equivalent of KS 4.4's KIP-1331 topology-description push — StoatFlow is a classic consumer group, so `describeStreamsGroups` never lists it (KSC-113). |
| `/state` | GET | Lifecycle state + transition history. |
| `/consumer` | GET | Consumer-group metadata, subscription, last commit time, per-partition buffer + pause state (`paused` for any reason: full buffer, the application pause, start-up restoration; watermark alignment never pauses). |
| `/offsets` | GET | Per-partition committed source offsets + lag, and committed changelog offsets per store. |
| `/watermarks` | GET | Global + per-partition event-time watermarks, idle flags, last event times. A partition whose next record is stamped in the future and held behind older records also shows `previewWatermarkMs` (what it counts with in the global watermark, while that is above its own) and `headHeldSinceMs`. With watermark alignment: `alignmentGroup`, `alignmentHeld`, `driftFromGroupMinMs` per partition, `alignmentGroupMins`, and `alignmentBinding` — `{group, partition, emptyLaggards, sinceMs}` of the hold keeping the whole application waiting, or `null` (also while a pause or backpressure, not the hold, is what stops dispatch; `sinceMs` survives that). A partition that waited in a hold with records buffered does not count that wait as silence: its `lastEventTimeMs` moves forward by it afterwards. Bounded timer cache (every timer backend): `rawGlobalMs`/`rawGlobalIso` (event time without the timer hold — ahead of `globalMs` only while it binds), `timerFrontierMs` (the first stored event-time timer not yet loaded, `null` when all are; `globalMs` never passes it; it moves back when a timer registers below it, `globalMs` never does), `timerHeld`/`timerHeldSinceMs`, and per partition `timerFrontierHeld` with `timerFrontierHeadMs` (the waiting record's time) and `timerFrontierContributionMs` (what it counts with — capped at the frontier, so it moves back with it). |
| `/license` | GET | Runtime license-validation state: status, tier, expiry, `machinesUsed` (instances **registered** against the license — briefly includes ones that just stopped) vs `machinesAllowed` (instances it **covers**). |
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
`stoatflow.topology.sub-topology-split: eager` restores the old numbering. **1.0.0 renumbers a second time** for Processor-API topologies: `process()`/`processValues()` no longer open a boundary after a re-key (`stoatflow.topology.processor-api-key-affinity: off`, matching Kafka Streams); `presumed` restores those.

**Naming modes** — use `both` to light up existing Kafka Streams dashboards during a migration:

```yaml
runtime:
  metrics:
    naming: both               # stoatflow (default) | kafka-streams | both
    ks-compat:
      shape: micrometer-binder # micrometer-binder | jmx-exporter
```

In the Kafka Streams naming modes, store `commit-rate` / `commit-latency-{avg,max}` (debug) map from the per-store
epoch flush timer — StoatFlow commits every store at every barrier, so the rate is the barrier rate — and task
`restore-remaining-records-total` sums `stoatflow.restoration.remaining.records`.

Main metric families (level `info`): throughput/latency (`stoatflow.lane.records.processed.total`,
`stoatflow.lane.process.latency`, `stoatflow.e2e.latency`), barrier commits
(`stoatflow.barrier.{completed,failed,abandoned}.total` — abandoned = deliberately not committed, `reason` `pre_assignment`/`poison_abort`/`abort_teardown`/`not_broadcast`, not a failure; `failed`'s `error_type` `fatal_error` = an `Error` (`TODO()`, `AssertionError`) thrown by user serde/store code during a commit; `unhandled_send_failure` = an at-least-once send that failed with no production handler to decide it (a changelog record, a dead letter, an output under the default handler without a `dlqTopic`) or fatally, and `poison_epoch` = a handler's `FAIL` for an asynchronous send failure (a handler that throws or returns `null` included) — both stop the commit before it writes anything, offsets held, so the restart sends again (a record too large for its topic recurs on every start until `max.message.bytes` is raised) — `stoatflow.barrier.commit.latency`,
`stoatflow.barrier.trigger.total{TIME,RECORD_COUNT,MEMORY_PRESSURE,CACHE_PRESSURE}`), watermarks
(`stoatflow.watermark.{current,lag.ms,late.records.total,preview.partitions}`), changelog/restoration
(`stoatflow.restoration.{in.progress,records.restored.total}`; `stoatflow.restoration.remaining.records{store_name}` — records a store still has to restore, 0 once done), consumer/producer
(`stoatflow.consumer.lag.records`; `stoatflow.consumer.partition.forgotten.total{reason}` — source partitions
dropped because their topic is gone (`topic_deleted` = a deleted pattern topic, answered by an in-place engine
restart; `ha_source_shrink` = the HA detector's confirmed shrink);
`stoatflow.source.pattern.matched.topics{source}` — topics a pattern source
currently matches, `0` = matches nothing yet), client state (`stoatflow.client.state` — ordinal, `== 4` = RUNNING =
healthy-and-processing), HA (`stoatflow.ha.*`, only when on), and license
(`stoatflow.license.valid{tier}`; `…validations_total{result}`, `…heartbeats_total{result}`, `…pings_total{result}`
— see below). JVM/Kafka-client metrics via `bind-jvm-metrics` (default true).

**License: instances, limit, expiry.**
- One `application.id` + environment is one licensed instance; replicas and an HA pair count once. A running
  instance stays counted through a heartbeat ping (`stoatflow.license.pings_total{result=success|machine_lost|network_failure|other}`
  — a failed ping never stops anything); a stopped one frees its slot within about the license's heartbeat window.
  Renaming an environment registers a new instance.
- **Instance limit.** A new instance whose license is full is refused at its deferred validation (~5 min after
  start: "StoatFlow instance allowance exhausted…") and shuts down. A *running* instance that loses its
  registration and cannot register again keeps processing, reports `INVALID` + `reason: instance_limit` (readiness
  503, liveness UP), retries every heartbeat and recovers once a slot frees. Alert:
  `increase(stoatflow_license_validations_total{result="instance_limit"}[1h]) > 0`.
- **Over-registered license** (its limit was lowered below the registered instances): every instance logs a
  warning and keeps running, readiness UP, liveness detail `licenseOverRegistered`, `result="over_registered"`.
  The fix is the vendor's (contact your StoatFlow administrator).
- `validations_total` / `heartbeats_total` `result` is a fixed set: `success`, `invalid`, `expired`, `revoked`,
  `network_failure`, `offline_grace_exhausted`, `environment_required`, `instance_limit`, `over_registered`,
  `machine_lost`, `signature_unverified` (an extra event: the license server's signed answer could not be
  verified), `internal_dev_expired`, `unexpected_error`, `other`.
- **Trial expiry:** from 5 days before, the log names the end date; at expiry an ERROR, then the instance stops
  1 hour later. Paid licenses keep running past expiry until their next restart (which is refused).
- **Renewal / trial extension:** the same key keeps working — the engine reads the new expiry at its next online
  validation. The offline grace counts from the last successful online validation.

**`stoatflow.timer.lane.mismatch.total{processor}`** — record keys whose re-serialization with the processor's timer
key serde picks another lane than the record's, so the key's timers would run on another lane than its records.
Pre-registered at 0 per timer-declaring Processor API processor. A split for a timer on the record's own key stops the
app by default (`TimerLaneAffinityException`, `timer.lane-affinity.guard`); a sampled split is WARN-only by default
(`timer.lane-affinity.sampled-check`). Alert on any increase AND on application state — a fatal detection can stop the
app before the next scrape. Recorded at every level. The fix is a
declared key serde — `Topology.addSource(name, Consumed.with(keySerde, valueSerde), topics)` instead of a key
deserializer — never a restart.

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

- `stoatflow.dlq.poison.replays.total` — **the one to alert on.** Poison-epoch replays: an asynchronous
  broker-side production failure aborted an epoch, and rather than advancing past it StoatFlow held the
  source offsets, restarted the engine in place, and reprocessed the epoch with the poison quarantined.
  Each replay costs a full engine restart and briefly makes the pod unready. Bounded by
  `stoatflow.commit-barrier.max-poison-replays` (default 10) within `poison-replay-window-ms` (default
  60000) — but that budget is **in-memory**, so exhausting it ends the process, Kubernetes restarts the
  pod, and the budget returns clean. `CONTINUE` is bounded **per process**, never end to end; a poison
  derived from accumulated state does not converge and will crash-loop the pod.
- `stoatflow.dlq.poison.quarantined.total` — source records the replay is skipping. A quarantined record
  loses **all** of its outputs, across every sink it fans out to: bounded loss, not zero.
- `stoatflow.dlq.poison.quarantine.size` — gauge, `0` in steady state; non-zero only between a poison
  abort and the replay's first clean commit.
- `stoatflow.dlq.poison.replay.budget.exhausted.total` — **alert on this one too.** The budget ran out
  and the instance shut down with the offsets held instead of replaying again. It is the necessary
  companion to `…replays.total`: that meter counts *replays*, so it stops incrementing at exactly the
  moment a non-converging poison starts crash-looping the pod. Any non-zero value is an incident.
- Two cases stop the instance immediately instead of replaying, with the offsets held so an operator
  restart replays the epoch: a `FAIL` verdict (which now means what it says), and a `continue` on a
  poison with no attributable source record — emitted by a punctuator, a window close, a suppression
  flush, a timer or a scheduled source. Those planes fire on their own schedule and would re-poison
  every attempt, so the shutdown message names the plane. A `retry` on one of those planes is different
  and does **not** stop the instance: it means the send failed rather than the emission, so the epoch
  replays and the send is re-attempted. All of this is exactly-once with a production handler that is
  asked: under at-least-once a `retry` is a `FAIL`, and `DefaultProductionExceptionHandler` without a
  dead-letter topic is never asked — such a failure stops the instance with the offsets held under both
  guarantees, and the pod's restart replays the epoch (KSC-151; Kafka Streams revives the task in place). Prevent the whole class by sizing the destination topic's
  `max.message.bytes`. The same offset hold covers every **synchronous** `FAIL` too (processing, sync
  production, punctuator/timer/scheduled-source): the epoch aborts, the instance stops, an operator
  restart replays it (the consumer records via the held offsets — a punctuator tick or scheduled-source
  firing does not itself re-fire retroactively) — and no dead letter is written on those routes (the
  record was preserved, not dropped).
- `stoatflow.dlq.abort.dropped.records` was **retired** (ADR-140). It counted the source records an abort
  permanently dropped; aborts no longer drop them.
- `stoatflow.dropped.records.total{reason}` — eight reasons: `no_event_time` (a source record with no event time —
  Kafka's `-1` "no timestamp" on a source whose time is the record timestamp, or `Long.MIN_VALUE` from a timestamp
  extractor or assigner, as `LogAndSkipOnInvalidTimestamp` returns for a negative timestamp; skipped at ingestion with
  a WARN at most once a minute per partition, its offset committed), `null_key`, `late_window` (also a record within
  one window size of `Long.MAX_VALUE`, which joins no window), `late_session`,
  `join_null_key` (also a foreign-key join's record with no foreign key — `null`, serializing to `null`, or reading
  back as `null` — counted where Kafka Streams counts it), `join_null_value`,
  `table_source_null_key`, `null_window_value` (a time or sliding window whose value
  the value serde reads back as `null`: its final `OnWindowClose` result is not emitted, counted once at close — a
  session in that state is forwarded as `null`, as in Kafka Streams). The tag *is* the semantics — always group
  by it rather than alerting on the bare total.
- `stoatflow.sink.records.negative.timestamp.total{sink_name,topic}` — output records whose event time was negative
  (a valid event time in StoatFlow). Kafka cannot store a negative timestamp, so the sink sends the record without one
  and the producer (`CreateTime`) or broker (`LogAppendTime`) stamps the wall-clock time, as Flink's `KafkaSink` does.
  Counted per record, one WARN per sink. An unexpected non-zero value usually means an upstream slip (a wrong-sign
  extractor); set `FailOnInvalidTimestamp` / `LogAndSkipOnInvalidTimestamp` as the extractor to refuse negative input.
- Store operation latency is `stoatflow.store.{get,put,delete,range,all,flush}.latency` (all `debug`).
- `stoatflow.store.segments{store_name,store_type}` (`info`) — live RocksDB instances a **segmented**
  window/session store holds (~7 file descriptors each, no upper bound). Plateaus at
  `retention / segmentInterval + 1` while event time advances; **climbs** for as long as the watermark is
  pinned by a lagging-but-live partition, or an idle one whose effective timeout is 0. `GET /watermarks`
  reports `idlenessTimeoutMs` per partition — the timeout actually in force for it (the source's own
  `withIdleness` if set, else `watermark.idleness-timeout-ms`), without which `idle` and `lastEventTimeMs`
  cannot be interpreted once timeouts differ per source. The
  RocksDB metric family sums its properties across segments and never publishes this count.
- `stoatflow.store.segment.checkpoint.failures.total{store_name,store_type}` (`info`) — failed checkpoints of a
  segmented window/session store, on the active or an HA standby. A checkpoint flushes the segments written since the
  last one, then records the store's changelog position on disk; a failed one keeps the last recorded position (nothing
  is lost, a WARN is logged), but while it keeps failing every restart replays more of the changelog. Alert on any
  increase.
- `stoatflow.barrier.epoch.uncommitted.bytes` (`info`) — uncommitted state bytes across all stores, the timer store
  included (every timer backend), sampled ~1 Hz: the state total `stoatflow.state.uncommitted-max-bytes` caps (under exactly-once the cap also counts `stoatflow.eos.send.buffer.bytes`, below). Alert on it against that cap; the
  dispatcher checks a projection (this plus the next records' estimated cost), so a `MEMORY_PRESSURE` barrier can fire
  slightly below it. `stoatflow.store.uncommitted.bytes{store_name,store_type}` (`info`, KIP-892 `uncommitted-bytes`)
  is the per-store share (sum + timer store = the global) — it shows which store drives memory-pressure commits; the timer
  store has no series of its own, its share is the global minus the sum. It
  counts the committed epoch still being flushed (memory, not visibility, KSC-117), is emitted for every store family
  (KS: key-value only, KSC-118) and is `0` for `lruMap` / in-memory versioned stores.
- `stoatflow.eos.send.buffer.bytes` (`info`, exactly-once only) — estimated heap bytes of the output held back while a
  commit runs (the next epoch's records, kept until that commit is done). While the commit runs it counts with the
  state bytes toward `stoatflow.state.uncommitted-max-bytes`, and past it dispatch pauses until the commit is done, so
  dispatcher stalls with a low `epoch.uncommitted.bytes` point here: high fan-out, large output records or slow commits. Near 0 between commits; not
  listed by an at-least-once application.
- `stoatflow.watermark.lag.ms` (`info`) — `now − global watermark`, sampled every second **whether or not
  the watermark advanced** (until 2026-09-03 it was written only on an advance, so it froze at a healthy
  value during exactly the stall it exists to reveal); `-1` until the first watermark. Alert on it, and
  read the segment gauge to see which stores are paying for the stall.
- `stoatflow.watermark.preview.partitions` (`info`) — partitions whose next record waits behind older records of other
  partitions: a record stamped in the future, or a partition far ahead during a skewed catch-up or replay. With the
  built-in watermark strategies such a partition counts towards the global watermark as if that record had been
  processed, so it does **not** stall event time. Idle partitions are not counted, nor one whose own watermark is
  already as high. Non-zero for long with no catch-up
  under way usually means a producer clock or a timestamp unit needs checking (`/watermarks` names the partition). It
  can also read non-zero while the application is paused or fully backpressured with such a record waiting.
- Watermark alignment (`info` unless noted): `stoatflow.watermark.alignment.partitions.held{group}` (gauge — partitions
  held: more than the drift ahead of a lagging group member; they keep being fetched), `…held.time.total{group}`
  (counter, **seconds** a hold in the group stopped **all** dispatch — `rate()` is the waiting fraction; a sustained climb
  = the drift is too small for a sparse source's gaps, size `drift ≥ G + b(sparse) − b(held)`), `…yields.total{group}`
  (held records dispatched because every laggard had data — a flat `held.time.total` with rising yields means the
  group's out-of-orderness bounds differ). `…drift.ms` / `…group.min` are `debug`. A hold that stops all dispatch for more
  than 60 s logs one WARN naming the group, partition, empty laggards and the sizing rule; so does a group whose holds
  keep the application waiting at least half of a 5-minute window (a sparse source sending more often than once a
  minute), once until the share drops below half.
- Bounded timer cache (`info`; only with a timer store — a processor declares timers, on any `timer-backend` — and,
  for the four `watermark.*` meters, event-time tracking on and dispatch not `DIRECT`;
  otherwise the series do not exist): `stoatflow.watermark.timer.held` (gauge 0/1 — event time
  waits at the first stored event-time timer not yet loaded because more are due than `timer-backend.max-cached-timers`
  holds; expected during a catch-up, frequent = cache too small; stuck at 1 with `timer.fired.total` flat = a stuck
  `onTimer`), `stoatflow.watermark.timer.held.time.total` (counter, seconds; `rate()` = the waiting fraction),
  `stoatflow.watermark.timer.frontier` (gauge, epoch ms, `NaN` when every timer is loaded), `stoatflow.watermark.raw`
  (gauge — event time without the hold), `stoatflow.timer.cache.loaded{domain}` (timers in memory per domain, bounded by
  the setting) and `stoatflow.timer.cache.overshoot.max{domain}` (how far above it since the last sample — a timer
  registered for a time already loaded joins a full cache). A hold past 60 s logs one WARN `Event time has been held at
  the timer frontier …` naming the loaded count, the bound and the frontier.
- `stoatflow.store.versioned.sweep.lag.ms{store_name}` — ms since a versioned (KIP-889) store's last
  **completed** history-reclamation pass. Versioned retention is a *minimum* bound: reads stop answering
  for expired versions immediately, but the bytes go on a rolling sweep targeting one
  `stoatflow.state.versioned.sweep-period-ms`. Alert when this stays above that period. The bound holds on
  a **quiet** application too, under both processing guarantees: an idle app still commits on a slow
  cadence (30 s by default) precisely so the sweep keeps running, and goes quiet again once nothing is
  left to reclaim — so a flat, healthy reading on an idle app is the real one, not a frozen gauge. Its companion
  `stoatflow.store.versioned.sweep.budget.exhausted.total{store_name}` climbing steadily means the
  configured period is not achievable at that store's size — raise
  `stoatflow.state.versioned.max-scan-bytes-per-barrier` or accept a longer period. Both are `info`;
  `stoatflow.store.versioned.sweep.position` is `debug` progress detail. The per-barrier budget is shared
  across every versioned store on a sub-topology, so adding stores does not multiply the commit cost.
  A hot standby never sweeps (no commit barriers), so these read as idle there — but it does apply the
  active's reclamation tombstones through the restore seam, so its footprint tracks the active within
  replication lag rather than growing until promotion. Size a standby's disk like the active's; a standby
  whose footprint grows while the active's stays flat is a real signal, not the documented normal.
- `stoatflow.store.versioned.sweep.reclaimed.total{store_name}` and
  `stoatflow.store.versioned.sweep.tombstones.total{store_name}` (`info`) — versions the sweep removed
  from the **local store**, and the reclamation tombstones it wrote to the **changelog**. Read them as a
  pair. A versioned changelog's key is the composite `[key][timestamp]`, so log compaction can never
  collapse versions on its own; the topic is bounded only because the sweep writes one Kafka null per
  reclaimed version. In a healthy steady state the two counters track each other. **A persistent gap —
  reclaiming but not tombstoning — means the local store is shedding while the topic grows without
  bound.** Expected to be zero for a store with logging disabled (there is nothing to write to). The
  tombstone counter counts records handed to the changelog *writer*: under exactly-once that send is
  buffered into the epoch's transaction, so a broker-side failure shows up as a failed commit rather
  than as a gap here. The topic's own shrink lags further still — the broker frees space on its
  compaction cadence, which StoatFlow now sizes on creation (`segment.ms` and `max.compaction.lag.ms`
  set to the sweep period, `min.cleanable.dirty.ratio` 0.1); changelogs created before that shipped keep
  Kafka's defaults, where a 7-day `segment.ms` leaves a tombstone unable to free anything for a week.
- `stoatflow.store.versioned.sweep.failures.total` (untagged, `info`) — reclamation ticks that failed.
  The tick catches everything by design, because a sweep failure has no business unwinding into the
  commit protocol; the consequence is that a persistently-broken sweep is otherwise invisible outside the
  log while the app looks healthy. **Any sustained increase means reclamation is not running at all** and
  the store is growing without bound. Untagged deliberately: the failure can precede store resolution,
  and the accompanying WARN names the sub-topology.

**Headline alerts:**

- `rate(stoatflow_barrier_failed_total[5m]) > 0` — commit failures (Critical).
- `increase(stoatflow_epoch_invariant_violations_total[5m]) > 0` — an epoch-isolation invariant tripwire fired
  (`kind` names which; most kinds guard exactly-once, `skipped_commit_with_pending` and `partition_offset_regression`
  both guarantees; `partition_offset_regression` also counts each broker-side seek-back — log truncation, an offset
  reset — after which the re-fetched records are processed again; `partition_buffer_over_bound` means a poll delivered
  records to a partition whose input buffer was already full — the pause that bounds its memory failed, usually logged
  just before it; the records are fetched again later, so memory stays bounded; the five `timer_*` kinds —
  `timer_claim_without_slot`, `timer_claim_while_firing`, `timer_registry_not_empty_at_reload`,
  `timer_event_without_handler`, `timer_claim_behind_queue_epoch` — guard the in-flight timer registry, both
  guarantees). Otherwise unreachable by design: any increment is a StoatFlow bug — capture the ERROR (or, for
  `partition_offset_regression` and `partition_buffer_over_bound`, WARN) log line and report it (Critical).
- `stoatflow_license_valid == 0` **`for: 10m`** — the `for` matters: every deploy reads 0 during the ~5-min
  license `PENDING` window (Critical).
- `max(stoatflow_consumer_lag_records) > 100000` — high lag.
- watermark stall / backpressure / HA standby lag / redundancy below desired. A record stamped far in the future at a
  partition's head waits there while other partitions are backlogged; the log carries a WARN naming the partition
  ("stamped … ms in the future at its head"). With the built-in watermark strategies the partition waits but event
  time does not — the WARN says event time is set by the other partitions, and `stoatflow.watermark.preview.partitions`
  counts it. With a custom `WatermarkGenerator` the WARN says the watermark stands still instead: event time is pinned
  until the record is processed or the partition goes idle (`watermark.idleness-timeout-ms`). A WARN saying the record
  is stamped "… ms ahead of the wall clock" instead means nothing is held back (a single partition, or every partition
  ahead): event time follows suspect timestamps — check the producer clock, LogAppendTime on a broker clock that runs
  ahead, or the timestamp extractor's unit.
