# Tuning levers

Most of StoatFlow self-tunes; touch these when a symptom demands it. Every key also has a
`STOATFLOW__<SECTION>__<KEY>` env override (double underscore between path segments).

## Lanes (parallelism)

`stoatflow.lanes.count` — default `max(2, CPU cores)`. Start at cores for CPU-bound work; **4×–8× cores for
I/O-bound** work (blocking calls). Skewed keys won't benefit (per-key ordering pins a key to one lane).
Fixed at startup — changing it needs a restart. `stoatflow.lanes.queue-capacity` (default 300) is the
backpressure buffer per lane. Lane count is **decoupled from the input partition count**.

## Commit cadence (self-tuning)

The barrier cadence self-tunes between bounds you set — you don't set the exact interval:

```yaml
stoatflow:
  commit-barrier:
    interval-ms: 500        # seed
    min-interval-ms: 150
    max-interval-ms: 5000
    interval-factor: 1.5
    timeout-ms: 60000       # SAFETY bound, not a cadence knob (must be > interval-ms)
```

Lower the bounds for latency-sensitive work; raise them for throughput (fewer, larger transactions).
`timeout-ms` cascades to the producer's `transaction.timeout.ms` / `max.block.ms` / `delivery.timeout.ms`.
Epoch size (`commit-barrier.max-epoch-records`, default no cap) normally stays untouched.

## Memory & uncommitted state

- `stoatflow.state.uncommitted-max-bytes` — default 256 MiB. A backstop: exceeding it fires an early
  `MEMORY_PRESSURE` commit barrier. **Lower it if you see OOMKills.** Rough container budget: 4 GiB → ~1 GiB,
  8 GiB → 1–2 GiB, 16 GiB → 2–4 GiB.
- `stoatflow.caching.{max-estimated-bytes (256 MiB), max-entries (unbounded)}` — per-store cache caps →
  `CACHE_PRESSURE` early commit.
- `stoatflow.rocks-db.preset` — `LOW_MEMORY` (64 MiB) | `DEFAULT` (256 MiB) | `HIGH_PERFORMANCE` (1 GiB),
  shared across all stores. Raise it when the block-cache hit ratio falls while the cache is at capacity.
  RocksDB memory is **bounded by default** — the deliberate difference from Kafka Streams (whose RocksDB is
  unbounded).
- `stoatflow.rocks-db.backend` — `AUTO` (FFM on JVM, JNI on native). If you force `JNI` on the JVM, also set
  `stoatflow.lanes.thread-type: PLATFORM`.

## Restoration

Cold-start / standby restore uses dedicated consumers under `stoatflow.kafka.restoration-consumer`
(framework defaults `max.poll.records=1000`, `fetch.max.bytes=50MiB`). For a faster restore raise
`max.poll.records: 5000` and `receive.buffer.bytes`. `group.id` / `enable.auto.commit=false` /
`auto.offset.reset=earliest` / `isolation.level` are forced (not overridable).
`stoatflow.state.segment-checkpoint-{records (100000),ms (30000)}` bound ungraceful-restart replay and
standby drain to ≤ one checkpoint interval.

## Changing a windowed/session store's retention

Change `retention` and restart — no restore, **on the segmented backend** (the default for every windowed
aggregation and session store). Each segment records its own time range on disk, so a changed retention
(and therefore segment interval) **remaps** rather than being reinterpreted. Kafka Streams requires wiping
local state for the same change. The stream-stream join buffers are segmented too (since 2026-09-03), so
a changed join window remaps the same way; only a hand-built non-segmented store
keeps the interval in every key, where a retention change that moves it needs a restore.

- Segments are sized `max(retention / 3, 60s, windowSize)` on every DSL window/session store
  (`max(retention / 3, 60s)` only on a hand-built non-segmented one); not configurable. Below ~3 min of retention the
  60 s floor governs **provided `windowSize <= 60s`** (same floor as KS, likewise unclamped — though KS
  reaches its own floor below **120 s**, since its rule is `retention / 2`). A larger window still wins
  on a segmented store: at `retention = 2min, windowSize = 90s` the interval is 90 s.
- **Live segment count is watermark-dependent, and being on the floor says nothing about it.** Segments
  are fixed-width and epoch-aligned, so the count is `retention / segmentInterval` whole intervals plus
  the boundary one when the watermark sits on a segment edge, and `ceil(retention / segmentInterval) + 1` in the worst phase — the same number when the retention divides evenly, one higher when it does not. Size
  against the maximum; 2 min of retention on a 60 s interval keeps three. A single segment is never
  guaranteed — even 30 s of retention on a 60 s interval spans two off a boundary. A stalled watermark
  accumulates more again (`RS-03`).
- Retention is a **minimum** bound: whole segments are dropped, so effective retention lands in
  `[retention, retention + segmentInterval)` — up to ~4/3× configured where the divisor governs
  (`retention >= 180s`), **up to 2×** where `windowSize` governs (segmented only, and the default case
  since `TimeWindows.ofSizeWithNoGrace` makes `retention == windowSize`), and **`retention + 60s`** where
  the floor governs, which at 30 s retention is **3×** — the floor is ours as much as KS's, so short
  retentions are a large multiple, not a small one. Exactly 1× in-memory. Size disk against the upper
  bound.
- **`EmitStrategy.onWindowClose()` adds a term the bound above does not cover.** Expiry is then gated on
  the durable emit frontier rather than the committed watermark, so effective retention is
  `retention + segmentInterval + (watermark - emit frontier)`. Normally small, zero-slack by design (a
  window is never expired before its final is emitted), but **not** bounded by the retention contract.
- Narrowing retention frees disk **gradually**: existing segments keep the span they were written with,
  so effective retention stays at `retention + widest existing span` until they age out. One `INFO` line
  at startup says so. Matters if retention is a deletion deadline.

**Needs a full restore** (detected at startup, `WARN` naming the field and both values, then a changelog
replay): changing the **window size**, switching **plain↔timestamped**, the **store type**
(window ↔ session), **`retainDuplicates`** (a semantic flip — coexist vs overwrite — recorded in the layout
stamp), or **retention on a hand-built non-segmented store**. Plan these with a deploy that can absorb
restore time.

**Watch the segment count.** `stoatflow.store.segments{store_name,store_type}` is the live number of data segments
a segmented store holds (~7 file descriptors each, no upper bound): a plateau at
`retention / segmentInterval + 1` is healthy; a climb means the watermark is pinned by a lagging-but-live
partition (or an idle one whose effective idleness timeout is 0 — the source's own `withIdleness` if it
sets one, else `watermark.idleness-timeout-ms`; the engine WARNs, naming the uncovered sources, at
start-up) and nothing is expiring. Alert on `stoatflow_watermark_lag_ms`, which is sampled every second and
so climbs during the stall.

**No changelog to restore from?** A store with `withLoggingDisabled()`, or any store under
`changelog.enabled: false`, refuses to start on a layout change — its local state is the only copy and
StoatFlow will not delete it for you. The message names every affected store and the field that moved.
Put the setting back, enable the changelog, or set `stoatflow.state.layout-mismatch: wipe-and-restore` to
accept the loss (those stores start **empty**; remove the setting afterwards, it does not expire).

State written before the segment-layout change cannot be read (old directory names carried no time
ranges). The app refuses to start and names the remedy: delete the store's local state dir; the changelog
restores it. One-time pre-1.0 break, no migration path.

## When to touch what (symptom → lever)

| Symptom | Lever |
|---|---|
| I/O-bound throughput plateau | raise `lanes.count` (4×–8× cores) |
| OOMKilled | lower `state.uncommitted-max-bytes`; check `rocks-db.preset` |
| Block-cache hit ratio dropping at capacity | `rocks-db.preset: HIGH_PERFORMANCE` |
| Commit latency too high | raise the `commit-barrier` interval bounds |
| Slow cold start | raise `restoration-consumer.max.poll.records`; ensure a warm PVC |
