# Tuning levers

Most of StoatFlow self-tunes; touch these when a symptom demands it. Every key also has a
`STOATFLOW__<SECTION>__<KEY>` env override (double underscore between path segments).

## Goal recipes

Measured on StoatFlow's benchmark apps on 8-core pods, against the defaults (2 `PLATFORM` lanes per
sub-topology, zstd output, the self-tuning barrier, `linger.ms` 20). Keys go under `stoatflow:`; the producer
recipes go in the `kafka.producer` map (`linger.ms: 50`, `compression.type: lz4`).

| Goal | Recipe | Measured trade-off |
|---|---|---|
| Low latency, stateless or exactly-once | `commit-barrier.interval-ms: 250`, `min-interval-ms: 130`, `interval-factor: 1.2` | P99 −8…−17 % on the stateless apps (EOS), same CPU; more commits per second |
| Low latency, stateful, eager emission | `lanes.count` = cores / 2 (4 on 8 cores) | P99 −10…−14 %, CPU +23…+49 % (word-count) |
| High throughput, CPU-bound stateful | raise `lanes.count` toward the cores (or `withNumberOfLanes(n)`) when lag grows with the CPU idle; signal: `stoatflow.lane.queue.size` near `stoatflow.lane.queue.capacity`, rising `stoatflow.lane.queue.full.total` | 2 lanes topped out word-count at ~15,000–20,700 lines/s on 8 cores; each lane costs CPU per record; KS runs 1 stream thread by default |
| Consistent, cheap aggregations | `emit.mode: on-barrier` + `commit-barrier.min-interval-ms: 250` — one update per key per commit (semantics) | word-count −42 % CPU, −57 % P99 against the defaults at 25 % load |
| Resource saver, high-volume stateless | producer `linger.ms: 50` | −14…−17 % CPU, +11…+34 % P99; not for word-count (−6 %, noise) |
| Resource saver, high-volume at-least-once output | producer `compression.type: lz4` | −30 % CPU, 1.8–2.1× bytes, +23 % P99, ~+1 GB memory; not under EOS (P99 ×2.3); **JVM only** (no lz4 in native images) |
| Resource saver, small pod, stateless | `lanes.count: 1` | −3…−34 % CPU; P99 doubles near one lane's capacity |
| Blocking I/O in user code | many lanes + `lanes.thread-type: VIRTUAL` (engine-wide) | throughput ≈ lanes ÷ call latency |
| At-least-once, stateless | `processing.dispatch-mode: DIRECT`, `processing.direct-dispatch-threads: 2` — **experimental**; `AUTO` picks it for an eligible topology | 0.80 vs 1.06 cores on LANE at 7,000 msg/s; refused under EOS |
| Large state, RocksDB-heavy | `rocks-db.preset: HIGH_PERFORMANCE` | −17 % CPU on the join chain, +0.7 GB — give the pod the memory (the off-heap reservation does not grow with the preset) |

## Lanes (parallelism)

`stoatflow.lanes.count` — default `max(2, CPU cores / 4)` per sub-topology (2 up to 11 cores, 3 at 12, 4 at
16); `stoatflow.lanes.thread-type` — default `PLATFORM`. Two lanes measured the cheapest from 1.5 to 8 cores.
Raise the count toward the cores when a CPU-bound stateful sub-topology lags while the CPU is idle; for
**I/O-bound** work (blocking calls) go **4×–8× cores with `thread-type: VIRTUAL`**. The thread type is
engine-wide; punctuation lanes are always virtual; a sub-topology with more `PLATFORM` lanes than cores logs a
WARN at startup, and the `Record lanes: …` INFO line shows each count, the thread type and their source.
Skewed keys won't benefit (per-key ordering pins a key to one lane). Fixed at startup — changing it needs a
restart. `stoatflow.lanes.queue-capacity` (default 300) is the backpressure buffer per lane. Lane count is
**decoupled from the input partition count**.

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
  `MEMORY_PRESSURE` commit barrier. **Lower it if you see OOMKills.** Rough container budget, never benchmarked:
  4 GiB → ~1 GiB, 8 GiB → 1–2 GiB, 16 GiB → 2–4 GiB — raise it only while `MEMORY_PRESSURE` barriers end most
  epochs and the heap has room. Timer writes count toward it too, on every timer backend,
  so a topology that registers many timers per record can fire these barriers even with no other state store.
  Under exactly-once the output held back while a commit runs counts too (a thread past the barrier keeps the next
  epoch's records until that commit is done), so high fan-out with large output records or slow commits can trip the
  cap with no state store at all; held output never fires a barrier — dispatch pauses until the commit completes
  (`stoatflow.dispatcher.stall.latency`).
  KS's `statestore.uncommitted.max.bytes` sets the same cap (its `-1`/disable is refused); KS's 64 MiB default is not
  adopted — a committing epoch stays in memory until flushed while the next accumulates, and the cap also bounds the
  epoch size. Watch `stoatflow.barrier.epoch.uncommitted.bytes` (the state total the cap applies to, the persistent
  timer store included), `stoatflow.store.uncommitted.bytes{store_name}` (which store drives `MEMORY_PRESSURE` commits;
  the timer store's share is the total minus their sum) and, under exactly-once, `stoatflow.eos.send.buffer.bytes` (the
  output held during a commit, counted with the state total).
- `stoatflow.caching.{max-estimated-bytes (256 MiB), max-entries (unbounded)}` — per-store cache caps →
  `CACHE_PRESSURE` early commit.
- `stoatflow.rocks-db.preset` — the block cache, shared across all stores, with every store's memtables charged
  inside it: `LOW_MEMORY` (32 MiB, 8 MiB × 2 write buffers per store) | `DEFAULT` (128 MiB, 16 MiB × 2) |
  `HIGH_PERFORMANCE` (512 MiB, 64 MiB × 4). Raise it when the block-cache hit ratio falls while the cache is at
  capacity. It is a budget, not a hard limit: memtables can pass the cache until their flush completes, and table
  readers, compaction and allocator overhead sit outside it. Kafka Streams instead gives each store its own cache and
  memtables. The image's `STOATFLOW_ROCKSDB_MB` reservation does not follow the preset — raise it with
  `HIGH_PERFORMANCE`.
- `stoatflow.rocks-db.backend` — `AUTO` (FFM on JVM, JNI on native). If you force `JNI` on the JVM, keep the
  lanes on `PLATFORM` threads (the default): a JNI call pins a virtual thread's carrier.
- `stoatflow.rocks-db-config-setter` — per-store RocksDB options. The write-ahead log is off by default (the
  changelog is the WAL); while it is, keep `options.db.atomicFlush` on (turning it off WARNs: offsets could reach disk
  ahead of data) and leave `options.write.sync` false (true refuses the store's open — RocksDB rejects synced writes
  without a WAL).

## Timer cache (every timer backend)

- `stoatflow.timer-backend.max-cached-timers` (default `100000`): at most that many timers **per time domain** in memory
  (heap + collected-not-fired), loaded in timestamp order — a catch-up, a burst at one instant, a restart or an HA
  failover never loads them all. Budget ~`2 ×` it entries, each a timer key plus its headers when ≤ 1 KiB (larger
  ones are read from the timer store at fire; ~200 B of tracing headers ≈ 70 MB at the default, a full 1 KiB on every timer ≈ 230 MB). A timer registered for a time already loaded
  joins a full cache (`timer.cache.overshoot.max`). `MEMORY` (formerly `HEAP`) bounds only this cache: it stores every
  timer in memory, so its total grows with the timer count, and it replays the timer changelog at every start and in-place
  restart (with its changelog and state restoration on (the defaults); otherwise its timers are lost) — prefer `HYBRID` for large timer volumes. `load-interval-ms` is gone.
- More event-time timers due than fit → event time waits at the first one not yet loaded while the loaded ones fire;
  records that would move event time past it wait in their buffers. A backlog `B` drains in ~`B / N` steps, each as
  long as `onTimer` takes for one cache-full. Raise N when holds are frequent; every watermark-driven effect (window
  closes, expiry, suppress) waits during a hold, so window segments accumulate meanwhile.

## Restoration

Cold-start / standby restore uses dedicated consumers under `stoatflow.kafka.restoration-consumer`
(framework defaults `max.poll.records=1000`, `fetch.max.bytes=50MiB`). For a faster restore raise
`max.poll.records: 5000` and `receive.buffer.bytes`. `group.id` / `enable.auto.commit=false` /
`auto.offset.reset=none` / `isolation.level=read_committed` (exactly-once only) are forced (not overridable). Under
at-least-once the restore reads at the `consumer` (or `restoration-consumer`) isolation level; set it there, not under
`main.consumer.`, which does not reach the restore. With `none`, a restore whose offset left the topic (retention or
`deleteRecords` moved the log start past it) fails the start, naming the store, and marks the store: the next start
restores it in full. A partition that holds nothing and has read nothing yet just starts at the new log start; a hot
standby's in-place re-restore makes three attempts in all before the pod restarts.
`stoatflow.state.segment-checkpoint-{records (100000),ms (30000)}` bound ungraceful-restart replay and
standby drain to ≤ one checkpoint interval, under both processing guarantees; the checkpoint runs off the commit path,
and an application that goes quiet is checkpointed once the interval passes.
A failed checkpoint loses nothing but makes later restarts replay more — alert on
`stoatflow.store.segment.checkpoint.failures.total`.

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
replay): changing **retention on a hand-built non-segmented store**, or a hand-built store's segmented ↔
non-segmented backend — the restore re-derives the segment id each key carries. Plan these with a deploy that can
absorb restore time.

**Refuses to start — the changelog cannot rebuild it:** on a store whose changelog is its recovery source,
switching **plain↔timestamped** (key-value or window), the **store type** (key-value ↔ versioned, window ↔
session), the **window size**, or **`retainDuplicates`** (coexist vs overwrite). The changelog holds the old
layout — a timestamped store's changelog values are `[ts 8B][value]`, a plain one's the bare value; a window
changelog's keys carry the window start only, the size being what every read adds to it; a duplicate-retaining
store's records each keep their own sequence number — so a rebuild would misread every record not rewritten since.
`StoreLayoutMismatchException` names the store, the field and both values; the store is left untouched, and
`stoatflow.state.layout-mismatch` does not apply. Remedies: put the previous setting back, use a **new store
name** (starts empty), or reset the application. Only a Processor API store can flip its value format or
`retainDuplicates` — every DSL store is timestamped whatever supplier `Materialized` gets; a **windowed
aggregation's window size** is the DSL case (resize it under a new `Materialized.as` name). A stream-stream join's
**`JoinWindows`** change is no layout change: a join reads its buffers by record timestamp, so it starts on its
local state with no restore. A source-topic table is rebuilt instead. Kafka Streams upgrades plain→timestamped in
place (its changelog carries the timestamp on the record — impossible with StoatFlow's negative event times) and
checks nothing else: a resized windowed aggregation reads its old windows with the new size; KSC-150.

**Watch the segment count.** `stoatflow.store.segments{store_name,store_type}` is the live number of data segments
a segmented store holds (~7 file descriptors each, no upper bound): a plateau at
`retention / segmentInterval + 1` is healthy; a climb means the watermark is pinned by a lagging-but-live
partition (or an idle one whose effective idleness timeout is 0 — the source's own `withIdleness` if it
sets one, else `watermark.idleness-timeout-ms`; the engine WARNs, naming the uncovered sources, at
start-up) and nothing is expiring. Alert on `stoatflow_watermark_lag_ms`, which is sampled every second and
so climbs during the stall.

**No changelog to restore from?** A store with `withLoggingDisabled()`, or any store under
`changelog.enabled: false`, and not restored from its source topic refuses to start on a layout change — its local state is the only copy and
StoatFlow will not delete it for you. The message names every affected store and the field that moved.
Put the setting back, enable the changelog, or set `stoatflow.state.layout-mismatch: wipe-and-restore` to
accept the loss (those stores start **empty**; remove the setting afterwards, it does not expire).
The check covers every RocksDB store a built-in builder creates (key-value, versioned and timer too, not only
window/session; a custom `StateStoreBuilder`'s store is not compared) and also catches a **rollback**: a
no-recovery-source store whose local format or layout-stamp schema another build wrote refuses — the message
says run the build that wrote it (roll forward), or accept the loss — or starts empty under `wipe-and-restore`. A store holding
changelog records a newer build wrote is never wiped, whatever the setting — roll forward (SKILL.md →
Upgrades and rollback).

State written before the segment-layout change cannot be read (old directory names carried no time
ranges). The app refuses to start and names the remedy: delete the store's local state dir; the changelog
restores it. One-time pre-1.0 break, no migration path.

## When to touch what (symptom → lever)

| Symptom | Lever |
|---|---|
| Lag grows while the CPU is idle (CPU-bound stateful) | raise `lanes.count` toward the cores |
| I/O-bound throughput plateau | raise `lanes.count` (4×–8× cores) + `lanes.thread-type: VIRTUAL` |
| OOMKilled | lower `state.uncommitted-max-bytes`; check `rocks-db.preset` |
| Block-cache hit ratio dropping at capacity | `rocks-db.preset: HIGH_PERFORMANCE` |
| Commit latency too high | raise the `commit-barrier` interval bounds |
| Slow cold start | raise `restoration-consumer.max.poll.records`; ensure a warm PVC |
| Event time advances in steps during a catch-up, `watermark.timer.held` toggling or `rate(watermark_timer_held_time_total)` high | raise `timer-backend.max-cached-timers` (memory ≈ 2 × it entries, each a key plus ≤ 1 KiB of headers); keep I/O out of `onTimer` |
| `watermark.timer.held` stuck at 1, `timer.fired.total` flat, `Event time has been held at the timer frontier` WARN | a stuck or very slow `onTimer` — fix it; the cache size is not the cause |
