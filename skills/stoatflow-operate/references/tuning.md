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

- `stoatflow.state.uncommitted-max-bytes` — default 256 MB. A backstop: exceeding it fires an early
  `MEMORY_PRESSURE` commit barrier. **Lower it if you see OOMKills.** Rough container budget: 4 GiB → ~1 GiB,
  8 GiB → 1–2 GiB, 16 GiB → 2–4 GiB.
- `stoatflow.caching.{max-estimated-bytes (256 MB), max-entries (unbounded)}` — per-store cache caps →
  `CACHE_PRESSURE` early commit.
- `stoatflow.rocks-db.preset` — `LOW_MEMORY` (64 MB) | `DEFAULT` (256 MB) | `HIGH_PERFORMANCE` (1 GB),
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

## When to touch what (symptom → lever)

| Symptom | Lever |
|---|---|
| I/O-bound throughput plateau | raise `lanes.count` (4×–8× cores) |
| OOMKilled | lower `state.uncommitted-max-bytes`; check `rocks-db.preset` |
| Block-cache hit ratio dropping at capacity | `rocks-db.preset: HIGH_PERFORMANCE` |
| Commit latency too high | raise the `commit-barrier` interval bounds |
| Slow cold start | raise `restoration-consumer.max.poll.records`; ensure a warm PVC |
