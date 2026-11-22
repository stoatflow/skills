# Carrying state across — the data half of a port

The `stoatflow-port-from-ks` code swap gets your topology running on StoatFlow. For a **stateful** app
whose state is too expensive to rebuild by reprocessing (long-window aggregates, KTables over compacted
topics with lost history, high-volume sources with short retention), you also carry the **state** across
so the ported app resumes exactly where Kafka Streams stopped. That is the **`stoatflow-migration-tool`**.

**Skip this** — reprocess instead (fresh `application.id` + consumer group, `auto.offset.reset=earliest`,
rebuild from source/changelogs) — when the app is stateless or the state is cheap to rebuild. Reprocessing
is the simpler default path.

## The tool

`io.stoatflow:stoatflow-migration-tool` is a **standalone offline translator CLI** — `kafka-clients` only,
JVM 17+, a shadow fat jar, published to `maven.stoatflow.io` **only** (version-lockstep with the StoatFlow
release you are migrating *to*), **no licence gate**. It reads the KS changelog topics `read_committed`,
applies per-store-type **byte-level translation** into pre-created StoatFlow changelog topics, and carries
the input consumer-group offsets over. **The engine needs zero changes**: a first StoatFlow start over
seeded changelogs is a forced full restore.

> **It is a separate CLI, not part of your app.** Use only its four documented commands and the config
> fields below. Do **not** invent tool flags, and do not try to describe or reproduce its internal
> byte-layout translation — that is an implementation detail of the tool.

Get it (customer/trial credentials, as for any StoatFlow artifact):

```bash
curl -u <user>:<token> -O \
  https://maven.stoatflow.io/releases/io/stoatflow/stoatflow-migration-tool/<version>/stoatflow-migration-tool-<version>-all.jar
java -jar stoatflow-migration-tool-<version>-all.jar plan -c migration.yaml
```

Match the tool version to the StoatFlow version you are migrating to.

## The four commands

All run **inside one quiesce window with the KS app cleanly stopped**; each is re-runnable.

| Command | Does |
|---|---|
| `plan -c migration.yaml` | Verifies everything, **writes nothing**. Discovers `{ks-app}-*-changelog` topics and cross-checks them against the config; asserts the KS group is EMPTY and repartition lag is 0; warns loudly under ALO (clean shutdown mandatory); runs structural type heuristics; resolves source-KTable paths; asserts target partition counts; checks `message.timestamp.type=CreateTime` and headers-vs-declared; captures the consistency point. |
| `translate -c migration.yaml [--force]` | Creates the target changelog topics with StoatFlow's exact configs and seeds them (`read_committed`, earliest→LSO); seeds the `{sfName}-emitfrontier` companions for stores marked `emitFrontier: true`. Re-run needs an empty target or `--force` (delete + recreate; no partial resume). |
| `seed-offsets -c migration.yaml` | Copies the KS committed offsets for `inputTopics` into the (empty) StoatFlow consumer group. Run **before** the first StoatFlow start. Recommend `AutoOffsetReset.none()` on migrated sources so a partial seed fails loud. |
| `verify -c migration.yaml` | Per store: count + order-independent checksum of the seeded topic vs a re-read through the same translation, spot byte-comparisons, partition-count assert, emit-frontier check. It proves faithful *seeding*, not classification correctness — so a **post-start IQ spot-check is a required runbook gate**, not optional. It **refuses** (exits 1, `ABORTED: …` naming the topic, partition, offset and version) when a target changelog record carries a StoatFlow changelog format version (`__stoatflow.v` header) newer than the tool knows — it cannot check what it cannot decode; use a tool version that matches the application — or a malformed one (not a single byte, or `0`), which no tool version reads: something other than StoatFlow wrote that header. |

## The config (`migration.yaml`)

```yaml
kafka:
  bootstrap.servers: broker:9092
  # + optional security client props passthrough
source:                       # the Kafka Streams app you migrate FROM
  applicationId: my-ks-app
  processingGuarantee: exactly_once   # REQUIRED: exactly_once | at_least_once (drives the ALO warning)
  inputTopics: [orders, customers]    # topics whose group offsets carry over
  ignoreStores: []                    # opt-out for deliberately-left-behind stores (e.g. a drained suppress buffer)
target:                       # the StoatFlow app you migrate TO
  applicationId: my-sf-app
  changelogNumPartitions: 3           # DEFAULT for every store; must match the SF app's EFFECTIVE count.
                                      # plan/verify assert each store against its OWN effective count, so a
                                      # store using Materialized.withChangelogNumPartitions(n) sets
                                      # stores[].changelogNumPartitions to match (see the stores block).
                                      # against live broker metadata. A store using
                                      # Materialized.withChangelogNumPartitions(n) needs a matching value.
                                      # The tool cannot read the app's topology, and a changelog seeded at
                                      # the wrong count is not repairable in place.
  replicationFactor: 3
stores:
  - ksName: order-totals              # KS store name  → changelog {ks-app}-order-totals-changelog
    sfName: order-totals              # SF store name  (default = ksName; differs if the port renamed a store)
    type: kv-timestamped              # see the classification table below
    extraTopicConfigs: {}             # optional per-store topic-config overrides
    recordHeaders: false              # true iff the store is headers-aware (KIP-1271) — carries native headers
    emitFrontier: true                # OnWindowClose aggregations only: seed {sfName}-emitfrontier
    retainDuplicates: false           # window-timestamped only: a timestamped / headers-aware store built with
                                      #   retainDuplicates = true (a plain one is window-duplicates)
    sourceTopic: null                 # for source-KTable stores: the source topic (path resolution)
```

**Name every migrated store explicitly on both sides** (`Materialized.as(...)`,
`StreamJoined.withStoreName(...)`, `TableJoined.as(...)`) so the `ksName → sfName` mapping is trivial — KS
auto-generated names (`KSTREAM-AGGREGATE-STATE-STORE-0000000007`) are not guaranteed to match the ported
topology's generated names. This is mandatory for FK joins: with `TableJoined.as("{join-name}")` the
subscription store is `{join-name}-subscription-store` on **both** sides, but an *unnamed* StoatFlow FK
join auto-generates a counter-suffixed base (`ktable-fkjoin-0`, `ktable-fkjoin-1`, …) that never matches
the KS auto-generated name.

## Classifying your stores (KS DSL operation → `type`)

Legal `type` values: `kv`, `kv-timestamped`, `window`, `window-timestamped`, `window-duplicates`,
`join-window`, `session`, `versioned`, `fk-subscription`, `outer-join`.

| KS DSL operation / store | `type` |
|---|---|
| `count` / `reduce` / `aggregate` (KTable aggregation) | `kv-timestamped` |
| `builder.table(...)` source / `toTable()` / `KTable` operator store | `kv-timestamped` |
| Foreign-key-join or table-table join **result** store | `kv-timestamped` |
| `windowedBy(TimeWindows/SlidingWindows).aggregate` | `window-timestamped` |
| `windowedBy(SessionWindows).aggregate` | `session` |
| Stream-stream join window stores (`-this-join-store`, `-other-join-store`, `KSTREAM-JOINTHIS-…`) | `join-window` — `window-duplicates` is refused for these names, and for a StoatFlow `sfName` ending `-left-store` / `-right-store`. A buffer named by a store supplier keeps that name on both sides, which the tool cannot recognise: declare it `join-window` yourself (as `window-duplicates` its values would decode wrongly after cutover). `recordHeaders: true` **only** if the KS app ran `dsl.store.format=headers` or in-memory join stores; **never** for the default PLAIN format (its buffers dropped the headers locally, while the 4.3+ changelog still carries them — migrating them would decode the first post-cutover match against the input's schema id). `plan` WARNs on every `join-window` with `recordHeaders: true`, and notes one left at `false` whose changelog carries headers (right for PLAIN; a HEADERS or in-memory app with header-mode join serdes needs `true`) |
| Processor API duplicate-retaining window store (plain) | `window-duplicates` |
| Processor API duplicate-retaining **timestamped or headers-aware** window store (`Stores.persistentTimestampedWindowStore[WithHeaders](…, true)`) | `window-timestamped` + `retainDuplicates: true` (+ `recordHeaders: true` for headers) — as `window-duplicates` its values would lose their timestamp; without the flag `plan` reads the non-zero seqnums as a misclassified store |
| LEFT/OUTER stream-stream **shared** (outer) store | `outer-join` |
| Foreign-key-join **subscription** store | `fk-subscription` |
| Versioned KTable (KIP-889) | `versioned` *(experimental — see the tombstone note below)* |
| Any of the above that is **headers-aware** (KIP-1271) | family type **+ `recordHeaders: true`** |

**Carried headers (`recordHeaders: true`).** The tool copies the KS changelog's native record headers minus: Kafka
Streams' internal **trailing** consistency pair (`v` = `0x00`, then `c` — KS appends it last, only with its internal
IQ consistency setting on; a `v` or `c` anywhere else is the application's and stays), StoatFlow's reserved names
(`__stoatflow.v`, `__stoatflow.v.*`) and its engine stamp `stoatflow.epoch`. A StoatFlow headers-aware store never
holds those (KSC-149), so the migrated store's headers match what it would have written itself. A `join-window` store
keeps them inside each value (its serde-headers block), not on the record; there the tool drops only the trailing
consistency pair, since a StoatFlow join buffer keeps every header its serializers write.

> **Versioned stores: deletes are re-encoded, and must be.** Kafka Streams logs `delete(key, ts)` as a
> Kafka **null**. StoatFlow reserves a changelog null for its retention sweep's *reclamation* tombstones
> — with a composite `[key][ts]` changelog key, compaction can never collapse versions, so a per-version
> null is the only thing that can bound the topic — and writes a logical delete as the 5-byte sentinel
> `\0TOMB`. `translate` handles this for `type: versioned`; nothing is required of you. It matters only
> if you build your own translation: passing a KS null through restores as the *removal of that version*,
> and since a versioned `get` has no retention floor guard the key then answers with its **pre-delete
> value** — a silent resurrection, not an error.
>
> **The Kafka Streams changelog topic may already be missing versions, and the tool cannot recover
> them.** Kafka Streams' versioned changelog is a bare key with the version timestamp as the record timestamp; the log cleaner
> keeps the *highest offset* per key, and Kafka Streams logs even a write its store rejected. So an
> out-of-order or expired write that landed last can be the only record left for a key once the topic
> has compacted, and the migrated store answers that older version as the latest where the KS
> application answered the newer one. Not detectable from the changelog itself; confirmed against a
> live cleaner. StoatFlow's own composite `[key][ts]` changelog cannot lose versions this way.

> **The `kv` vs `kv-timestamped` trap (byte-silent).** Every StoatFlow DSL key-value store is timestamped,
> as in KS — so every DSL KV store is `kv-timestamped`; `kv` is only for a Processor API store you built
> plain. A mismatch either way is **silent corruption** (a prepended timestamp read as value bytes, or a
> value too short to hold one). `plan`'s heuristics can't catch it (KS writes bare values for both) — the
> **post-start IQ spot-check catches it end to end**.

**Source-KTable stores** are conditional: a **compacted** source topic restores for free (omit the store
from `stores`); a **non-compacted** source needs the changelog translated as `kv-timestamped`, or the ported code to
force `Consumed.materializeFromSourceTopic(true)`. `plan` resolves and prints the path per store.

## Cutover runbook

1. **Prepare (KS running):** port the code; name all migrated stores explicitly; build the ported topology
   once with `topology.validation.declared-serde-over-default=error` (a `TopologyTestDriver` run suffices) and
   declare the serde at every site it lists — StoatFlow uses an upstream serde where KS used a differing
   default at `groupByKey(Grouped)`, stream-stream joins, cogroup and `merge`, and the tool copies KS's bytes
   without seeing the app's serdes; mark OnWindowClose aggregation stores `emitFrontier: true`; write
   `migration.yaml`; dry-run `plan` (expect "group not empty" — everything else validates).
2. **Quiesce:** stop input production if the SLA requires; let KS drain repartition topics; if suppress is
   in play, reach a windows-closed point so the buffer drains; cleanly stop KS (group → EMPTY). **Under
   ALO, a clean shutdown is mandatory** — never migrate a crashed ALO snapshot.
3. **`plan`** — all checks green; consistency point captured.
4. **`translate`** — seed all store changelogs (+ the emit-frontier companions). KS stays down.
5. **`seed-offsets`** — carry input offsets into the StoatFlow group.
6. **`verify`** — counts / checksums / partition counts green.
7. **First StoatFlow start** — forced full restore (watch `stoatflow.restoration.*`; `/health/ready` gates
   traffic); confirm `/offsets`; **required IQ spot-check** against the still-stopped KS state.
8. **Validate & switch** — bounded side-by-side output inspection, then point downstream at the SF outputs.
9. **Retire** — after sign-off: delete the KS deployment, group, and internal topics.

**Rollback line:** before step 7 the KS side is untouched (the tool only *reads* KS topics), so rollback =
restart KS and delete the seeded SF topics/group. **The point of no return is the first StoatFlow
transactional commit into shared sinks** (step 7 onward) — after that, rolling back means KS re-produces
output StoatFlow already produced, which EOS cannot dedupe across apps. Keep KS deployable until sign-off.

## Cutover caveats

- **Suppress buffers do not translate (v1).** Any final still sitting in the KS suppress buffer at
  shutdown is lost. Mitigation: cut over at a windows-closed point (quiesce, let stream time advance past
  window-end + grace so KS emits, then stop). A hard input stop *before* the drain strands the buffer.
- **Emit-frontier / duplicate re-emission.** Without `emitFrontier: true`, the first watermark tick
  re-emits a "final" for every restored closed window (a one-time burst; harmless for idempotent-upsert
  consumers, disruptive for append/incremental ones). Set the flag to seed the frontier and avoid it.
- **ALO clean-shutdown.** Only a clean KS shutdown aligns changelog state with committed input offsets;
  migrating a crashed ALO snapshot inherits at-least-once duplication. EOS snapshots are crash-safe.
- **Watermark / retention boundaries.** StoatFlow's stream time is the **watermark** — at resume it
  rebuilds from live records with the configured strategy, so window closure/expiry around the boundary can
  differ from KS by up to the out-of-orderness bound. Restored records older than retention expire from the
  first post-restore watermark tick onward (same as native state) — but the boundary is **not** exactly
  `retention`: a segmented store drops a segment only once its *entire* range has cleared
  `watermark - retention`, so records survive up to `retention + segmentInterval`. Expect that overhang on
  both sides when diffing StoatFlow against KS record by record; KS makes the same minimum-bound promise,
  sized `max(retention / 2, 60 s)`. Under `EmitStrategy.onWindowClose()` the overhang is larger again —
  expiry is gated on the durable emit frontier, not the watermark — so do not treat
  `retention + segmentInterval` as the ceiling when diffing such a topology.
- **Logging-disabled stores** have no changelog to translate from — reprocess or accept the loss.
- **Versioned stores are experimental** — extra `verify` scrutiny on history retention at the boundary.

## See also

- Website — strategy + classification: <https://stoatflow.io/docs/migration/with-data-migration>
- Website — the tool how-to: <https://stoatflow.io/docs/migration/migration-tool>
- `references/stoatflow-primer.md` — the shared identity rules
