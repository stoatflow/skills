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
release you are migrating *to*), **no license gate**. It reads the KS changelog topics `read_committed`,
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
| `verify -c migration.yaml` | Per store: count + order-independent checksum of the seeded topic vs a re-read through the same translation, spot byte-comparisons, partition-count assert, emit-frontier check. It proves faithful *seeding*, not classification correctness — so a **post-start IQ spot-check is a required runbook gate**, not optional. |

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
  changelogNumPartitions: 1           # must match the SF app's config (default 1); plan/verify assert it
  replicationFactor: 3
stores:
  - ksName: order-totals              # KS store name  → changelog {ks-app}-order-totals-changelog
    sfName: order-totals              # SF store name  (default = ksName; differs if the port renamed a store)
    type: kv-timestamped              # see the classification table below
    extraTopicConfigs: {}             # optional per-store topic-config overrides
    recordHeaders: false              # true iff the store is headers-aware (KIP-1271) — carries native headers
    emitFrontier: true                # OnWindowClose aggregations only: seed {sfName}-emitfrontier
    sourceTopic: null                 # for source-KTable stores: the source topic (path resolution)
```

**Name every migrated store explicitly on both sides** (`Materialized.as(...)`,
`StreamJoined.withStoreName(...)`, `TableJoined.as(...)`) so the `ksName → sfName` mapping is trivial — KS
auto-generated names (`KSTREAM-AGGREGATE-STATE-STORE-0000000007`) are not guaranteed to match the ported
topology's generated names.

## Classifying your stores (KS DSL operation → `type`)

Legal `type` values: `kv`, `kv-timestamped`, `window`, `window-timestamped`, `window-duplicates`,
`session`, `versioned`, `fk-subscription`, `outer-join`.

| KS DSL operation / store | `type` |
|---|---|
| `count` / `reduce` / `aggregate` (KTable aggregation) | `kv-timestamped` |
| `builder.table(...)` **source** store | **`kv`** (plain — see the trap below) |
| Foreign-key-join **result** store | **`kv`** |
| `windowedBy(TimeWindows/SlidingWindows).aggregate` | `window-timestamped` |
| `windowedBy(SessionWindows).aggregate` | `session` |
| Stream-stream join window stores | `window-duplicates` |
| LEFT/OUTER stream-stream **shared** (outer) store | `outer-join` |
| Foreign-key-join **subscription** store | `fk-subscription` |
| Versioned KTable (KIP-889) | `versioned` *(experimental)* |
| Any of the above that is **headers-aware** (KIP-1271) | family type **+ `recordHeaders: true`** |

> **The `kv` vs `kv-timestamped` trap (byte-silent).** StoatFlow's `table()` **source** stores and the
> FK-join **result** store are **plain KV** — even though the *KS* equivalents are timestamped. Declaring
> `kv-timestamped` for one of these prepends a timestamp the StoatFlow store reads as value bytes →
> **silent corruption**. When in doubt, `plan`'s heuristics can't fully catch this (plain and timestamped
> KV changelogs are wire-identical) — the **post-start IQ spot-check catches it end to end**.

**Source-KTable stores** are conditional: a **compacted** source topic restores for free (omit the store
from `stores`); a **non-compacted** source needs the changelog translated as `kv`, or the ported code to
force `Consumed.materializeFromSourceTopic(true)`. `plan` resolves and prints the path per store.

## Cutover runbook

1. **Prepare (KS running):** port the code; name all migrated stores explicitly; mark OnWindowClose
   aggregation stores `emitFrontier: true`; write `migration.yaml`; dry-run `plan` (expect "group not
   empty" — everything else validates).
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
  differ from KS by up to the out-of-orderness bound. Restored records older than retention expire on the
  first post-restore watermark tick (same as native state).
- **Logging-disabled stores** have no changelog to translate from — reprocess or accept the loss.
- **Versioned stores are experimental** — extra `verify` scrutiny on history retention at the boundary.

## See also

- Website — strategy + classification: <https://stoatflow.io/docs/migration/with-data-migration>
- Website — the tool how-to: <https://stoatflow.io/docs/migration/migration-tool>
- `references/stoatflow-primer.md` — the shared identity rules
