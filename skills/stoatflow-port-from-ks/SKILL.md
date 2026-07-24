---
name: stoatflow-port-from-ks
description: Port or migrate an existing Kafka Streams (or KSML) application to StoatFlow — code and state. Rewrite org.apache.kafka.streams imports to io.stoatflow.core, apply the known divergence fixes, triage post-swap compile errors, and carry existing state across with the stoatflow-migration-tool (translate KS changelog topics, seed input offsets) for a stateful cutover. Use for migrating existing Kafka Streams code; for writing new StoatFlow code use stoatflow-build-topology.
license: Apache-2.0
---

# Port a Kafka Streams app to StoatFlow

Read `references/stoatflow-primer.md` first — the identity rules there (never import
`org.apache.kafka.streams.*`, never depend on `kafka-streams`, EXACTLY_ONCE default, single-instance
scaling) are exactly what a Kafka-Streams-trained assistant gets wrong on a port.

StoatFlow reimplements the **Kafka Streams 4.3** DSL/API surface under `io.stoatflow.core.*` and depends
on **`kafka-clients` only**. A port is therefore mostly a **mechanical import rewrite** plus a short,
enumerated list of intentional divergences (the register). Binary compatibility (dropping in the real
`kafka-streams` jar) is **not** pursued and never will be.

A port has **two halves**:

1. **Code** — this skill: the import codemod (§1), the residual divergence fixes (§2), the do-not-change
   list (§3), compile-error triage.
2. **State** — carrying existing changelog/state across so the app resumes where KS stopped, instead of
   reprocessing. See **`references/data-migration.md`** (the `stoatflow-migration-tool`). Skip this for
   stateless apps or when reprocessing is acceptable.

## Runbook (code half)

1. **Automated port (recommended).** Run the OpenRewrite recipe `io.stoatflow:stoatflow-openrewrite-recipe`
   — it rewrites imports, the entry point, and the build across **Java and Kotlin** on a type-attributed
   tree (never touches string literals or look-alike names), and fixes the mis-routes a blind sweep
   introduces.
   - **Run it against a project that still compiles against Kafka Streams** — the rules match resolved
     types; a source that does not type-check is silently skipped (the recipe's `FlagUnparsedSources`
     reports any such file, so this fails loudly).
   - **Use JDK 17, 21, or 25** to run the recipe. The *ported app* then requires **JDK 25**.
   - Configure the StoatFlow Maven repo + credentials first (see `stoatflow-project-setup`); for Maven,
     make it visible to `<pluginRepositories>` too.
   ```bash
   # Maven — writes target/rewrite/rewrite.patch, changes nothing on dryRun
   mvn org.openrewrite.maven:rewrite-maven-plugin:dryRun \
     -Drewrite.recipeArtifactCoordinates=io.stoatflow:stoatflow-openrewrite-recipe:<version> \
     -Drewrite.activeRecipes=io.stoatflow.rewrite.MigrateFromKafkaStreams
   mvn org.openrewrite.maven:rewrite-maven-plugin:run   # same flags, applies it

   # Gradle — see the recipe README for rewrite.init.gradle.kts
   ./gradlew --init-script rewrite.init.gradle.kts rewriteDryRun
   ./gradlew --init-script rewrite.init.gradle.kts rewriteRun
   ```
   The recipe version **is** the StoatFlow version you are adopting.

2. **Or the manual import sweep** (no-tooling fallback) — see the `references/divergences.md` §1 table.

3. **Build.** Remaining compile errors should map one-to-one onto the §2 rows in
   `references/divergences.md`. Anything else is a recipe bug.

4. **Carry state** (stateful apps) — `references/data-migration.md`.

## §1 — The import swap (the whole of step one)

Replace the package prefix and **collapse the KS sub-package**. `StreamsBuilder`, `KStream`,
`Materialized`, `Stores`, `Processor`, etc. keep their simple names.

| Kafka Streams prefix | StoatFlow prefix |
|---|---|
| `org.apache.kafka.streams.kstream.` | `io.stoatflow.core.topology.` |
| `org.apache.kafka.streams.state.` | `io.stoatflow.core.state.` |
| `org.apache.kafka.streams.processor.api.` | `io.stoatflow.core.processor.` |
| `org.apache.kafka.streams.processor.` | `io.stoatflow.core.processor.` |
| `org.apache.kafka.streams.errors.` | `io.stoatflow.core.exception.` |
| `org.apache.kafka.streams.StreamsBuilder` | `io.stoatflow.core.topology.StreamsBuilder` |
| `org.apache.kafka.streams.TopologyConfig` | `io.stoatflow.core.config.TopologyConfig` (rewrite **before** `Topology`) |
| `org.apache.kafka.streams.Topology` | `io.stoatflow.core.topology.Topology` |
| `org.apache.kafka.streams.KeyValue` | `io.stoatflow.core.state.KeyValue` |
| `org.apache.kafka.streams.KafkaStreams.State` | `io.stoatflow.core.KafkaStreamsState` |
| `org.apache.kafka.streams.CloseOptions` | `io.stoatflow.core.CloseOptions` |

`org.apache.kafka.common.*` and `org.apache.kafka.clients.*` (serdes, `Headers`, `ConsumerRecord`,
`ProducerRecord`, …) are **unchanged** — those are `kafka-clients` types StoatFlow uses directly.

A handful of `processor.*` types mis-route under the blind swap (they don't live under
`io.stoatflow.core.processor.`) — `StreamPartitioner`, `StateStore`, `TimestampExtractor`,
`TopicNameExtractor`, `RecordContext`, `TaskId`, `RocksDBConfigSetter`. The recipe rewrites them; by hand,
fix them per `references/divergences.md`.

## §2 — The residual fixes (full table in references/divergences.md)

The import swap gets you compiling except for a small set of **intentional divergences**. The ones a
typical port always hits:

- **Entry point (KSC-86):** `new KafkaStreams(topology, props)` → `StoatFlow.fromBuilder(new
  StreamsConfig(props), builder)` — pass the **builder**, not the topology. `StoatFlow.fromBuilder` is
  `@JvmStatic`. The recipe rewrites this when the builder is in the same method.
- **`processing.guarantee` default (KSC-84):** absent ⇒ StoatFlow runs **EXACTLY_ONCE** (KS: `at_least_once`).
  A ported app that relied on the KS default **silently changes semantics**. Set `at_least_once`
  explicitly if that's what you want.
- **`StreamPartitioner` (KSC-34):** lives in `io.stoatflow.core.topology`, not `processor`. Multicast on
  `repartition()` throws (single-instance) — multicast only on sinks.
- **Exception handlers (KSC-01):** `ProcessingExceptionHandler` takes a `processor.Record<*,*>`, not
  separate key/value; production serialization errors fold into `handle(...)` (branch on
  `(context as ProductionContext).failedOn`).
- **`Suppressed.maxBytes` (KSC-35):** throws (in-memory object storage) — use `maxRecords`/`unbounded`.

The **complete, register-ID-tagged** table (every `KSC-NN` / `D-N`, with the exact fix) is in
**`references/divergences.md`**. Work it against your build errors.

## §3 — What you do NOT change

The DSL itself, functional-interface lambdas, windowed `Materialized`, sliding windows, state-store
types, `Named`, IQ metadata — all source-compatible. Details + the exceptions in
`references/compat-notes.md`.

## Hard rules (from the primer)

- **NEVER** leave an `org.apache.kafka.streams.*` import — always `io.stoatflow.core.*`.
- **NEVER** add a `kafka-streams` dependency — swap it for `io.stoatflow:stoatflow-core`.
- After porting, **assume EXACTLY_ONCE** unless the config sets `at_least_once`.
- The **migration tool is a separate CLI** (`io.stoatflow:stoatflow-migration-tool`) — teach only its
  four documented commands and config; never invent tool flags. See `references/data-migration.md`.

## See also

- `references/divergences.md` — the full §1 codemod + §2 residual table (KSC-tagged)
- `references/compat-notes.md` — the do-not-change list + compat-matrix pointers
- `references/data-migration.md` — carrying state across (the `stoatflow-migration-tool`, ADR-137)
- `references/stoatflow-primer.md` — shared identity + architecture
- Website: <https://stoatflow.io/docs/migration> · Compat matrix:
  <https://stoatflow.io/docs/reference/ks-compatibility-matrix>
