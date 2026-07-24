# StoatFlow AI Assistant Skills

Teach your AI coding assistant to write **correct StoatFlow code**.

StoatFlow is source-compatible with Kafka Streams 4.3 via an import swap — which means every assistant's
Kafka-Streams training makes it *confidently wrong* on exactly the points where StoatFlow diverges: the
import root, the `EXACTLY_ONCE` default, single-instance scaling (lanes, not replicas), the private Maven
repository, watermark semantics, and more. This pack puts the right answers in front of the assistant.

> **Targets StoatFlow 1.0.0-beta.3** (Kafka Streams 4.3 API surface). Pre-GA — the API may still
> change. If your StoatFlow version differs, use the matching tagged release of this pack (`git tag` /
> release list).

## What's in it

Six task-cut skills + a shared primer:

| Skill | Fires when you're… |
|---|---|
| `stoatflow-build-topology` | writing topology code (DSL, Processor API, serdes, state stores, DLQ) |
| `stoatflow-test` | writing tests (`TopologyTestDriver`, integration tests) |
| `stoatflow-port-from-ks` | porting a Kafka Streams / KSML app (code **and** state) |
| `stoatflow-configure` | configuring an app (`application.yaml`, guarantees, lanes, HA) |
| `stoatflow-project-setup` | wiring the build (private Maven repo, license, JDK 25, Docker, native image) |
| `stoatflow-operate` | deploying and running it (single-instance Kubernetes, HA, probes, metrics, tuning) |

## Install

| Tool | Install |
|---|---|
| **Claude Code** | `/plugin marketplace add stoatflow/skills` → `/plugin install stoatflow@stoatflow` |
| **Any agent via the skills CLI** | `npx skills add stoatflow/skills` |
| **Codex / AGENTS.md tools** | copy `AGENTS.md` into your app repo |
| **Cursor** | copy `cursor/rules/stoatflow.mdc` → `.cursor/rules/` (or use `AGENTS.md`) |
| **GitHub Copilot** | copy `copilot/stoatflow.instructions.md` → `.github/instructions/` |
| **JetBrains AI / Junie** | copy `jetbrains/guidelines.md` → `.junie/guidelines.md` |

## Keeping it current

This pack ships **in lockstep with StoatFlow releases** and is drift-checked against the porting guide,
the compatibility matrix, and the config schema on every publish — so it does not rot into authoritative
stale answers. Pin the tag that matches your StoatFlow version.

## Support & license

- **Docs:** <https://stoatflow.io/docs> · **Compatibility matrix:** <https://stoatflow.io/docs/reference/ks-compatibility-matrix>
- **Trial / access:** <https://stoatflow.io> — StoatFlow itself is commercial and resolved from
  `maven.stoatflow.io` with customer credentials (this pack is free and open, but the library it
  documents is not).
- **Issues / PRs:** welcome here; fixes land upstream in the StoatFlow monorepo and mirror back.

This pack is licensed under **Apache-2.0** (see `LICENSE`/`NOTICE`). The StoatFlow software is separate,
commercial, closed-source, and trademarked.
