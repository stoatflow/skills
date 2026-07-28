# Changelog — StoatFlow AI Assistant Skills

Versioning tracks StoatFlow releases: the pack version **is** the StoatFlow version it targets. Docs-only
fixes between releases are tagged `vX.Y.Z-skills.N`.

## Unreleased

- Initial pack: six task-cut skills (`stoatflow-build-topology`, `stoatflow-test`,
  `stoatflow-port-from-ks`, `stoatflow-configure`, `stoatflow-project-setup`, `stoatflow-operate`) + a
  shared primer, the cross-tool `AGENTS.md` distillate, and generated Cursor / Copilot / JetBrains wrappers.
- Claude Code plugin + self-hosted marketplace (`stoatflow@stoatflow`); `npx skills add stoatflow/skills`.
- Drift-checked against `KS-PORTING.md`, the compatibility matrix, and the config JSON schema.
- Point to the retrieval-side complement — `stoatflow.io/llms.txt` / `llms-full.txt` and the
  `/raw/<path>.md` pages — from the README, the `AGENTS.md` links block, and the shared primer, so an
  assistant can look a detail up instead of guessing.
