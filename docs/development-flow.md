# Development Flow

This project uses OpenSpec for spec-driven development. `AGENTS.md` is the
authoritative contributor and agent guide; this file is a short checklist.

## One Change

1. Explore current specs and code before editing:
   - `openspec list --specs`
   - `openspec list`
   - read relevant files under `openspec/specs/`
2. Propose the change:
   - `openspec new change "<change-name>"`
   - write `proposal.md`, `design.md`, `tasks.md`, and delta specs
   - commit as `docs(<change-name>): propose <summary>`
3. Apply the change:
   - implement against `openspec/changes/<change-name>/specs/`
   - check off tasks only after code and tests pass
   - commit coherent compiling milestones as `feat(...)` or `fix(...)`
4. Sync verified semantics:
   - promote verified delta specs into `openspec/specs/`
   - commit as `docs(specs): sync <change-name>`
5. Archive the completed change:
   - `openspec archive <change-name>`
   - commit as `chore(openspec): archive <change-name>`

## Commit Granularity

Apply commits should be larger than individual task checkboxes and smaller than
an entire risky feature. Prefer one commit per coherent milestone that builds,
tests, and preserves the spec contract.

Avoid:

- committing unrelated docs, refactors, and behavior together
- checking off `tasks.md` before the Definition of Done passes
- syncing `openspec/specs/` before implementation has been verified

## Definition Of Done

Run these from the workspace root:

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```
