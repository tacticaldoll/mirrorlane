# AGENTS.md

Meta-guideline for any AI coding agent working in this repository. Read this
first.

## This Project Uses OpenSpec

The source of truth lives in `openspec/`, which is version-controlled and
agent-agnostic.

- `openspec/specs/` - the living specification of what the system currently is.
- `openspec/changes/` - active change proposals as delta specs (created when you
  propose a change; absent on a clean tree).
- `openspec/changes/archive/` - completed changes, once any have been archived.

Per-agent command files such as `.codex/`, `.claude/`, and editor-specific shims
are per-clone generated files and are not committed. After cloning, generate
your own with:

```bash
openspec init --tools codex
# or: openspec init --tools claude,cursor,github-copilot
```

## Workflow

Follow this lifecycle:

```text
explore -> propose -> apply -> sync -> archive
```

1. **Explore**: think and investigate only. Do not write feature code outside of
   a change.
2. **Propose**: create a change with `proposal.md`, `design.md`, `tasks.md`, and
   delta specs.
3. **Apply**: implement tasks one at a time, checking each off in `tasks.md`
   only after verification.
4. **Sync**: merge verified delta specs back into `openspec/specs/`.
5. **Archive**: move the completed change to
   `openspec/changes/archive/YYYY-MM-DD-<name>/`.

## OpenSpec CLI

If your agent has no OpenSpec slash commands, use the CLI:

```bash
openspec list [--json] [--specs]
openspec new change "<name>"
openspec status --change "<name>" --json
openspec instructions <artifact> --change "<name>"
openspec archive <name>
```

## Rules

- Before implementing anything, read the relevant files in `openspec/specs/` and
  the active change's artifacts.
- Do not write feature code without an active change proposal that contains
  tasks.
- Keep changes minimal and scoped to the task being implemented.
- Treat `openspec/specs/` as the truth. Reflect requirement changes there via
  the sync step, not by editing code silently.
- Keep project-specific contract, terms, and priorities in `PROJECT.md`.
- **Do not modify Worklane from within Mirrorlane.** The `worklane/` submodule is
  an external dependency boundary, not Mirrorlane source. Do not edit files under
  `worklane/` as part of a Mirrorlane change. When Mirrorlane needs a Worklane
  behavior change, raise it upstream in the Worklane repository; once it lands
  there, bump the pinned submodule SHA in a separate, dedicated Mirrorlane change.

## Language

- Write OpenSpec artifacts, ADRs, code comments, and commit messages in English.
- Converse with users in the language they use.

## Commits

Use Conventional Commits:

```text
type(scope): summary
```

Use lowercase imperative mood and keep the summary at 72 characters or fewer.
Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `build`,
`ci`.

### Commit Flow

- **Propose**: `docs(<change>): propose <summary>`
- **Apply**: `feat(<change>): <summary>` or `fix(<change>): <summary>`
- **Sync**: `docs(specs): sync <change>`
- **Archive**: `chore(openspec): archive <change>`

Never bundle unrelated changes into one commit.

## Definition Of Done

Run these from the workspace root before checking off a task, syncing specs, or
archiving a change:

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

Before the first real crate exists, these Rust commands are not yet meaningful.
The first project-specific OpenSpec change should add the real crate layout and
make the Definition of Done runnable from the workspace root.

If a command cannot run in the current environment, report that explicitly.
