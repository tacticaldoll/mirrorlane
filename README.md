# Mirrorlane

**AI-native job runner.** Mirrorlane is a runtime for non-deterministic
AI/agent work that is **replayable, semantically cached, and confidence-routed**.
The unit of work is a `Step` — a typed, deterministic-at-replay computation.

```text
input
    -> Step (cached, replayable, routed)
    -> output -> Human / Agent / GitHub / another job
```

It builds on a **frozen** [Worklane](https://github.com/tacticaldoll/worklane)
substrate (durable broker, retries, dead-letter, lanes) but is not bound by it:
the runtime spine never imports Worklane types. Its first **reference strategy**
is a message projection pipeline that maps team communication into structured,
consumable context.

Mirrorlane is a **glass box**: inject a strategy (a composition of `Step`s),
consume the output, and let the runtime handle the machinery — while every run
stays **replayable, traceable, inspectable, and deterministic**. That glass
interior is the differentiator. Mirrorlane is one plane of a three-plane stack:
[Triggerlane](https://github.com/tacticaldoll/triggerlane) is the event plane
("what happened?"), Worklane the execution plane ("what should run?"), and
Mirrorlane the runtime that turns a strategy into consumable output — it is a
*consumer*, and does not own triggers or events.

See [PROJECT.md](PROJECT.md) for the project contract, core invariants, and
terminology.

## Status

The runtime spine — the generic `Step` abstraction and the semantic cache
(`Cache`) — is in place, and deterministic replay is a per-`Step` property (cache +
idempotent stores). **Strategy-injectability is delivered**: a strategy is a
composition of `Step`s, selected at runtime via a registry (`--strategy`),
submittable as a durable job, and composed through a fan-out vocabulary — with the
message projection pipeline as the reference strategy. Consumable output is
delivered through a reproducible derived-output cache, readable **in-process**
(`replay`/`warmup`) or via the **durable asynchronous path** (`submit`/`work`).

The reference strategy ships the pipeline — projection, then the derivations built
from it:

```text
Message -> Projection -> { Scope, Warm-up, Skill index, Routing hint, Developer snapshot }
```

See [docs/architecture.md](docs/architecture.md) for the runtime spine and crate
layout, and [openspec/specs/mirrorlane/spec.md](openspec/specs/mirrorlane/spec.md)
for the full behavioral specification.

## Quickstart

Build, ingest a message, and get its structured context as JSON:

```bash
# 1. Initialize the Worklane submodule and build (see Build for details).
git submodule update --init --recursive
cargo build

# 2. Ingest a message into a fresh durable log.
echo '{"id":"m-1","source":"discord",
       "author":{"id":"u-alice","display_name":"Alice"},
       "conversation":{"id":"c-1","thread":null},
       "body":"We will use sqlite for the auth sdk oauth refresh-token store."}' \
  | cargo run -q -p mirrorlane-cli -- --db mirrorlane.db ingest

# 3. Re-derive the session context and print it as JSON for an agent or workflow.
cargo run -q -p mirrorlane-cli -- --db mirrorlane.db --format json \
  warmup --conversation c-1
```

The last command prints a `SessionContext` — the warm-up plus the derived scope,
the session's developers, and a routing hint per message (elided here):

```json
{
  "schema": "mirrorlane.session_context/1",
  "derivation_version": "projection/1:mock:v1",
  "conversation": "c-1",
  "warmup": { "focus": ["auth-sdk", "oauth2", "token-refresh"], "decisions": ["m-1"], "...": "..." },
  "scope": { "load": ["auth-sdk", "oauth2", "token-refresh"], "ignore": ["web-ui", "analytics", "billing"], "...": "..." },
  "developers": { "developers": [ { "author": "u-alice", "topics": [ { "topic": "auth", "weight": 1.0 } ] } ] },
  "hints": [ { "message_id": "m-1", "human_hint": { "author": "u-alice", "score": 1.0 } } ]
}
```

Drop `--format json` (the default is `text`) for human-readable summaries.

For a fuller end-to-end walkthrough — ingest a short conversation, then show both
the warm-up summaries and the full JSON `SessionContext` — run the demo (offline,
deterministic):

```bash
./demo.sh
```

## Contract

What you give Mirrorlane, what it gives back, and what it guarantees. The normative
version is [openspec/specs/mirrorlane/spec.md](openspec/specs/mirrorlane/spec.md).

**Inbound**

- **Ingest** a `MessageEnvelope` (JSON on stdin): `id`, `source`, `author`,
  `conversation`, `body`. The log is append-only and idempotent by `id`.
- **Submit** a strategy run (durable path): a `StrategyRunRequest { strategy }` onto
  the strategy-run lane. An unknown strategy id dead-letters (it never panics) and is
  recoverable via `dlq`.

**Outbound** — the `SessionContext` (the `--format json` payload), versioned so a
machine consumer can detect change:

- `schema` — the output schema id (`mirrorlane.session_context/1`), bumped on a
  breaking shape change.
- `derivation_version` — which `(strategy + projector + schema)` version produced it
  (provenance).
- `conversation`, `warmup`, `scope`, `developers`, `hints`, `decisions`, `drafts` —
  in message order; an absent `scope`/`developers` serializes to `null`.

**Guarantees**

- **Source of truth** — the message log. Everything else is derived and reproducible
  by replay; the log is never auto-pruned (the disposable surfaces are bounded).
- **Determinism** — relative to the populated semantic cache and `Step` versions: a
  step's output is fixed for `(kind, version, input)`, composed with idempotent
  persistence. A real-LLM cold run is not bit-identical; the cache fixes it
  thereafter. `verify` proves it on demand (recompute and compare; non-zero on drift).
- **At-least-once** — work runs on the durable substrate, so every `Step` is
  idempotent and redelivery is bounded.

## Architecture

Mirrorlane is a **downstream consumer** of a frozen Worklane substrate, and its own
crates depend **inward** toward the generic `Step` spine — where determinism lives.
Only the outer ring binds a `Step` to a Worklane job; the spine and domain core never
import Worklane.

```text
  consumers (Triggerlane, or your own plane)
        |  submit a strategy run            ^  read SessionContext (text / JSON)
        v                                   |
  +------------------------ mirrorlane (glass-box runtime) ------------------------+
  |  cli    worker    github         <-- only these crates import worklane         |
  |           |  (binds a Step to a durable Worklane job)                          |
  |           v                                                                    |
  |  llm . ollama . openai . provider . storage      (adapters: LLM, stores)       |
  |           |                                                                    |
  |           v                                                                    |
  |  core        (domain model + the reference strategy: projection pipeline)      |
  |           |                                                                    |
  |           v                                                                    |
  |  runtime     (the generic Step spine: no domain, no I/O, no worklane;          |
  |               the semantic cache + per-Step determinism live here)             |
  +-------------------------------------------------------------------------------+
        |  durable execution (at-least-once)
        v
  Worklane (frozen submodule: broker, retries, dead-letter, lanes)
```

To a consumer Mirrorlane is a **glass-box executor**: inject a strategy, get back
consumable, reproducible output — replayable, traceable, inspectable, deterministic.
It is not an orchestrator; sequencing and triggers live in the consumer's plane.

## How it works

### Data flow

Messages enter the durable log; a strategy re-derives consumable context from the
log (in-process or over a durable queue) into a per-conversation cache; reads serve
that context and re-derive routing at read time. The log is the **source of truth**
— everything else is derived and reproducible by replay.

```text
INBOUND                               SOURCE OF TRUTH
  ingest (stdin JSON) --+
                        +--> message log  (SQLite, append-only)
  github --repo --------+          |
                                   |
      in-process (replay/warmup)   |   submit --> strategy-run queue
                                   |              (sqlite | postgres | redis)
                                   |                   |
                                   |                   | work
                                   v                   v
  +------------------ strategy: projection pipeline ------------------+
  |  project (per message) --LlmClient--> provider (mock|ollama|openai)|
  |       |   ^                                  |                      |
  |       |   +--------- projection cache <------+   miss -> call       |
  |       |             (SQLite, versioned)          hit  -> frozen     |
  |       +--> scope (per conversation) --> warm-up                     |
  |       +--> skill index (global) ------> developers, routing hints   |
  +-------------------------------------------------------------------+
                                   |
                                   v
              derived-output cache   (SQLite, per conversation)
                                   |
                                   v  read
              SessionContext (text / JSON)
                                   |
                                   v  routing re-derived at read time
              RuleRouter (data-driven rules + confidence escalation)
                                   |
                                   v
              Human | Agent | WorklaneJob | GitHub draft
```

Routing decisions and GitHub drafts are **re-derived at read time** for display;
the outbound dispatch path (the `WorklaneJob`/`GitHub` consumers) is separate from
replay, so external delivery is never re-run by a replay.

The derived-output cache is a *cache of a deterministic derivation*, never a store of
record — so `verify` recomputes this whole path and compares it to the cache, failing
on any drift (the determinism gate). `dlq` operates the durable queue's dead letters.
Both are in the [CLI](#cli) below.

### Lifecycle

The same log + cache, driven two ways — which CLI command runs each step:

```text
                                  ingest / github      (append to the log)
                                        |
                                        v
                                   message log
                                        |
        in-process (one shot) ----------+---------- durable (async, retried)
              |                                            |
              v                                            v
        replay / warmup                              submit  (enqueue a run)
        (re-derive now)                                    |
              |                                            v
              |                                          work    (consume to idle)
              +------------> derived-output cache <--------+
                                        |
            read    --> warmup / --format json   serve SessionContext (or recompute on miss)
            prove   --> verify                   recompute & compare; non-zero on drift
            operate --> dlq                       inspect / requeue / purge poison jobs
```

Both modes populate the **same** cache, so a `submit` + `work` makes a later `warmup`
serve without recomputing — and `verify` proves the served output still matches a
fresh derivation.

## CLI

`mirrorlane` drives the durable message log from a terminal. Messages are read as
JSON on stdin. `replay`/`warmup` re-derive context from the log **in-process**;
`submit`/`work` run a strategy **asynchronously** over a durable queue, populating
the same cache `warmup` reads. Global options:

- `--strategy <id>` — strategy to run (default `projection`).
- `--provider mock|ollama|openai` — projector backend (default `mock`); `ollama`
  and `openai` go through an `LlmClient`, cached + replay-safe.
- `--broker sqlite|postgres|redis` — strategy-run queue backend (default `sqlite`);
  `--queue-db <path>` for sqlite, `--queue-url <url>` for postgres/redis.

See [Configuration](#configuration) for endpoints, secrets, and multi-instance
isolation.

```bash
# Append a message to the log (idempotent by id).
echo '{"id":"m-1","source":"discord",
       "author":{"id":"u-alice","display_name":"Alice"},
       "conversation":{"id":"c-1","thread":null},
       "body":"We will use sqlite for the auth sdk oauth refresh-token store."}' \
  | cargo run -p mirrorlane-cli -- --db mirrorlane.db ingest

# Replay the whole log; prints a warm-up per conversation.
cargo run -p mirrorlane-cli -- --db mirrorlane.db replay

# Print one conversation's warm-up.
cargo run -p mirrorlane-cli -- --db mirrorlane.db warmup --conversation c-1

# Emit the full session context as JSON for an agent or workflow consumer.
# (Add --format json to any command; replay yields an array, warmup one object.)
cargo run -p mirrorlane-cli -- --db mirrorlane.db --format json warmup --conversation c-1

# Ingest a GitHub repository's issues, PRs, and comments into the log.
cargo run -p mirrorlane-cli -- --db mirrorlane.db github --repo owner/name

# Inspect one message's projection and routing-decision trace.
cargo run -p mirrorlane-cli -- --db mirrorlane.db inspect --message m-1

# Asynchronous path: submit a strategy run onto the durable queue, then work it
# to idle (populating the cache that `warmup` then serves without recomputing).
cargo run -p mirrorlane-cli -- --db mirrorlane.db submit --strategy projection
cargo run -p mirrorlane-cli -- --db mirrorlane.db work
cargo run -p mirrorlane-cli -- --db mirrorlane.db warmup --conversation c-1

# Prove determinism: recompute every derivation and compare to the durable cache.
# Exits non-zero if any conversation diverged (a stale cache or a non-deterministic
# step) — so it can gate a pipeline.
cargo run -p mirrorlane-cli -- --db mirrorlane.db verify

# Operate the durable queue's dead letters (inspect / recover poison jobs).
cargo run -p mirrorlane-cli -- --queue-db mirrorlane-queue.db dlq --lane strategy-run --action count
```

## Configuration

Every setting resolves by precedence — an explicit flag wins, then the config
value, then a built-in default. Settings live in `mirrorlane.json` (auto-loaded
from the working directory, or `--config <path>`); **secrets and deployment knobs
come from the environment.**

**Environment**

| Variable | Purpose |
| --- | --- |
| `GITHUB_TOKEN` | GitHub REST auth — sent **only** to the default `api.github.com` host, withheld (with a warning) for any other base URL. |
| `OPENAI_API_KEY` | OpenAI-compatible auth — env only, never the config file. |
| `WORKLANE_URL`, `DATABASE_URL`, `REDIS_URL` | `--queue-url` fallbacks for the postgres/redis broker (in that precedence). |
| `MIRRORLANE_WORKLANE_QUEUE_SCHEMA` | Postgres schema for the strategy-run queue — set distinct values to run several Mirrorlane instances on one Postgres without colliding. |
| `MIRRORLANE_WORKLANE_QUEUE_NAMESPACE` | Redis key namespace — same isolation for a shared Redis. |
| `MIRRORLANE_WORKLANE_POOL_SIZE` / `_LEASE_SECS` / `_MAX_DELIVERIES` | Broker tuning (Postgres pool size; lease; max deliveries before dead-letter — bounded by a built-in backstop when unset). |
| `MIRRORLANE_WORKLANE_HANDLER_TIMEOUT_SECS` | The `work` worker's per-run handler timeout (default 300s) — bounds a hung LLM-backed strategy run. |

Third-party-convention secrets (`GITHUB_TOKEN`, `OPENAI_API_KEY`) and the shared
`WORKLANE_URL` keep their established names; the `MIRRORLANE_WORKLANE_*` namespace
is for worklane-substrate settings, overriding the `worklane` config section below.

**`mirrorlane.json` keys** (all optional)

```jsonc
{
  "db": "mirrorlane.db",
  "queue_db": "mirrorlane-queue.db",
  "broker": "sqlite",                 // sqlite | postgres | redis
  "queue_url": "postgres://…",        // postgres/redis
  "provider": "mock",                 // mock | ollama | openai
  "strategy": "projection",
  "ollama_base_url": "http://localhost:11434", "ollama_model": "…", "ollama_prompt_version": "v2",
  "openai_base_url": "https://api.openai.com/v1", "openai_model": "gpt-4o-mini",
  "github_base_url": "https://api.github.com",
  "routing": {                        // data-driven routing rules (default: built-in)
    "rules": [{ "intent": "issue", "target": "github" }],
    "escalation_threshold": 0.6, "default_target": "human"
  },
  "retention": {                      // bound the disposable surfaces (default: bounded)
    "dead_letter_max_count": 1000, "dead_letter_max_age_secs": null, "trace_max_count": 10000
  },
  "worklane": {                       // worklane-substrate (also MIRRORLANE_WORKLANE_* env)
    "queue_schema": null, "queue_namespace": null,
    "pool_size": null, "lease_secs": null, "max_deliveries": null,
    "handler_timeout_secs": null      // work-worker per-run timeout (default 300s)
  }
}
```

Unset values use built-in defaults, so an unconfigured run is unchanged. The
message log is the source of truth and is never auto-pruned; the disposable
surfaces (dead-letters, routing traces, caches) are bounded.

## Development

This project uses OpenSpec for spec-driven development and folds work into a single
squashed baseline — durable design rationale lives in
[docs/architecture.md](docs/architecture.md#design-rationale), not in commit history.

- `AGENTS.md` — authoritative rules for AI agents and humans. Read it first.
- `PROJECT.md` — project contract, terminology, and change priorities.
- `BACKLOG.md` — forward-looking work that is deliberately deferred.
- `CHANGELOG.md` — notable changes, Keep a Changelog format.
- `docs/architecture.md` — the runtime spine, lanes, crate layout, and design rationale.
- `docs/development-flow.md` — the OpenSpec and commit checklist.
- `openspec/specs/mirrorlane/spec.md` — the single, authoritative behavioral spec.

Generate local agent shims after cloning (they are not committed):

```bash
openspec init --tools claude,codex
```

### Build

Worklane (the execution substrate) is pinned — **frozen** — as a git submodule at
the repository root `worklane/` and consumed by path dependency. Mirrorlane does
not track upstream HEAD; the upstream `worklane` repository is the baseline, and a
submodule bump is a deliberate, dedicated change. Initialize the submodule before
building:

```bash
git clone --recurse-submodules <repo-url>
# or, in an existing clone:
git submodule update --init --recursive

cargo build
cargo run -p mirrorlane-cli --example durable_replay   # ingest -> "restart" -> replay -> warm-up
```

### Definition of Done

Run from the workspace root before checking off a task, syncing specs, or
archiving a change (CI runs the same gates plus a determinism smoke):

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all --check
```

Plus `openspec validate <change> --strict` when a change touches the spec.

## License

Licensed under either of Apache License 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
or MIT license ([LICENSE-MIT](LICENSE-MIT)) at your option.
