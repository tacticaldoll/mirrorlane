# Mirrorlane Architecture

## What Mirrorlane Is

Mirrorlane is a **glass-box strategy executor**, and a **downstream consumer of a
frozen Worklane substrate**. It owns the entire runtime value — the `Step` spine,
the semantic cache, replay determinism, confidence/routing, and the glass-box
guarantees (every run replayable, traceable, inspectable, deterministic) — and uses
Worklane only as the upstream *job-execution substrate* that lands that work
durably. In dependency terms it **consumes a broker; it does not own one**.

To a consumer the shape is simple: **inject a strategy, get back consumable,
reproducible output**. That makes Mirrorlane an *executor* to its callers — but a
glass-box one, not a bare task queue: the differentiator is the guarantees, not that
work runs. It is deliberately **not an orchestrator** — it executes a single
injected strategy into output; it does not own multi-step orchestration loops or
world-reactive triggers (that is Triggerlane's plane, below).

## The Three-Plane Stack

Mirrorlane is one plane of a typed orchestration stack; each plane stays in its
lane (see the [Design rationale](#design-rationale)).

```text
Event ─▶ Triggerlane ──(typed job)─▶ Worklane ──(executes)─▶ Mirrorlane Step
         event plane                 execution plane          glass-box runtime
         "what happened?"            "what should run?"        "strategy → output"
```

- **Triggerlane** (event plane) turns events into typed Worklane jobs. Mirrorlane
  does **not** own triggers or events — it is a *consumer* that exposes its
  strategy as jobs Triggerlane can submit.
- **Worklane** (execution plane) is the reused, **frozen** durable substrate
  (broker, retries, dead-letter, lanes). Mirrorlane *uses* it but is **not bound by
  it**: the runtime spine never imports Worklane types; turning a `Step` into a
  durable job is a worker concern.
- **Mirrorlane** is the **glass box**: a user injects a **strategy** (a composition
  of `Step`s), it runs asynchronously on Worklane, and yields consumable output —
  while every run stays **replayable, traceable, inspectable, and deterministic**.
  That glass interior is the differentiator. The unit of work is a **`Step`**:
  typed, sync, infallible, cached, replayed, and routed.

## The Step spine

`mirrorlane-runtime` defines the generic `Step`:

```rust
trait Step {
    type In;
    type Out;
    fn kind(&self) -> &'static str;
    fn version(&self) -> StepVersion;
    fn run(&self, input: &Self::In) -> Self::Out;
}
```

- **Sync + infallible.** A semantic cache makes the live call miss-only, so replay
  performs no I/O; failures **panic at the boundary**, so nothing caches a bad
  result and the substrate retries then dead-letters.
- **Generic.** `In`/`Out` reference no domain type — proven by a probe `Step` that
  shares no types with projection. `kind`/`version` are methods (not associated
  constants) so the trait stays **object-safe**, letting a projector live behind
  `Arc<dyn Projector>`.

The first primitive built on `Step` — the **semantic cache** (`Cache`, `CacheKey`,
`Cached<S>`) — lives in `mirrorlane-runtime` too. Deterministic replay is a
**per-`Step`** property: the cache fixes a step's output for `(kind, version,
input)` and idempotent stores make persistence re-delivery-safe, so whole-strategy
determinism **composes** — no bespoke orchestration loop is needed to guarantee it.
Trace and confidence/routing are the remaining primitives to generalize off the
reference strategy. The projection pipeline is that **reference strategy**: its
message projector is `Step<In = MessageEnvelope, Out = Projection>`, and the
hardcoded multi-phase `Replay` is a reference strategy / recovery harness, not the
core guarantee.

## Lanes

Work runs on **lanes** (partitions on the substrate):

- **User lanes** carry submitted workloads.
- **System lanes** are runtime-owned and cross-cutting. Planned: a **gateway /
  model-arbitration** lane (OpenRouter-style 1-of-N model choice, but with
  replayable, cached arbitration) and a **security / input-screening** lane
  (auditable, replayable). None is built yet; the vocabulary is fixed here so the
  spine is designed with them in mind.

## Worklane as a frozen substrate

Worklane is pinned as a **git submodule** at `worklane/` and consumed through
**path dependencies**:

```toml
[workspace.dependencies]
worklane = { path = "worklane/crates/worklane" }
worklane-memory = { path = "worklane/crates/worklane-memory" }
```

The pin is **frozen**: Mirrorlane does **not** track upstream HEAD. The upstream
`worklane` repository is the baseline for the pinned revision; a bump is a
deliberate, dedicated change. Only `mirrorlane-worker` (and examples/tests) depend
on Worklane; the spine and the domain core stay substrate-free.

## Substrate configuration stance

Being a downstream consumer means taking a **deliberate position on each substrate
knob** — not mirroring Worklane's API here (that drifts with the pin), but recording
*our stance*: what we set, what we leave to Worklane's default on purpose, and what
we decline. The stance is the consumer relationship made concrete; the operator-facing
config keys and defaults live in the README.

**Set deliberately** — where Mirrorlane's guarantees or operability require it:

- **Dead-letter retention** bounded out of the box (default `max_count` 1000) — the
  failure store must not grow without limit.
- **Redelivery backstop** (a default `max_deliveries`) — `max_attempts` bounds only
  handler-error retries, not lease-expiry redeliveries; this bounds the latter so a
  repeatedly-abandoned job eventually dead-letters.
- **Lease keepalive + handler timeout** on the `work` worker — a strategy run may make
  many sequential LLM calls, so we keep its lease alive (a long-but-healthy run is not
  redelivered) and bound a hung run (it cannot hold the job forever).
- **Job observer** — the glass-box guarantee: a durable run reports what it ran.
- **Schema / namespace / pool** (Postgres / Redis) — so several instances share one
  backend without colliding.

**Left at Worklane's default by intent** — where the default fits our shape:

- **Concurrency = 1** (sequential): a run re-derives the whole log in memory and writes
  the SQLite stores; serial execution avoids write contention and memory multiplication.
- **Fail-fast (not resilient)**: `work` drains to idle and returns — a batch, not a
  daemon — so stopping on a broker error is correct.
- **Retry policy, poll interval, idle backoff, shutdown timeout**: the first is adequate
  at current volume; the rest only affect a long-running `run` loop, and we use
  `run_until_idle`.

**Deliberately declined** — where using the knob would blur who owns what:

- **Result store**: the derived-output cache is *our* read model, keyed by derivation
  version + content; we do not store outputs in Worklane's result backend. We own the
  runtime value; Worklane executes.
- **Payload offload / claim-check**: payloads are tiny (a strategy id; a single
  projection), so the large-payload machinery is unnecessary.

## Crate Layout

```text
mirrorlane/
├── crates/
│   ├── mirrorlane-runtime    # the Step spine (generic, domain-free, infra-free)
│   ├── mirrorlane-core       # domain model + reference strategy (projection/scope/skill/warmup/routing); Projector = a Step
│   ├── mirrorlane-worker     # Worklane jobs that run Steps durably
│   ├── mirrorlane-provider   # deterministic Step implementations: mock, caching projector, rule router
│   ├── mirrorlane-llm        # the LlmClient transport port + provider-agnostic LlmProjector (prompt + parser)
│   ├── mirrorlane-ollama     # OllamaClient: LlmClient over a local Ollama server (HTTP)
│   ├── mirrorlane-openai     # OpenAiClient: LlmClient over an OpenAI-compatible API (HTTP)
│   ├── mirrorlane-storage    # sqlite, memory
│   ├── mirrorlane-github     # GitHub source + draft consumer (HTTP)
│   └── mirrorlane-cli        # ingest, warmup, replay, submit/work; hosts examples/
├── worklane/                 # frozen submodule (substrate)
└── docs/
```

The root is a **pure virtual workspace** (no root package); the runnable
end-to-end demos live in `crates/mirrorlane-cli/examples/`
(`cargo run -p mirrorlane-cli --example <name>`).

### Dependency direction

Dependencies point **inward**, toward `mirrorlane-runtime`.

- `mirrorlane-runtime` is the innermost ring: the generic `Step`, depending on no
  other `mirrorlane-*` crate and on no I/O, async runtime, broker, storage, or
  `worklane`.
- `mirrorlane-core` holds the domain model and the reference strategy; it depends
  on `mirrorlane-runtime` and stays free of I/O, async runtime, broker, and
  Worklane.
- `mirrorlane-worker` depends on `core` + `runtime` + Worklane.
- `mirrorlane-llm` holds the LLM seam (`LlmClient` + `LlmProjector`) depending on
  `core` + `runtime`; `mirrorlane-ollama`/`mirrorlane-openai` are thin transports
  over it. `mirrorlane-provider`, `mirrorlane-storage`, `mirrorlane-github`, and
  `mirrorlane-cli` depend inward, never the reverse.

## Core Contract

Protected first: **deterministic replay of non-deterministic work** — no loss, no
duplication, replay determinism. Because the substrate delivers **at-least-once**,
every `Step` must be **idempotent**. This is a **per-`Step`** guarantee
(cache-deterministic output + idempotent persistence) that composes into
whole-strategy determinism, so replaying the same input log yields identical
output. Distinct from Triggerlane's *event replay*. See
[PROJECT.md](../PROJECT.md) and the [Design rationale](#design-rationale).

## Design rationale

These are architectural **invariants**: changing one is itself an architecture
decision, not an incidental refactor. Change one by updating this section and
`openspec/specs/mirrorlane/spec.md` through the OpenSpec flow (see
[development-flow.md](development-flow.md)) — never silently. (They were previously
kept as dated ADRs; in this baseline they live here as living rationale, not
history.)

### Mirrorlane is an AI-native runtime, not a consumer of a generic job runner

Mirrorlane **is** a replayable, semantically-cached, confidence-routed runtime for
non-deterministic AI/agent work; the projection pipeline is its **reference
workload**, not the product. Framing it as a mere consumer of a generic job runner
was rejected: that makes the build hostage to upstream history and mislabels the
runtime primitives (versioned caching, replay, confidence, routing) as "context
projection" features. The load-bearing invariants: `Step` is the unit of work;
dependencies point **inward** (`mirrorlane-runtime` is domain- and infra-free); the
spine never imports Worklane types; Worklane is **frozen** (a bump is a deliberate,
dedicated change); the core contract — no loss, no duplication, replay determinism
under at-least-once delivery — is protected first. *Rejected:* co-evolving or
vendoring Worklane / tracking its HEAD; folding the runtime into `mirrorlane-core`
(a crate boundary makes "domain-free" compiler-enforced).

### Black box at the interface, glass box for its guarantees

A user injects a strategy and consumes its output without managing the machinery
(cache, idempotency, enqueue, retry, dead-letter) — but the machinery is **not
opaque**: every run is replayable, traceable, inspectable, and deterministic. That
glass interior is the differentiator over a Celery-class task queue, so making the
internals opaque was rejected. The strategy is **injected, not hardcoded** (the
projection pipeline is the *reference* strategy, not a privileged one). Replay is
**per-`Step` self-consistency** (cache-deterministic output + idempotent
persistence); a bespoke whole-pipeline replay orchestrator as the core guarantee was
rejected — determinism composes. Plane boundaries: **Triggerlane** owns
events/triggers ("what happened?"), **Worklane** owns durable execution ("what
should run?"), **Mirrorlane** owns strategy → consumable output. "Replay" is
plane-scoped: Triggerlane's *event replay* ≠ Mirrorlane's *derivation replay*.

### Strategy dependencies and fan-out are typed composition, not a data DAG

A strategy declares its dependencies and fan-out through a small **closed vocabulary
of combinators** — `per_message`, `per_conversation`, `global`, and `then`
(sequencing) — matching the three real fan-out shapes, not a general declarative
graph. A typed-DAG engine (needing a dynamically-typed value bus to wire
heterogeneously-typed `Step`s) was rejected as research-grade for expressiveness the
three shapes do not need, and redundant with per-`Step` determinism. Worklane's
Chord is the durable-parallel **execution backing** for a combinator if/when wanted
— never the composition surface itself.

### Consumable output is a cache of a deterministic derivation

Persisted output is a **cache** of a reproducible function of `(log, Step versions)`,
never an authoritative mutable store — recompute (replay over the log) is always the
source of truth, and a cache miss is recoverable by recompute. The durable path is
**submit → consume → observe**: a caller enqueues a `StrategyRunJob`; a worker
resolves and runs the strategy and writes its output to the durable derived-output
cache; a read surface serves cached-or-recomputed output. In-process replay coexists
as a first-class mode. *Rejected:* authoritative mutable derived stores (they can
drift from the derivation); pure recompute-on-read with no persistence (an async run
would then deliver nothing directly readable).

## Operational bounds and the scale ceiling

What grows, and what holds it back:

- **Dead-letter store** — bounded by a `RetentionPolicy` (default `max_count` 1000
  per lane), applied to every durable broker the CLI builds.
- **Routing-trace store** — bounded by a most-recent-N cap (default 10,000),
  write-driven prune on upsert.
- **Projection cache** — bounded by version: stale-version projections are pruned
  when the projector version bumps.
- **Derived-output cache** — bounded to roughly one row per conversation: each
  derivation cycle reclaims a conversation's superseded `(version, content)` rows,
  so it tracks the live conversation set, not the history of versions.
- **Message log** — **unbounded by design**: it is the source of truth for
  deterministic replay, so it is never auto-pruned. Operator-driven
  archival/compaction is a deferred concern (see `BACKLOG.md`).

**The scale ceiling is replay's whole-log load.** `replay` and `work` materialize
the entire message log into memory in one pass (and `work` re-derives the whole log
per run), so memory grows linearly with the log. For current scales this is
negligible; at very large logs (millions of messages) it becomes the bound to lift.
Streaming/per-conversation replay is the deferred answer (`BACKLOG.md`); until then
the log's unbounded growth is the transitive memory ceiling, and is documented, not
silent.
