# Project Contract

Mirrorlane's orientation layer for humans and AI agents: the behavior to protect,
the canonical vocabulary, and how to prioritize changes. Keep it short.

For what Mirrorlane is, see [README.md](README.md); for the runtime spine and
crate layout, see [docs/architecture.md](docs/architecture.md); for the full
behavioral specification, see [openspec/specs/mirrorlane/spec.md](openspec/specs/mirrorlane/spec.md).

## What Mirrorlane Is

Mirrorlane is an **AI-native job runner**: a runtime for non-deterministic
AI/agent work that is **replayable, semantically cached, and confidence-routed**.
The unit of work is a **`Step`** — a typed, synchronous, infallible computation
that is deterministic at replay. Mirrorlane builds on a **frozen** Worklane
substrate (durable broker, retries, dead-letter, lanes) but is not bound by it.

It is a **glass box**: a user injects a **strategy** (a composition of `Step`s),
Mirrorlane executes it asynchronously on Worklane, and yields output the user
consumes — without managing the machinery (cache, idempotency, enqueue, retry).
But the machinery is **not opaque**: every run is **replayable, traceable,
inspectable, and deterministic**. That glass interior — not the task running — is
the differentiator (see the [Design rationale](docs/architecture.md#design-rationale)).

The message projection pipeline (`Message → Projection → Scope → Warm-up`) is the
**reference strategy**, not the product itself; the runtime's job is to run any
injected strategy.

Mirrorlane is one plane of a three-plane stack and stays in its lane:
**Triggerlane** (event plane — "what happened?") submits jobs; **Worklane**
(execution plane — "what should run?") runs them durably; Mirrorlane is the
**glass-box runtime** that turns a strategy into consumable output. Mirrorlane is
a *consumer* — it does **not** own triggers, events, or a world-reactive loop.

## Core Contract

The behavior protected first is **deterministic replay of non-deterministic
work** — the runtime's headline guarantee:

- **No loss, no duplication.** Every input is accounted for exactly once, even
  though the substrate delivers **at-least-once**. Steps must therefore be
  **idempotent**.
- **Replayable.** Re-running steps from the same input log produces identical
  output. A **semantic cache** makes a non-deterministic (model) call miss-only,
  so replay is deterministic relative to the populated cache.

```text
input -> Step -> output   (deterministic at replay, via the semantic cache)
```

Everything else builds on this contract and must not weaken it.

## Terminology

Prefer these canonical terms over synonyms.

- **Step** — a typed, sync, infallible unit of AI work (`In -> Out`), identified
  by a `kind` and a `StepVersion`. The runtime's unit of caching, replay, and
  routing. Implement `Step`, not a bespoke port.
- **Lane** — a partition of work on the substrate. **User lanes** carry submitted
  workloads; **system lanes** are runtime-owned, cross-cutting lanes (planned: a
  gateway / model-arbitration lane, and a security / input-screening lane).
- **Semantic cache** — memoizes a Step's output keyed by `(kind, version, input)`,
  the mechanism that makes a non-deterministic step replay-safe.
- **Replay** — re-deriving outputs from the input log; a **per-`Step`** property
  (cache-deterministic + idempotent persistence) that composes into whole-strategy
  determinism, needing no bespoke orchestration loop. Distinct from Triggerlane's
  *event replay*.
- **Strategy** — a user-injected composition of `Step`s: what to compute and how it
  composes. The projection pipeline is the **reference strategy**, not privileged.
- **Glass box** — the guarantee that the runtime's internals are replayable,
  traceable, inspectable, and deterministic; black box at the interface, glass box
  for guarantees. The differentiator.
- **Confidence / routing** — a Step output may carry a confidence; routing directs
  an output to a consumer (human, agent, GitHub, or another job), escalating on
  low confidence.
- **Worklane** — the **frozen** execution substrate underneath the runtime.
- **Triggerlane** — the sibling **event plane** that turns events into typed jobs;
  it owns triggers/events, Mirrorlane does not.
- **Projection / Scope / Skill / Warm-up** — the reference strategy's domain
  terms: the structured interpretation of team messages.

## Change Prioritization

When comparing possible changes, prefer the one that protects the core contract
earliest:

1. The core contract: no loss, no duplication, replay determinism, and
   idempotency under at-least-once delivery.
2. The runtime spine: `Step`, semantic cache, replay, trace, confidence/routing.
3. Specified feature completeness for concepts already declared in OpenSpec.
4. Operator and developer ergonomics.
5. System lanes, integrations, and scale-out.

Do not add system-lane or integration scope merely because a spine change enables
it. Keep enabling spine changes separate and small.
