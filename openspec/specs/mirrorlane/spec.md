# mirrorlane Specification

## Purpose
The single, authoritative specification of Mirrorlane: an **AI-native job runner**
— a runtime for non-deterministic AI/agent work that is **replayable, semantically
cached, and confidence-routed**. The unit of work is a `Step` (typed, synchronous,
infallible, deterministic at replay). Mirrorlane builds on a **frozen** Worklane
substrate but its runtime spine never imports Worklane types.

The behavior protected first is the **core contract** — no loss, no duplication,
replay determinism under at-least-once delivery. Everything below builds on it and
must not weaken it.

Requirements are grouped by concern: the runtime spine and workspace; the message
projection reference workload (projection → scope → skill → developers → routing
hint → warm-up); the durable log, replay, and cache that prove the core contract; a
real projector adapter; the routing/dispatch edge; GitHub as a source and consumer;
and the CLI.
## Requirements

<!-- Runtime spine -->

### Requirement: Generic Step abstraction

Mirrorlane SHALL define a generic `Step` abstraction in a dedicated crate: a
typed unit of AI work with an input type, an output type, a stable `kind()`, and
a `version()`. A `Step` SHALL be synchronous and infallible at its port; failures
SHALL panic at the boundary rather than returning, so that no result is produced
(and later, cached) for a failed run. The trait SHALL be **object-safe** so that
a step can be held behind `dyn` (e.g. a projector behind `Arc<dyn Projector>`);
`kind`/`version` are therefore methods, not associated constants.

#### Scenario: Step exposes input, output, kind, and version

- **WHEN** a type implements `Step`
- **THEN** it declares associated input and output types, a `kind()` method
  returning a stable identifier, and a `version()` returning a `StepVersion`

#### Scenario: Step can be held behind a trait object

- **WHEN** a `Step` with concrete associated types is boxed as
  `dyn Step<In = _, Out = _>`
- **THEN** it compiles and `run` dispatches through the trait object

#### Scenario: Step runs synchronously and returns its output type

- **WHEN** `run` is called on a `Step` with a value of its input type
- **THEN** it returns a value of its output type without an async runtime and
  without a `Result` wrapper

### Requirement: The runtime crate is domain-free and innermost

The `Step` abstraction SHALL live in `mirrorlane-runtime`, which SHALL NOT depend
on any `mirrorlane-*` crate, on a domain type, on an async runtime, on a broker,
on storage, or on `worklane`. It is the innermost ring of the dependency
direction; other crates MAY depend on it.

#### Scenario: Runtime declares no domain or infrastructure dependency

- **WHEN** `mirrorlane-runtime`'s manifest is inspected
- **THEN** its dependency set excludes every `mirrorlane-*` crate, `worklane`, any
  async runtime, any broker, and any storage crate

#### Scenario: Core depends on the runtime, not the reverse

- **WHEN** `cargo metadata` is inspected
- **THEN** `mirrorlane-core` depends on `mirrorlane-runtime`, and
  `mirrorlane-runtime` does not depend on `mirrorlane-core`

### Requirement: Genericity proven by a non-domain consumer

The runtime SHALL include a probe `Step` whose input and output types reference no
domain type (no `Message`, `MessageId`, or `Projection`), demonstrating that
`Step` is genuinely generic and not projection-shaped.

#### Scenario: A domain-free Step compiles and runs

- **WHEN** the probe `Step`'s input is supplied and `run` is called within
  `mirrorlane-runtime` (where no domain type is in scope)
- **THEN** it returns the expected output and the test passes

### Requirement: Projection expressed as a Step

The message projector SHALL be expressed in terms of
`Step<In = Message, Out = Projection>`, with no change to projection behavior.
Replaying the same message log SHALL still produce identical projections.

#### Scenario: Projection behavior is unchanged

- **WHEN** the existing projection, scope, skill, and warm-up tests run after the
  projector is re-expressed as a `Step`
- **THEN** they pass unchanged

#### Scenario: The projector is a Step instance

- **WHEN** the projector type is inspected
- **THEN** it satisfies `Step` with input `Message` and output `Projection`

<!-- Semantic cache -->

### Requirement: Generic semantic cache port

`mirrorlane-runtime` SHALL define a generic, domain-free `Cache<V>` port that
stores and retrieves a value keyed by a step `kind`, a `StepVersion`, and a
string key, and SHALL provide an in-memory, std-only adapter. A lookup with a
different version SHALL miss, so bumping a step's version invalidates prior
entries. The runtime SHALL take no serde, async, broker, storage, or `worklane`
dependency for this port; durable adapters add serialization in their own crate.

#### Scenario: A cached value is retrieved by kind, version, and key

- **WHEN** a value is put under a `(kind, version, key)` and then fetched with the
  same `(kind, version, key)`
- **THEN** the fetched value equals the stored one

#### Scenario: A different version misses

- **WHEN** a value is put under one version and fetched under a different version
  for the same kind and key
- **THEN** the lookup returns nothing

### Requirement: Cache key derivation

`mirrorlane-runtime` SHALL define a `CacheKey` trait whose implementor yields a
stable string key, so a cache can derive an identity from a step's input without
the runtime knowing any domain type. The same input SHALL always yield the same
key.

#### Scenario: An input yields a stable key

- **WHEN** `cache_key` is called twice on equal inputs
- **THEN** it returns the same string both times

### Requirement: Cached step decorator

`mirrorlane-runtime` SHALL provide a `Cached<S: Step>` decorator that is itself a
`Step` where `S::In: CacheKey` and `S::Out: Clone`. On `run` it SHALL return the
cached output when present for the input's key under the decorator's
`(kind, version)`, and otherwise run the inner step, cache its output, and return
it — so the inner step is invoked at most once per `(version, key)`. Its `version`
SHALL change whenever the inner step's `run` behavior changes, so a behavior change
can never silently reuse a stale cache entry; where the behavior depends on text the
author edits (a prompt template), the version SHALL incorporate a **content
fingerprint** of that text so the change is automatic rather than a remembered manual
bump. When the inner step panics, nothing SHALL be cached.

#### Scenario: The inner step runs only on a miss

- **WHEN** the same input is run twice through a `Cached` step with a fixed version
- **THEN** the inner step is invoked exactly once and both runs return the same
  output

#### Scenario: A changed version recomputes

- **WHEN** the same input is run under two different version tags
- **THEN** the inner step is invoked for each version

#### Scenario: Editing fingerprinted text changes the version

- **WHEN** a step's behavior depends on a text template incorporated into its version
  by content fingerprint, and that template is edited
- **THEN** the version changes without a manual bump, so prior cache entries miss

### Requirement: A trait-object step is itself a Step

`mirrorlane-runtime` SHALL provide a blanket implementation making
`Arc<dyn Step<In = I, Out = O>>` itself a `Step`, so a step held behind a trait
object (e.g. a projector behind `Arc<dyn Projector>`) can be wrapped by `Cached`.

#### Scenario: Cached wraps a trait-object step

- **WHEN** a `Cached` decorator wraps an `Arc<dyn Step<In = _, Out = _>>`
- **THEN** it compiles and `run` dispatches through the trait object, caching as
  usual

### Requirement: Cache genericity proven by a non-domain consumer

`mirrorlane-runtime` SHALL include a cached probe `Step` whose input and output
reference no domain type (no `Message`, `MessageId`, or `Projection`),
demonstrating that `Cached` and `Cache` are genuinely generic and not
projection-shaped.

#### Scenario: A domain-free cached step caches and recomputes

- **WHEN** a non-domain `Step` (e.g. a doubler) is wrapped in `Cached` and run
  within `mirrorlane-runtime` (where no domain type is in scope)
- **THEN** the inner step runs once per key, a hit returns the cached output, and a
  version change recomputes

<!-- Strategy -->

### Requirement: Strategy abstraction

`mirrorlane-worker` SHALL define a `Strategy` abstraction: a typed `Input → Output`
unit that **composes `Step`s and runs them asynchronously** with the runtime's
glass-box guarantees (semantic cache, idempotent persistence, per-`Step` replay
determinism). A `Strategy` SHALL differ from a `Step` by contract — it orchestrates
a composition of Steps and runs them durably — not by a different call shape. The
abstraction makes the strategy a swappable seam rather than a hardcoded pipeline.

#### Scenario: A strategy runs an input to an output

- **WHEN** a `Strategy` is run with a value of its `Input` type
- **THEN** it returns a value of its `Output` type, having composed and run its
  `Step`s

### Requirement: Projection expressed as the reference strategy

The projection pipeline SHALL be expressed as a `ProjectionStrategy` — the
**reference strategy** — implementing `Strategy` with the message log as input and
the replay stores as output, with **no change** to projection, scope, skill,
warm-up, routing-hint, or developer-snapshot behavior. Replaying the same message
log SHALL still produce identical results.

#### Scenario: Projection behavior is unchanged

- **WHEN** the existing replay, CLI, and Ollama tests run after the pipeline is
  re-expressed as `ProjectionStrategy`
- **THEN** they pass unchanged

#### Scenario: Two runs of the reference strategy agree

- **WHEN** the same message log is run through `ProjectionStrategy` twice
- **THEN** the two outputs are identical (the per-`Step` determinism composing into
  whole-strategy determinism)

### Requirement: Strategy genericity proven by a non-domain consumer

`mirrorlane-worker` SHALL include a probe `Strategy` whose `Input` and `Output`
reference no domain type (no `Message`, `MessageId`, or `Projection`),
demonstrating that `Strategy` is genuinely generic and not projection-shaped.

#### Scenario: A domain-free strategy composes and runs steps

- **WHEN** a probe `Strategy` composing non-domain `Step`s is run over a non-domain
  input
- **THEN** it returns the expected output with no domain type in scope

<!-- Workspace & substrate -->

### Requirement: Crate layout

The Mirrorlane workspace SHALL place its crates under `crates/` and register them
as workspace members: `mirrorlane-runtime`, `mirrorlane-core`,
`mirrorlane-provider`, `mirrorlane-storage`, `mirrorlane-worker`,
`mirrorlane-cli`, `mirrorlane-github`, and `mirrorlane-ollama`. An `examples/`
directory SHALL exist at the workspace root for runnable example targets.

#### Scenario: Workspace builds all crates

- **WHEN** `cargo build` runs from the workspace root
- **THEN** every member crate compiles successfully

#### Scenario: Members are declared

- **WHEN** the workspace `Cargo.toml` is inspected
- **THEN** its `members` resolve to the `crates/*` member crates

### Requirement: Inward dependency direction

`mirrorlane-runtime` SHALL be the innermost crate: a generic, domain-free home for
the `Step` abstraction that depends on no other `mirrorlane-*` crate and on no
I/O, async runtime, broker, storage, or `worklane`. `mirrorlane-core` SHALL hold
the projection domain model, MAY depend on `mirrorlane-runtime`, and SHALL NOT
depend on `mirrorlane-worker`, `mirrorlane-cli`, or Worklane, remaining free of
I/O and execution concerns. Outer crates MAY depend inward toward `core` and
`runtime`.

#### Scenario: Runtime is innermost and domain-free

- **WHEN** `cargo metadata` is inspected for `mirrorlane-runtime`
- **THEN** its dependency set excludes every other `mirrorlane-*` crate and
  excludes `worklane`, async runtime, broker, and storage dependencies

#### Scenario: Core depends only inward

- **WHEN** `cargo metadata` is inspected for `mirrorlane-core`
- **THEN** its dependency set excludes `mirrorlane-worker` and `mirrorlane-cli`
  and includes no async runtime, broker, storage, or `worklane` dependency

### Requirement: Worklane execution substrate

Worklane SHALL be consumed as a **frozen** git-submodule substrate pinned at a
specific buildable revision and consumed through path dependencies. The upstream
Worklane repository is the baseline for that revision. Mirrorlane SHALL NOT track
upstream Worklane HEAD; a submodule bump SHALL be a deliberate, dedicated change.

#### Scenario: Worklane is pinned to a frozen revision

- **WHEN** the recorded submodule revision is initialized with
  `git submodule update --init`
- **THEN** it resolves to the pinned SHA and the workspace builds

### Requirement: Runnable Definition of Done

The workspace SHALL make the project Definition of Done runnable and passing
from the workspace root, given an initialized Worklane submodule.

#### Scenario: Definition of Done passes

- **WHEN** the Worklane submodule is initialized and `cargo build`, `cargo
  test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --all
  --check` run from the workspace root
- **THEN** each command exits with status zero

<!-- Reference workload: message projection -->

### Requirement: Message envelope model

`mirrorlane-core` SHALL define a `MessageEnvelope` that carries a stable
`MessageId`, a `Source`, an `Author`, a `Conversation`, and the message body.
`Source` SHALL enumerate `Discord`, `Slack`, `GitHub`, and `Manual`. A
`Conversation` SHALL carry a `ConversationId` and an optional `ThreadId`. The
model SHALL serialize to and deserialize from JSON.

#### Scenario: Envelope round-trips through JSON

- **WHEN** a `MessageEnvelope` is serialized to JSON and deserialized back
- **THEN** the result equals the original, including its source, author, and
  conversation

#### Scenario: Source covers the supported origins

- **WHEN** a message originates from Discord, Slack, GitHub, or a manual entry
- **THEN** its `Source` can represent that origin

### Requirement: Projection model

`mirrorlane-core` SHALL define a `Projection` keyed by `MessageId` carrying an
`Intent`, a list of `Topic`s, a list of `Entity`s, and a `Confidence`. `Intent`
SHALL enumerate `Question`, `Decision`, `Proposal`, `Task`, `Issue`, and
`Social`. `Confidence` SHALL be constrained to the range `0.0..=1.0`. The model
SHALL serialize to JSON matching the documented shape.

#### Scenario: Confidence outside range is rejected

- **WHEN** a `Confidence` is constructed from a value below `0.0` or above `1.0`
- **THEN** construction fails or the value is clamped into `0.0..=1.0`, so no
  out-of-range confidence can exist

#### Scenario: Projection serializes to the documented shape

- **WHEN** a `Projection` is serialized to JSON
- **THEN** it contains `intent`, `topics`, `entities`, and `confidence` fields

### Requirement: Deterministic projector port

`mirrorlane-core` SHALL define a `Projector` port that maps a `MessageEnvelope`
to a `Projection` without I/O. A given message SHALL always project to the same
result. `mirrorlane-provider` SHALL supply a deterministic mock `Projector`.

#### Scenario: Same message projects identically

- **WHEN** the mock projector projects the same message twice
- **THEN** both projections are equal

### Requirement: Projection store port

`mirrorlane-core` SHALL define a `ProjectionStore` port that upserts a
`Projection` and retrieves it by `MessageId`, and SHALL provide an in-memory
adapter. Upserting a projection for an existing `MessageId` SHALL replace the
prior entry rather than add a duplicate.

#### Scenario: Upsert is keyed by message id

- **WHEN** two projections for the same `MessageId` are upserted
- **THEN** the store holds exactly one projection for that id, the most recent

#### Scenario: Get returns a stored projection

- **WHEN** a projection is upserted and then fetched by its `MessageId`
- **THEN** the fetched projection equals the upserted one

### Requirement: Idempotent process-message job

`mirrorlane-worker` SHALL provide a `ProcessMessageJob` Worklane job whose
payload is a `MessageEnvelope`. On run it SHALL project the message and upsert
the projection into a `ProjectionStore`. Because Worklane delivers
at-least-once, processing the same message more than once SHALL leave exactly
one stored projection.

#### Scenario: Processing a message stores its projection

- **WHEN** a `MessageEnvelope` is enqueued and the worker runs to idle
- **THEN** the store holds a projection for that message id

#### Scenario: Re-delivery is idempotent

- **WHEN** the same `MessageEnvelope` is processed twice
- **THEN** the store holds exactly one projection for that message id

### Requirement: Scope model

`mirrorlane-core` SHALL define a `Scope` for a `ConversationId` carrying a
`load` list and an `ignore` list of `Component`s, a `reason` string, and a
`Confidence`. It SHALL also define a `ScopeRequest` carrying the
`ConversationId` and the `MessageId`s that form the session. Both SHALL
serialize to and from JSON, and the scope SHALL serialize to a shape containing
`load`, `ignore`, and `reason`.

#### Scenario: Scope serializes to the documented shape

- **WHEN** a `Scope` is serialized to JSON
- **THEN** it contains `load`, `ignore`, and `reason` fields

#### Scenario: A component is never both loaded and ignored

- **WHEN** a `Scope` is produced
- **THEN** its `load` and `ignore` lists share no `Component`

### Requirement: Deterministic scope projector port

`mirrorlane-core` SHALL define a `ScopeProjector` port that maps a
`ConversationId` and a set of `Projection`s to a `Scope` without I/O. The same
inputs SHALL always produce the same scope. `mirrorlane-provider` SHALL supply a
deterministic mock `ScopeProjector` that loads the components detected from the
projections' topics and entities and ignores the remainder of its catalog.

#### Scenario: Same projections scope identically

- **WHEN** the mock scoper scopes the same projections twice
- **THEN** both scopes are equal

#### Scenario: Detected components are loaded, others ignored

- **WHEN** the mock scoper scopes projections whose topics/entities match part of
  its catalog
- **THEN** the matched components appear in `load` and the unmatched catalog
  components appear in `ignore`

#### Scenario: Empty session yields an empty load and zero confidence

- **WHEN** the mock scoper scopes an empty set of projections
- **THEN** `load` is empty, `ignore` is the full catalog, and `confidence` is
  `0.0`

### Requirement: Scope store port

`mirrorlane-core` SHALL define a `ScopeStore` port that upserts a `Scope` and
retrieves it by `ConversationId`, and SHALL provide an in-memory adapter.
Upserting a scope for an existing `ConversationId` SHALL replace the prior entry.

#### Scenario: Upsert is keyed by conversation id

- **WHEN** two scopes for the same `ConversationId` are upserted
- **THEN** the store holds exactly one scope for that id, the most recent

### Requirement: Idempotent build-scope job

`mirrorlane-worker` SHALL provide a `BuildScopeJob` Worklane job whose payload is
a `ScopeRequest`. On run it SHALL read the session's projections from a
`ProjectionStore`, build the scope, and upsert it into a `ScopeStore`, skipping
message ids that have no stored projection. Because Worklane delivers
at-least-once, processing the same request more than once SHALL leave exactly
one stored scope.

#### Scenario: Building a scope stores it by conversation

- **WHEN** a `ScopeRequest` is enqueued for messages whose projections are stored
  and the worker runs to idle
- **THEN** the scope store holds a scope for that conversation id

#### Scenario: Re-delivery is idempotent

- **WHEN** the same `ScopeRequest` is processed twice
- **THEN** the scope store holds exactly one scope for that conversation id

#### Scenario: Missing projections are skipped

- **WHEN** a `ScopeRequest` names a message that has no stored projection
- **THEN** the job builds a scope from the available projections without failing

### Requirement: Skill model

`mirrorlane-core` SHALL define a skill model: a `DeveloperProfile` pairing an
author with the topics they touch, an `ExpertCandidate` pairing an author with a
`SkillScore`, and a `TopicOwnership` pairing a `Topic` with its ranked
`ExpertCandidate`s. `SkillScore` SHALL be constrained to `0.0..=1.0`, clamping on
construction like `Confidence`.

#### Scenario: A skill score is clamped into range

- **WHEN** a `SkillScore` is constructed from a value outside `0.0..=1.0`
- **THEN** it is clamped into `0.0..=1.0`

#### Scenario: Topic ownership round-trips through JSON

- **WHEN** a `TopicOwnership` with ranked candidates is serialized and
  deserialized
- **THEN** the result equals the original

### Requirement: Skill store

`mirrorlane-core` SHALL define a `SkillStore` port keyed by `Topic` that upserts
and fetches a `TopicOwnership`, and SHALL provide an in-memory adapter. `upsert`
SHALL replace any existing ownership for the same topic, so rebuilding the index
under Worklane's at-least-once delivery leaves exactly one ownership per topic.

#### Scenario: Upsert is keyed by topic

- **WHEN** two `TopicOwnership`s for the same topic are upserted
- **THEN** the store holds exactly one ownership for that topic, the most recent

### Requirement: Deterministic skill builder

`mirrorlane-provider` SHALL provide a `MessageSkillBuilder` implementing a
deterministic, I/O-free `SkillBuilder` port. Given each author paired with that
author's message topics, it SHALL tally per-author topic involvement weighted by
projection confidence, and produce one `TopicOwnership` per topic whose
candidates are ranked by descending score with ties broken by ascending author
id. Within a topic, scores SHALL be normalized so the strongest candidate scores
`1.0`. The same input SHALL always produce the same index.

#### Scenario: The strongest contributor on a topic scores highest

- **WHEN** author A contributes to a topic more (by confidence-weighted message
  count) than author B
- **THEN** A precedes B in that topic's candidates and A's score is `1.0`

#### Scenario: The same input builds an identical index

- **WHEN** the same author/topic input is built twice
- **THEN** the two skill indexes are equal

#### Scenario: Equal contributors are ordered by author id

- **WHEN** two authors have equal score on a topic
- **THEN** they appear in ascending author-id order

### Requirement: Build skill job

`mirrorlane-worker` SHALL provide a `BuildSkillJob` that, given a request pairing
authors with their message ids, joins each message id to its stored `Projection`
to obtain that message's topics, builds the skill index via the `SkillBuilder`,
and upserts each `TopicOwnership` into the `SkillStore`. The job SHALL be
idempotent: re-delivering the same request leaves exactly one ownership per
topic. Message ids with no stored projection SHALL be skipped.

#### Scenario: Building the index stores ownership per topic

- **WHEN** authors and their projected messages are processed by `BuildSkillJob`
- **THEN** the skill store holds a `TopicOwnership` for each topic those
  projections touch

#### Scenario: Re-delivery is idempotent

- **WHEN** the same skill request is processed twice
- **THEN** the store holds exactly one ownership per topic

#### Scenario: Messages without a projection are skipped

- **WHEN** a request references a message id that has no stored projection
- **THEN** the job still builds the index from the projections that do exist

### Requirement: Developer snapshot model

`mirrorlane-core` SHALL define a `DeveloperSnapshot` carrying an author's
`AuthorId`, `display_name`, and `topics` (the `TopicWeight`s for the session
topics they own, ranked best-first), and a `SessionDevelopers` aggregate carrying
a `ConversationId` and the `DeveloperSnapshot`s for that conversation's
participants. Both SHALL round-trip through JSON. A participant who owns none of
the session's topics SHALL appear with an empty `topics` list.

#### Scenario: Session developers round-trip through JSON

- **WHEN** a `SessionDevelopers` with ranked developer snapshots is serialized
  and deserialized
- **THEN** the result equals the original

#### Scenario: A participant who owns no session topic has empty topics

- **WHEN** a snapshot is built for a participant who is not a candidate on any of
  the session's topics
- **THEN** that participant appears with an empty `topics` list

### Requirement: Deterministic developer snapshotter

`mirrorlane-provider` SHALL provide a `SkillDeveloperSnapshotter` implementing a
deterministic, I/O-free `DeveloperSnapshotBuilder` port that, given a
conversation's participants (each an author id and display name) and the
`TopicOwnership`s for the session's topics, produces a `SessionDevelopers`. For
each participant it SHALL collect the topics on which they are a candidate, using
their ranking score as the weight, rank those `topics` by descending weight with
ties broken by ascending topic name, and order developers by ascending author id.
The same inputs SHALL always produce the same result.

#### Scenario: A developer's topics are ranked by descending weight

- **WHEN** a participant owns several of the session's topics with different
  scores
- **THEN** their `topics` are ordered strongest-first

#### Scenario: Developers are ordered by author id

- **WHEN** a conversation has multiple participants
- **THEN** their snapshots appear in ascending author-id order

#### Scenario: The same inputs build an identical result

- **WHEN** the same participants and topic ownerships are snapshotted twice
- **THEN** the two `SessionDevelopers` are equal

### Requirement: Developer snapshot store

`mirrorlane-core` SHALL define a `DeveloperSnapshotStore` port keyed by
`ConversationId` that upserts and fetches a `SessionDevelopers`, and SHALL provide
an in-memory adapter. `upsert` SHALL replace any existing entry for the same
conversation, so re-deriving the snapshot under Worklane's at-least-once delivery
leaves exactly one per conversation.

#### Scenario: Upsert is keyed by conversation id

- **WHEN** two `SessionDevelopers` for the same conversation are upserted
- **THEN** the store holds exactly one entry for that conversation, the most
  recent

### Requirement: Build developer snapshot job

`mirrorlane-worker` SHALL provide a `BuildDeveloperSnapshotJob` that, given a
request of a conversation, its participants, and its message ids, gathers the
session topics from those messages' stored `Projection`s, fetches each topic's
`TopicOwnership` from the `SkillStore`, builds the `SessionDevelopers` via the
`DeveloperSnapshotBuilder`, and upserts it into the `DeveloperSnapshotStore`. The
job SHALL be idempotent: re-delivering the same request leaves exactly one entry
per conversation. Message ids with no stored projection SHALL be skipped when
collecting topics.

#### Scenario: Building stores developers per conversation

- **WHEN** a conversation's participants and messages are processed by
  `BuildDeveloperSnapshotJob`
- **THEN** the developer snapshot store holds one `SessionDevelopers` for that
  conversation

#### Scenario: Re-delivery leaves exactly one entry

- **WHEN** the same request is processed twice
- **THEN** the store still holds exactly one entry for the conversation

### Requirement: Replay re-derives developer snapshots

Replay SHALL re-derive developer snapshots as a deterministic phase after the
skill phase: replaying the same message log SHALL produce identical session
developers. Snapshot derivation SHALL NOT trigger routing dispatch or alter the
warm-up document — it is replayable derived state.

#### Scenario: Replaying the log re-derives identical snapshots

- **WHEN** the same message log is replayed twice
- **THEN** the session developers produced are identical

### Requirement: Routing hint model

`mirrorlane-core` SHALL define a `RoutingHint` carrying the `MessageId` it
describes, a `reviewers` list of ranked `ExpertCandidate`s drawn from the skill
index across the message's projected topics, and an optional `human_hint`
(`ExpertCandidate`) naming the single best person to route to. The model SHALL
round-trip through JSON. A hint with no qualified candidates SHALL have an empty
`reviewers` list and a `None` `human_hint`.

#### Scenario: A routing hint round-trips through JSON

- **WHEN** a `RoutingHint` with ranked reviewers and a human hint is serialized
  and deserialized
- **THEN** the result equals the original

#### Scenario: No candidates yields an empty hint

- **WHEN** a `RoutingHint` is built for a projection whose topics have no owners
- **THEN** its `reviewers` list is empty and its `human_hint` is `None`

### Requirement: Deterministic routing hinter

`mirrorlane-provider` SHALL provide a `SkillRoutingHinter` implementing a
deterministic, I/O-free `RoutingHinter` port that, given a `Projection` and the
`TopicOwnership`s for that projection's topics, produces a `RoutingHint`. It
SHALL aggregate each candidate's score across the projection's topics, rank
reviewers by descending aggregate score with ties broken by ascending author id,
and set `human_hint` to the top-ranked reviewer. The same inputs SHALL always
produce the same hint.

#### Scenario: The strongest candidate across topics is recommended first

- **WHEN** a projection's topics are owned such that author A's aggregate score
  exceeds author B's
- **THEN** A precedes B in `reviewers` and `human_hint` is A

#### Scenario: Equal candidates are ordered by author id

- **WHEN** two candidates have equal aggregate score across the projection's
  topics
- **THEN** they appear in ascending author-id order

#### Scenario: The same inputs build an identical hint

- **WHEN** the same projection and topic ownerships are hinted twice
- **THEN** the two routing hints are equal

### Requirement: Routing hint store

`mirrorlane-core` SHALL define a `RoutingHintStore` port keyed by `MessageId`
that upserts and fetches a `RoutingHint`, and SHALL provide an in-memory adapter.
`upsert` SHALL replace any existing hint for the same message id, so re-deriving
a hint under Worklane's at-least-once delivery leaves exactly one hint per
message.

#### Scenario: Upsert is keyed by message id

- **WHEN** two hints for the same message id are upserted
- **THEN** the store holds exactly one hint for that message, the most recent

### Requirement: Build routing hint job

`mirrorlane-worker` SHALL provide a `BuildRoutingHintJob` that, given a request
of message ids, joins each message id to its stored `Projection`, gathers the
`TopicOwnership`s for that projection's topics from the `SkillStore`, builds the
`RoutingHint` via the `RoutingHinter`, and upserts it into the
`RoutingHintStore`. The job SHALL be idempotent: re-delivering the same request
leaves exactly one hint per message. Message ids with no stored projection SHALL
be skipped.

#### Scenario: Building hints stores one hint per projected message

- **WHEN** projected messages are processed by `BuildRoutingHintJob`
- **THEN** the routing hint store holds exactly one `RoutingHint` for each of
  those messages

#### Scenario: Re-delivery leaves exactly one hint

- **WHEN** the same request is processed twice
- **THEN** the store still holds exactly one hint per message

#### Scenario: A message with no projection is skipped

- **WHEN** a request includes a message id with no stored projection
- **THEN** no hint is stored for that message and the rest are processed

### Requirement: Replay re-derives routing hints

Replay SHALL re-derive routing hints as a deterministic phase after the skill
phase: replaying the same message log SHALL produce identical routing hints.
Hint derivation SHALL NOT trigger routing dispatch — the hint is replayable
derived state attached to a message, not an external side effect.

#### Scenario: Replaying the log re-derives identical hints

- **WHEN** the same message log is replayed twice
- **THEN** the routing hints produced are identical

#### Scenario: Re-deriving a hint does not dispatch

- **WHEN** routing hints are re-derived during replay
- **THEN** no consumer receives a routed message as a result of hint derivation

### Requirement: Warm-up document model

`mirrorlane-core` SHALL define a `WarmupDocument` for a `ConversationId` carrying
a `focus` list of `Component`s, `decisions`, `open_questions`, and `tasks` lists
of `MessageId`s, and a rendered `summary` string. It SHALL also define a
`WarmupRequest` carrying the `ConversationId` and the `MessageId`s of the
session. Both SHALL serialize to and from JSON.

#### Scenario: Document round-trips through JSON

- **WHEN** a `WarmupDocument` is serialized to JSON and deserialized back
- **THEN** the result equals the original

### Requirement: Deterministic warm-up builder port

`mirrorlane-core` SHALL define a `WarmupBuilder` port that maps a
`ConversationId`, an optional `Scope`, and a set of `Projection`s to a
`WarmupDocument` without I/O. The same inputs SHALL always produce the same
document. `mirrorlane-provider` SHALL supply a deterministic mock `WarmupBuilder`
that groups projections by `Intent` into decisions, open questions, and tasks,
takes `focus` from the scope, and renders a summary.

#### Scenario: Same inputs build the same document

- **WHEN** the mock builder builds from the same scope and projections twice
- **THEN** both documents are equal

#### Scenario: Projections are grouped by intent

- **WHEN** the mock builder builds from projections with `Decision`, `Question`,
  and `Task` intents
- **THEN** each message id appears in the matching list — `decisions`,
  `open_questions`, or `tasks`

#### Scenario: Focus comes from the scope when present

- **WHEN** the mock builder builds with a scope whose load list is non-empty
- **THEN** the document's `focus` equals the scope's load list

#### Scenario: Absent scope degrades gracefully

- **WHEN** the mock builder builds with no scope
- **THEN** `focus` is empty and a document is still produced

### Requirement: Warm-up store port and resume

`mirrorlane-core` SHALL define a `WarmupStore` port that upserts a
`WarmupDocument` and retrieves it by `ConversationId`, and SHALL provide an
in-memory adapter. Retrieving a stored document by its conversation id is how a
session resumes. Upserting for an existing `ConversationId` SHALL replace the
prior entry.

#### Scenario: Resume returns the stored document

- **WHEN** a document is upserted and then fetched by its `ConversationId`
- **THEN** the fetched document equals the upserted one

#### Scenario: Upsert is keyed by conversation id

- **WHEN** two documents for the same `ConversationId` are upserted
- **THEN** the store holds exactly one document for that id, the most recent

### Requirement: Idempotent build-warmup job

`mirrorlane-worker` SHALL provide a `BuildWarmupJob` Worklane job whose payload
is a `WarmupRequest`. On run it SHALL read the session's projections from a
`ProjectionStore` and its scope from a `ScopeStore`, build the document, and
upsert it into a `WarmupStore`, skipping message ids with no stored projection.
Because Worklane delivers at-least-once, processing the same request more than
once SHALL leave exactly one stored document.

#### Scenario: Building a warm-up stores it by conversation

- **WHEN** a `WarmupRequest` is enqueued for a session whose projections and
  scope are stored and the worker runs to idle
- **THEN** the warm-up store holds a document for that conversation id

#### Scenario: Re-delivery is idempotent

- **WHEN** the same `WarmupRequest` is processed twice
- **THEN** the warm-up store holds exactly one document for that conversation id

#### Scenario: Missing scope does not fail the job

- **WHEN** a `WarmupRequest` is processed for a conversation that has no stored
  scope
- **THEN** the job stores a document with an empty `focus` without failing

<!-- Core contract: log, replay, cache, durable storage -->

### Requirement: Message log

`mirrorlane-core` SHALL define a `MessageStore` port that appends a
`MessageEnvelope`, fetches one by `MessageId`, and returns all messages in append
order, and SHALL provide an in-memory adapter. Appending a message whose id is
already present SHALL replace it in place rather than add a duplicate, so the log
holds each message id exactly once.

#### Scenario: Appending the same id twice keeps one entry

- **WHEN** two messages with the same `MessageId` are appended
- **THEN** the log holds exactly one message for that id, the most recent

#### Scenario: The log preserves append order

- **WHEN** messages with distinct ids are appended in some order
- **THEN** `all()` returns them in that order

### Requirement: Per-conversation message reads

`mirrorlane-core`'s `MessageStore` port SHALL provide two reads in addition to
`all()`: `conversation_ids()` returning the distinct conversation ids in first-seen
order, and `messages_for(&ConversationId)` returning that conversation's messages in
append order. Both SHALL have default implementations derived from `all()`, so every
adapter is correct without overriding. `SqliteMessageStore` SHALL override them with
SQL that materializes only the distinct ids / the one conversation's messages
(rather than the whole log), but the results SHALL equal the `all()`-derived
defaults. `append`, `get`, and `all` SHALL be unchanged.

#### Scenario: messages_for returns one conversation in append order

- **WHEN** messages across several conversations are stored and `messages_for(c)` is
  called
- **THEN** it returns exactly conversation `c`'s messages, in append order, matching
  what filtering `all()` would yield

#### Scenario: conversation_ids returns distinct ids in first-seen order

- **WHEN** messages across several conversations are stored
- **THEN** `conversation_ids()` returns each conversation id once, in first-seen
  order

#### Scenario: The SQLite override agrees with the default

- **WHEN** the same messages are read through `SqliteMessageStore` and through an
  adapter using the default `all()`-based implementations
- **THEN** `conversation_ids()` and `messages_for(c)` return equal results

### Requirement: Deterministic replay

`mirrorlane-worker` SHALL provide a `Replay` that reads a `MessageStore` and
re-runs the projection pipeline — project, then build scope per conversation,
then build warm-up per conversation, then build the global skill index across all
conversations — through the Worklane jobs into fresh stores. Replaying the same log
SHALL produce identical results **relative to the populated semantic cache and the
Step versions**: determinism is per-`Step` (the semantic cache fixes a step's output
for a given `(kind, version, input)`) composed with idempotent persistence. A first
or cache-miss run of a non-deterministic projector (a real LLM) is therefore not
required to be bit-identical to another cold run; the semantic cache fixes its output
thereafter, and recompute over the log plus the Step versions is the source of truth.

#### Scenario: Two replays of the same log agree

- **WHEN** the same log is replayed twice with the semantic cache populated
- **THEN** for every logged message id the two projections are equal, for every
  conversation id the two scopes and the two warm-up documents are equal, and for
  every topic the two skill ownerships are equal

### Requirement: Replay accounts for every message

A replay SHALL produce a projection for every message in the log (no loss) and
SHALL NOT produce more projections than there are distinct message ids (no
duplication).

#### Scenario: Every logged message is projected

- **WHEN** a log of distinct messages is replayed
- **THEN** each message id has a projection in the result, and the projection
  count equals the number of distinct message ids

### Requirement: Projection cache port

`mirrorlane-core` SHALL express projection caching as an instance of the generic
runtime `Cache<Projection>`: a port that stores and retrieves a `Projection` keyed
by a projector **version** and a message key, and SHALL provide an in-memory
adapter. The message key SHALL come from `MessageEnvelope`'s `CacheKey`
implementation, derived from the message's **id and body**, so that editing a message
in place (the store replaces by id) yields a different key and misses rather than
serving a stale projection. A lookup with a different version SHALL miss, so changing
the projector version invalidates prior entries.

#### Scenario: A cached projection is retrieved

- **WHEN** a projection is put under a version and then fetched with the same
  version and message key
- **THEN** the fetched projection equals the stored one

#### Scenario: A different version misses

- **WHEN** a projection is put under one version and fetched under a different
  version for the same message
- **THEN** the lookup returns nothing

#### Scenario: An in-place body edit misses

- **WHEN** a message is re-appended under the same id with a different body
- **THEN** its cache key changes, so the prior projection is not served

### Requirement: Durable projection cache

`mirrorlane-storage` SHALL provide a `SqliteProjectionCache` implementing the
generic `Cache<Projection>` port over a file-backed SQLite database, persisting
entries so they survive reopening the database at the same path. Serialization
SHALL live in `mirrorlane-storage`, not in the runtime, and the on-disk key form
for a given (version, message id) SHALL remain compatible so previously cached
entries are not orphaned.

#### Scenario: Cached projections survive reopening

- **WHEN** a projection is cached, the cache is dropped, and a new cache is opened
  at the same path
- **THEN** fetching with the same version and message id returns the cached
  projection

#### Scenario: A previously cached entry still hits

- **WHEN** a projection cached before this change is fetched with the same version
  and message id afterward
- **THEN** the lookup returns it (no silent orphaning)

### Requirement: Caching projector preserves determinism

`mirrorlane-provider` SHALL provide a `CachingProjector` that is a `Cached`
instance over an inner `Projector` and a projection cache, itself implementing
`Projector`. On `project` it SHALL return the cached projection when present and
otherwise compute via the inner projector and cache the result. The inner
projector SHALL be invoked at most once per (version, message id). Its public
constructor and the `Arc<dyn Projector>` it is used behind SHALL be unchanged.

#### Scenario: The inner projector runs only on a miss

- **WHEN** the same message is projected twice through a `CachingProjector` with a
  fixed version
- **THEN** the inner projector is invoked exactly once and both calls return the
  same projection

#### Scenario: A changed version recomputes

- **WHEN** the same message is projected under two different version tags
- **THEN** the inner projector is invoked for each version

### Requirement: Durable SQLite message store

`mirrorlane-storage` SHALL provide a `SqliteMessageStore` that implements the
`MessageStore` port over a file-backed SQLite database. It SHALL persist appended
messages so they survive reopening the database at the same path. `append` SHALL
be idempotent by message id — re-appending an id replaces it in place without
duplicating — and `all()` SHALL return messages in append order. The append
sequence used to order messages SHALL be allocated **concurrency-safely**: `seq`
SHALL carry a `UNIQUE` constraint and SHALL be allocated such that no two appended
messages can ever receive the same sequence number, even when more than one writer
connection is used — so adopting a pooled or multi-process writer path cannot
duplicate a sequence number or corrupt append order. Re-appending an existing id
SHALL preserve that id's original sequence number.

#### Scenario: Messages survive reopening the database

- **WHEN** messages are appended to a store at a path, the store is dropped, and a
  new store is opened at the same path
- **THEN** the reopened store returns the appended messages, in append order

#### Scenario: Appending an existing id replaces in place

- **WHEN** a message is appended whose id already exists
- **THEN** the store holds one entry for that id, the most recent, and append
  order is unchanged

#### Scenario: Concurrent appends never share a sequence number

- **WHEN** distinct messages are appended through more than one writer connection to
  the same database
- **THEN** every appended message has a distinct `seq`, and `all()` returns a total
  order with no duplicate or missing sequence numbers

### Requirement: SQLite connections are WAL-configured

Each SQLite store in `mirrorlane-storage` SHALL configure every connection it opens with `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=NORMAL`, and `PRAGMA busy_timeout=5000` before any table is created or queried. This applies to all four stores — `SqliteMessageStore`, `SqliteProjectionCache`, `SqliteDerivedOutputCache`, and `SqliteRoutingTraceStore` — for both file-backed and in-memory databases. `busy_timeout` SHALL be non-zero so
that a statement meeting another connection's lock waits rather than failing
immediately with `SQLITE_BUSY`. Applying the PRAGMAs SHALL NOT change the result of
any read or write — only the durability/concurrency posture of the connection.

#### Scenario: A store connection is opened in WAL mode with a busy timeout

- **WHEN** any of the four stores opens a file-backed database
- **THEN** the connection reports `journal_mode=wal` and a non-zero `busy_timeout`

#### Scenario: A contended write waits instead of failing immediately

- **WHEN** two connections to the same database file contend on a write lock and
  the holder releases within the busy timeout
- **THEN** the contending statement completes rather than returning `SQLITE_BUSY`

### Requirement: Stores open fallibly and recover from a poisoned lock

The constructors of the four `mirrorlane-storage` SQLite stores SHALL return a
`Result` rather than `.expect()`-ing on open, so a transient open failure
(permissions, a locked file, a disk error, or a failed PRAGMA/migration) is a
propagated error a caller or supervisor can handle, not a process panic. The stores
SHALL recover from a poisoned `Mutex` when the guarded connection is still
consistent, rather than panicking on lock acquisition. The infallible domain ports
(`MessageStore`, `Cache`, `DerivedOutputCache`, `RoutingTraceStore`) SHALL be
unchanged — only the concrete constructors, which are not trait methods, gain
`Result`.

#### Scenario: An open failure is a returned error, not a panic

- **WHEN** a store is opened at a path that cannot be opened or whose migration
  fails
- **THEN** the constructor returns an `Err` and the process does not panic

#### Scenario: The domain ports remain infallible

- **WHEN** the `MessageStore`, `Cache`, `DerivedOutputCache`, and
  `RoutingTraceStore` trait method signatures are inspected after this change
- **THEN** their operation methods still return values/`Option`, not `Result`

#### Scenario: A poisoned lock does not crash a consistent store

- **WHEN** a store's `Mutex` is poisoned by an unrelated panic but the connection
  is still consistent
- **THEN** a subsequent store operation recovers the guard and proceeds

### Requirement: SQLite stores carry a schema version

Each `mirrorlane-storage` SQLite store SHALL record and check a schema version via
`PRAGMA user_version`, applying ordered migrations to bring an older on-disk schema
forward, rather than relying solely on `CREATE TABLE IF NOT EXISTS` (which silently
no-ops against a stale schema). A database created before this change (with
`user_version=0`) SHALL open and migrate forward without data loss, and its
previously stored rows SHALL remain readable.

#### Scenario: A fresh database is stamped with the current schema version

- **WHEN** a store creates a new database
- **THEN** its `user_version` equals the store's current schema version

#### Scenario: A pre-existing database migrates forward and keeps its rows

- **WHEN** a store opens a database written before this change
- **THEN** it is migrated to the current schema version and its existing rows are
  still readable

### Requirement: Caches reclaim superseded rows; the log does not

`mirrorlane-storage` SHALL provide a reclamation path for the two
delivery-optimization caches (`SqliteProjectionCache`, `SqliteDerivedOutputCache`)
that prunes rows superseded by a newer `(version, …)` entry for the same
conversation/message after a delivery cycle completes, and SHALL provide a `VACUUM`
path to reclaim file space. Pruning SHALL remove only rows that are stale-by-key —
provably never returned by a subsequent read because a newer version/content for
the same key space exists — and SHALL NOT remove a row a later read could hit. The
`SqliteMessageStore` is the source of truth and SHALL NOT be pruned by this
mechanism; reclamation applies only to the caches.

#### Scenario: A superseded cache row is reclaimed after the cycle

- **WHEN** a delivery cycle writes a new derived output for a conversation under a
  new derivation version or content hash, and reclamation runs
- **THEN** the prior `(version, content)` rows for that conversation are removed and
  the current row is retained

#### Scenario: The current cache entry still hits after reclamation

- **WHEN** reclamation has run and a read is requested for the current
  `(version, conversation, content)`
- **THEN** the read returns the cached output without recomputing

#### Scenario: The message log is never pruned by reclamation

- **WHEN** cache reclamation runs
- **THEN** the `SqliteMessageStore` retains every appended message, in append order

### Requirement: Recovery by replay after restart

Replaying a reopened durable log SHALL reproduce the projection pipeline, so a
process recovers its in-memory projections, scope, and warm-up from the durable
log alone.

#### Scenario: Replay rebuilds the pipeline from a reopened log

- **WHEN** messages are appended to a durable store, the store is reopened at the
  same path, and the reopened log is replayed
- **THEN** the replay produces a projection for every logged message and a
  warm-up document for the conversation

<!-- Real projector adapter -->

### Requirement: Generic LLM client port

`mirrorlane-llm` SHALL define an `LlmClient` transport port that, given a model
name and a prompt, requests strict JSON output from a backend and returns the
model's raw response text. The port SHALL be synchronous and infallible at its
surface, panicking at the boundary on a transport error, a non-success status, or
an unreadable response — consistent with the projector convention, so a failure
freezes nothing. It SHALL expose a stable `provider_tag` (e.g. `ollama`, `openai`)
used to namespace cache versions. The port SHALL carry no projection/domain type —
it returns raw text, leaving prompt construction and parsing to its caller.

#### Scenario: A client returns the model's raw response text

- **WHEN** `LlmClient` is called with a model and a JSON-mode prompt and the backend
  responds successfully
- **THEN** it returns the backend's raw response text, unparsed

#### Scenario: A transport failure panics at the boundary

- **WHEN** the backend is unreachable or returns a non-success status
- **THEN** the client panics at the boundary rather than returning a fabricated
  response

#### Scenario: The provider tag is stable per backend

- **WHEN** `provider_tag` is read for a given client
- **THEN** it returns that backend's stable identifier, distinct from other backends

### Requirement: Provider-agnostic LLM projector

`mirrorlane-llm` SHALL provide an `LlmProjector` that implements
`mirrorlane_core::Projector` over any `Arc<dyn LlmClient>`, owning the projection
prompt and the `Projection` parser **once** (not per backend). On `project` it
SHALL build the projection prompt from the message body, call the client, parse the
strict JSON `{intent, topics, entities, confidence}` into a `Projection`, set the
`message_id` from the input envelope (never the model output), and clamp confidence
into `0.0..=1.0`. Parsing SHALL be a pure function of the response text and the
message id, unit-testable with canned responses and no I/O. Its `version` SHALL be
`{provider_tag}:{model}:{prompt_version}`.

#### Scenario: Canned JSON parses into a projection without a server

- **WHEN** a canned model response and a message id are passed to the parser
- **THEN** it returns the corresponding `Projection` with the id from the envelope
  and confidence clamped into range, with no network access

#### Scenario: The prompt and parser are shared across backends

- **WHEN** `LlmProjector` is constructed over two different `LlmClient`s
- **THEN** both use the same prompt and the same parser, differing only in transport

### Requirement: OpenAI-compatible LLM client

`mirrorlane-openai` SHALL provide an `OpenAiClient` implementing `LlmClient` over
the OpenAI-compatible `/v1/chat/completions` API, requesting JSON output. It SHALL
be constructed with a base URL (default `https://api.openai.com/v1`), a model, and
an API key taken **only** from the `OPENAI_API_KEY` environment variable — never
the config file. Its `provider_tag` SHALL be `openai`. Its live path SHALL be
exercised only by an `#[ignore]`d test so the crate builds and unit-tests without
network access.

#### Scenario: A successful completion yields the response text

- **WHEN** the OpenAI-compatible endpoint returns a chat completion with JSON
  content
- **THEN** `OpenAiClient` returns that content as the raw response text

#### Scenario: The live path is gated behind an ignored test

- **WHEN** the test suite runs without network access
- **THEN** the real OpenAI call is not exercised and the build and unit tests pass

### Requirement: Ollama-backed projector

`mirrorlane-ollama` SHALL provide an `OllamaClient` implementing the
`mirrorlane-llm` `LlmClient` port over a local Ollama HTTP server: given a model
and a JSON-mode prompt it SHALL call `/api/generate` with `format=json`, and return
the model's raw response text. Projection construction (prompt, parse, id, clamp)
SHALL be performed by the provider-agnostic `LlmProjector`, not by the client. The
`LlmClient` surface remains synchronous and infallible; `OllamaClient` performs a
blocking request behind it. Its `provider_tag` SHALL be `ollama`.

#### Scenario: A well-formed response becomes a projection

- **WHEN** an `LlmProjector` over an `OllamaClient` projects a message and the model
  returns JSON `{intent, topics, entities, confidence}`
- **THEN** the resulting `Projection` carries that intent, those topics and
  entities, and a confidence clamped to `0.0..=1.0`, with its `message_id` taken
  from the input envelope

#### Scenario: Ollama is reachable through the client port

- **WHEN** the `OllamaClient` is called with a model and a JSON-mode prompt
- **THEN** it posts to the Ollama `/api/generate` endpoint and returns the raw
  response text

### Requirement: Strict JSON parsing

The `LlmProjector` SHALL parse only the documented projection shape. The `intent`
field SHALL map to the existing `Intent` variants; `confidence` SHALL be clamped
into `0.0..=1.0`. Parsing SHALL be a pure function of the response text and the
message id, independent of any I/O, so it is unit-testable with canned responses
regardless of which `LlmClient` produced the text.

#### Scenario: Canned JSON parses without a live server

- **WHEN** a canned model response string and a message id are passed to the
  parser
- **THEN** it returns the corresponding `Projection` with no network access

#### Scenario: An out-of-range confidence is clamped

- **WHEN** the model returns a confidence outside `0.0..=1.0`
- **THEN** the parsed projection's confidence is clamped into range

### Requirement: Failures panic at the boundary

The LLM projection path SHALL NOT fabricate a projection on failure. On a transport
error or non-success status (in the `LlmClient`) or a response that is not the
documented JSON shape including an unknown `intent` (in the `LlmProjector` parser),
it SHALL panic. Because `project` does not return on failure, the wrapping
`CachingProjector` SHALL NOT cache anything for that message, and Worklane SHALL
contain the panic and retry the job (then dead-letter when attempts are exhausted).
A transient failure therefore never freezes an incorrect projection in the cache.

#### Scenario: Malformed output does not produce or cache a projection

- **WHEN** the model returns text that is not the documented projection JSON
- **THEN** `project` panics and no projection is cached for that message

#### Scenario: A transport failure does not cache a projection

- **WHEN** the `LlmClient` cannot reach the backend
- **THEN** `project` panics and no projection is cached for that message

### Requirement: Cache version encodes model and prompt

`mirrorlane-llm` SHALL derive the projector version tag (used to wrap an
`LlmProjector` in a `CachingProjector`) as
`{provider_tag}:{model}:{prompt_version}:{prompt_fingerprint}`, where the
`prompt_fingerprint` is a stable content hash of the projection **prompt template**
and the parser version. Changing the provider, the model, the prompt version, **or
the prompt template text** SHALL yield a different version and invalidate prior cache
entries — so editing the prompt cannot silently serve stale projections, with no
manual version bump required. The Ollama provider tag SHALL remain `ollama`, so an
Ollama projector's tag still leads with its established prefix.

#### Scenario: Changing the model invalidates cached projections

- **WHEN** the same message is projected under two different model names with
  otherwise identical configuration
- **THEN** the two projections are cached under different version tags

#### Scenario: Editing the prompt template invalidates cached projections

- **WHEN** the projection prompt template is edited but no version constant is bumped
- **THEN** the appended prompt fingerprint changes, so prior cache entries miss and
  the projection is recomputed

#### Scenario: A different provider does not alias the same cache entry

- **WHEN** the same message is projected by two different providers under the same
  model and prompt version
- **THEN** the two projections are cached under different version tags

#### Scenario: The Ollama tag is unchanged

- **WHEN** an Ollama projector's version tag is computed for a given model and
  prompt version
- **THEN** it equals `ollama:{model}:{prompt_version}`, identical to before this
  change

<!-- Routing & dispatch -->

### Requirement: Routing model

`mirrorlane-core` SHALL define a routing model: a `ConsumerKind` enum
(`Human`, `Agent`, `WorklaneJob`, `GitHub`), a `RoutingRule` mapping a projection
`Intent` to a target `ConsumerKind`, and a `RoutingDecision` carrying the routed
`MessageId`, the target `ConsumerKind`, a human-readable `reason`, and an
`escalated` flag. The model SHALL round-trip through JSON.

#### Scenario: A routing decision round-trips through JSON

- **WHEN** a `RoutingDecision` is serialized and deserialized
- **THEN** the result equals the original

### Requirement: Deterministic router with escalation

`mirrorlane-provider` SHALL provide a `RuleRouter` implementing a deterministic,
I/O-free `Router` port that maps a `Projection` to a `RoutingDecision` by
evaluating a **rule set** — a list of `RoutingRule` (`intent → target`) — together
with an escalation threshold and a default target. It SHALL choose the target by
the first rule whose `intent` matches the projection's, falling back to the default
target when no rule matches — EXCEPT that when the projection's confidence is below
the threshold it SHALL route to `Human` and set `escalated` to true. `RuleRouter::new()`
SHALL use a default rule set that reproduces the current mapping (`Question`→`Human`,
`Decision`→`Agent`, `Issue`/`Proposal`→`GitHub`, `Task`→`WorklaneJob`,
`Social`→`Human`), a `0.6` threshold, and a `Human` default target, so default
behavior is unchanged. The same projection under the same rule set SHALL always
produce the same decision.

#### Scenario: Intent selects the target

- **WHEN** a projection with sufficient confidence is routed
- **THEN** its target is the `ConsumerKind` its `Intent` maps to in the rule set and
  `escalated` is false

#### Scenario: A GitHub-bound intent routes to GitHub under the default rules

- **WHEN** an `Issue` or `Proposal` projection with sufficient confidence is routed
  through `RuleRouter::new()`
- **THEN** its target is `GitHub`

#### Scenario: Low confidence escalates to a human

- **WHEN** a projection whose confidence is below the threshold is routed
- **THEN** the decision targets `Human` and `escalated` is true

#### Scenario: An intent with no matching rule routes to the default target

- **WHEN** a projection with sufficient confidence whose intent has no rule in the
  set is routed
- **THEN** its target is the router's default target

#### Scenario: The same projection routes identically

- **WHEN** the same projection is routed twice through the same rule set
- **THEN** the two decisions are equal

### Requirement: Configurable routing rule set

`RuleRouter` SHALL be constructible from a supplied rule set, escalation threshold,
and default target (e.g. `RuleRouter::with_rules(rules, threshold, default_target)`),
so routing destinations are data rather than code. The CLI SHALL load an optional
routing rule set from its configuration (a `routing` key carrying the rules, the
threshold, and the default target); when the key is absent the default rule set
(`RuleRouter::new()`) SHALL be used, and behavior SHALL be identical to before.
The resolved router SHALL be used by both the read-time routing (the `replay`,
`warmup`, and `inspect` display path) and the route job, so configuration and
in-process display agree.

#### Scenario: A configured rule set retargets an intent

- **WHEN** the configuration supplies a routing rule mapping an intent to a target
  different from the default, and a sufficiently-confident projection of that intent
  is routed
- **THEN** the decision targets the configured target

#### Scenario: Absent routing configuration uses the default rules

- **WHEN** no `routing` key is present in the configuration
- **THEN** the router behaves identically to `RuleRouter::new()`

#### Scenario: A configured router is deterministic

- **WHEN** the same projection is routed twice through a router built from the same
  configured rule set
- **THEN** the two decisions are equal

### Requirement: Routing decision store

`mirrorlane-core` SHALL define a `RoutingStore` port keyed by `MessageId` that
upserts and fetches a `RoutingDecision`, and SHALL provide an in-memory adapter.
`upsert` SHALL replace any existing decision for the same message id, so
re-deriving a decision under Worklane's at-least-once delivery leaves exactly one
decision per message.

#### Scenario: Upsert is keyed by message id

- **WHEN** two decisions for the same message id are upserted
- **THEN** the store holds exactly one decision for that message, the most recent

### Requirement: Consumer registry and recording consumer

`mirrorlane-core` SHALL define a `Consumer` port and a `ConsumerRegistry` keyed by
`ConsumerKind` that dispatches a `RoutingDecision` and its `Projection` to the
registered consumer for the decision's target. The `Consumer::consume` operation
SHALL be **asynchronous and fallible** (returning a `Result`), so a real consumer
that performs I/O — such as enqueuing onto a Worklane broker — can report failure
instead of being forced to swallow it. `ConsumerRegistry::dispatch` SHALL be
asynchronous and SHALL propagate a consumer's error to its caller; dispatch to an
unregistered target SHALL remain a successful no-op. `mirrorlane-provider` SHALL
provide a `RecordingConsumer` that captures a `ConsumerReceipt` per `(kind,
message id)` and returns success; recording the same receipt twice SHALL leave
exactly one, so a well-behaved consumer is idempotent under re-delivery.

#### Scenario: A decision is dispatched to its target consumer

- **WHEN** a decision targeting a registered kind is dispatched through the
  registry
- **THEN** that kind's consumer receives the decision and its projection

#### Scenario: Dispatch to an unregistered target is a successful no-op

- **WHEN** a decision targeting a kind with no registered consumer is dispatched
- **THEN** dispatch succeeds and nothing is consumed

#### Scenario: A consumer error propagates from dispatch

- **WHEN** the target consumer's `consume` returns an error
- **THEN** `dispatch` returns that error to its caller

#### Scenario: Re-delivery records one receipt

- **WHEN** the same decision is dispatched to a `RecordingConsumer` twice
- **THEN** the consumer holds exactly one receipt for that message

### Requirement: Route job

`mirrorlane-worker` SHALL provide a `RouteJob` that, given a request of message
ids, routes each message's stored `Projection` via the `Router`, upserts the
`RoutingDecision` into the `RoutingStore`, upserts the `RoutingTrace` into the
`RoutingTraceStore`, and dispatches it through the `ConsumerRegistry`. The job
SHALL be idempotent: re-delivering the same request leaves exactly one decision
and one trace per message. Message ids with no stored projection SHALL be skipped.
When a consumer's dispatch fails, `RouteJob` SHALL return an error so Worklane
retries and ultimately dead-letters the routing job; because the decision and
trace upserts are idempotent and the Worklane enqueue consumer dedups by message
id, retrying after a partial failure SHALL NOT produce duplicate decisions,
duplicate traces, or duplicate enqueued jobs. Routing SHALL NOT be part of
`Replay`; it is a separate dispatch path so external delivery is never re-run by
replay.

#### Scenario: Routing stores a decision, trace, and dispatches it

- **WHEN** a message with a stored projection is processed by `RouteJob`
- **THEN** the routing store holds its decision, the trace store holds its trace,
  and the target consumer has received it

#### Scenario: Re-delivery is idempotent

- **WHEN** the same routing request is processed twice
- **THEN** the routing store holds exactly one decision and one trace for each
  message and no duplicate job is enqueued

#### Scenario: A consumer failure fails the routing job

- **WHEN** dispatch fails for one of the request's messages
- **THEN** `RouteJob` returns an error so Worklane can retry the routing job

#### Scenario: A message without a projection is skipped

- **WHEN** a request references a message id that has no stored projection
- **THEN** that message yields no decision, no trace, and the job still routes the
  others

### Requirement: Routing decision trace

`mirrorlane-core` SHALL define a `RoutingTrace` model that records the
step-by-step evaluation of rules during the routing of a message. A trace SHALL
contain the evaluated rules, the matched condition (if any), confidence scores,
and the final target. The `RoutingTrace` SHALL round-trip through JSON.

#### Scenario: A routing trace round-trips through JSON

- **WHEN** a `RoutingTrace` is serialized and deserialized
- **THEN** the result equals the original

### Requirement: Capturing trace in router

The `Router` port SHALL optionally return or emit a `RoutingTrace` alongside the
`RoutingDecision`. The deterministic rule evaluation SHALL populate this trace
with the specific rules evaluated.

#### Scenario: Rule evaluation is captured

- **WHEN** a projection is routed
- **THEN** a `RoutingTrace` is produced containing the evaluated rules and the
  reason for the final decision

### Requirement: Trace storage

`mirrorlane-core` SHALL define a `RoutingTraceStore` port keyed by `MessageId` to
upsert and fetch a `RoutingTrace`. Like the `RoutingDecision`, `upsert` SHALL
replace any existing trace for the same message id.

#### Scenario: Trace upsert is keyed by message id

- **WHEN** two traces for the same message id are upserted
- **THEN** the store holds exactly one trace for that message, the most recent

### Requirement: Bounded routing-trace retention

`SqliteRoutingTraceStore` SHALL support an optional most-recent-N cap on stored
traces: when a cap is set, an `upsert` SHALL prune traces beyond the cap by insertion
order (keeping the most recent N), so the trace store — observability, not a store of
record — cannot grow without limit. With no cap the store SHALL remain unbounded
(behavior-preserving for direct library use). The CLI SHALL apply a built-in default
cap, overridable via a `trace_max_count` key in the `retention` configuration
section. The `RoutingTraceStore` port SHALL be unchanged.

#### Scenario: A capped trace store keeps only the most recent N

- **WHEN** a trace store has a cap of N and more than N distinct traces are upserted
- **THEN** the store holds exactly N traces, the most recently written, and older
  traces are pruned

#### Scenario: An uncapped store is unbounded

- **WHEN** a trace store is opened without a cap
- **THEN** it retains every upserted trace, as before this change

#### Scenario: The most recent trace is always retained

- **WHEN** a trace is upserted into a capped store
- **THEN** that trace is present immediately afterward (the cap never drops the
  just-written trace)

### Requirement: Routed-work job

Mirrorlane SHALL define a typed Worklane `Job` with a stable `KIND` whose
payload carries the routed `MessageId` and the `Projection` required for
downstream processing. The payload SHALL round-trip through its serialization.

#### Scenario: Routed-work payload round-trips

- **WHEN** a routed-work payload is serialized and deserialized
- **THEN** the result equals the original

### Requirement: Durable Worklane enqueue consumer

Mirrorlane SHALL provide a Worklane-backed `Consumer`, registered for
`ConsumerKind::WorklaneJob`, that on `consume` enqueues a routed-work job onto a
**durable** Worklane broker (`worklane-sqlite`). A successful enqueue SHALL leave
the job reservable from the broker; an enqueue failure SHALL be returned as an
error from `consume` rather than silently dropped.

#### Scenario: Routing to WorklaneJob enqueues a durable job

- **WHEN** a projection routed to `WorklaneJob` is consumed
- **THEN** a routed-work job is enqueued onto the durable broker and is reservable
  on its lane

#### Scenario: An enqueue failure surfaces as an error

- **WHEN** the broker rejects an enqueue
- **THEN** `consume` returns an error and no receipt of success is reported

### Requirement: Unique-key enqueue dedup

The Worklane enqueue consumer SHALL enqueue with a uniqueness key derived
deterministically from the routed `MessageId` (via `Client::enqueue_unique`), so
that while a live job already holds the key, re-consuming the same message
enqueues nothing and yields the existing job id. Distinct messages SHALL enqueue
distinct jobs. This makes re-dispatch under at-least-once delivery — and
re-routing during replay/re-derivation — idempotent at the broker.

#### Scenario: Re-consuming the same message does not enqueue a duplicate

- **WHEN** the same message is consumed twice while its job is still live
- **THEN** exactly one routed-work job exists on the broker

#### Scenario: Distinct messages enqueue distinct jobs

- **WHEN** two different messages are consumed
- **THEN** two distinct routed-work jobs exist on the broker

### Requirement: Dead-letter inspection and requeue

Mirrorlane SHALL expose an operability surface to read dead-lettered
routed-work jobs for the routed-work lane (via `Broker::read_dead_letters`), to
count them (via `Broker::count_dead_letters`), to requeue a dead-lettered job back
onto its lane, and to purge the lane's dead-letter store (via
`Broker::purge_dead_letters`). Reading and counting dead letters SHALL be
non-destructive. After a requeue the job SHALL be reservable again. After a purge the
lane's dead-letter store SHALL be empty and the purge SHALL report how many records
were removed.

#### Scenario: A dead-lettered routed job is readable

- **WHEN** a routed-work job has exhausted its attempts and been dead-lettered
- **THEN** it appears in the dead-letter read for the routed-work lane

#### Scenario: Requeue makes a dead letter reservable again

- **WHEN** a dead-lettered routed-work job is requeued
- **THEN** a job for it is reservable on the routed-work lane again

#### Scenario: Counting dead letters is non-destructive

- **WHEN** the routed-work lane has dead-lettered jobs and the count is read
- **THEN** the count equals the number of dead letters and a subsequent read still
  returns them

#### Scenario: Purge empties the lane's dead-letter store

- **WHEN** the routed-work lane has dead-lettered jobs and the lane is purged
- **THEN** the purge reports the number removed and a later count returns zero

<!-- GitHub source & consumer -->

### Requirement: GitHub item model

`mirrorlane-github` SHALL define a `Repo` (owner and name) and a `GitHubItem`
carrying a `kind` (`Issue`, `PullRequest`, or `Comment`), the repo, the
issue/PR `number` the item belongs to, a stable `id`, the author login, an
optional `title`, and a `body`. The model SHALL round-trip through JSON so it can
back both canned fixtures and parsed API responses.

#### Scenario: A GitHub item round-trips through JSON

- **WHEN** a `GitHubItem` is serialized and deserialized
- **THEN** the result equals the original

### Requirement: Deterministic mapping to messages

`mirrorlane-github` SHALL map a `GitHubItem` to a `MessageEnvelope`
deterministically: a stable `MessageId` derived from the repo, kind, and item id;
`Source::GitHub`; an author from the GitHub login; a `ConversationId` derived from
the repo and issue/PR number, so an issue or PR and its comments share one
conversation; and a composed body. The same item SHALL always map to the same
message.

#### Scenario: An item maps to a GitHub-sourced message

- **WHEN** a `GitHubItem` is mapped
- **THEN** the message has `Source::GitHub`, an author from the item's login, and
  a `ConversationId` derived from the item's repo and number

#### Scenario: An issue and its comment share a conversation

- **WHEN** an issue and a comment on that issue are mapped
- **THEN** both messages carry the same `ConversationId` and distinct
  `MessageId`s

#### Scenario: Mapping is stable

- **WHEN** the same item is mapped twice
- **THEN** the two messages are equal

### Requirement: GitHub source port and fixture

`mirrorlane-github` SHALL define a `GitHubSource` port that fetches the
`GitHubItem`s for a `Repo`, and SHALL provide a deterministic `FixtureGitHubSource`
backed by canned items. The port is synchronous and infallible.

#### Scenario: The fixture source returns its canned items

- **WHEN** a `FixtureGitHubSource` seeded with items is fetched for a repo
- **THEN** it returns those items

### Requirement: Idempotent repository ingestion

`mirrorlane-github` SHALL provide `ingest_repo` that fetches a repo's items
through a `GitHubSource`, maps each to a `MessageEnvelope`, and appends it to a
`MessageStore`. Because message ids are stable and the log dedups by id,
re-ingesting the same items SHALL leave exactly one message per item.

#### Scenario: Ingesting appends one message per item

- **WHEN** a repo whose source yields N distinct items is ingested into an empty
  log
- **THEN** the log holds N messages, each `Source::GitHub`

#### Scenario: Re-ingesting does not duplicate

- **WHEN** the same repo is ingested twice
- **THEN** the log still holds one message per item

### Requirement: Real REST source with isolated failure

`mirrorlane-github` SHALL provide a `RestGitHubSource` implementing `GitHubSource`
over the GitHub REST API, configured with a base URL and an optional token. On a
transport error, a non-success status, or an unparseable response it SHALL panic
at the boundary rather than fabricate items, consistent with the projector
convention. Its live path SHALL be exercised only by an `#[ignore]`d test, so the
crate builds and unit-tests without network access.

#### Scenario: The live fetch is gated behind an ignored test

- **WHEN** the test suite runs without network access
- **THEN** the real REST fetch is not exercised and the build and unit tests pass

### Requirement: Fallible GitHub fetch surfaced at the CLI

`mirrorlane-github` SHALL provide a fallible fetch on `RestGitHubSource` —
`try_fetch(&self, repo) -> Result<Vec<GitHubItem>, GitHubFetchError>` — that
converts a transport error, a non-success HTTP status, an unreadable response, or
an unparseable body into a typed `GitHubFetchError` (carrying the HTTP status code
when one is available) rather than panicking. The infallible `GitHubSource::fetch`
SHALL be implemented in terms of `try_fetch`, panicking at the boundary on `Err` —
so the port stays infallible and the existing panic-at-the-boundary behavior is
preserved for the replay/Worklane path. The `github` CLI command SHALL use the
fallible path: on a fetch failure it SHALL report the error and exit non-zero
**without ingesting any message**, instead of panicking.

#### Scenario: A CLI fetch failure is a clean error, not a panic

- **WHEN** the `github` command runs and the REST fetch fails (transport,
  non-success status, or unparseable body)
- **THEN** the command reports the failure and exits non-zero, and no message is
  appended to the log

#### Scenario: The infallible port still panics at the boundary

- **WHEN** `GitHubSource::fetch` (the infallible port) is used and the underlying
  fetch fails
- **THEN** it panics at the boundary, unchanged from the prior convention

#### Scenario: A successful CLI fetch ingests as before

- **WHEN** the `github` command runs against a reachable repository
- **THEN** it appends one message per fetched item, exactly as before this change

### Requirement: GitHub token is scoped to the default host

`RestGitHubSource` SHALL attach the `GITHUB_TOKEN` `Authorization` header **only**
when the resolved base URL is the default `https://api.github.com`. For any other
base URL it SHALL NOT send the token, and SHALL warn on stderr that the token is
withheld for a non-default host — so a redirected or attacker-supplied endpoint
never silently receives the user's credential. The token SHALL continue to come
only from the `GITHUB_TOKEN` environment variable.

#### Scenario: The token is sent to the default GitHub host

- **WHEN** a request is made with the default base URL `https://api.github.com` and
  `GITHUB_TOKEN` is set
- **THEN** the request carries the `Authorization` header

#### Scenario: The token is withheld from a non-default host

- **WHEN** a request is made with a base URL other than `https://api.github.com`
  and `GITHUB_TOKEN` is set
- **THEN** the request carries no `Authorization` header and a warning is emitted

#### Scenario: An unauthenticated default-host request still works

- **WHEN** a request is made with the default base URL and `GITHUB_TOKEN` is unset
- **THEN** the request is sent without an `Authorization` header and no warning is
  emitted

### Requirement: GitHub draft model

`mirrorlane-github` SHALL define a `GitHubDraft` carrying a `kind` (`Issue` or
`PullRequest`), the source `MessageId`, a `title`, and a `body`. The model SHALL
round-trip through JSON.

#### Scenario: A draft round-trips through JSON

- **WHEN** a `GitHubDraft` is serialized and deserialized
- **THEN** the result equals the original

### Requirement: Deterministic drafting from a projection

`mirrorlane-github` SHALL provide a `draft_for(&Projection) -> GitHubDraft` that
is deterministic and I/O-free. It SHALL draft a `PullRequest` when the projection
intent is `Proposal` and an `Issue` otherwise, derive a title from the
projection's primary topic and intent, and compose a body summarizing the
projection (topics, entities, confidence, source message). The same projection
SHALL always produce the same draft.

#### Scenario: A proposal drafts a PR description

- **WHEN** a projection whose intent is `Proposal` is drafted
- **THEN** the draft's kind is `PullRequest`

#### Scenario: Other intents draft an issue

- **WHEN** a projection whose intent is not `Proposal` is drafted
- **THEN** the draft's kind is `Issue`

#### Scenario: Drafting is stable

- **WHEN** the same projection is drafted twice
- **THEN** the two drafts are equal

### Requirement: GitHub consumer records drafts idempotently

`mirrorlane-github` SHALL provide a `GitHubConsumer` implementing
`mirrorlane_core::Consumer`. On `consume`, it SHALL draft from the projection via
`draft_for` and record the draft keyed by the projection's `MessageId`. Recording
the same message twice SHALL leave exactly one draft, so the consumer is
idempotent under at-least-once dispatch. The draft SHALL be recorded, not posted
to GitHub.

#### Scenario: Consuming records a draft for the message

- **WHEN** a routed projection is consumed by a `GitHubConsumer`
- **THEN** the consumer holds a draft for that message id

#### Scenario: Re-delivery records one draft

- **WHEN** the same routed projection is consumed twice
- **THEN** the consumer holds exactly one draft for that message

<!-- CLI -->

### Requirement: Ingest command

The CLI SHALL provide an `ingest` command that reads one `MessageEnvelope` as
JSON from standard input and appends it to the durable message log at the path
given by `--db`. Appending SHALL be idempotent by message id.

#### Scenario: A piped message is appended to the log

- **WHEN** a `MessageEnvelope` JSON is piped to `ingest --db <path>`
- **THEN** the durable log at that path contains the message

#### Scenario: Re-ingesting the same id does not duplicate

- **WHEN** the same `MessageEnvelope` JSON is ingested twice
- **THEN** the log holds exactly one message for that id

### Requirement: Replay command

The CLI SHALL provide a `replay` command that replays the durable log at `--db`
and prints a warm-up for each conversation in the log.

#### Scenario: Replay reports a warm-up per conversation

- **WHEN** messages across one or more conversations have been ingested and
  `replay --db <path>` is run
- **THEN** the output includes a warm-up for each conversation present in the log

### Requirement: Warmup command

The CLI SHALL provide a `warmup --conversation <id>` command that replays the
durable log at `--db` and prints the warm-up document for the given conversation.

#### Scenario: Warmup prints the conversation's document

- **WHEN** a conversation's messages have been ingested and `warmup
  --conversation <id> --db <path>` is run
- **THEN** the output includes that conversation's warm-up summary

#### Scenario: Unknown conversation reports no warm-up

- **WHEN** `warmup` is run for a conversation id absent from the log
- **THEN** the command reports that no warm-up exists, without error

### Requirement: Provider selection

The CLI SHALL accept a `--provider mock|ollama|openai` option (default `mock`) that
selects the `Projector` backing the `replay` and `warmup` commands. With `mock`, the
deterministic mock projector is used. With `ollama` or `openai`, an `LlmProjector`
over the corresponding `LlmClient` (Ollama or OpenAI-compatible) is wrapped in a
`CachingProjector` (versioned by provider, model, and prompt) and backed by a
**durable** `SqliteProjectionCache` at the `--db` path, so a real model run stays
replay-safe and a successful projection is frozen and re-read without calling the
model.

#### Scenario: Default provider is the mock

- **WHEN** a `replay` or `warmup` command is run without `--provider`
- **THEN** the deterministic mock projector is used

#### Scenario: A real provider is selectable

- **WHEN** `--provider ollama` or `--provider openai` is passed
- **THEN** the command projects through an `LlmProjector` over that provider's
  client, wrapped in a `CachingProjector` over a durable `SqliteProjectionCache`

#### Scenario: A cached projection is reused without calling the model

- **WHEN** a real provider projects a message successfully, then the same command is
  re-run against the same `--db`
- **THEN** the second run reads the frozen projection from the durable cache and does
  not call the model

#### Scenario: An unknown provider is rejected

- **WHEN** a value other than `mock`, `ollama`, or `openai` is passed to `--provider`
- **THEN** the CLI reports an error and does not run the command

### Requirement: Output format selection

The CLI SHALL accept a `--format text|json` option (default `text`) that selects
how `ingest`, `replay`, and `warmup` render their output. With `text`, the
commands print the human summaries they print today. With `json`, the commands
SHALL emit serialized JSON: `ingest` an object carrying the appended message id;
`replay` a JSON array of per-conversation `SessionContext`s; and `warmup` the
conversation's `SessionContext`, or JSON `null` when the conversation is absent. A
`SessionContext` SHALL carry a `schema` field identifying the output schema version
(so a machine consumer can detect a breaking shape change), a `derivation_version`
field identifying the derivation version that produced it (its provenance), the
conversation's `WarmupDocument`, its `Scope` (or null), its `SessionDevelopers` (or
null), the `RoutingHint`s for the conversation's messages, the `RoutingDecision`s for
those messages, and a `GitHubDraft` for each message whose decision targets GitHub —
all in message order. The routing decisions and GitHub drafts SHALL be derived for
display only — the CLI SHALL NOT dispatch decisions, post drafts, or persist either.
Selecting `json` SHALL NOT change the durable log, the replay, or any projection. An
unknown format value SHALL be rejected without running the command.

#### Scenario: Default format is text

- **WHEN** a command is run without `--format`
- **THEN** it prints the human summary output, as today

#### Scenario: Replay emits a JSON array of session contexts

- **WHEN** messages across one or more conversations have been ingested and
  `replay --format json` is run
- **THEN** the output is a JSON array with one `SessionContext` per conversation,
  each carrying its schema version, derivation version, warm-up, scope, developers,
  routing hints, routing decisions, and any GitHub drafts

#### Scenario: A GitHub-targeted message contributes a draft

- **WHEN** a message's routing decision targets GitHub
- **THEN** the `SessionContext` includes a `GitHubDraft` for that message; messages
  routed elsewhere contribute no draft

#### Scenario: A session context carries its schema and derivation version

- **WHEN** a `SessionContext` is emitted as JSON
- **THEN** it carries a `schema` field identifying the output schema version and a
  `derivation_version` field identifying the derivation that produced it

#### Scenario: Warmup emits the session context as JSON

- **WHEN** a conversation's messages have been ingested and `warmup --conversation
  <id> --format json` is run
- **THEN** the output is that conversation's `SessionContext` serialized as JSON,
  including a routing decision per message and a GitHub draft for each
  GitHub-targeted message

#### Scenario: Warmup for an unknown conversation emits JSON null

- **WHEN** `warmup --format json` is run for a conversation id absent from the log
- **THEN** the output is JSON `null` and the command exits without error

#### Scenario: Ingest emits the message id as JSON

- **WHEN** a `MessageEnvelope` JSON is piped to `ingest --format json`
- **THEN** the output is a JSON object carrying the appended message id

#### Scenario: An unknown format is rejected

- **WHEN** a value other than `text` or `json` is passed to `--format`
- **THEN** the CLI reports an error and does not run the command

### Requirement: Configuration file

The CLI SHALL accept an optional `--config <path>` flag pointing to a JSON
configuration file, and when the flag is absent SHALL auto-load `mirrorlane.json`
from the working directory if it exists, proceeding without it if not. The
configuration MAY set any of `db`, `provider`, `format`, `ollama_base_url`,
`ollama_model`, `ollama_prompt_version`, and `github_base_url`; each key is
optional and unknown keys are ignored. For each setting the CLI SHALL resolve the
value by precedence: an explicit command-line flag first, then the configuration
value, then the built-in default (`mirrorlane.db`, `mock`, `text`, and the adapter
endpoint defaults). An explicitly passed `--config` path that cannot be read or
parsed SHALL be an error, and the command SHALL NOT run.

#### Scenario: A flag overrides the configuration

- **WHEN** the configuration sets `provider` to one value and the command line
  passes a different `--provider`
- **THEN** the command-line value is used

#### Scenario: The configuration supplies a default when no flag is given

- **WHEN** the configuration sets `db` and no `--db` flag is passed
- **THEN** the configured `db` is used

#### Scenario: Built-in defaults apply when neither flag nor config sets a value

- **WHEN** neither a flag nor the configuration sets `format`
- **THEN** the built-in default `text` is used

#### Scenario: Endpoint keys are read from the configuration

- **WHEN** the configuration sets `ollama_base_url` or `github_base_url` and no
  corresponding flag is passed
- **THEN** the configured endpoint is used

#### Scenario: A missing auto-loaded config is not an error

- **WHEN** no `--config` is passed and no `mirrorlane.json` exists in the working
  directory
- **THEN** the command runs with flags and built-in defaults, without error

#### Scenario: An explicit unreadable config is an error

- **WHEN** `--config <path>` names a file that cannot be read or is invalid JSON
- **THEN** the CLI reports an error and does not run the command

### Requirement: Configurable API endpoints

The CLI SHALL let a user set the API endpoints the real adapters talk to, by flag
or configuration, resolved by the same precedence as other settings (an explicit
flag first, then the configuration value, then the built-in default). It SHALL
accept `--ollama-base-url`, `--ollama-model`, `--ollama-prompt-version`,
`--openai-base-url`, `--openai-model`, and `--github-base-url`, with matching config
keys `ollama_base_url`, `ollama_model`, `ollama_prompt_version`, `openai_base_url`,
`openai_model`, and `github_base_url`. The defaults SHALL be the adapter crate
defaults (`http://localhost:11434` and the default Ollama model/prompt;
`https://api.openai.com/v1` and the default OpenAI model; `https://api.github.com`).
When `--provider ollama`/`openai` is selected, the resolved values for that provider
SHALL construct its client. A GitHub token SHALL continue to come only from
`GITHUB_TOKEN`, and an OpenAI key only from `OPENAI_API_KEY`; neither SHALL be read
from the config file.

#### Scenario: A flag sets the Ollama endpoint

- **WHEN** `--provider ollama --ollama-base-url http://host:1234 --ollama-model m`
  is run
- **THEN** the projector targets `http://host:1234` with model `m`

#### Scenario: A flag sets the OpenAI endpoint

- **WHEN** `--provider openai --openai-base-url https://host/v1 --openai-model m` is
  run
- **THEN** the projector targets `https://host/v1` with model `m`, taking the key
  only from `OPENAI_API_KEY`

#### Scenario: The configuration supplies an endpoint when no flag is given

- **WHEN** `mirrorlane.json` sets `openai_base_url` and no `--openai-base-url` flag
  is passed
- **THEN** the configured base URL is used

#### Scenario: Secrets are read only from the environment

- **WHEN** the config file contains an OpenAI key or GitHub token value
- **THEN** it is ignored; the key comes only from `OPENAI_API_KEY` and the token
  only from `GITHUB_TOKEN`

### Requirement: GitHub ingestion command

The CLI SHALL provide a `github --repo <owner/name>` command that fetches the
repository's issues, pull requests, and comments through a `GitHubSource` and
appends each as a message to the durable log at `--db`. Appending SHALL be
idempotent by message id, so re-running leaves exactly one message per item. A
`--repo` value not of the form `owner/name` SHALL be rejected as an error, and no
message SHALL be ingested. The command SHALL honor `--format`: `text` reports the
number of items ingested; `json` emits an object carrying the repo and the
ingested message ids.

#### Scenario: Ingesting a repo appends one message per item

- **WHEN** `github --repo <owner/name>` is run and the source returns items
- **THEN** the durable log gains one message per fetched item, each with source
  GitHub

#### Scenario: Re-ingesting a repo does not duplicate

- **WHEN** `github --repo <owner/name>` is run twice over the same items
- **THEN** the log holds exactly one message per item

#### Scenario: An invalid repo is rejected

- **WHEN** `--repo` is not of the form `owner/name`
- **THEN** the CLI reports an error and ingests nothing

#### Scenario: JSON output reports the ingested ids

- **WHEN** `github --repo <owner/name> --format json` is run
- **THEN** the output is a JSON object carrying the repo and the ingested message
  ids

### Requirement: CLI inspect command

The `mirrorlane-cli` SHALL provide an `inspect` command that takes a `MessageId`
and outputs its context, including the `RoutingTrace` if available. When
`--format json` is used, the JSON output SHALL include the `RoutingTrace`
structured data. When using the default text format, the CLI SHALL render the
trace in a human-readable step-by-step format.

#### Scenario: Inspecting a routed message

- **WHEN** a user runs `mirrorlane inspect <message-id>` for a routed message
- **THEN** the CLI outputs the routing trace showing why the target was selected

#### Scenario: Inspecting a message with JSON format

- **WHEN** a user runs `mirrorlane inspect <message-id> --format json`
- **THEN** the CLI outputs the full session context including the `RoutingTrace`
  as JSON

### Requirement: Projection failure is surfaced

The `replay` and `warmup` commands SHALL surface a projection failure rather than
silently emitting incomplete context: when a projector fails to produce a
projection for a logged message (a real projector such as Ollama panics at the
boundary on non-conforming model output rather than fabricating a result), the
command SHALL report the affected message ids and exit non-zero. Because
successful projections are cached durably, re-running retries only the messages
that still failed; once every message has a projection the command exits zero.

#### Scenario: A failed projection makes the command exit non-zero

- **WHEN** `--provider ollama` is run and the model returns non-conforming output
  for a message, so no projection is produced for it
- **THEN** the command reports that message id and exits non-zero

#### Scenario: Re-running retries only the still-failing messages

- **WHEN** a prior run cached some messages' projections and a message is
  re-projected successfully on a later run
- **THEN** the cached messages are not recomputed and, once every message has a
  projection, the command exits zero

### Requirement: Object-safe strategy surface

`mirrorlane-worker` SHALL define a `ReplayStrategy` trait — the object-safe erased
domain shape of `Strategy` for `Input = dyn MessageStore` and `Output =
ReplayStores` — that can be held behind a trait object (`Arc<dyn ReplayStrategy>`).
Every `Strategy` over that domain shape SHALL satisfy `ReplayStrategy` without
per-type code, so the reference `ProjectionStrategy` is usable as a
`ReplayStrategy`. The generic `Strategy` abstraction SHALL be unchanged.

#### Scenario: The reference strategy is usable as a trait object

- **WHEN** `ProjectionStrategy` is held as `Arc<dyn ReplayStrategy>` and run with a
  message log
- **THEN** it returns the same `ReplayStores` it would produce when run directly

### Requirement: Data-driven strategy selection

`mirrorlane-worker` SHALL provide a `StrategyRegistry` that maps a strategy id to a
factory producing an `Arc<dyn ReplayStrategy>` from a resolved context. The
registry SHALL register `projection` as the default reference strategy, SHALL
return the strategy registered under a requested id, and SHALL report an unknown id
as an error rather than panicking. Which strategy runs SHALL therefore be a
data-supplied choice, not a compile-time constant.

#### Scenario: The default id yields the reference strategy

- **WHEN** the registry is asked to build the `projection` id with a context
- **THEN** it returns the projection pipeline, and running it reproduces the
  existing replay behavior unchanged

#### Scenario: A different id yields a different strategy

- **WHEN** the registry is asked to build a second registered id with the same
  context
- **THEN** it returns that other strategy, not the projection pipeline

#### Scenario: An unknown id is an error

- **WHEN** the registry is asked to build an id that was never registered
- **THEN** it returns an error and runs no strategy

### Requirement: Strategy resolved by flag, config, then default

The CLI SHALL select the strategy by the same precedence as its other settings —
the `--strategy` flag, then the `strategy` key in `mirrorlane.json`, then the
default `projection`. The `replay`, `warmup`, and `inspect` commands SHALL run the
resolved strategy, building it through the registry rather than constructing a
fixed pipeline. With no flag or config key, behavior SHALL be identical to the
projection pipeline.

#### Scenario: No configuration selects projection

- **WHEN** neither the `--strategy` flag nor the `strategy` config key is set
- **THEN** the resolved strategy is `projection` and command behavior is unchanged

#### Scenario: The flag selects a strategy by id

- **WHEN** `--strategy <id>` names a registered non-default strategy
- **THEN** that strategy is built and run instead of projection

#### Scenario: An unknown configured strategy is reported

- **WHEN** the resolved strategy id is not registered
- **THEN** the CLI reports the error through its configuration-error path and runs
  no strategy

### Requirement: Submittable strategy-run job

`mirrorlane-worker` SHALL define a typed, submittable Worklane job that names a
strategy by id: a stable kind `mirrorlane.strategy_run`, a serializable request
payload carrying the strategy id, and a validated lane `mirrorlane.strategy_run`.
The job SHALL be the public, inbound counterpart to the outbound `RoutedWork`
surface, so an external plane (Triggerlane) holding a Worklane client can submit a
strategy run as it would any typed job.

#### Scenario: A strategy run is submitted as a typed job

- **WHEN** a `mirrorlane.strategy_run` job naming a registered strategy is enqueued
  onto a Worklane broker and a worker runs to idle
- **THEN** the named strategy runs over the worker's message log

### Requirement: Submitted runs resolve through the strategy registry

The strategy-run handler SHALL resolve the submitted strategy id through the
strategy registry (the same data-driven selection the CLI uses) and run the
resolved strategy. A submitted id that is not registered SHALL fail the job — so
Worklane retries and ultimately dead-letters it — rather than panicking.

#### Scenario: A registered id runs the selected strategy

- **WHEN** a strategy-run job naming a registered id is consumed
- **THEN** the handler resolves that id through the registry and runs that strategy

#### Scenario: An unknown id fails the job rather than panicking

- **WHEN** a strategy-run job names an id that is not registered
- **THEN** the job fails and is retried and ultimately dead-lettered, with no panic

### Requirement: Strategy-run dead-letter inspection and requeue

`mirrorlane-worker` SHALL provide an operability surface for the strategy-run lane
that can read its dead-lettered jobs, count them, requeue one by id, and purge the
lane's dead-letter store, so a poison submission is observable and recoverable,
mirroring the routed-work dead-letter surface. Reading and counting SHALL be
non-destructive, and a purge SHALL report how many records were removed.

#### Scenario: A dead-lettered submission can be inspected and requeued

- **WHEN** a strategy-run job has exhausted its retries and been dead-lettered
- **THEN** it can be read from the strategy-run dead-letter surface and requeued by
  its job id

#### Scenario: A dead-lettered submission can be counted and purged

- **WHEN** the strategy-run lane has dead-lettered submissions
- **THEN** the count equals their number, and purging the lane reports the number
  removed and leaves a later count at zero

### Requirement: Dead-letter operability from the CLI

The CLI SHALL provide a `dlq` command that operates a lane's dead-letter store on
the **resolved durable Worklane broker** (see "Queue broker selection"), so an
operator can recover from poison jobs without writing code. The command SHALL accept
a lane selector (the strategy-run lane or the routed-work lane) and one of four
actions: `read` (list up to a limit of dead-lettered jobs, non-destructive), `count`
(the number of dead-lettered jobs, non-destructive), `requeue` (restore one job to
its lane by job id), and `purge` (empty the lane's dead-letter store, reporting the
number removed). `requeue` SHALL require a job id and report a clean error when it is
missing or unparseable, never panicking. The command SHALL honor `--format
text|json`. It SHALL add no broker capability beyond the existing operability
surface — it is the CLI wrapper over it.

#### Scenario: Counting dead letters from the CLI

- **WHEN** a lane has dead-lettered jobs and `dlq` is run with that lane and the
  count action
- **THEN** the command reports the number of dead-lettered jobs and does not remove
  them

#### Scenario: Requeue a dead-lettered job by id from the CLI

- **WHEN** `dlq` is run with a lane, the requeue action, and the id of a
  dead-lettered job
- **THEN** the job becomes reservable on its lane again and is removed from the
  dead-letter store

#### Scenario: Requeue without an id is a clean error

- **WHEN** `dlq` is run with the requeue action but no job id
- **THEN** the command exits with an error and does not panic

#### Scenario: Purge empties a lane's dead-letter store from the CLI

- **WHEN** a lane has dead-lettered jobs and `dlq` is run with that lane and the
  purge action
- **THEN** the command reports how many records were removed and a later count
  returns zero

### Requirement: Observable job execution on the durable worker

`mirrorlane-worker` SHALL provide a `JobObserver` (the worklane worker
observability seam) that records each finished job's lane, kind, outcome
(acked, retried, or dead-lettered), and duration into a readable in-memory log, so a
durable worker's activity is observable rather than opaque. The observer SHALL expose
the collected records as typed values a caller can read after a run. Mounting the
observer SHALL NOT change job execution, ordering, or outcomes.

#### Scenario: A finished job produces a record

- **WHEN** a worker with the observer mounted runs a job to a successful ack
- **THEN** the observer's log contains a record carrying that job's kind and an
  acked outcome

#### Scenario: A dead-lettered job is recorded as dead-lettered

- **WHEN** a worker with the observer mounted runs a job that exhausts its attempts
  and is dead-lettered
- **THEN** the observer's log contains a record for that job with a dead-lettered
  outcome

### Requirement: Strategy composition vocabulary

`mirrorlane-worker` SHALL provide a composition vocabulary through which a strategy
declares its fan-out without re-deriving the Worklane mechanic: a stage runner that,
given a job and its payloads, runs the job over each payload to completion, and
fan-out builders that produce a stage's payloads for the three real shapes —
per-message, per-conversation, and global (a single payload). The builders SHALL be
generic over a payload-building closure, since each job's request type differs. The
vocabulary SHALL be the authoring surface for strategies, including injected ones.

#### Scenario: A stage runs a job over its fan-out payloads

- **WHEN** a stage is run with a job and the payloads produced by a fan-out builder
- **THEN** the job runs once per payload, to completion

#### Scenario: Each fan-out shape produces the matching payloads

- **WHEN** the per-message, per-conversation, and global builders are applied to a
  log
- **THEN** they produce one payload per message, one per conversation, and a single
  payload respectively

### Requirement: Projection expressed through the composition vocabulary

The projection pipeline SHALL be expressed as a composition of its six jobs through
the stage runner and fan-out builders, with **no change** to projection, scope,
skill, warm-up, routing-hint, or developer-snapshot behavior. Replaying the same
message log SHALL still produce identical results.

#### Scenario: Projection behavior is unchanged

- **WHEN** the existing replay, CLI, and Ollama tests run after the pipeline is
  re-expressed through the composition vocabulary
- **THEN** they pass unchanged

### Requirement: Composition genericity proven by a different-shaped strategy

`mirrorlane-worker` SHALL include, beyond the reference projection pipeline, a
non-domain strategy and a strategy whose fan-out shape differs from projection's,
both composed through the same vocabulary — demonstrating the vocabulary is generic
and not projection-shaped, as befits an authoring surface for injected strategies.

#### Scenario: A different-shaped strategy composes through the same vocabulary

- **WHEN** a strategy with a non-projection fan-out shape is composed through the
  stage runner and fan-out builders and run
- **THEN** it produces its expected output, with the vocabulary unchanged

### Requirement: Per-conversation derived-output unit

`mirrorlane-core` SHALL define a serializable per-conversation derived-output unit —
the reproducible, consumable derivation for one conversation — built only from core
types (projections, scope, warm-up, developers, routing hints). It SHALL exclude
read-time routing decisions and GitHub drafts, which are re-derived from the
projections at read time, keeping the unit free of any non-core dependency.

#### Scenario: The derived-output unit round-trips as data

- **WHEN** a per-conversation derived-output unit is serialized and deserialized
- **THEN** it is unchanged, carrying the conversation's reproducible derivation

### Requirement: Durable derived-output cache

`mirrorlane-storage` SHALL provide a durable cache of the per-conversation
derived-output unit, keyed by a derivation version, a conversation id, and a hash of
the conversation's message content — a **cache of a deterministic derivation**, not a
store of record. A change to the version or to the conversation's content SHALL miss,
so the output is recomputed. The cache SHALL be openable on a file or in memory.

#### Scenario: Cached output survives reopening

- **WHEN** a derived-output unit is put under a version, conversation, and content
  hash, and the cache is reopened at the same path
- **THEN** getting that key returns the unit unchanged

#### Scenario: A version or content change misses

- **WHEN** the cache is queried for a conversation under a different derivation
  version or a different content hash than it was stored with
- **THEN** it returns nothing, so the output is recomputed

### Requirement: Consumable output served from the cache

The runtime SHALL populate the derived-output cache when it replays, and SHALL serve
a per-conversation read from the cache without recomputing when the version and
content match, recomputing (replaying) only on a miss. The served `SessionContext`
SHALL equal the one a fresh replay would produce — the cache is a delivery
optimization, never a divergent source of truth.

#### Scenario: A populated conversation reads without recomputing

- **WHEN** a replay has populated the cache and a per-conversation read is requested
  for an unchanged conversation
- **THEN** the read returns the conversation's context from the cache without
  running the strategy again

#### Scenario: A changed conversation recomputes

- **WHEN** a conversation's messages have changed since the cache was populated and a
  read is requested
- **THEN** the read misses, recomputes by replay, and returns the up-to-date context

### Requirement: Composed derivation version

`mirrorlane-core` SHALL compose the derivation version that keys the derived-output
cache from a global derivation schema version, the strategy id, and the projector's
runtime version. The schema version SHALL be a single constant bumped when any
derivation step's behavior or the pipeline composition changes — covering the
non-projector builders, which carry no runtime version of their own. Including the
strategy id SHALL prevent two strategies that share a projector from colliding on
one cache entry.

#### Scenario: A different strategy or projector yields a different version

- **WHEN** the derivation version is composed for two different strategy ids, or for
  two different projector versions
- **THEN** the composed versions differ

#### Scenario: The schema version is part of the key

- **WHEN** the derivation schema version is bumped
- **THEN** previously cached output is keyed under the old version and misses, so it
  is recomputed

### Requirement: A consumed strategy run populates the derived-output cache

When a submitted strategy-run job is consumed, the handler SHALL run the resolved
strategy and then write its per-conversation derived output to the derived-output
cache, keyed by the composed derivation version and the conversation's content hash
— the same read model the in-process replay populates. A submitted run SHALL
therefore leave consumable output that a later read can serve, rather than running
only for effect. An unknown strategy id SHALL still fail the job before any run.

#### Scenario: Consuming a run leaves readable output

- **WHEN** a strategy-run job naming a registered strategy is consumed with a
  derived-output cache
- **THEN** the cache holds each derived conversation's output, keyed by the run's
  derivation version and content hash

### Requirement: One shared run-to-cache path

The runtime SHALL share one implementation of turning a strategy run's stores into
cached per-conversation derived output, used by both the in-process replay and the
async consume handler, so both paths populate the cache identically.

#### Scenario: Both paths populate the same way

- **WHEN** the in-process replay and the async consume handler each run the same
  strategy over the same log with the same derivation version
- **THEN** they populate the cache with the same per-conversation derivations

### Requirement: Single-pass run-to-cache derivation

The shared run-to-cache path SHALL derive every conversation's output in a **single
pass** over the message log — one traversal that groups messages by conversation and
computes each conversation's content hash — rather than re-scanning the log per
conversation. The `warmup` cache-**hit** path SHALL read and hash only the requested
conversation (via `messages_for`), not the whole log. These are efficiency changes:
the cached output, content hashes, cache keys, and replay results SHALL be identical
to before.

#### Scenario: Populating the cache scans the log once

- **WHEN** a replay populates the derived-output cache for a log of C conversations
- **THEN** it produces the same per-conversation output and content hashes as before,
  computed from a single pass over the log rather than a per-conversation re-scan

#### Scenario: A warm-up cache hit does not load the whole log

- **WHEN** a `warmup` request for one conversation is served from the cache (a hit)
- **THEN** it reads and hashes only that conversation's messages, and returns the
  same cached context as before

### Requirement: Queue broker selection

The CLI SHALL select the durable broker backing `submit` and `work` from a
`--broker sqlite|postgres|redis` option, defaulting to `sqlite`, resolved by the
same flag → config → default precedence as other settings. With `sqlite`, the
broker SHALL open the database at the resolved `--queue-db` path (the existing
behavior). With `postgres` or `redis`, the broker SHALL connect to a URL resolved
in the documented precedence — the `--queue-url` flag, then `$WORKLANE_URL`, then
the backend's conventional variable (`$DATABASE_URL` for postgres, `$REDIS_URL` for
redis) — and the CLI SHALL announce on stderr which source supplied the URL without
printing the URL itself (it may carry a credential). An unknown `--broker` value, or
a selected networked backend with no resolvable URL, SHALL be a reported error and
the command SHALL NOT run.

#### Scenario: Default broker is sqlite at the queue-db path

- **WHEN** `submit` or `work` runs without `--broker`
- **THEN** the durable SQLite broker at the resolved `--queue-db` path backs the
  command, unchanged from prior behavior

#### Scenario: A networked broker is selected by URL

- **WHEN** `--broker postgres` (or `redis`) is passed with a resolvable URL
- **THEN** the command connects to that backend's broker and announces the URL's
  source on stderr without printing the URL

#### Scenario: A missing URL for a networked broker is an error

- **WHEN** `--broker postgres` (or `redis`) is selected and no `--queue-url`,
  `$WORKLANE_URL`, or backend variable is set
- **THEN** the CLI reports an error and does not run the command

#### Scenario: An unknown broker is rejected

- **WHEN** a value other than `sqlite`, `postgres`, or `redis` is passed to
  `--broker`
- **THEN** the CLI reports an error and does not run the command

### Requirement: Submit a strategy run from the CLI

The CLI SHALL provide a command that enqueues a strategy-run job, naming the
resolved strategy, onto the strategy-run lane of the **resolved durable Worklane
broker** (see "Queue broker selection"), and reports the job id. With the default
`sqlite` broker the queue SHALL live in a database resolved by the existing flag →
config → default precedence, separate from the message-log database; with a
networked broker it SHALL use the resolved connection URL. The enqueued job and lane
SHALL be identical across backends — only the broker's storage differs.

#### Scenario: Submitting enqueues a strategy-run job

- **WHEN** the submit command runs with a resolved strategy id
- **THEN** a strategy-run job naming that strategy is enqueued on the resolved
  broker's strategy-run lane, and its id is reported

#### Scenario: The default broker enqueues to the SQLite queue

- **WHEN** the submit command runs without `--broker`
- **THEN** the job is enqueued onto the durable SQLite broker at the resolved
  `--queue-db` path

### Requirement: Work the strategy-run lane from the CLI

The CLI SHALL provide a command that consumes the strategy-run lane of the
**resolved durable Worklane broker** (see "Queue broker selection") to idle: it
registers the strategy-run handler — wired with the message log, the resolved
derivation ports, and the derived-output cache — and runs queued jobs to
completion, populating the cache. Consuming SHALL drain the currently-queued jobs
and return (not run indefinitely). The handler wiring SHALL be identical across
broker backends. The command SHALL mount a job-execution observer (see "Observable
job execution on the durable worker") and surface the per-job execution records it
collected alongside the idle summary. Because a strategy run may make many sequential
LLM calls, the worker SHALL be built with **lease keepalive enabled** (so a
long-but-healthy run is not redelivered for losing its lease) and with a
**configurable handler timeout** (a built-in default, overridable per
"Worklane-substrate configuration namespace") that bounds a hung run.

#### Scenario: Working drains queued runs into the cache

- **WHEN** a strategy run has been submitted and the work command runs against the
  same resolved broker
- **THEN** the run is consumed, its output is written to the derived-output cache,
  and the command returns once the lane is idle

#### Scenario: A submitted run becomes readable end to end

- **WHEN** a run is submitted, then worked, then a per-conversation read is
  requested for an affected conversation
- **THEN** the read serves that run's output from the cache

#### Scenario: Working reports what it ran

- **WHEN** the work command drains one or more jobs to idle
- **THEN** its output includes, for each finished job, the job kind, its outcome,
  and its duration

#### Scenario: A long run keeps its lease and a hung run is bounded

- **WHEN** the work command runs a strategy whose handler is long-running
- **THEN** the worker keeps the job's lease alive while it makes progress, and a run
  exceeding the configured handler timeout is bounded rather than holding the job
  indefinitely

### Requirement: Replay verification from the CLI

The CLI SHALL provide a `verify` command that proves determinism on demand: it
recomputes each conversation's derivation and compares it to the durable
derived-output cache, reporting per conversation whether the stored output is
**verified** (equal to a fresh recompute), **diverged** (differs — a cache written
under a version that should have changed, or a non-deterministic step), or
**not-cached** (nothing stored at the current version). The recompute SHALL only read
the durable cache (writing to a scratch cache), and the command SHALL exit non-zero
when any conversation diverged, so it can gate a pipeline. It SHALL honor `--format
text|json` and MAY be limited to one conversation.

#### Scenario: A faithful cache verifies

- **WHEN** the durable cache was populated by the current code and `verify` runs
- **THEN** every cached conversation reports verified and the command exits zero

#### Scenario: A diverged cache is detected and fails

- **WHEN** a stored derivation no longer equals what recomputing now produces
- **THEN** `verify` reports that conversation as diverged and exits non-zero

#### Scenario: Nothing cached is reported, not failed

- **WHEN** `verify` runs against a conversation with no stored derivation at the
  current version
- **THEN** it reports not-cached and does not, by itself, fail the command

### Requirement: Bounded dead-letter retention

The CLI SHALL construct the strategy-run broker (for `submit` and `work`, across all
backends) with a dead-letter `RetentionPolicy`, so the dead-letter store is bounded
rather than growing without limit. A built-in default `max_count` SHALL apply when
no configuration is given, so dead-letters are bounded out of the box. The CLI
configuration SHALL expose `dead_letter_max_count` and `dead_letter_max_age_secs`
under a `retention` section to override or extend the bound; completed jobs (deleted
on acknowledgement) and the message log SHALL be unaffected.

#### Scenario: Dead-letters are bounded by default

- **WHEN** the broker is constructed without retention configuration
- **THEN** it carries a non-empty `RetentionPolicy` with the built-in default
  `max_count`, so the dead-letter store cannot grow without limit

#### Scenario: Configuration overrides the dead-letter bound

- **WHEN** the `retention` config sets `dead_letter_max_count` and/or
  `dead_letter_max_age_secs`
- **THEN** the broker's `RetentionPolicy` reflects those values

#### Scenario: Completed jobs and the log are unaffected

- **WHEN** dead-letter retention is applied
- **THEN** acknowledged (completed) jobs and the durable message log are not pruned
  by it

### Requirement: Broker isolation on a shared backend

The CLI SHALL let two or more Mirrorlane instances share one networked queue backend
without colliding, by making the broker's storage namespace configurable. For
Postgres the CLI SHALL connect under a configurable **schema** (worklane's default
when unset); for Redis under a configurable **key namespace** (worklane's default
when unset). For SQLite, isolation is the `--queue-db` file path and no schema is
used. Distinct schema/namespace values SHALL give instances independent queues
(jobs, dead-letters, unique keys) on the same backend.

#### Scenario: Two Postgres instances under distinct schemas do not collide

- **WHEN** two instances connect to the same Postgres URL with different configured
  schemas
- **THEN** each has its own jobs/dead-letter tables and neither sees the other's jobs

#### Scenario: Two Redis instances under distinct namespaces do not collide

- **WHEN** two instances connect to the same Redis URL with different configured
  namespaces
- **THEN** their keys are prefixed distinctly and neither sees the other's jobs

#### Scenario: Unset isolation uses the worklane defaults

- **WHEN** no schema/namespace is configured
- **THEN** the broker uses worklane's default schema (`public`) / namespace
  (`worklane`), unchanged from before

### Requirement: Worklane-substrate configuration namespace

The CLI SHALL resolve worklane-substrate settings from a `MIRRORLANE_WORKLANE_*`
environment namespace, falling back to an optional `worklane` section in the
configuration file, then worklane's built-in defaults. The settings SHALL be:
`MIRRORLANE_WORKLANE_QUEUE_SCHEMA` (Postgres), `MIRRORLANE_WORKLANE_QUEUE_NAMESPACE`
(Redis), `MIRRORLANE_WORKLANE_POOL_SIZE` (Postgres), `MIRRORLANE_WORKLANE_LEASE_SECS`
(all backends), `MIRRORLANE_WORKLANE_MAX_DELIVERIES` (all backends), and
`MIRRORLANE_WORKLANE_HANDLER_TIMEOUT_SECS` (the `work` worker's per-run handler
timeout). These SHALL be applied to the broker backing `submit` and `work` (and, for
the handler timeout, to the `work` worker). When `max_deliveries` is set by neither
the environment nor the config, the CLI SHALL apply a built-in backstop default
(rather than worklane's unbounded default) so redelivery is bounded out of the box.
The third-party-convention secrets `GITHUB_TOKEN` and `OPENAI_API_KEY`, and the
shared `WORKLANE_URL`, SHALL keep their established names and SHALL NOT be moved into
this namespace.

#### Scenario: An env var overrides the config and default

- **WHEN** `MIRRORLANE_WORKLANE_LEASE_SECS` is set and a `worklane` config section
  also sets a lease
- **THEN** the environment value is used

#### Scenario: The config section applies when no env var is set

- **WHEN** no `MIRRORLANE_WORKLANE_*` var is set but the `worklane` config section
  sets a value
- **THEN** the configured value is used

#### Scenario: Defaults apply when neither is set

- **WHEN** neither the environment nor the config sets a substrate value
- **THEN** the built-in default applies: worklane's own default for most settings, a
  bounded backstop for `max_deliveries`, and the built-in handler-timeout default for
  the `work` worker

#### Scenario: The handler timeout is configurable in the namespace

- **WHEN** `MIRRORLANE_WORKLANE_HANDLER_TIMEOUT_SECS` is set
- **THEN** the `work` worker uses it as its per-run handler timeout, over any config
  value and the built-in default

#### Scenario: Conventional secrets are not namespaced

- **WHEN** the substrate namespace is resolved
- **THEN** `GITHUB_TOKEN`, `OPENAI_API_KEY`, and `WORKLANE_URL` are still read under
  their established names, not under `MIRRORLANE_WORKLANE_*`

