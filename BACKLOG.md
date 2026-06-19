# Backlog

Forward-looking work, deliberately deferred. This is the durable home for it —
implementation decision history is not kept in-repo (changes squash into the
baseline). Promote an item to an OpenSpec change when you pick it up.

## Deferred

- **Full streaming replay (bound the full-replay memory peak).** `ProjectionStrategy::run`
  still materializes all projections in memory to build the **global** skill index
  that developers/hints depend on, so a large log's resident set is O(messages).
  Bounding it to ~one conversation needs a two-pass, streaming rewrite of the
  `Strategy`/`ReplayStrategy` abstraction (stream the skill index, then
  derive-emit-drop per conversation). Large blast radius — its own change.
  *(The per-conversation re-scan quadratic and the warm-up-hit whole-log load are
  already fixed; this is only the full-replay peak.)*

- **Drop the phantom source variants.** `Source::Discord` / `Source::Slack` are enum
  variants with no ingestion path — only GitHub and manual are real. If multi-source
  is decorative, remove them and commit to GitHub-first; if real, build their
  adapters. Decide by roadmap, not aesthetics.

- **Age-based routing-trace retention.** Trace retention is count-based only
  (`trace_max_count`); age-based needs a `created_at` column (a schema migration) and
  a write clock. Add if operators want time-windowed traces.

- **Operator lifecycle commands for the trace/cache stores.** Dead-letter operability
  is now CLI-reachable (`dlq read|count|requeue|purge` over both lanes). What remains
  is `prune` / `vacuum` / `stats` over Mirrorlane's *own* stores (routing-trace,
  derived-output, projection cache): the reclamation mechanisms exist internally, but
  there is no operator-facing command to invoke or observe them.

- **Message-log retention/archival.** The log is the source of truth and is never
  auto-pruned. A long-lived deployment may want operator-driven archival/compaction
  or a keep-recent window — explicitly, never a silent auto-drop.

- **Structural version-drift prevention for the parser and non-projector steps.**
  The prompt template is now content-fingerprinted, so editing it invalidates the
  cache automatically. The **parser** (`PARSER_VERSION`) and the **downstream
  builders** (scope/warm-up/skill/hint/snapshot, covered only by the global
  `DERIVATION_SCHEMA_VERSION`) still rely on a manual bump. `verify` **detects** drift
  in these, but prevention is not yet structural. Closing it means content-deriving
  those versions too (or a build-time check) — diminishing returns now that `verify`
  is the net, so deferred.

- **Deeper provenance and cache observability.** A derived output now carries its
  `derivation_version`, but not per-`Step` lineage (which step versions ran, cache
  hit vs miss). And there is no `cache stats|inspect` command (analogous to `dlq`) to
  see what is cached, sizes, or stale/superseded rows. Together these complete the
  "traceable / inspectable" half of the glass-box claim beyond what `verify` proves.

- **Consumer integration interface.** Whether a consuming plane (e.g. Triggerlane)
  links Mirrorlane as a library or supervises it as a service is the **consumer's**
  architectural choice, not Mirrorlane's contract. Mirrorlane's contract — the typed
  `strategy_run` job on a selectable broker + the JSON `SessionContext` — already
  exists; revisit only if a concrete integration shape demands more.
