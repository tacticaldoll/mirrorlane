#!/usr/bin/env bash
# Mirrorlane end-to-end demo.
#
# Ingests a short conversation into a throwaway durable log, then shows the
# structured context the engine derives from it — first as human-readable
# warm-up summaries, then as the full JSON `SessionContext` an agent or workflow
# would consume (warm-up, scope, developers, routing hints, routing decisions).
#
# Offline and deterministic (the default `mock` projector). Run from the repo
# root:
#
#     ./demo.sh
set -euo pipefail

DB="$(mktemp -u)"
trap 'rm -f "$DB"' EXIT

cli() { cargo run -q -p mirrorlane-cli -- --db "$DB" "$@"; }
ingest() { printf '%s' "$1" | cli ingest; }

echo "== Ingest a short conversation (c-1) =="
ingest '{"id":"m-1","source":"discord","author":{"id":"u-alice","display_name":"Alice"},"conversation":{"id":"c-1","thread":null},"body":"We will use sqlite for the auth sdk oauth refresh-token store."}'
ingest '{"id":"m-2","source":"discord","author":{"id":"u-bob","display_name":"Bob"},"conversation":{"id":"c-1","thread":null},"body":"Should we expose a refresh endpoint in the sdk?"}'
ingest '{"id":"m-3","source":"discord","author":{"id":"u-alice","display_name":"Alice"},"conversation":{"id":"c-1","thread":null},"body":"Add ci for the auth crate."}'

echo
echo "== Warm-up summary (text) =="
cli replay

echo
echo "== Full session context (json) =="
cli --format json warmup --conversation c-1
echo
