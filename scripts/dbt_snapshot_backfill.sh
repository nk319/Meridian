#!/usr/bin/env bash
#
# Replay customer history into the SCD2 snapshot. Run once, on a fresh warehouse.
#
# Why this exists at all: `silver.customers` holds current state, and dbt's
# snapshot records what it sees when it runs. Snapshot a static table once and
# every customer gets exactly one version, `is_current` is true everywhere,
# `dbt_valid_to` is null everywhere — and the three SCD2 tests CONTRACTS.md §8
# calls for all pass against a result set with no history in it. The tests are
# not wrong; there is genuinely nothing to find.
#
# `silver.customer_change_log` is the source system's own record of what changed
# and when, so the history is recoverable. This walks its distinct timestamps
# oldest-first and, at each one, snapshots the customer table as it stood on
# that date (dbt/models/intermediate/int_customers_point_in_time.sql does the
# reconstruction; dbt/macros/snapshot_get_time.sql makes dbt agree about what
# "now" means, so `dbt_valid_from` carries the date the tier actually changed
# rather than the date somebody ran this).
#
# NOT IDEMPOTENT, and guarded rather than made so. Replaying an old checkpoint
# after the snapshot has reached the present would see the customer's *old*
# tier, treat it as a fresh change, and append a version that records time
# running backwards. A snapshot cannot see its own history, so the check cannot
# live inside it: if gold_int.customers_snapshot already exists, this exits 0
# and does nothing.
#
# Deliberately not an Airflow task. The daily `dbt snapshot` in meridian_batch
# is the ordinary path and is idempotent; this is a one-time bootstrap, which is
# a different kind of thing and belongs somewhere else.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DBT_DIR="$REPO_ROOT/dbt"
DBT="${DBT_BIN:-$REPO_ROOT/.dbt-venv/bin/dbt}"
PYTHON="${PYTHON_BIN:-python3}"

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$DBT_DIR}"
cd "$DBT_DIR"

# `dbt show` truncates wide columns in its table output, which silently turns a
# timestamp into a prefix. --output json is the form that round-trips.
query_json() {
    "$DBT" show --quiet --output json --limit 1000 --inline "$1"
}

pluck() {
    "$PYTHON" -c '
import json, sys
key = sys.argv[1]
payload = json.load(sys.stdin)
rows = next(iter(payload.values())) if isinstance(payload, dict) and "show" not in payload else payload["show"]
for row in rows:
    print(row[key])
' "$1"
}

# The guard. Asked through dbt so there is one connection story, not two.
existing=$(query_json "select count(*) as n from information_schema.tables
                       where table_schema = 'gold_int'
                         and table_name = 'customers_snapshot'" | pluck n)

if [ "${existing:-0}" != "0" ]; then
    echo "gold_int.customers_snapshot already exists — backfill skipped."
    echo "To rebuild history: drop that table, then re-run this script."
    exit 0
fi

# The snapshot reads staging views. On a freshly reset warehouse they do not
# exist yet, and the failure ("relation gold_stg.stg_customer_change_log does
# not exist") points at the symptom rather than the ordering. Building them here
# makes the script safe to run as the first dbt command after a reset.
echo "Building the models the snapshot reads..."
"$DBT" run --quiet --select staging

echo "Reconstructing customer history from silver.customer_change_log..."

# Oldest first. The order is the whole point: each reconstruction has to be
# snapshotted before the next change is applied, or the intermediate versions
# never exist.
checkpoints=$(query_json "select distinct changed_at
                          from {{ source('silver', 'customer_change_log') }}
                          order by changed_at" | pluck changed_at)

if [ -z "$checkpoints" ]; then
    echo "No change log entries — snapshotting current state only."
else
    while IFS= read -r ts; do
        [ -z "$ts" ] && continue
        echo "  snapshot as of $ts"
        "$DBT" snapshot --quiet --vars "{snapshot_as_of: '$ts'}"
    done <<< "$checkpoints"
fi

# The present. Unpinned, so snapshot_get_time() falls back to now() and the
# snapshot behaves exactly as it will on every scheduled run afterwards. This is
# also the run that closes out the hard-deleted customer, who is absent from the
# current reconstruction and so trips hard_deletes='new_record'.
echo "  snapshot as of now"
"$DBT" snapshot --quiet

echo "Backfill complete."
