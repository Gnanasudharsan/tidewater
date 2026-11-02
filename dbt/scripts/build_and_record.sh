#!/usr/bin/env bash
# Run a dbt command, then record its run results in the warehouse (even on failure).
cd /dbt
# A stale run_results.json from an earlier run must never be recorded as this one.
rm -f target/run_results.json
dbt "$@"
status=$?
[ -f target/run_results.json ] && python /dbt/scripts/record_run_results.py ||
  echo "warning: no run results recorded"
exit $status
