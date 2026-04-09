#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INPUT_FILE="$SCRIPT_DIR/batch-input.tsv"
STATE_FILE="$SCRIPT_DIR/batch-state.tsv"
LOGS_DIR="$SCRIPT_DIR/logs"

DRY_RUN=false
START_FROM=0
RETRY_FAILED=false
MODEL=""
AGENT=""

usage() {
  cat <<'USAGE'
market-validation batch runner

Usage: batch-runner.sh [--dry-run] [--start-from N] [--retry-failed]
                       [--model provider/model] [--agent name]
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --start-from) START_FROM="$2"; shift 2 ;;
    --retry-failed) RETRY_FAILED=true; shift ;;
    --model) MODEL="$2"; shift 2 ;;
    --agent) AGENT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

mkdir -p "$LOGS_DIR"

if [[ ! -f "$INPUT_FILE" ]]; then
  printf 'id\tmarket\tgeography\tprofile\ttemplate\tnotes\n' > "$INPUT_FILE"
  echo "Created $INPUT_FILE. Add rows and run again."
  exit 0
fi

if [[ ! -f "$STATE_FILE" ]]; then
  printf 'id\tmarket\tstatus\treport_num\terror\n' > "$STATE_FILE"
fi

next_report_num() {
  local max_num=0
  for file in "$ROOT_DIR"/reports/*.md; do
    [[ -f "$file" ]] || continue
    local name
    name="$(basename "$file")"
    local prefix
    prefix="${name%%-*}"
    [[ "$prefix" =~ ^[0-9]+$ ]] || continue
    (( 10#$prefix > max_num )) && max_num=$((10#$prefix))
  done
  printf '%03d' $((max_num + 1))
}

get_state_status() {
  local id="$1"
  awk -F '\t' -v id="$id" '$1==id {print $3}' "$STATE_FILE" | head -n1
}

upsert_state() {
  local id="$1" market="$2" status="$3" report_num="$4" error="$5"
  local tmp="$STATE_FILE.tmp"
  awk -F '\t' -v OFS='\t' -v id="$id" -v market="$market" -v status="$status" -v report_num="$report_num" -v error="$error" '
    BEGIN { found=0 }
    NR==1 { print $0; next }
    $1==id { print id, market, status, report_num, error; found=1; next }
    { print $0 }
    END { if (!found) print id, market, status, report_num, error }
  ' "$STATE_FILE" > "$tmp"
  mv "$tmp" "$STATE_FILE"
}

while IFS=$'\t' read -r id market geography profile template notes; do
  [[ "$id" == "id" ]] && continue
  [[ -z "$id" || -z "$market" ]] && continue
  (( id < START_FROM )) && continue

  current_status="$(get_state_status "$id")"
  if [[ "$RETRY_FAILED" == "true" ]]; then
    [[ "$current_status" == "failed" ]] || continue
  else
    [[ "$current_status" == "completed" ]] && continue
  fi

  report_num="$(next_report_num)"
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY RUN: #$id $market -> report $report_num"
    continue
  fi

  log_file="$LOGS_DIR/${report_num}-${id}.log"
  set +e
  worker_cmd=(
    bash "$ROOT_DIR/batch/worker-and-store.sh"
    --id "$id"
    --market "$market"
    --geography "${geography:-global}"
    --profile "${profile:-general}"
    --template "${template:-}"
    --report-num "$report_num"
    --root "$ROOT_DIR"
  )
  if [[ -n "$MODEL" ]]; then
    worker_cmd+=(--model "$MODEL")
  fi
  if [[ -n "$AGENT" ]]; then
    worker_cmd+=(--agent "$AGENT")
  fi

  "${worker_cmd[@]}" > "$log_file" 2>&1
  rc=$?
  set -e

  if [[ $rc -eq 0 ]]; then
    upsert_state "$id" "$market" "completed" "$report_num" "-"
    echo "Completed #$id ($market)"
  else
    err="$(tail -n 5 "$log_file" | tr '\n' ' ' | cut -c1-200)"
    upsert_state "$id" "$market" "failed" "$report_num" "$err"
    echo "Failed #$id ($market)"
  fi

done < "$INPUT_FILE"

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run complete. Skipping merge and verify."
  exit 0
fi

echo "Merging staged tracker additions..."
python "$ROOT_DIR/merge-tracker.py"

echo "Verifying pipeline..."
python "$ROOT_DIR/verify-pipeline.py" || true

echo "Done."
