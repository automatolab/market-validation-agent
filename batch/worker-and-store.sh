#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ $# -lt 1 ]]; then
  echo "Usage: worker-and-store.sh <worker args...>"
  echo "Example: worker-and-store.sh --id 1 --market \"brisket\" --report-num 001"
  exit 2
fi

set +e
worker_output="$(python -m market_validation.batch_worker "$@" 2>&1)"
worker_rc=$?
set -e

json_payload=""
if command -v python >/dev/null 2>&1; then
  set +e
  json_payload="$(printf '%s\n' "$worker_output" | python -c 'import json,sys\nlines=[ln.strip() for ln in sys.stdin.read().splitlines() if ln.strip()]\nfor ln in reversed(lines):\n    try:\n        obj=json.loads(ln)\n    except Exception:\n        continue\n    if isinstance(obj, dict):\n        print(json.dumps(obj, ensure_ascii=True))\n        break\n')"
  extractor_rc=$?
  set -e
  if [[ $extractor_rc -ne 0 ]]; then
    json_payload=""
  fi
fi

if [[ -n "$json_payload" ]]; then
  set +e
  store_output="$(printf '%s\n' "$json_payload" | python "$ROOT_DIR/store-output.py" --stage worker_result --root "$ROOT_DIR" 2>&1)"
  store_rc=$?
  set -e
else
  store_output='{"result":"failed","error":"No JSON payload extracted from worker output"}'
  store_rc=1
fi

printf '%s\n' "$worker_output"
printf '%s\n' "$store_output"

if [[ $worker_rc -ne 0 ]]; then
  exit $worker_rc
fi

if [[ $store_rc -ne 0 ]]; then
  exit $store_rc
fi
