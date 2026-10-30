#!/usr/bin/env bash
set -euo pipefail
repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd -- "$repo_dir"
python3 -c 'import sys; assert sys.version_info >= (3,11), "Python 3.11+ required; use Ubuntu in Termux"'
runtime_record="$repo_dir/.runtime/account-location"
if [[ -z "${CASCADE_RUNTIME_DIR:-}" && -f "$runtime_record" ]]; then
 IFS= read -r CASCADE_RUNTIME_DIR < "$runtime_record"
fi
export CASCADE_RUNTIME_DIR="${CASCADE_RUNTIME_DIR:-$repo_dir/.runtime}"
CASCADE_RUNTIME_DIR="$(python3 -c 'import os; from pathlib import Path; print(Path(os.environ["CASCADE_RUNTIME_DIR"]).expanduser().resolve())')"
export CASCADE_RUNTIME_DIR
export CASCADE_FINAL2_DIR="${CASCADE_FINAL2_DIR:-$repo_dir/.runtime/repos/Final-2}"
mkdir -p -- "$CASCADE_RUNTIME_DIR/data"
# Refuse setup while another dashboard uses the selected account directory.
python3 - <<'PY'
import fcntl,os
from pathlib import Path
for name in ('server','final','final2'):
 with (Path(os.environ['CASCADE_RUNTIME_DIR'])/'data'/(name+'.lock')).open('a') as f:
  try: fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError: raise SystemExit('Dashboard or bot already running. Stop it before starting another copy.')
PY
if [[ ! -d "$CASCADE_FINAL2_DIR/.git" ]]; then
 if [[ -e "$CASCADE_FINAL2_DIR" ]]; then
  echo 'Final-2 source path already exists but is not a Git checkout; choose another CASCADE_FINAL2_DIR.' >&2
  exit 1
 fi
 mkdir -p -- "$(dirname -- "$CASCADE_FINAL2_DIR")"
 git clone --depth 1 --branch main https://github.com/khaledhasneen1993/Final-2.git "$CASCADE_FINAL2_DIR"
fi
if [[ ! -f "$CASCADE_FINAL2_DIR/cascade_final2_paper.py" ]]; then
 echo 'Final-2 entry point is missing.' >&2; exit 1
fi
if [[ ! -x "$repo_dir/.venv-dashboard/bin/python" ]]; then
 python3 -m venv "$repo_dir/.venv-dashboard"
fi
expected_hash="$(sha256sum dashboard/requirements.txt | cut -d ' ' -f 1)"
installed_hash="$(cat .venv-dashboard/requirements.sha256 2>/dev/null || true)"
if [[ "$installed_hash" != "$expected_hash" ]]; then
 .venv-dashboard/bin/python -m pip install -r dashboard/requirements.txt
 printf '%s\n' "$expected_hash" > .venv-dashboard/requirements.sha256
fi
mkdir -p -- "$repo_dir/.runtime"
printf '%s\n' "$CASCADE_RUNTIME_DIR" > "$runtime_record"
printf '\nDashboard: http://127.0.0.1:8787\nAccounts: %s/data\nCtrl+C stops both bots and saves their state.\n\n' "$CASCADE_RUNTIME_DIR"
exec "$repo_dir/.venv-dashboard/bin/python" -u dashboard/server.py "$@"
