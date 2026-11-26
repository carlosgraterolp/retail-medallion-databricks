#!/usr/bin/env bash
# Fails if staged files contain credentials or raw dataset files.
# Runs as a git pre-commit hook (see README: git config core.hooksPath .githooks)
# and can be run by hand: scripts/check_repo_safety.sh [--all]
set -euo pipefail

if [[ "${1:-}" == "--all" ]]; then
  files=$(git ls-files)
else
  files=$(git diff --cached --name-only --diff-filter=ACM)
fi
[[ -z "$files" ]] && exit 0

fail=0

# 1. Raw data and local config must never be tracked.
blocked=$(echo "$files" | grep -Ei '(^|/)data/|\.(csv|xlsx|xls|zip|parquet)$|\.databrickscfg$|(^|/)\.databricks/|(^|/)\.env' || true)
if [[ -n "$blocked" ]]; then
  echo "BLOCKED: raw data or local config files:"; echo "$blocked"; fail=1
fi

# 2. Large files (> 1 MB) are almost certainly data.
while IFS= read -r f; do
  [[ -f "$f" ]] || continue
  size=$(wc -c < "$f")
  if (( size > 1048576 )); then echo "BLOCKED: $f is larger than 1 MB"; fail=1; fi
done <<< "$files"

# 3. Credential patterns (Databricks PAT, GitHub tokens, OAuth secrets, private keys).
pattern='dapi[0-9a-f]{32}|dose[0-9a-f]{32}|gh[pousr]_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{40,}|-----BEGIN [A-Z ]*PRIVATE KEY-----|(client_secret|token|password)[[:space:]]*[:=][[:space:]]*["'"'"']?[A-Za-z0-9/+_-]{16,}'
self="scripts/check_repo_safety.sh"
while IFS= read -r f; do
  [[ -f "$f" && "$f" != "$self" ]] || continue
  if grep -EnI "$pattern" "$f" >/dev/null 2>&1; then
    echo "BLOCKED: possible credential in $f (line numbers: $(grep -EnI "$pattern" "$f" | cut -d: -f1 | tr '\n' ' '))"
    fail=1
  fi
done <<< "$files"

if (( fail )); then echo "Repo safety check FAILED"; exit 1; fi
echo "Repo safety check passed ($(echo "$files" | wc -l | tr -d ' ') files)"
