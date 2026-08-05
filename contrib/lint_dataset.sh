#!/usr/bin/env bash
#
# Run every lint check CI applies to changed dataset files, in one command.
#
# Reach for this after editing anything under datasets/ — a crawler, a metadata
# YAML, a static data file — to confirm the change will survive CI before
# opening a PR. It mirrors the two lint steps in
# .github/workflows/lint_crawlers.yml, so a pass here means a pass there.
#
# Usage: contrib/lint_dataset.sh <path> [<path> ...]
#
# Passing a dataset's YAML is enough: its crawler is looked up via the
# entry_point and linted too. Enrichment datasets run an installed module rather
# than dataset-local code, so for those only the YAML is checked. Paths are
# otherwise dispatched by extension — .yml/.yaml to yamllint, .py to ruff and
# mypy — and everything is passed to the pre-commit hooks, which own the
# per-dataset mypy exclusions. Do not invoke ruff or mypy by hand instead of this
# script, or you will chase findings CI does not enforce.

set -uo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: contrib/lint_dataset.sh <path> [<path> ...]" >&2
  exit 2
fi

# The ruff invocations below name a config relative to the repository root, and
# crawler_code is imported as a package module, so run from the root regardless
# of where the caller sits.
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"

status=0
all=()
yamls=()
pythons=()

# Arrays are expanded via ${arr[@]+"${arr[@]}"} throughout: under `set -u`,
# bash 3.2 (still the system bash on macOS) errors on an empty array otherwise.
# Returns non-zero when the path was already collected, so callers can avoid
# queueing it twice.
add_python() {
  local candidate existing
  candidate="$1"
  for existing in ${pythons[@]+"${pythons[@]}"}; do
    [ "$existing" = "$candidate" ] && return 1
  done
  pythons+=("$candidate")
}

for path in "$@"; do
  if [ ! -e "$path" ]; then
    echo "lint_dataset: no such file: $path" >&2
    status=1
    continue
  fi
  all+=("$path")
  case "$path" in
    *.yml | *.yaml) yamls+=("$path") ;;
    *.py) add_python "$path" ;;
  esac
done

# Resolve each YAML to its crawler, if it has one. Datasets whose entry_point
# names an installed module contribute no line, so this stays empty for them.
if [ "${#yamls[@]}" -gt 0 ]; then
  while IFS= read -r resolved; do
    [ -n "$resolved" ] || continue
    if add_python "$resolved"; then
      all+=("$resolved")
    fi
  done < <("$PYTHON" -m contrib.maintenance.crawler_code "${yamls[@]}")
fi

run() {
  echo "+ $*"
  "$@" || status=1
}

if [ "${#yamls[@]}" -gt 0 ] && command -v yamllint >/dev/null; then
  run yamllint "${yamls[@]}"
fi

if [ "${#pythons[@]}" -gt 0 ]; then
  run ruff check --config zavod/pyproject.toml "${pythons[@]}"
  run ruff format --check --diff --config zavod/pyproject.toml "${pythons[@]}"
fi

# Covers check-yaml, yamllint, ruff and the mypy-datasets hook with its
# exclusions applied. Note the ruff hook runs with --fix, so this may edit files.
if [ "${#all[@]}" -gt 0 ]; then
  run prek run --files ${all[@]+"${all[@]}"}
fi

if [ "$status" -eq 0 ]; then
  echo "lint_dataset: OK"
else
  echo "lint_dataset: FAILED" >&2
fi
exit "$status"
