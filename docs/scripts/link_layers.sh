#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# link_layers.sh
# ---------------------------------------------------------------------------
# Docusaurus serves static assets from docs/static. The Riverscapes tools keep
# their `layer_definitions.json` files under packages/<tool>/<tool>/, so this
# script builds a mirror set of symlinks in docs/static/layers that the site
# can reference (for example, rendering data tables in MDX). It walks every
# package, finds files matching the requested pattern, and creates
# <tool>_<filename>.json symlinks that point back to the original files using
# relative paths so local dev servers and published builds both work.
#
# Usage: ./link_layers.sh [filename_pattern]
#        Defaults to `layer_definitions.json` when no pattern is supplied.
#
# Run this script whenever new layer definition files are added or when
# rebuilding the docs site to guarantee the static/layers directory stays in
# sync with the authoritative files inside packages/.
# ---------------------------------------------------------------------------

# Symlink every file matching the provided pattern (defaults to layer_definitions.json)
# from packages/*/** into docs/static/layers with the package name prefixed.
PATTERN="${1:-layer_definitions.json}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DOCS_DIR}/.." && pwd)"
LAYERS_DIR="${DOCS_DIR}/static/layers"
PACKAGES_DIR="${REPO_ROOT}/packages"

if [[ ! -d "${PACKAGES_DIR}" ]]; then
  echo "Packages directory not found: ${PACKAGES_DIR}" >&2
  exit 1
fi

mkdir -p "${LAYERS_DIR}"

match_count=0
while IFS= read -r -d '' file; do
  match_count=$((match_count + 1))

  relative_path="${file#"${REPO_ROOT}/"}"
  if [[ "${relative_path}" == "${file}" ]]; then
    echo "Skipping ${file}: not under repo root" >&2
    continue
  fi

  relative_packages_path="${relative_path#packages/}"
  if [[ "${relative_packages_path}" == "${relative_path}" ]]; then
    echo "Skipping ${file}: not under packages/" >&2
    continue
  fi

  package_name="${relative_packages_path%%/*}"
  base_name="$(basename "${file}")"
  link_name="${package_name}_${base_name}"
  link_path="${LAYERS_DIR}/${link_name}"

  target_rel=$(LAYERS_DIR="${LAYERS_DIR}" TARGET="${file}" python3 - <<'PY'
import os
layers_dir = os.environ["LAYERS_DIR"]
target = os.environ["TARGET"]
print(os.path.relpath(target, layers_dir))
PY
)

  if [[ -e "${link_path}" || -L "${link_path}" ]]; then
    rm -f "${link_path}"
  fi

  ln -s "${target_rel}" "${link_path}"
  echo "Linked ${link_name} -> ${target_rel}"
done < <(find "${PACKAGES_DIR}" -type f -name "${PATTERN}" -print0)

if [[ ${match_count} -eq 0 ]]; then
  echo "No files matching pattern '${PATTERN}' were found under ${PACKAGES_DIR}" >&2
  exit 0
fi
