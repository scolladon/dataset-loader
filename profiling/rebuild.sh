#!/usr/bin/env bash
# Prepare the dataset-loader plugin for profiling the current checkout:
#   1. sync node_modules to package-lock.json via `npm ci` — never mutates
#      the lockfile, fails loudly if lock and package.json drift
#   2. rebuild lib/ from src/ (incremental tsc)
#   3. verify the sf CLI plugin is linked to this directory, so `sf dataset
#      load` resolves to this checkout rather than the published npm version
# Run from the project root on the branch you want to profile.
# Safe to re-run: idempotent.
#
# Usage: bash profiling/rebuild.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

# Fail loudly if the working tree has tracked modifications that might be
# lost on a sandwich branch-switch. Untracked files are fine; those are the
# runtime artefacts (state, config, csvs) that profiling.gitignore covers.
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "[rebuild] ERROR: working tree has uncommitted changes to tracked files."
  echo "[rebuild] Commit or stash them before profiling across branches."
  git status --short
  exit 1
fi

echo "[rebuild] Syncing node_modules with package-lock.json (npm ci)..."
# --prefer-offline reuses the npm cache; --no-audit / --no-fund suppress the
# interactive warnings that corrupt timed output if this script is ever
# piped into a tee log.
npm ci --prefer-offline --no-audit --no-fund

echo "[rebuild] Compiling TypeScript (npm run compile)..."
npm run compile

echo "[rebuild] Verifying sf plugin link..."
# `sf plugins --json` has a stable shape across sf CLI versions.
LINK_INFO=$(
  sf plugins --json 2>/dev/null |
    node -e "
      let buf = '';
      process.stdin.on('data', c => { buf += c; });
      process.stdin.on('end', () => {
        const plugins = JSON.parse(buf);
        const p = plugins.find(x => x.name === 'dataset-loader');
        if (!p) { console.log('NOT_INSTALLED'); return; }
        if (p.type !== 'link') { console.log('WRONG_TYPE:' + p.type); return; }
        console.log('LINK:' + (p.root || p.path || ''));
      });
    "
)

case "$LINK_INFO" in
  NOT_INSTALLED)
    echo "[rebuild] ERROR: dataset-loader is not installed as an sf plugin."
    echo "[rebuild] Run once: sf plugins link ."
    exit 1
    ;;
  WRONG_TYPE:*)
    echo "[rebuild] ERROR: dataset-loader is installed but not linked (type: ${LINK_INFO#WRONG_TYPE:})."
    echo "[rebuild] This means \`sf dataset load\` would run the published npm version, not this checkout."
    echo "[rebuild] Fix: sf plugins uninstall dataset-loader && sf plugins link ."
    exit 1
    ;;
  LINK:*)
    LINKED_ROOT="${LINK_INFO#LINK:}"
    if [[ "$LINKED_ROOT" != "$ROOT_DIR" ]]; then
      echo "[rebuild] ERROR: plugin linked to $LINKED_ROOT (expected $ROOT_DIR)."
      echo "[rebuild] Fix: (cd '$ROOT_DIR' && sf plugins link .)"
      exit 1
    fi
    echo "[rebuild] Plugin linked to this checkout: $LINKED_ROOT"
    ;;
esac

BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT=$(git rev-parse --short HEAD)
echo "[rebuild] Ready on $BRANCH @ $COMMIT."
echo "[rebuild] Next: bash profiling/prepare.sh && time sf dataset load"
