#!/usr/bin/env bash
# One-time-per-machine setup for profiling the dataset-loader plugin.
#
# `sf dataset load` resolves the command from the sf CLI plugin registry, not
# from `lib/` in this checkout. If the plugin is installed from npm (type
# `user`) every profiling run silently measures the published version instead
# of this source tree. This script detects that state and re-links.
#
# Also handles the hazard that `sf plugins link .` can rewrite
# package-lock.json as a side effect (yarn normalization differences); the
# lockfile is restored afterwards so the working tree stays clean for a
# branch-switch sandwich.
#
# Safe to re-run: idempotent. Prints "Nothing to do" when already correct.
#
# Usage: bash profiling/setup-plugin.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

read_status() {
  sf plugins --json 2>/dev/null |
    node -e "
      let buf = '';
      process.stdin.on('data', c => { buf += c; });
      process.stdin.on('end', () => {
        const plugins = JSON.parse(buf);
        const p = plugins.find(x => x.name === 'dataset-loader');
        if (!p) { console.log('MISSING'); return; }
        if (p.type !== 'link') { console.log('INSTALLED:' + p.type); return; }
        console.log('LINKED:' + (p.root || p.path || ''));
      });
    "
}

STATUS=$(read_status)
case "$STATUS" in
  LINKED:*)
    LINKED_ROOT="${STATUS#LINKED:}"
    if [[ "$LINKED_ROOT" == "$ROOT_DIR" ]]; then
      echo "[setup-plugin] Already linked to this checkout: $LINKED_ROOT"
      echo "[setup-plugin] Nothing to do."
      exit 0
    fi
    echo "[setup-plugin] Linked to a different directory: $LINKED_ROOT"
    echo "[setup-plugin] Uninstalling and re-linking to this checkout..."
    sf plugins uninstall dataset-loader 2>&1 | tail -2
    sf plugins link . 2>&1 | tail -2
    ;;
  INSTALLED:*)
    echo "[setup-plugin] Plugin installed from npm (type: ${STATUS#INSTALLED:})."
    echo "[setup-plugin] Uninstalling and linking to this checkout..."
    sf plugins uninstall dataset-loader 2>&1 | tail -2
    sf plugins link . 2>&1 | tail -2
    ;;
  MISSING)
    echo "[setup-plugin] Plugin not installed. Linking to this checkout..."
    sf plugins link . 2>&1 | tail -2
    ;;
esac

# `sf plugins link` sometimes rewrites package-lock.json (yarn normalization);
# restore the checked-in version so the sandwich protocol starts clean.
if ! git diff --quiet package-lock.json 2>/dev/null; then
  echo "[setup-plugin] sf plugins link rewrote package-lock.json; restoring."
  git checkout -- package-lock.json
fi

# Verify the final state.
FINAL=$(read_status)
if [[ "$FINAL" != "LINKED:$ROOT_DIR" ]]; then
  echo "[setup-plugin] ERROR: final verification failed (state: $FINAL)."
  sf plugins | grep dataset-loader || true
  exit 1
fi

echo "[setup-plugin] Done. dataset-loader plugin is linked to $ROOT_DIR."
