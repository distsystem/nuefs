#!/usr/bin/env bash
# End-to-end test: pixi install on FUSE-mounted mundi workspace
# Mirrors CI test-mundi-install job for local verification
set -euo pipefail

MUNDI_DIR="/tmp/mundi"
META_FORGE_DIR="/tmp/meta-forge"

# --- Prerequisites ---
for cmd in nue pixi gh; do
    command -v "$cmd" >/dev/null || { echo "error: $cmd not found"; exit 1; }
done
echo ">>> pixi $(pixi --version), nue available"

# --- Clone repos if needed ---
if [[ ! -d "$MUNDI_DIR" ]]; then
    echo ">>> Cloning mundi..."
    gh repo clone distsystem/mundi "$MUNDI_DIR" -- --depth 1
else
    echo ">>> Using existing $MUNDI_DIR"
fi

if [[ ! -d "$META_FORGE_DIR" ]]; then
    echo ">>> Cloning meta-forge..."
    gh repo clone distsystem/meta-forge "$META_FORGE_DIR" -- --depth 1
else
    echo ">>> Using existing $META_FORGE_DIR"
fi

# --- Build and install nuefs ---
echo ">>> Building nuefs..."
pixi run develop

# --- Cleanup stale mount/daemon ---
fusermount3 -uz "$MUNDI_DIR" 2>/dev/null || true
pkill -9 nuefsd 2>/dev/null || true
sleep 0.5

# --- Mount ---
echo ">>> Mounting FUSE on $MUNDI_DIR..."
(cd /tmp && nue mount -c "$MUNDI_DIR/nue.yaml")

cleanup() {
    echo ">>> Cleanup..."
    fusermount3 -uz "$MUNDI_DIR" 2>/dev/null || true
    pkill -9 nuefsd 2>/dev/null || true
}
trap cleanup EXIT

# --- pixi install ---
echo ">>> Running pixi install..."
rm -f "$MUNDI_DIR/pixi.lock"
(cd /tmp && pixi install --manifest-path "$MUNDI_DIR/pixi.toml")

# --- Verify ---
echo ">>> Verifying meta --help..."
(cd /tmp && pixi run --manifest-path "$MUNDI_DIR/pixi.toml" meta --help)

echo ">>> All tests passed."
