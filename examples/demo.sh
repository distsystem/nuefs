#!/usr/bin/env bash
# End-to-end smoke test for nue.
#
# Creates a synthetic upstream repo (with 3 commits touching different files),
# creates a host repo with a nue.yaml pointing at the upstream, runs
# `nue sync`, and asserts that vendored files land at the expected paths
# with the expected content and that the host commit history carries
# `Nue-Source-*:` trailers.
#
# Requires:
#   * git
#   * josh-filter  (from josh-project: `cargo install --git
#                   https://github.com/josh-project/josh --bin josh-filter
#                   josh-cli`)
#   * nue          (this crate, `cargo build` first)

set -euo pipefail

NUE="${NUE:-$(pwd)/target/debug/nue}"

if [[ ! -x "$NUE" ]]; then
  echo "nue binary not found at $NUE" >&2
  echo "Run: cargo build" >&2
  exit 1
fi
if ! command -v josh-filter >/dev/null && [[ -z "${NUE_JOSH_FILTER:-}" ]]; then
  echo "josh-filter not on PATH and NUE_JOSH_FILTER not set" >&2
  echo "Install: cargo install --git https://github.com/josh-project/josh \\" >&2
  echo "                --bin josh-filter josh-cli" >&2
  exit 1
fi

WORK="$(mktemp -d -t nue-demo.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT
echo "==> work dir: $WORK"

# 1. Build a synthetic upstream repo with 3 commits.
UPSTREAM="$WORK/upstream.git"
WT="$WORK/upstream-wt"
git init -q -b main "$WT"
(
  cd "$WT"
  git config user.email "up@example.com"
  git config user.name "Upstream Dev"

  mkdir -p utils schemas
  echo "def log(): return 'v1'" > utils/logger.py
  echo "version=1" > utils/config.py
  echo '{"v":1}' > schemas/user.json
  git add . && git commit -q -m "feat: initial utils + schemas"

  echo "def log(): return 'v2'" > utils/logger.py
  git add . && git commit -q -m "fix: logger return"

  echo '{"v":2,"name":"string"}' > schemas/user.json
  git add . && git commit -q -m "schema: add name field"
)
git clone -q --bare "$WT" "$UPSTREAM"

# 2. Build host repo + nue.yaml referencing the upstream.
HOST="$WORK/host"
git init -q -b main "$HOST"
(
  cd "$HOST"
  git config user.email "host@example.com"
  git config user.name "Host Dev"
  echo "# my-app" > README.md
  git add . && git commit -q -m "init host"
)

cat > "$HOST/nue.yaml" <<EOF
version: 1
sources:
  common:
    url: $UPSTREAM
    ref: refs/heads/main
grafts:
  - from: common
    trees:
      - src: schemas/
        dst: vendor/schemas/
    files:
      - src: utils/logger.py
        dst: libs/log.py
EOF
(cd "$HOST" && git add nue.yaml && git commit -q -m "add nue manifest")

# 3. Sync.
echo "==> running nue sync"
"$NUE" --repo "$HOST" sync

# 4. Assertions.
echo "==> assertions"
test -d "$HOST/.git/nue/common.git" || { echo "missing .git/nue/common.git"; exit 1; }
test -f "$HOST/libs/log.py" || { echo "missing libs/log.py"; exit 1; }
test -f "$HOST/vendor/schemas/user.json" || { echo "missing vendor/schemas/user.json"; exit 1; }

grep -q "def log(): return 'v2'" "$HOST/libs/log.py" \
  || { echo "libs/log.py has wrong content"; cat "$HOST/libs/log.py"; exit 1; }
grep -q '"name":"string"' "$HOST/vendor/schemas/user.json" \
  || { echo "user.json wrong content"; cat "$HOST/vendor/schemas/user.json"; exit 1; }

(cd "$HOST" && git log --format='%s%n%b' | grep -q 'Nue-Source-common:') \
  || { echo "missing Nue-Source trailer in host history"; (cd "$HOST" && git log --oneline); exit 1; }

# 5. Idempotency: second sync should report "up-to-date".
echo "==> running nue sync again (should be no-op)"
"$NUE" --repo "$HOST" sync | tee "$WORK/second.log"
grep -q 'up-to-date' "$WORK/second.log" \
  || { echo "expected up-to-date marker in second sync output"; exit 1; }

echo "==> OK"
echo
echo "host log:"
(cd "$HOST" && git log --oneline --graph --decorate)
