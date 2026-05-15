# nue

> _鵺 — a folkloric chimera assembled from the limbs of disparate beasts._

**nue** is a vendoring tool for git. You declare which fragments of which
upstream repos make up your project; `nue sync` pulls each upstream,
filters its history, and **replays the resulting commits onto your branch**
with full author / message / commit-graph fidelity. Vendored files end up in
your git history as ordinary tracked content, ready to commit, review, and
ship.

- No daemon, no FUSE, no kernel modules — single Rust binary
- Powered by [josh](https://github.com/josh-project/josh) as the history-filter engine
- File-level mapping (precise) and tree-level mapping (recursive)
- Bidirectional: `nue push` lifts host-side edits back to an upstream
- State lives in `Nue-Source-<name>: <sha>` commit trailers (à la Copybara `GitOrigin-RevId`) — no separate state file

## Quick start

```bash
# install nue
cargo install --path .

# install josh-filter (history-filter engine)
cargo install --git https://github.com/josh-project/josh \
  --bin josh-filter josh-cli

# in your git repo, write nue.yaml:
cat > nue.yaml <<'EOF'
version: 1
sources:
  common:
    url: git@github.com:myorg/common-libs.git
    ref: refs/heads/main
grafts:
  - from: common
    files:
      - {src: utils/logger.py, dst: libs/log.py}
    trees:
      - {src: schemas/, dst: vendor/schemas/}
EOF

# pull upstream, filter, replay onto your branch
nue sync
```

End-to-end self-contained demo: `./examples/demo.sh`.

## Vocabulary

| Term | Meaning |
|------|---------|
| **source** | an upstream repo declared in `sources:` |
| **graft** | one `(upstream path → host path)` mapping |
| **file graft / tree graft** | single file vs directory grafts |
| **host** | your work repo (term-of-art for "graft recipient") |
| **sync** | pull sources, filter, replay onto host HEAD |
| **push** | host edits → upstream commit on `refs/heads/nue-push/<source>` |

## Architecture

```
nue.yaml ──► Manifest ──► filter_spec ──► josh filter string
                                                 │
                                                 ▼
.git/nue/<src>.git (bare clone) ──┬── git fetch
                                  ├── josh-filter <spec> → refs/nue/filtered
                                  └── git fetch into host → refs/nue/<src>/filtered
                                                 │
                                                 ▼
                                       host repo HEAD
                                       (git merge with
                                        Nue-Source-<src>: trailer)
```

No daemon, no IPC, no codegen. All state:

- `.git/nue/<src>.git/` — bare clones (one per source, shared across worktrees)
- `Nue-Source-<src>: <sha>` trailer on the latest sync's merge commit

See [`docs/design.md`](docs/design.md) for the longer story.

## CLI

```
nue sync                       # pull sources, filter, replay onto HEAD
nue push <source>              # snapshot host edits → upstream branch
                               # refs/heads/nue-push/<source>
nue debug filter-spec          # show the generated josh filter (debugging)

# common flags:
nue --repo PATH ...            # operate on PATH (default: cwd)
nue --manifest FILE ...        # use FILE instead of nue.yaml
```

## Status

Pre-1.0. Currently working:

- `sync` for file and tree grafts (multi-source orchestration)
- `push` for file grafts (single-commit snapshot)
- End-to-end smoke test (`./examples/demo.sh`)

Not yet:

- Tree graft push (only file grafts can push)
- Conflict policy on local edits vs upstream changes
- `eject` / `which` / `status` subcommands
- Authentication helpers for private remotes
- Pre-built `josh-filter` distribution

The project previously implemented this concept via a FUSE daemon. That
architecture has been replaced; see git history if you need it (last commit
of the FUSE branch is `cabd706`).

## Comparison to alternatives

| Tool | Why nue exists anyway |
|------|----------------------|
| `git submodule` | whole-repo granularity; we want file/dir-level |
| `git subtree` | tree-only, awkward to update, no replay control |
| `josh link` (in josh-cli) | works; manifest is per-dir `.link.josh`; view-oriented modes. nue trades that for one yaml + Nue-Source trailer that survives PR review |
| [Copybara](https://github.com/google/copybara) | feature-rich and battle-tested at Google; JVM/Bazel, Starlark config |
| [vendir](https://carvel.dev/vendir/) | snapshot vendoring; no commit replay |

## License

Apache-2.0
