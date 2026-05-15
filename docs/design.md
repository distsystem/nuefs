# nue design

Status: Draft · Date: 2026-05-14

## Problem statement

We want **declarative, file-level vendoring with replayed history**:

1. **File-level**, not whole-repo. `git submodule` and `git subtree` are
   coarser than the typical "we use these 3 files from that big repo" case.
2. **Declarative manifest**, not imperative scripts. One `nue.yaml` at repo
   root is the source of truth; CI and reviewers see the same thing.
3. **Commit-level replay**. Vendored upstream commits land as ordinary host
   commits with original author / date / message, so `git log --follow` and
   `git blame` keep working across the import boundary.
4. **No daemon, no FUSE, no kernel modules**. Must work on Linux, macOS,
   Windows, CI containers, and locked-down developer machines.

Previously this project tried (1)+(2) via a FUSE daemon — that hit too many
cross-platform / CI / operational issues to justify the complexity. The
current design drops the daemon entirely and uses [josh](https://github.com/josh-project/josh)
to do the heavy lifting in (3).

## Solution

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

- Each upstream **source** gets a bare clone at `.git/nue/<src>.git/`.
  (`.git/` is natively excluded from tracking; clones are shared across
  linked worktrees, mirroring `git submodule`'s `.git/modules/` pattern.)
- For each source, `nue` translates the manifest into a single josh filter
  spec (e.g. `:[::libs/log.py=utils/logger.py, :/schemas:prefix=vendor/schemas]`)
  and invokes the standalone `josh-filter` binary, which rewrites the source's
  commit graph in place into the filtered shape.
- The filtered ref is fetched into the host repo and `git merge`d onto HEAD
  with a `Nue-Source-<src>: <upstream-sha>` trailer recording provenance.

Result: every relevant upstream commit appears as an ancestor of host HEAD,
with the upstream's metadata preserved and the file paths rewritten.

## Implementation decisions

### Storage: `.git/nue/<src>.git/` (bare clones)

Lives inside `.git/` so it's git-managed metadata, never accidentally
tracked. Bare clones (no working tree) save disk and let multiple linked
worktrees share one clone. Same pattern as `git submodule`'s `.git/modules/`.

### Engine: `josh-filter` subprocess, not `josh-core` as a Rust library

`josh-core` is a real published crate (uses `gitoxide`), and we can link it.
But the subprocess boundary is significantly cleaner for a tool whose value
is "translate manifest → filter spec → result"; we're not in the inner
loop of josh's hot path. Adding `git2` + `gitoxide` + `sled` and a pile of
josh's internal types to nue's surface area buys us nothing the subprocess
doesn't already give us. The migration to library-linked mode is one
function-body's worth of code if we ever need it.

### State tracking: commit-message trailer, not a lockfile

After each sync, the merge commit carries `Nue-Source-<src>: <upstream-sha>`.
The next sync greps for it (`git log --grep`) to find the base. This is
Copybara's `GitOrigin-RevId` pattern. No separate state file = nothing to
forget to commit, no merge conflicts on the state file, full audit trail in
`git log` for review.

### Push is single-commit, not commit-by-commit

`nue push <source>` snapshots the host's current vendored content as ONE
upstream-shaped commit on top of the last-synced base, then pushes it as
`refs/heads/nue-push/<source>`. Commit-by-commit reverse replay (running
`josh-filter --invert` over a range of host commits) is doable but adds
complexity and authorship questions for a PoC. Single-commit form is
adequate for "submit my vendored changes back as a PR" workflows.

### CLI is `nue`, not `git-nue`

We deliberately do not register as a git subcommand. Rationale: nue isn't a
git plumbing extension; it's a tool that uses git. Naming it `git-nue` would
imply lifecycle alignment with git releases that we don't actually have.

## Risks

1. **josh-filter dependency** — josh is a small project. Mitigation:
   subprocess boundary makes it easy to vendor or fork; the filter algebra
   is well-defined enough to reimplement if needed.
2. **Local edits clobbered on sync** — current `nue sync` does a plain
   `git merge`. If the user edited a vendored file without committing, merge
   may overwrite or conflict. Mitigation (future): `nue status` to surface
   local drift before sync.
3. **Force-push on upstream** — if a source rewrites history, the
   Nue-Source trailer points at a now-gone commit. Mitigation: detect this
   on sync, fall back to "find nearest reachable ancestor" or refuse with a
   clear error.
4. **Tree graft push not implemented** — `nue push` only handles file
   grafts. Tree grafts require enumerating host's tree at the dst prefix
   and computing the inverse mapping for each file. Mitigation: error
   clearly until implemented.
5. **No conflict policy on overlapping grafts** — if two grafts map to the
   same dst, the result is undefined (currently caught at manifest validate
   time only for trivial cases). Tighten validation.
6. **josh-filter binary distribution** — currently requires `cargo install`
   from source. Cross-platform pre-built distribution is a separate problem.

## Out of scope (for now)

- `nue eject <path>` — make a vendored file fully host-owned (drop the graft)
- `nue which <path>` — reverse-lookup which source/path a host file came from
- `nue update [source]` — bump source `ref` resolution explicitly (today
  `nue sync` always re-resolves)
- A `Cargo.toml`-style version-pinning syntax in `nue.yaml`
- Authentication helpers for private remotes (today we rely on git's own
  credential helpers, which is mostly fine)
- Watch mode (no FUSE-style continuous sync — sync is an explicit action)

## Alternatives considered

| Alternative | Why not |
|-------------|---------|
| Stay on FUSE | Daemon ops cost, cross-platform pain, CI hostility |
| `git submodule` | Whole-repo, host can't commit to submodule paths |
| `git subtree` | Tree-only, history munging is awkward |
| `josh link` (built into josh-cli) | Closest match; uses per-dir `.link.josh` files (one per mount path) and view-oriented modes. We trade that for one root `nue.yaml` and the Nue-Source trailer; if that distinction doesn't matter to you, use `josh link` directly |
| [Copybara](https://github.com/google/copybara) | The feature-complete answer at Google scale; JVM/Bazel/Starlark stack is heavy for a CLI utility |
| [vendir](https://carvel.dev/vendir/) | Snapshot vendoring, no commit replay |
