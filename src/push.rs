use anyhow::{Context, Result, bail};
use log::{debug, info};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::manifest::Manifest;

pub struct PushReport {
    pub source_name: String,
    pub base_upstream: String,
    pub new_upstream_commit: String,
    pub push_ref: String,
}

pub fn push(host_repo: &Path, manifest: &Manifest, source_name: &str) -> Result<PushReport> {
    let source = manifest
        .sources
        .get(source_name)
        .with_context(|| format!("unknown source '{source_name}' in manifest"))?;

    let grafts: Vec<_> = manifest.grafts_for(source_name).collect();
    if grafts.is_empty() {
        bail!("no grafts for source '{source_name}'");
    }
    for g in &grafts {
        if !g.trees.is_empty() {
            bail!(
                "push: tree grafts are not yet supported (source '{source_name}' has {} tree graft(s)); \
                 only file grafts work in this PoC",
                g.trees.len()
            );
        }
    }

    let bare = host_repo
        .join(".git")
        .join("nue")
        .join(format!("{source_name}.git"));
    if !bare.join("HEAD").exists() {
        bail!(
            "bare clone {} does not exist — run `nue sync` first",
            bare.display()
        );
    }

    // Step 1: find base upstream commit from the most recent Nue-Source-<name>: trailer.
    let base = find_last_synced_upstream(host_repo, source_name)?;
    debug!("base upstream commit for '{source_name}': {base}");

    // Step 2: stage host's HEAD into the bare clone so its blobs are reachable.
    let host_head_in_bare = format!("refs/nue/push/{source_name}/host");
    push_host_head_to_bare(host_repo, &bare, &host_head_in_bare)?;

    // Step 3: build a new upstream-shaped tree starting from `base`, then overlay
    // each host blob at its upstream path.
    let new_commit = build_inverse_commit(&bare, manifest, source_name, &base, &host_head_in_bare)?;
    debug!("new upstream commit: {new_commit}");

    // Step 4: update a local push branch pointing at it.
    let push_ref = format!("refs/heads/nue-push/{source_name}");
    update_ref(&bare, &push_ref, &new_commit)?;

    // Step 5: push to the upstream remote URL.
    git_push(&bare, &source.url, &push_ref)?;

    Ok(PushReport {
        source_name: source_name.to_string(),
        base_upstream: base,
        new_upstream_commit: new_commit,
        push_ref,
    })
}

fn find_last_synced_upstream(host_repo: &Path, source_name: &str) -> Result<String> {
    let trailer = format!("Nue-Source-{source_name}:");
    let out = Command::new("git")
        .arg("-C")
        .arg(host_repo)
        .args(["log", "-1", "--all", &format!("--grep={trailer}"), "--format=%B"])
        .output()
        .context("invoking git log to find Nue-Source trailer")?;
    if !out.status.success() {
        bail!(
            "git log failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let body = String::from_utf8(out.stdout)?;
    let sha = body
        .lines()
        .find_map(|line| line.strip_prefix(&trailer).map(|s| s.trim().to_string()))
        .with_context(|| {
            format!(
                "could not find a `{trailer} <sha>` trailer in host history. \
                 Did you run `nue sync` for this source?"
            )
        })?;
    Ok(sha)
}

fn push_host_head_to_bare(host_repo: &Path, bare: &Path, dst_ref: &str) -> Result<()> {
    debug!("staging host's HEAD into bare as {dst_ref}");
    let status = Command::new("git")
        .arg("-C")
        .arg(host_repo)
        .args(["push", "-q", "-f"])
        .arg(bare)
        .arg(format!("HEAD:{dst_ref}"))
        .status()
        .context("pushing host HEAD into bare clone")?;
    if !status.success() {
        bail!("failed to push host HEAD into bare");
    }
    Ok(())
}

fn build_inverse_commit(
    bare: &Path,
    manifest: &Manifest,
    source_name: &str,
    base_commit: &str,
    host_ref_in_bare: &str,
) -> Result<String> {
    // Use a temp index file in /tmp so the path is absolute (avoids confusion when
    // `bare` is relative and git -C re-resolves GIT_INDEX_FILE against the new cwd).
    let tmp_index = std::env::temp_dir().join(format!(
        "nue-push-{source_name}-{}.idx",
        std::process::id()
    ));
    let _cleanup = TempPath(tmp_index.clone());

    // Seed the temp index with the base upstream tree.
    run_git_with_index(bare, &tmp_index, &["read-tree", base_commit])
        .context("seeding temp index with base tree")?;

    // For each file graft, look up host's blob at dst and stage it at the upstream src path.
    let mut changed = 0;
    for graft in manifest.grafts_for(source_name) {
        for f in &graft.files {
            let host_blob = resolve_blob(bare, host_ref_in_bare, &f.dst)
                .with_context(|| format!("reading host blob at '{}'", f.dst))?;
            let cacheinfo = format!("100644,{host_blob},{}", f.src);
            run_git_with_index(
                bare,
                &tmp_index,
                &["update-index", "--add", "--cacheinfo", &cacheinfo],
            )
            .with_context(|| {
                format!(
                    "staging {} at upstream path {} (blob {host_blob})",
                    f.dst, f.src
                )
            })?;
            changed += 1;
        }
    }

    if changed == 0 {
        bail!("no files to push for source '{source_name}'");
    }

    // Write tree, then create commit object on top of base.
    let new_tree = run_git_with_index(bare, &tmp_index, &["write-tree"])
        .context("write-tree")?
        .trim()
        .to_string();

    if tree_equals_commit_tree(bare, &new_tree, base_commit)? {
        bail!(
            "no changes to push: host's vendored files at their dst paths are identical to the \
             last synced upstream commit {base_commit}. Edit something in libs/ and try again."
        );
    }

    let msg = format!(
        "nue: push from host\n\nReplays host-side edits onto upstream\nBase: {base_commit}\n"
    );
    let out = Command::new("git")
        .arg("-C")
        .arg(bare)
        .args(["commit-tree", &new_tree, "-p", base_commit, "-m", &msg])
        .output()
        .context("commit-tree")?;
    if !out.status.success() {
        bail!(
            "commit-tree failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

fn resolve_blob(bare: &Path, commit_or_ref: &str, path: &str) -> Result<String> {
    let target = format!("{commit_or_ref}:{path}");
    let out = Command::new("git")
        .arg("-C")
        .arg(bare)
        .args(["rev-parse", &target])
        .output()
        .context("rev-parse for host blob")?;
    if !out.status.success() {
        bail!(
            "could not resolve {target}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

fn tree_equals_commit_tree(bare: &Path, tree_oid: &str, commit_oid: &str) -> Result<bool> {
    let out = Command::new("git")
        .arg("-C")
        .arg(bare)
        .args(["rev-parse", &format!("{commit_oid}^{{tree}}")])
        .output()
        .context("rev-parse commit tree")?;
    let base_tree = String::from_utf8(out.stdout)?.trim().to_string();
    Ok(base_tree == tree_oid)
}

fn run_git_with_index(bare: &Path, index: &Path, args: &[&str]) -> Result<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(bare)
        .args(args)
        .env("GIT_INDEX_FILE", index)
        .output()
        .with_context(|| format!("git {}", args.join(" ")))?;
    if !out.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    Ok(String::from_utf8(out.stdout)?)
}

fn update_ref(bare: &Path, ref_: &str, commit: &str) -> Result<()> {
    let status = Command::new("git")
        .arg("-C")
        .arg(bare)
        .args(["update-ref", ref_, commit])
        .status()
        .context("update-ref")?;
    if !status.success() {
        bail!("update-ref {ref_} -> {commit} failed");
    }
    Ok(())
}

fn git_push(bare: &Path, url: &str, ref_: &str) -> Result<()> {
    info!("pushing {ref_} to {url}");
    let status = Command::new("git")
        .arg("-C")
        .arg(bare)
        .args(["push", "-f", url, &format!("{ref_}:{ref_}")])
        .status()
        .context("git push to upstream")?;
    if !status.success() {
        bail!("git push to {url} failed");
    }
    Ok(())
}

struct TempPath(PathBuf);
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
