use anyhow::{Context, Result, bail};
use log::{debug, info};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::filter_spec;
use crate::manifest::Manifest;

pub struct SyncReport {
    pub source_name: String,
    pub upstream_commit: String,
    pub filtered_ref: String,
    pub host_merge_commit: Option<String>,
}

pub fn sync(host_repo: &Path, manifest: &Manifest) -> Result<Vec<SyncReport>> {
    ensure_host_is_git_repo(host_repo)?;

    let nue_dir = host_repo.join(".git").join("nue");
    std::fs::create_dir_all(&nue_dir).context("creating .git/nue/")?;

    let mut reports = Vec::new();

    for (name, source) in &manifest.sources {
        let bare_path = nue_dir.join(format!("{name}.git"));
        ensure_bare_clone(&bare_path, &source.url)?;
        fetch(&bare_path, &source.url, &source.r#ref)?;
        let upstream_commit = resolve_ref(&bare_path, "FETCH_HEAD")?;

        let Some(spec) = filter_spec::build_for_source(manifest, name) else {
            info!("source '{name}' has no grafts, skipping");
            continue;
        };
        debug!("source '{name}' filter spec: {spec}");

        let filtered_local_ref = "refs/nue/filtered".to_string();
        run_josh_filter(&bare_path, &spec, &upstream_commit, &filtered_local_ref)?;

        let host_ref = format!("refs/nue/{name}/filtered");
        fetch_into_host(host_repo, &bare_path, &filtered_local_ref, &host_ref)?;

        let merge_commit = merge_into_head(host_repo, name, &host_ref, &upstream_commit)?;

        reports.push(SyncReport {
            source_name: name.clone(),
            upstream_commit,
            filtered_ref: host_ref,
            host_merge_commit: merge_commit,
        });
    }

    Ok(reports)
}

fn ensure_host_is_git_repo(p: &Path) -> Result<()> {
    let dot_git = p.join(".git");
    if !dot_git.exists() {
        bail!("{} is not a git repository (no .git/)", p.display());
    }
    Ok(())
}

fn ensure_bare_clone(bare_path: &Path, url: &str) -> Result<()> {
    if bare_path.join("HEAD").exists() {
        return Ok(());
    }
    info!("cloning {url} into {}", bare_path.display());
    let status = Command::new("git")
        .args(["clone", "--bare", url])
        .arg(bare_path)
        .status()
        .context("invoking git clone")?;
    if !status.success() {
        bail!("git clone failed for {url}");
    }
    Ok(())
}

fn fetch(bare_path: &Path, url: &str, ref_: &str) -> Result<()> {
    info!("fetching {ref_} from {url}");
    let status = Command::new("git")
        .arg("-C")
        .arg(bare_path)
        .args(["fetch", url, ref_])
        .status()
        .context("invoking git fetch")?;
    if !status.success() {
        bail!("git fetch failed for {url} {ref_}");
    }
    Ok(())
}

fn resolve_ref(bare_path: &Path, ref_: &str) -> Result<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(bare_path)
        .args(["rev-parse", ref_])
        .output()
        .context("invoking git rev-parse")?;
    if !out.status.success() {
        bail!("git rev-parse {ref_} failed: {}", String::from_utf8_lossy(&out.stderr));
    }
    Ok(String::from_utf8(out.stdout)?.trim().to_string())
}

fn run_josh_filter(bare_path: &Path, spec: &str, input_commit: &str, update_ref: &str) -> Result<()> {
    let bin = std::env::var("NUE_JOSH_FILTER").unwrap_or_else(|_| "josh-filter".to_string());
    debug!(
        "(cd {}) {bin} {spec} {input_commit} --update {update_ref}",
        bare_path.display()
    );
    let out = Command::new(&bin)
        .current_dir(bare_path)
        .args([spec, input_commit, "--update", update_ref])
        .output()
        .with_context(|| {
            format!(
                "nue requires `josh-filter` on PATH (override via NUE_JOSH_FILTER). \
                 Install with: cargo install --git https://github.com/josh-project/josh \
                 --bin josh-filter josh-cli"
            )
        })?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    debug!("josh-filter stdout: {}", stdout.trim());
    if !stderr.trim().is_empty() {
        debug!("josh-filter stderr: {}", stderr.trim());
    }
    if !out.status.success() {
        bail!(
            "josh-filter failed:\nstdout: {}\nstderr: {}",
            stdout, stderr
        );
    }
    Ok(())
}

fn fetch_into_host(host_repo: &Path, bare_path: &Path, src_ref: &str, dst_ref: &str) -> Result<()> {
    let refspec = format!("+{src_ref}:{dst_ref}");
    let status = Command::new("git")
        .arg("-C")
        .arg(host_repo)
        .args(["fetch"])
        .arg(bare_path)
        .arg(&refspec)
        .status()
        .context("invoking git fetch into host")?;
    if !status.success() {
        bail!("fetch {refspec} into host failed");
    }
    Ok(())
}

fn merge_into_head(
    host_repo: &Path,
    source_name: &str,
    src_ref: &str,
    upstream_commit: &str,
) -> Result<Option<String>> {
    let already_merged = is_ancestor(host_repo, src_ref, "HEAD")?;
    if already_merged {
        info!("source '{source_name}' already up-to-date");
        return Ok(None);
    }

    let msg = format!(
        "nue: sync {source_name}\n\nNue-Source-{source_name}: {upstream_commit}\n"
    );

    let out = Command::new("git")
        .arg("-C")
        .arg(host_repo)
        .args(["merge", "--no-ff", "--allow-unrelated-histories", "-m", &msg])
        .arg(src_ref)
        .output()
        .context("invoking git merge")?;
    if !out.status.success() {
        bail!(
            "git merge {src_ref} into host failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let head = resolve_ref(host_repo, "HEAD")?;
    Ok(Some(head))
}

fn is_ancestor(repo: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
    let status = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["merge-base", "--is-ancestor", ancestor, descendant])
        .status()
        .context("invoking git merge-base")?;
    Ok(status.success())
}

#[allow(dead_code)]
pub fn host_dir_default() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}
