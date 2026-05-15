mod filter_spec;
mod manifest;
mod push;
mod sync;

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

use crate::manifest::Manifest;

#[derive(Parser)]
#[command(name = "nue", about = "File-level git vendoring with commit-level replay")]
struct Cli {
    #[arg(long, global = true, default_value = ".")]
    repo: PathBuf,

    #[arg(long, global = true, default_value = "nue.yaml")]
    manifest: PathBuf,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Fetch sources, filter through josh, merge filtered refs into host HEAD
    Sync,
    /// Snapshot host's current vendored content as a single commit on the upstream
    /// remote (file grafts only), as branch refs/heads/nue-push/<source>.
    Push {
        /// Source name from the manifest.
        source: String,
    },
    /// Inspection / troubleshooting helpers (not the daily UX).
    Debug(DebugArgs),
}

#[derive(Args)]
struct DebugArgs {
    #[command(subcommand)]
    cmd: DebugCmd,
}

#[derive(Subcommand)]
enum DebugCmd {
    /// Print the josh filter spec generated from the manifest, per source.
    FilterSpec,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = Cli::parse();

    let manifest_path = if cli.manifest.is_absolute() {
        cli.manifest.clone()
    } else {
        cli.repo.join(&cli.manifest)
    };
    let manifest = Manifest::load(&manifest_path)
        .with_context(|| format!("loading {}", manifest_path.display()))?;

    match cli.cmd {
        Cmd::Sync => {
            let reports = sync::sync(&cli.repo, &manifest)?;
            for r in &reports {
                println!(
                    "source={} upstream={} filtered_ref={} merge={}",
                    r.source_name,
                    &r.upstream_commit[..12.min(r.upstream_commit.len())],
                    r.filtered_ref,
                    r.host_merge_commit
                        .as_deref()
                        .map(|s| &s[..12.min(s.len())])
                        .unwrap_or("(up-to-date)")
                );
            }
        }
        Cmd::Push { source } => {
            let r = push::push(&cli.repo, &manifest, &source)?;
            println!(
                "source={} base={} new_commit={} ref={}",
                r.source_name,
                &r.base_upstream[..12.min(r.base_upstream.len())],
                &r.new_upstream_commit[..12.min(r.new_upstream_commit.len())],
                r.push_ref
            );
        }
        Cmd::Debug(DebugArgs {
            cmd: DebugCmd::FilterSpec,
        }) => {
            for name in manifest.sources.keys() {
                let spec = filter_spec::build_for_source(&manifest, name)
                    .unwrap_or_else(|| "(no grafts)".to_string());
                println!("{name}\t{spec}");
            }
        }
    }
    Ok(())
}
