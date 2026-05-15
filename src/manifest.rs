use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub sources: BTreeMap<String, Source>,
    #[serde(default)]
    pub grafts: Vec<Graft>,
}

#[derive(Debug, Deserialize)]
pub struct Source {
    pub url: String,
    #[serde(default = "default_ref")]
    pub r#ref: String,
}

#[derive(Debug, Deserialize)]
pub struct Graft {
    pub from: String,
    #[serde(default)]
    pub trees: Vec<TreeMap>,
    #[serde(default)]
    pub files: Vec<FileMap>,
}

#[derive(Debug, Deserialize)]
pub struct TreeMap {
    pub src: String,
    pub dst: String,
}

#[derive(Debug, Deserialize)]
pub struct FileMap {
    pub src: String,
    pub dst: String,
}

fn default_ref() -> String {
    "HEAD".to_string()
}

impl Manifest {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading manifest {}", path.display()))?;
        let m: Manifest = serde_yaml::from_str(&text).context("parsing manifest yaml")?;
        m.validate()?;
        Ok(m)
    }

    fn validate(&self) -> Result<()> {
        if self.version != 1 {
            bail!("only version: 1 is supported, got {}", self.version);
        }
        for graft in &self.grafts {
            if !self.sources.contains_key(&graft.from) {
                bail!("graft references unknown source: {}", graft.from);
            }
            if graft.trees.is_empty() && graft.files.is_empty() {
                bail!("graft from '{}' has no trees or files", graft.from);
            }
        }
        Ok(())
    }

    pub fn grafts_for<'a>(&'a self, source: &'a str) -> impl Iterator<Item = &'a Graft> + 'a {
        self.grafts.iter().filter(move |g| g.from == source)
    }
}
