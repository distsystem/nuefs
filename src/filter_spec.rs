use crate::manifest::Manifest;

/// Translate the manifest entries for a given source into a single josh filter
/// spec that maps upstream content to the desired host paths.
///
/// Examples:
///   tree  utils/ → libs/utils/     becomes  :/utils:prefix=libs/utils
///   file  lib/logger.py → vendor/logger.py
///                                  becomes  ::vendor/logger.py=lib/logger.py
///   multiple                       composed as  :[ spec1 , spec2 , ... ]
pub fn build_for_source(manifest: &Manifest, source: &str) -> Option<String> {
    let mut atoms: Vec<String> = Vec::new();

    for graft in manifest.grafts_for(source) {
        for t in &graft.trees {
            atoms.push(tree_atom(&t.src, &t.dst));
        }
        for f in &graft.files {
            atoms.push(file_atom(&f.src, &f.dst));
        }
    }

    if atoms.is_empty() {
        return None;
    }

    if atoms.len() == 1 {
        Some(atoms.remove(0))
    } else {
        Some(format!(":[{}]", atoms.join(",")))
    }
}

fn tree_atom(src: &str, dst: &str) -> String {
    let src = src.trim_end_matches('/');
    let dst = dst.trim_end_matches('/');
    if dst.is_empty() {
        format!(":/{src}")
    } else {
        format!(":/{src}:prefix={dst}")
    }
}

fn file_atom(src: &str, dst: &str) -> String {
    format!("::{dst}={src}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{FileMap, Graft, Manifest, Source, TreeMap};
    use std::collections::BTreeMap;

    fn manifest_with(graft: Graft) -> Manifest {
        let mut sources = BTreeMap::new();
        sources.insert(
            "s".into(),
            Source {
                url: "x".into(),
                r#ref: "main".into(),
            },
        );
        Manifest {
            version: 1,
            sources,
            grafts: vec![graft],
        }
    }

    #[test]
    fn tree_only() {
        let m = manifest_with(Graft {
            from: "s".into(),
            trees: vec![TreeMap {
                src: "utils/".into(),
                dst: "libs/utils/".into(),
            }],
            files: vec![],
        });
        assert_eq!(build_for_source(&m, "s").unwrap(), ":/utils:prefix=libs/utils");
    }

    #[test]
    fn file_only() {
        let m = manifest_with(Graft {
            from: "s".into(),
            trees: vec![],
            files: vec![FileMap {
                src: "lib/logger.py".into(),
                dst: "vendor/logger.py".into(),
            }],
        });
        assert_eq!(build_for_source(&m, "s").unwrap(), "::vendor/logger.py=lib/logger.py");
    }

    #[test]
    fn composed() {
        let m = manifest_with(Graft {
            from: "s".into(),
            trees: vec![TreeMap {
                src: "schemas".into(),
                dst: "vendor/schemas".into(),
            }],
            files: vec![FileMap {
                src: "lib/a.py".into(),
                dst: "libs/a.py".into(),
            }],
        });
        assert_eq!(
            build_for_source(&m, "s").unwrap(),
            ":[:/schemas:prefix=vendor/schemas,::libs/a.py=lib/a.py]"
        );
    }
}
