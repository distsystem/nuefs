# NueFS

FUSE-based union filesystem for Python.

Named after [Nue (鵺)](https://en.wikipedia.org/wiki/Nue), a Japanese chimera with parts from different animals — just like NueFS composes files from different layers into a unified view.

## Why

You have a project template (cookiecutter, copier, etc.) and want to keep it in sync with your projects. But once you scaffold a project, the template files are copied — updates require manual merging or re-scaffolding.

NueFS creates a **live template overlay**: template files appear directly in your project without copying. The template repo stays independent with its own git history — but your project sees the latest template files instantly.

- **Write-through**: Edits to template files go directly to the template repo
- **No copies**: Template updates appear immediately in all projects
- **No symlinks**: Tools see real files, directories merge naturally

## Union View

```text
~/myproject/                    # your project (union view)
  .git/                         # from: ~/myproject/.git (project's own git)
  nue.yaml                      # from: ~/myproject/ (base layer)
  .eslintrc.json                # from: ~/templates/typescript/.eslintrc.json
  .prettierrc                   # from: ~/templates/typescript/.prettierrc
  tsconfig.json                 # from: ~/templates/typescript/tsconfig.json
  .github/
    workflows/
      ci.yml                    # from: ~/templates/typescript/.github/workflows/
  src/
    index.ts                    # from: ~/myproject/src/ (your code)
    utils.ts                    # from: ~/myproject/src/ (your code)

~/templates/typescript/         # template repo (independent git repo)
  .git/                         # stays here, NOT mounted to projects
  .eslintrc.json
  .prettierrc
  tsconfig.json
  .github/
    workflows/
      ci.yml
```

Unlike cookiecutter/copier, template files are not copied — they're mounted. Update the template once, all projects see the change.

## Mount Types

NueFS supports both directory and single-file mounts:

```yaml
# nue.yaml
mounts:
  # Directory mount: entire directory appears at target path
  - source: ~/repos/core
    target: packages/core

  # Single-file mount: individual file appears at target path
  - source: ~/repos/shared-configs/.eslintrc.json
    target: .eslintrc.json
```

This is the key advantage over git submodules — you can mount individual config files (`.eslintrc`, `.prettierrc`, `tsconfig.json`) directly into your workspace root without nesting them in subdirectories.

## Requirements

- Linux with FUSE support
- `fuse3` package installed
- User in `fuse` group, or `/etc/fuse.conf` with `user_allow_other`
