# NueFS

FUSE-based layered filesystem for Python.

Named after [Nue (鵺)](https://en.wikipedia.org/wiki/Nue), a Japanese chimera with parts from different animals — just like NueFS merges files from different sources into a unified view.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Python                               │
│  handle = nuefs.open(root, mounts)                          │
│  handle.which(path)                                         │
│  handle.close()                                             │
└─────────────────────────────────────────────────────────────┘
                              │ pyo3 FFI
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Rust Extension (_nuefs.so)                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ #[pyclass]  │    │ IPC Client  │    │ Auto-start  │     │
│  │ Mapping     │    │ Unix Socket │    │ nuefsd      │     │
│  │ RawHandle   │    │ serde_json  │    │ (if needed) │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
└─────────────────────────────────────────────────────────────┘
                              │ IPC (Unix Socket)
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Rust Daemon (nuefsd)                      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ IPC Server  │    │ Mount       │    │ FUSE        │     │
│  │ Dispatcher  │───▶│ Manager     │───▶│ Sessions    │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

## Project Structure

```
nuefs/
├── Cargo.toml
├── src/
│   ├── lib.rs          # pyo3 entry (Python API)
│   ├── client.rs       # IPC client
│   ├── types.rs        # Shared type definitions
│   ├── daemon/
│   │   ├── mod.rs
│   │   ├── server.rs   # IPC server
│   │   ├── manager.rs  # Mount manager
│   │   └── fuse.rs     # FUSE implementation
│   └── bin/
│       └── nuefsd.rs   # Daemon entry
└── python/
    └── nuefs/
        ├── __init__.py
        └── cli.py
```

## Usage

```python
import nuefs
from pathlib import Path

handle = nuefs.open(
    root=Path("~/project"),
    mounts=[
        nuefs.Mapping(target=".config/nvim", source="~/.layers/nvim"),
    ],
)

# Query ownership
info = handle.which(".config/nvim/init.lua")
if info:
    print(f"Owner: {info.owner}, Path: {info.backend_path}")

# Unmount
handle.close()
```

## Requirements

- Linux with FUSE support
- `fuse3` package installed
- User in `fuse` group, or `/etc/fuse.conf` with `user_allow_other`
