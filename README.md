# NueFS

FUSE-based layered filesystem for Python.

Named after [Nue (鵺)](https://en.wikipedia.org/wiki/Nue), a Japanese chimera with parts from different animals — just like NueFS merges files from different sources into a unified view.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Python                               │
│  handle = nuefs.mount(root, mounts)  # API unchanged        │
│  nuefs.which(handle, path)                                  │
│  nuefs.unmount(handle)                                      │
└─────────────────────────────────────────────────────────────┘
                              │ pyo3 FFI
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Rust Extension (_nuefs.so)                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ #[pyclass]  │    │ IPC Client  │    │ Auto-start  │     │
│  │ Mount       │    │ Unix Socket │    │ nuefsd      │     │
│  │ MountHandle │    │ serde_json  │    │ (if needed) │     │
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

handle = nuefs.mount(
    root=Path("~/project"),
    mounts=[
        nuefs.Mount(target=".config/nvim", source="~/.layers/nvim"),
    ],
)

# Query ownership
info = nuefs.which(handle, ".config/nvim/init.lua")
print(f"Owner: {info.owner}, Path: {info.backend_path}")

# Unmount
nuefs.unmount(handle)
```

## Requirements

- Linux with FUSE support
- `fuse3` package installed
- User in `fuse` group, or `/etc/fuse.conf` with `user_allow_other`
