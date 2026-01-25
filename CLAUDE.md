use python skill

## Development

```bash
pixi run develop  # Build and install the package
```

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
│  │ RawHandle   │    │ tarpc       │    │ (if needed) │     │
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
