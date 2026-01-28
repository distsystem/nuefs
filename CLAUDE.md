use python skill

## FUSE Mount Safety

Never `cd` into the mount directory when testing FUSE mounts. If the mount fails, all shell commands will hang with EIO errors.

Always operate from outside:
```bash
# Good: run commands from outside
(cd /tmp && ls /home/rok/distsystem/nuefs/sheaves/)

# Bad: don't cd into mount directory
cd /home/rok/distsystem/nuefs && ls sheaves/
```

Recovery when stuck:
```bash
fusermount3 -uz /home/rok/distsystem/nuefs
pkill -9 nuefsd
```

## Debugging

Daemon logs are written to `$XDG_RUNTIME_DIR/nuefsd.log` (typically `/run/user/1000/nuefsd.log`), NOT stdout.

```bash
# View daemon logs
cat /run/user/1000/nuefsd.log

# Tail logs in real-time (from another terminal)
tail -f /run/user/1000/nuefsd.log

# Start daemon with custom log path
nuefsd --log /tmp/nuefsd-debug.log

# Enable debug logging via RUST_LOG (set before daemon starts)
RUST_LOG=debug pixi run nue mount
```

Log levels: `error`, `warn`, `info`, `debug`, `trace`

## Development

```bash
pixi run develop  # Build and install the package
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Python                               │
│  handle = nuefs.open(root)                                  │
│  handle.update(entries)                                     │
│  handle.which(path)                                         │
│  handle.close()                                             │
└─────────────────────────────────────────────────────────────┘
                              │ pyo3 FFI
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Rust Extension (_nuefs.so)                │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ #[pyclass]  │    │ IPC Client  │    │ Auto-start  │     │
│  │ ManifestEntry│   │ Unix Socket │    │ nuefsd      │     │
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
