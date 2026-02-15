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
│                   Python (pure, no Rust ext)                 │
│  handle = nuefs.open(root)                                  │
│  handle.update(entries)                                     │
│  handle.which(path)                                         │
│  handle.close()                                             │
└─────────────────────────────────────────────────────────────┘
                    │ protobuf over Unix socket
                    │ (length-prefix framing, betterproto2)
                    ▼
┌─────────────────────────────────────────────────────────────┐
│                   Rust Daemon (nuefsd)                      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ IPC Server  │    │ Mount       │    │ FUSE        │     │
│  │ prost/proto │───▶│ Manager     │───▶│ Sessions    │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

IPC protocol: `proto/nuefs.proto` (single source of truth)
- Rust side: prost (build.rs compiles proto)
- Python side: betterproto2 (buf generate)

## Project Structure

```
nuefs/
├── proto/nuefs.proto    # IPC protocol definition
├── build.rs             # prost-build proto compilation
├── Cargo.toml           # Rust daemon package (nuefs_rs)
├── pyproject.toml       # Python package (hatchling)
├── src/
│   ├── lib.rs           # module declarations + proto include
│   ├── types.rs         # internal types + proto conversions
│   ├── daemon/
│   │   ├── mod.rs
│   │   ├── server.rs    # protobuf IPC server (tokio)
│   │   ├── manager.rs   # mount manager
│   │   └── fuse.rs      # FUSE implementation (fuse_mt)
│   └── bin/
│       └── nuefsd.rs    # daemon entry
├── python/
│   └── nuefs/
│       ├── __init__.py  # public API re-exports
│       ├── _ipc.py      # protobuf IPC client
│       ├── _proto/      # generated betterproto2 code
│       ├── core.py      # dataclasses + Handle
│       ├── manifest.py  # manifest parsing
│       └── cli.py       # CLI (nue mount/unmount/status/stop)
└── nuefsd/
    ├── pixi.toml        # separate pixi package for daemon
    └── recipe.yaml      # rattler-build recipe
```
