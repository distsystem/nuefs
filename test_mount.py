#!/usr/bin/env python
"""Test script for NueFS"""

import time
from pathlib import Path

import nuefs


def setup_test_dirs():
    """Create test directories and files."""
    base = Path("/tmp/nuefs-test")
    real_dir = base / "real"
    layer_dir = base / "layer"
    mount_point = base / "mnt"

    # Clean up old mount if exists
    import subprocess
    subprocess.run(["fusermount", "-uz", str(mount_point)], capture_output=True)

    # Create directories
    for d in [real_dir, layer_dir, mount_point]:
        d.mkdir(parents=True, exist_ok=True)

    # Create test files in layer
    (layer_dir / "file1.txt").write_text("from layer")
    (layer_dir / "subdir").mkdir(exist_ok=True)
    (layer_dir / "subdir" / "nested.txt").write_text("nested file")

    return real_dir, layer_dir, mount_point


def main():
    real_dir, layer_dir, mount_point = setup_test_dirs()

    print(f"Real dir: {real_dir}")
    print(f"Layer dir: {layer_dir}")
    print(f"Mount point: {mount_point}")
    print()

    # Create mount config
    mounts = [
        nuefs.Mount(target=Path("."), source=layer_dir),
    ]

    print("Mounting...")
    try:
        handle = nuefs.mount(mount_point, mounts)
        print("Mounted successfully!")

        # Give FUSE time to initialize
        time.sleep(0.5)

        # List files
        print("\nFiles in mount point:")
        for f in mount_point.iterdir():
            print(f"  {f.name}")

        # Query which
        print("\nQuerying ownership:")
        for name in ["file1.txt", "subdir"]:
            info = nuefs.which(handle, name)
            if info:
                print(f"  {name}: owner={info.owner}, path={info.backend_path}")
            else:
                print(f"  {name}: not found")

        print("\nUnmounting...")
        nuefs.unmount(handle)
        print("Unmounted successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
