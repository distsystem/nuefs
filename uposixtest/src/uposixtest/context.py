"""Per-test isolated working directory and helpers."""

import os
import pathlib
import shutil


class TestContext:
    def __init__(self, root: pathlib.Path, test_name: str) -> None:
        self.root = root
        safe_name = test_name.replace(" ", "_").replace("/", "_")
        self.workdir = root / ".uposixtest" / safe_name

    def setup(self) -> None:
        self.workdir.mkdir(parents=True, exist_ok=True)

    def tmp_path(self, name: str) -> pathlib.Path:
        return self.workdir / name

    def cleanup(self) -> None:
        if self.workdir.exists():
            shutil.rmtree(self.workdir, ignore_errors=True)
