"""readdir, scandir tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("lists created files")
def test_readdir_list(ctx: TestContext) -> None:
    for name in ("a.txt", "b.txt", "c.txt"):
        ctx.tmp_path(name).write_text(name)
    names = set(os.listdir(ctx.workdir))
    for name in ("a.txt", "b.txt", "c.txt"):
        assert name in names, f"{name} not found in listdir"


@test("scandir returns DirEntry with correct types")
def test_readdir_scandir(ctx: TestContext) -> None:
    ctx.tmp_path("file.txt").write_text("x")
    ctx.tmp_path("subdir").mkdir()
    entries = {e.name: e for e in os.scandir(ctx.workdir)}
    assert "file.txt" in entries
    assert entries["file.txt"].is_file()
    assert "subdir" in entries
    assert entries["subdir"].is_dir()
