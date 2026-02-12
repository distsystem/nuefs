"""rename tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("rename in same directory")
def test_rename_same_dir(ctx: TestContext) -> None:
    src = ctx.tmp_path("src.txt")
    dst = ctx.tmp_path("dst.txt")
    src.write_text("rename me")
    os.rename(src, dst)
    assert not src.exists()
    assert dst.read_text() == "rename me"


@test("rename overwrites existing target")
def test_rename_overwrite(ctx: TestContext) -> None:
    a = ctx.tmp_path("old.txt")
    b = ctx.tmp_path("new.txt")
    a.write_text("old")
    b.write_text("new")
    os.rename(b, a)
    assert not b.exists()
    assert a.read_text() == "new"


@test("rename directory")
def test_rename_directory(ctx: TestContext) -> None:
    src = ctx.tmp_path("dir_src")
    dst = ctx.tmp_path("dir_dst")
    os.mkdir(src)
    (src / "inner.txt").write_bytes(b"inside")
    os.rename(src, dst)
    assert not src.exists()
    assert dst.is_dir()
    assert (dst / "inner.txt").read_bytes() == b"inside"
