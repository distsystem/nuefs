"""symlink, readlink tests."""

import os
import stat

from uposixtest import test
from uposixtest.context import TestContext


@test("symlink + readlink roundtrip")
def test_symlink_readlink(ctx: TestContext) -> None:
    target = ctx.tmp_path("target.txt")
    target.write_text("target content")
    link = ctx.tmp_path("sym")
    os.symlink(target, link)
    assert os.readlink(link) == str(target)


@test("read through symlink")
def test_symlink_read_through(ctx: TestContext) -> None:
    target = ctx.tmp_path("real.txt")
    target.write_text("real content")
    link = ctx.tmp_path("sym_read")
    os.symlink(target, link)
    assert link.read_text() == "real content"


@test("dangling symlink: lstat succeeds, exists() returns False")
def test_symlink_dangling(ctx: TestContext) -> None:
    link = ctx.tmp_path("dangle")
    os.symlink("no_such_target", link)
    assert os.readlink(link) == "no_such_target"
    assert not link.exists()
    st = os.lstat(link)
    assert stat.S_ISLNK(st.st_mode)
