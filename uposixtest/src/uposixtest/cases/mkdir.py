"""mkdir, rmdir tests."""

import errno
import os
import stat

from uposixtest import test
from uposixtest.context import TestContext


@test("mkdir and rmdir lifecycle")
def test_mkdir_rmdir(ctx: TestContext) -> None:
    d = ctx.tmp_path("testdir")
    os.mkdir(d)
    assert d.is_dir()
    os.rmdir(d)
    assert not d.exists()


@test("mkdir with mode")
def test_mkdir_mode(ctx: TestContext) -> None:
    d = ctx.tmp_path("modedir")
    os.mkdir(d, 0o700)
    st = os.stat(d)
    assert stat.S_ISDIR(st.st_mode)
    assert st.st_mode & 0o700 == 0o700


@test("rmdir ENOTEMPTY on non-empty dir")
def test_rmdir_notempty(ctx: TestContext) -> None:
    d = ctx.tmp_path("notempty")
    os.mkdir(d)
    (d / "child.txt").write_bytes(b"x")  # use pathlib for convenience
    try:
        os.rmdir(d)
        raise AssertionError("expected ENOTEMPTY")
    except OSError as e:
        assert e.errno == errno.ENOTEMPTY, f"expected ENOTEMPTY, got errno {e.errno}"
