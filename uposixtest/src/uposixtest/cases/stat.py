"""stat, lstat, fstat tests."""

import os
import stat

from uposixtest import test
from uposixtest.context import TestContext


@test("regular file returns S_ISREG and correct size")
def test_stat_regular(ctx: TestContext) -> None:
    p = ctx.tmp_path("hello.txt")
    p.write_text("hello")
    st = os.stat(p)
    assert stat.S_ISREG(st.st_mode), f"expected S_ISREG, got mode {oct(st.st_mode)}"
    assert st.st_size == 5, f"expected size 5, got {st.st_size}"


@test("directory returns S_ISDIR")
def test_stat_directory(ctx: TestContext) -> None:
    d = ctx.tmp_path("subdir")
    d.mkdir()
    st = os.stat(d)
    assert stat.S_ISDIR(st.st_mode), f"expected S_ISDIR, got mode {oct(st.st_mode)}"


@test("lstat symlink returns S_ISLNK")
def test_lstat_symlink(ctx: TestContext) -> None:
    target = ctx.tmp_path("target.txt")
    target.write_text("x")
    link = ctx.tmp_path("link")
    os.symlink(target, link)
    st = os.lstat(link)
    assert stat.S_ISLNK(st.st_mode), f"expected S_ISLNK, got mode {oct(st.st_mode)}"
    st2 = os.stat(link)
    assert stat.S_ISREG(st2.st_mode), "stat follow should give S_ISREG"


@test("fstat on open fd")
def test_fstat_fd(ctx: TestContext) -> None:
    p = ctx.tmp_path("fstat.txt")
    p.write_text("fstat")
    fd = os.open(p, os.O_RDONLY)
    try:
        st = os.fstat(fd)
        assert stat.S_ISREG(st.st_mode)
        assert st.st_size == 5
    finally:
        os.close(fd)


@test("ENOENT for nonexistent file")
def test_stat_enoent(ctx: TestContext) -> None:
    try:
        os.stat(ctx.tmp_path("no_such_file"))
        raise AssertionError("expected FileNotFoundError")
    except FileNotFoundError:
        pass
