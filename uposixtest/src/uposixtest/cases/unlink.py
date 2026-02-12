"""unlink tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("unlink removes file")
def test_unlink_remove(ctx: TestContext) -> None:
    p = ctx.tmp_path("doomed.txt")
    p.write_text("bye")
    assert p.exists()
    os.unlink(p)
    assert not p.exists()


@test("unlink nonexistent raises ENOENT")
def test_unlink_enoent(ctx: TestContext) -> None:
    try:
        os.unlink(ctx.tmp_path("no_such_file"))
        raise AssertionError("expected FileNotFoundError")
    except FileNotFoundError:
        pass


@test("open then unlink, fd still readable")
def test_unlink_fd_survives(ctx: TestContext) -> None:
    p = ctx.tmp_path("fd_unlink.txt")
    p.write_text("still readable")
    fd = os.open(p, os.O_RDONLY)
    try:
        os.unlink(p)
        assert not p.exists()
        data = os.read(fd, 100)
        assert data == b"still readable", f"expected b'still readable', got {data!r}"
    finally:
        os.close(fd)
