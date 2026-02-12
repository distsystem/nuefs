"""fsync, fdatasync tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("fsync after write")
def test_fsync(ctx: TestContext) -> None:
    p = ctx.tmp_path("fsync.txt")
    fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(fd, b"sync me")
        os.fsync(fd)
    finally:
        os.close(fd)
    assert p.read_text() == "sync me"


@test("fdatasync after write")
def test_fdatasync(ctx: TestContext) -> None:
    p = ctx.tmp_path("fdatasync.txt")
    fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(fd, b"datasync me")
        os.fdatasync(fd)
    finally:
        os.close(fd)
    assert p.read_text() == "datasync me"
