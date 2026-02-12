"""truncate, ftruncate tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("truncate shrinks file")
def test_truncate_shrink(ctx: TestContext) -> None:
    p = ctx.tmp_path("trunc.txt")
    p.write_text("hello world")
    os.truncate(p, 5)
    assert p.read_text() == "hello"


@test("ftruncate via fd")
def test_ftruncate(ctx: TestContext) -> None:
    p = ctx.tmp_path("ftrunc.txt")
    p.write_text("hello world")
    fd = os.open(p, os.O_WRONLY)
    try:
        os.ftruncate(fd, 5)
    finally:
        os.close(fd)
    assert p.read_text() == "hello"


@test("truncate extend zero-fills")
def test_truncate_extend(ctx: TestContext) -> None:
    p = ctx.tmp_path("trunc_ext.txt")
    p.write_text("hi")
    os.truncate(p, 10)
    data = p.read_bytes()
    assert len(data) == 10, f"expected length 10, got {len(data)}"
    assert data[:2] == b"hi"
    assert data[2:] == b"\x00" * 8
