"""write, pwrite, O_APPEND tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("write and verify content")
def test_write_verify(ctx: TestContext) -> None:
    p = ctx.tmp_path("write.txt")
    p.write_text("hello")
    assert p.read_text() == "hello"


@test("O_APPEND appends data")
def test_write_append(ctx: TestContext) -> None:
    p = ctx.tmp_path("append.txt")
    p.write_text("line1\n")
    fd = os.open(p, os.O_WRONLY | os.O_APPEND)
    try:
        os.write(fd, b"line2\n")
    finally:
        os.close(fd)
    assert p.read_text() == "line1\nline2\n"


@test("pwrite at offset")
def test_pwrite_offset(ctx: TestContext) -> None:
    p = ctx.tmp_path("pwrite.txt")
    p.write_text("aaaaaaaaaa")
    fd = os.open(p, os.O_WRONLY)
    try:
        os.pwrite(fd, b"bbb", 3)
    finally:
        os.close(fd)
    assert p.read_text() == "aaabbbaaaa"
