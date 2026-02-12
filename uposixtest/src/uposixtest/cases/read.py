"""open, read, pread, large file tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("read file content")
def test_read_content(ctx: TestContext) -> None:
    p = ctx.tmp_path("read.txt")
    p.write_text("hello world")
    assert p.read_text() == "hello world"


@test("pread at offset")
def test_pread_offset(ctx: TestContext) -> None:
    p = ctx.tmp_path("pread.txt")
    p.write_text("abcdefghij")
    fd = os.open(p, os.O_RDONLY)
    try:
        data = os.pread(fd, 3, 4)
        assert data == b"efg", f"expected b'efg', got {data!r}"
    finally:
        os.close(fd)


@test("large file (1 MiB)")
def test_read_large(ctx: TestContext) -> None:
    p = ctx.tmp_path("large.bin")
    data = os.urandom(1024 * 1024)
    p.write_bytes(data)
    assert p.read_bytes() == data


@test("empty file reads as empty")
def test_read_empty(ctx: TestContext) -> None:
    p = ctx.tmp_path("empty.txt")
    p.write_bytes(b"")
    assert p.read_bytes() == b""
    assert p.stat().st_size == 0
