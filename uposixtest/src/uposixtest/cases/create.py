"""O_CREAT, O_EXCL tests."""

import os
import stat

from uposixtest import test
from uposixtest.context import TestContext


@test("O_CREAT creates new file")
def test_create_new(ctx: TestContext) -> None:
    p = ctx.tmp_path("created.txt")
    fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(fd, b"new file")
    finally:
        os.close(fd)
    assert p.read_text() == "new file"


@test("O_CREAT|O_EXCL on existing raises EEXIST")
def test_create_excl_exists(ctx: TestContext) -> None:
    p = ctx.tmp_path("exists.txt")
    p.write_text("already here")
    try:
        fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
        raise AssertionError("expected FileExistsError")
    except FileExistsError:
        pass


@test("create with specific mode")
def test_create_mode(ctx: TestContext) -> None:
    p = ctx.tmp_path("mode.txt")
    fd = os.open(p, os.O_CREAT | os.O_WRONLY, 0o600)
    os.close(fd)
    st = os.stat(p)
    mode = st.st_mode & 0o777
    assert mode == 0o600, f"expected 0o600, got {oct(mode)}"
