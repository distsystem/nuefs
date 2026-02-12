"""access(2) tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("F_OK for existing file")
def test_access_exists(ctx: TestContext) -> None:
    p = ctx.tmp_path("exists.txt")
    p.write_text("x")
    assert os.access(p, os.F_OK), "F_OK should be True for existing file"


@test("F_OK for nonexistent file returns False")
def test_access_nonexistent(ctx: TestContext) -> None:
    assert not os.access(ctx.tmp_path("no_such"), os.F_OK)


@test("R_OK and W_OK on writable file")
def test_access_rw(ctx: TestContext) -> None:
    p = ctx.tmp_path("rw.txt")
    p.write_text("x")
    assert os.access(p, os.R_OK), "R_OK should be True"
    assert os.access(p, os.W_OK), "W_OK should be True"
