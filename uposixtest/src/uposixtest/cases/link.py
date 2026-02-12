"""hard link tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("hard link creates second name with nlink=2")
def test_hard_link(ctx: TestContext) -> None:
    orig = ctx.tmp_path("orig.txt")
    hard = ctx.tmp_path("hard.txt")
    orig.write_text("linkable")
    os.link(orig, hard)
    assert hard.exists()
    assert hard.read_text() == "linkable"
    assert os.stat(hard).st_nlink == 2


@test("unlink one hard link, other survives")
def test_hard_link_unlink(ctx: TestContext) -> None:
    a = ctx.tmp_path("hl_a.txt")
    b = ctx.tmp_path("hl_b.txt")
    a.write_text("shared")
    os.link(a, b)
    os.unlink(a)
    assert not a.exists()
    assert b.read_text() == "shared"
