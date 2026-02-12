"""statvfs tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("f_bsize > 0")
def test_statvfs_bsize(ctx: TestContext) -> None:
    st = os.statvfs(ctx.root)
    assert st.f_bsize > 0, f"expected f_bsize > 0, got {st.f_bsize}"
