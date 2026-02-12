"""utime tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("utime changes mtime")
def test_utime_mtime(ctx: TestContext) -> None:
    p = ctx.tmp_path("utime.txt")
    p.write_text("utime test")
    before = p.stat().st_mtime_ns
    future_s = (before + 1_000_000_000) / 1e9
    os.utime(p, (future_s, future_s))
    after = p.stat().st_mtime_ns
    assert after > before, f"mtime did not advance: {before} -> {after}"


@test("utime with explicit atime and mtime")
def test_utime_explicit(ctx: TestContext) -> None:
    p = ctx.tmp_path("utime2.txt")
    p.write_text("explicit")
    atime = 1_700_000_000.0
    mtime = 1_700_000_500.0
    os.utime(p, (atime, mtime))
    st = os.stat(p)
    assert abs(st.st_atime - atime) < 1.0, f"atime mismatch: {st.st_atime} vs {atime}"
    assert abs(st.st_mtime - mtime) < 1.0, f"mtime mismatch: {st.st_mtime} vs {mtime}"
