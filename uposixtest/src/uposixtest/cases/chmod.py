"""chmod tests."""

import os

from uposixtest import test
from uposixtest.context import TestContext


@test("chmod changes file mode")
def test_chmod_file(ctx: TestContext) -> None:
    p = ctx.tmp_path("chmod.txt")
    p.write_text("test")
    os.chmod(p, 0o600)
    mode = os.stat(p).st_mode & 0o777
    assert mode == 0o600, f"expected 0o600, got {oct(mode)}"
    os.chmod(p, 0o755)
    mode = os.stat(p).st_mode & 0o777
    assert mode == 0o755, f"expected 0o755, got {oct(mode)}"


@test("chmod changes directory mode")
def test_chmod_dir(ctx: TestContext) -> None:
    d = ctx.tmp_path("chmod_dir")
    os.mkdir(d, 0o755)
    os.chmod(d, 0o700)
    mode = os.stat(d).st_mode & 0o777
    assert mode == 0o700, f"expected 0o700, got {oct(mode)}"
