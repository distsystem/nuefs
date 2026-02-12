"""Test discovery, execution, and TAP/JSON output."""

import fnmatch
import json
import pathlib
import shutil
import sys
import traceback

import uposixtest.cases  # noqa: F401 — triggers test registration
from uposixtest import _TESTS
from uposixtest.context import TestContext


class SkipTest(Exception):
    pass


class TestRunner:
    def run(
        self,
        root: pathlib.Path,
        *,
        filter: str | None = None,
        json_output: bool = False,
        list_only: bool = False,
    ) -> int:
        tests = _TESTS
        if filter:
            tests = [
                t for t in tests
                if fnmatch.fnmatch(t.name, f"*{filter}*")
                or fnmatch.fnmatch(t.category, f"*{filter}*")
            ]

        if list_only:
            for t in tests:
                print(f"{t.category}: {t.name}")
            return 0

        base_dir = root / ".uposixtest"
        base_dir.mkdir(parents=True, exist_ok=True)

        results: list[dict] = []
        failures = 0

        if not json_output:
            print("TAP version 13")
            print(f"1..{len(tests)}")

        for i, tc in enumerate(tests, 1):
            ctx = TestContext(root, tc.name)
            ctx.setup()
            result: dict = {"id": i, "category": tc.category, "name": tc.name}
            try:
                tc.func(ctx)
                result["ok"] = True
                if not json_output:
                    print(f"ok {i} - {tc.category}: {tc.name}")
            except SkipTest as e:
                result["ok"] = True
                result["skip"] = str(e)
                if not json_output:
                    print(f"ok {i} - {tc.category}: {tc.name} # SKIP {e}")
            except Exception as e:
                failures += 1
                result["ok"] = False
                result["message"] = str(e)
                result["traceback"] = traceback.format_exc()
                if not json_output:
                    print(f"not ok {i} - {tc.category}: {tc.name}")
                    print("  ---")
                    print(f'  message: "{e}"')
                    print("  ...")
            finally:
                ctx.cleanup()

            results.append(result)

        shutil.rmtree(base_dir, ignore_errors=True)

        if json_output:
            summary = {
                "total": len(tests),
                "passed": len(tests) - failures,
                "failed": failures,
                "results": results,
            }
            json.dump(summary, sys.stdout, indent=2)
            print()

        return 1 if failures else 0
