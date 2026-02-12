"""Unprivileged POSIX filesystem conformance tests."""

from dataclasses import dataclass, field
from typing import Callable

from uposixtest.context import TestContext as TestContext


@dataclass
class TestCase:
    name: str
    func: Callable[[TestContext], None]
    category: str


_TESTS: list[TestCase] = []


def test(name: str, *, category: str = ""):
    def decorator(fn: Callable[[TestContext], None]) -> Callable[[TestContext], None]:
        cat = category or fn.__module__.rsplit(".", 1)[-1]
        _TESTS.append(TestCase(name=name, func=fn, category=cat))
        return fn
    return decorator


def run(root: str, *, filter: str | None = None, json_output: bool = False) -> int:
    from uposixtest.runner import TestRunner
    import pathlib

    runner = TestRunner()
    return runner.run(pathlib.Path(root), filter=filter, json_output=json_output)
