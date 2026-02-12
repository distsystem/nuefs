"""CLI entry: python -m uposixtest [-f FILTER] [-j] [-l] PATH."""

import argparse
import sys

from uposixtest.runner import TestRunner


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="uposixtest",
        description="Unprivileged POSIX filesystem conformance tests",
    )
    parser.add_argument("path", metavar="PATH", help="mount path to test")
    parser.add_argument("-f", "--filter", help="only run tests matching pattern")
    parser.add_argument("-j", "--json", action="store_true", help="output JSON instead of TAP")
    parser.add_argument("-l", "--list", action="store_true", help="list available tests")

    args = parser.parse_args()

    import pathlib

    root = pathlib.Path(args.path)
    if not root.is_dir():
        print(f"error: {root} is not a directory", file=sys.stderr)
        sys.exit(2)

    runner = TestRunner()
    code = runner.run(root, filter=args.filter, json_output=args.json, list_only=args.list)
    sys.exit(code)


if __name__ == "__main__":
    main()
