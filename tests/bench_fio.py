"""fio benchmark for nuefs FUSE mount."""

import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

from tests.nue_mount import mount

YAML_CONTENT = """\
apiVersion: nue/v1
mounts:
- source: ./data/
  gitignore: false
"""

FIO_COMMON = {
    "direct": 0,
    "ioengine": "posixaio",
    "runtime": 10,
    "time_based": 1,
}

JOBS = [
    ("seq_read", {"rw": "read", "bs": "128k", "size": "64m"}),
    ("seq_write", {"rw": "write", "bs": "128k", "size": "64m"}),
    ("rand_read", {"rw": "randread", "bs": "4k", "size": "16m"}),
    ("rand_write", {"rw": "randwrite", "bs": "4k", "size": "16m"}),
    ("mixed_rw", {"rw": "randrw", "bs": "4k", "size": "16m", "rwmixread": 70}),
    ("seq_read_multi", {"rw": "read", "bs": "128k", "size": "32m", "numjobs": 4}),
]


def _check_prereqs() -> None:
    missing = []
    if not pathlib.Path("/dev/fuse").exists():
        missing.append("/dev/fuse")
    for cmd in ("nuefsd", "nue", "fio", "findmnt"):
        if shutil.which(cmd) is None:
            missing.append(cmd)
    if missing:
        sys.exit(f"missing prerequisites: {', '.join(missing)}")


def _run_fio(directory: pathlib.Path, *, name: str, **kwargs: int | str) -> dict:
    params = {**FIO_COMMON, "directory": str(directory), **kwargs}
    args = ["fio", f"--name={name}", "--output-format=json"]
    args.extend(f"--{k}={v}" for k, v in params.items())
    r = subprocess.run(args, check=True, capture_output=True, text=True)
    return json.loads(r.stdout)


def _extract(job: dict, rw: str) -> tuple[float, float, float, float]:
    s = job[rw]
    lat = s["clat_ns"]["percentile"]
    return (
        s["iops"],
        s["bw"] / 1024,  # KiB/s -> MiB/s
        lat["50.000000"] / 1e6,  # ns -> ms
        lat["99.000000"] / 1e6,
    )


def _print_row(name: str, iops: float, bw: float, p50: float, p99: float) -> None:
    print(f"  {name:<20s} {iops:>10.0f} {bw:>10.1f} {p50:>10.2f} {p99:>10.2f}")


def main() -> None:
    _check_prereqs()

    with tempfile.TemporaryDirectory(prefix="bench_fio_") as tmpdir:
        root = pathlib.Path(tmpdir)
        (root / "data").mkdir()
        (root / "data" / "seed.bin").write_bytes(os.urandom(1024))
        (root / "nue.yaml").write_text(YAML_CONTENT)

        with mount(root):
            print()
            print(f"  {'workload':<20s} {'IOPS':>10s} {'BW MiB/s':>10s} {'p50 ms':>10s} {'p99 ms':>10s}")
            print(f"  {'─' * 20} {'─' * 10} {'─' * 10} {'─' * 10} {'─' * 10}")

            for name, params in JOBS:
                rw_mode = params["rw"]
                data = _run_fio(root, name=name, **params)

                if rw_mode == "randrw":
                    for rw in ("read", "write"):
                        iops, bw, p50, p99 = _extract(data["jobs"][0], rw)
                        _print_row(f"{name}/{rw}", iops, bw, p50, p99)
                elif params.get("numjobs", 1) > 1:
                    for i, job in enumerate(data["jobs"]):
                        rw = "read" if "read" in rw_mode else "write"
                        iops, bw, p50, p99 = _extract(job, rw)
                        _print_row(f"{name}[{i}]", iops, bw, p50, p99)
                else:
                    rw = "read" if "read" in rw_mode else "write"
                    iops, bw, p50, p99 = _extract(data["jobs"][0], rw)
                    _print_row(name, iops, bw, p50, p99)

            print()


if __name__ == "__main__":
    main()
