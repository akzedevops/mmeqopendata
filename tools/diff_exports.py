#!/usr/bin/env python3
"""Compare two quake_exports trees (e.g. Python vs Go exporter output).

Used by the shadow CI job (specs/002 increment I5): both exporters run against the
same API state into separate directories, then this script asserts the trees are
equivalent. Comparison is byte-equality first; on mismatch, CSV/JSON files are
compared structurally with a float tolerance (default 1e-9) so a documented
float-repr difference doesn't fail the gate while any real divergence does.

Usage: python tools/diff_exports.py DIR_A DIR_B [--tolerance 1e-9] [--max-report 20]
Exit code 0 = equivalent, 1 = divergent (differences printed).
"""

import argparse
import csv
import json
import math
import sys
from pathlib import Path

REL_EXTS = {".csv", ".json"}


def collect(root: Path) -> dict:
    return {
        str(p.relative_to(root)): p
        for p in sorted(root.rglob("*"))
        if p.is_file() and p.suffix in REL_EXTS
    }


def close(a: str, b: str, tol: float) -> bool:
    """Cell equality: exact string, else both-parse-as-float within tolerance."""
    if a == b:
        return True
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    if math.isnan(fa) and math.isnan(fb):
        return True
    return math.isclose(fa, fb, rel_tol=tol, abs_tol=tol)


def pair_rows(ra: list, rb: list, id_idx_a: int, id_idx_b: int):
    """Pair data rows by event id (order-insensitive) when both sides have a
    usable, duplicate-free id column; otherwise pair positionally.

    Order-insensitivity is deliberate: the Python exporter writes rows in
    ThreadPoolExecutor completion order, which is nondeterministic run-to-run,
    while the Go exporter writes deterministic date order. Downstream consumers
    key/sort the data, so row order is not part of the artifact contract.
    """
    if id_idx_a >= 0 and id_idx_b >= 0:
        ka = [r[id_idx_a] if id_idx_a < len(r) else None for r in ra]
        kb = [r[id_idx_b] if id_idx_b < len(r) else None for r in rb]
        if (None not in ka and None not in kb
                and len(set(ka)) == len(ka) and len(set(kb)) == len(kb)):
            ma, mb = dict(zip(ka, ra)), dict(zip(kb, rb))
            pairs = [(f"id={k}", ma[k], mb[k]) for k in ka if k in mb]
            unmatched = [f"row only in a: id={k}" for k in ka if k not in mb]
            unmatched += [f"row only in b: id={k}" for k in kb if k not in ma]
            return pairs, unmatched
    pairs = [(f"line {i}", a, b) for i, (a, b) in enumerate(zip(ra, rb), start=2)]
    return pairs, []


def diff_csv(pa: Path, pb: Path, tol: float) -> list:
    problems = []
    with open(pa, newline="", encoding="utf-8") as fa, open(
        pb, newline="", encoding="utf-8"
    ) as fb:
        ra, rb = list(csv.reader(fa)), list(csv.reader(fb))
    if not ra or not rb:
        return [f"empty file (a={len(ra)} rows, b={len(rb)} rows)"] if ra != rb else []
    if ra[0] != rb[0]:
        return [f"header mismatch:\n  a: {ra[0]}\n  b: {rb[0]}"]
    header = ra[0]
    if len(ra) != len(rb):
        problems.append(f"row count: a={len(ra) - 1} b={len(rb) - 1}")
    id_idx = header.index("id") if "id" in header else -1
    pairs, unmatched = pair_rows(ra[1:], rb[1:], id_idx, id_idx)
    problems.extend(unmatched)
    for label, rowa, rowb in pairs:
        if len(rowa) != len(rowb):
            problems.append(f"{label}: field count {len(rowa)} vs {len(rowb)}")
            continue
        for col, (a, b) in zip(header, zip(rowa, rowb)):
            if not close(a, b, tol):
                problems.append(f"{label} col {col!r}: {a!r} != {b!r}")
    return problems


def by_id(records: list):
    """Return {id: record} when every element is a dict with a unique id."""
    out = {}
    for r in records:
        if not isinstance(r, dict) or "id" not in r or r["id"] in out:
            return None
        out[r["id"]] = r
    return out


def json_equal(a, b, tol: float, path: str, problems: list) -> None:
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                problems.append(f"{path}.{k}: present in only one side")
                continue
            json_equal(a[k], b[k], tol, f"{path}.{k}", problems)
    elif isinstance(a, list) and isinstance(b, list):
        # Event arrays are compared keyed by id (see pair_rows for rationale).
        ma, mb = by_id(a), by_id(b)
        if ma is not None and mb is not None:
            for k in sorted(set(ma) | set(mb), key=str):
                if k not in ma or k not in mb:
                    problems.append(f"{path}[id={k}]: present in only one side")
                    continue
                json_equal(ma[k], mb[k], tol, f"{path}[id={k}]", problems)
            return
        if len(a) != len(b):
            problems.append(f"{path}: length {len(a)} vs {len(b)}")
            return
        for i, (xa, xb) in enumerate(zip(a, b)):
            json_equal(xa, xb, tol, f"{path}[{i}]", problems)
    elif isinstance(a, (int, float)) and isinstance(b, (int, float)) and not (
        isinstance(a, bool) or isinstance(b, bool)
    ):
        fa, fb = float(a), float(b)
        if not (
            (math.isnan(fa) and math.isnan(fb))
            or math.isclose(fa, fb, rel_tol=tol, abs_tol=tol)
        ):
            problems.append(f"{path}: {a!r} != {b!r}")
    elif a != b:
        problems.append(f"{path}: {a!r} != {b!r}")


def diff_json(pa: Path, pb: Path, tol: float) -> list:
    try:
        a = json.loads(pa.read_text(encoding="utf-8"))
        b = json.loads(pb.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        return [f"unparseable JSON: {e}"]
    problems: list = []
    json_equal(a, b, tol, "$", problems)
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dir_a", type=Path)
    ap.add_argument("dir_b", type=Path)
    ap.add_argument("--tolerance", type=float, default=1e-9)
    ap.add_argument("--max-report", type=int, default=20,
                    help="max differences printed per file")
    args = ap.parse_args()

    fa, fb = collect(args.dir_a), collect(args.dir_b)
    failed = False

    only_a = sorted(set(fa) - set(fb))
    only_b = sorted(set(fb) - set(fa))
    for rel in only_a:
        print(f"ONLY IN {args.dir_a}: {rel}")
        failed = True
    for rel in only_b:
        print(f"ONLY IN {args.dir_b}: {rel}")
        failed = True

    byte_identical = 0
    for rel in sorted(set(fa) & set(fb)):
        pa, pb = fa[rel], fb[rel]
        if pa.read_bytes() == pb.read_bytes():
            byte_identical += 1
            continue
        problems = (
            diff_csv(pa, pb, args.tolerance)
            if pa.suffix == ".csv"
            else diff_json(pa, pb, args.tolerance)
        )
        if not problems:
            # Bytes differ but content is equivalent within tolerance —
            # report it (it should become a golden-tested repr rule) but pass.
            print(f"TOLERATED (bytes differ, content equivalent): {rel}")
            continue
        failed = True
        print(f"DIFFERS: {rel} ({len(problems)} differences)")
        for p in problems[: args.max_report]:
            print(f"  {p}")
        if len(problems) > args.max_report:
            print(f"  ... and {len(problems) - args.max_report} more")

    total = len(set(fa) & set(fb))
    print(
        f"\nCompared {total} common files: {byte_identical} byte-identical, "
        f"{len(only_a) + len(only_b)} unmatched."
    )
    if failed:
        print("RESULT: DIVERGENT")
        return 1
    print("RESULT: EQUIVALENT")
    return 0


if __name__ == "__main__":
    sys.exit(main())
