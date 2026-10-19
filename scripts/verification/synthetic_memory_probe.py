"""Reproducible resource probe; synthetic mechanics, never baseline acceptance."""

import argparse
import json
import resource
import time
from pathlib import Path

from dagster_hifld.validation import compare_full_rows, sha256_file


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()
    root = args.work_dir
    root.mkdir(parents=True, exist_ok=True)
    left, right = root / "baseline.jsonl", root / "candidate.jsonl"
    with left.open("w") as baseline, right.open("w") as candidate:
        for i in range(args.rows):
            row = {"type": "Feature", "properties": {"id": str(i), "group": str(i), "value": "x" * 512}, "geometry": None}
            baseline.write(json.dumps(row) + "\n")
            if i == args.rows - 1:
                row["properties"]["value"] = "changed"
            candidate.write(json.dumps(row) + "\n")
    contract = {"id_fields": ["id"], "coverage_fields": ["group"], "geometry": False, "crs": None,
                "columns": {name: {"type": "string", "nullable": False} for name in ("id", "group", "value")}}
    started = time.monotonic()
    baseline_sha = sha256_file(left)
    baseline_reference = {"identity": "SYNTHETIC ONLY", "comparison_input": {
        "adapter": "typed-geojson-feature-jsonl-v1", "sha256": baseline_sha, "bytes": left.stat().st_size}}
    report = compare_full_rows(left, right, contract, root / "metadata", baseline_sha256=baseline_sha,
                               candidate_sha256=sha256_file(right), baseline_reference=baseline_reference)
    result = {"rows_each": args.rows, "input_bytes": left.stat().st_size + right.stat().st_size,
              "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
              "elapsed_seconds": round(time.monotonic() - started, 3), "status": report["status"],
              "differences": report["differences"]}
    print(json.dumps(result))
    if report["status"] != "needs_decision" or report["differences"]["attributes"] != 1:
        raise SystemExit("Incomplete or incorrect comparison")


if __name__ == "__main__":
    main()
