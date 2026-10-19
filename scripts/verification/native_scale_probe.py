"""Synthetic bounded-batch native comparison probe; no source or release acceptance."""
import argparse
import json
import resource
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.native_verification import NativeReadLimits, compare_native_inputs
from dagster_hifld.validation import sha256_file


def run(root: Path, rows: int) -> dict:
    if root.exists() and any(root.iterdir()):
        raise ValueError("Use a new empty synthetic probe directory")
    left, right = root / "baseline", root / "candidate"
    left.mkdir(parents=True); right.mkdir()
    schema = pa.schema([("id", pa.int64()), ("region", pa.string()), ("value", pa.string())])
    for offset in range(0, rows, 1024):
        count = min(1024, rows - offset)
        table = pa.Table.from_pydict({"id": list(range(2**53 + offset, 2**53 + offset + count)), "region": ["synthetic"] * count,
                                    "value": ["same"] * count}, schema=schema)
        name = f"{offset:012d}.parquet"
        pq.write_table(table, left / name, row_group_size=256)
        if offset + count == rows:
            values = table.to_pylist()
            values[-1]["value"] = "changed-final-row"
            pq.write_table(pa.Table.from_pylist(values, schema=schema), right / name, row_group_size=256)
        else:
            shutil.copyfile(left / name, right / name)
    def spec(folder):
        return {"schema_version": 1, "format": "parquet", "root": str(folder), "geometry_column": None,
                "objects": [{"key": p.name, "sha256": sha256_file(p), "bytes": p.stat().st_size} for p in sorted(folder.glob("*.parquet"))]}
    baseline, candidate = spec(left), spec(right)
    policy = {"schema_version": 1, "status": "proposed", "geometry": False, "crs": None, "id_fields": ["id"], "coverage_fields": ["region"]}
    result = compare_native_inputs(baseline, candidate, policy, root / "comparison", baseline_reference={"acceptance": "unapproved_candidate",
                                  "data_objects": [{"sha256": obj["sha256"], "bytes": obj["bytes"]} for obj in baseline["objects"]]},
                                  limits=NativeReadLimits(batch_rows=256))
    field_changes = [json.loads(line) for line in (root / "comparison/baseline_field_changes.jsonl").read_text().splitlines()]
    assert result["populations"]["baseline"]["rows"] == rows
    assert result["populations"]["candidate"]["rows"] == rows
    assert next(item for item in field_changes if item["field"] == "value")["changed"] == 1
    evidence = {"classification": "synthetic unapproved native resource probe", "rows_per_input": rows, "objects_per_input": len(baseline["objects"]),
                "batch_rows": 256, "peak_process_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "native_input_bytes": sum(obj["bytes"] for s in [baseline, candidate] for obj in s["objects"]),
                "status": result["status"], "sole_changed_final_row_found": True, "hard_os_memory_disk_quota": False}
    (root / "probe_result.json").write_text(json.dumps(evidence, indent=2) + "\n")
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    if args.rows < 1:
        parser.error("rows must be positive")
    print(json.dumps(run(args.work_dir.resolve(), args.rows)))
