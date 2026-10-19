import copy
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import fiona
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from pyproj import CRS
from shapely import Point, Polygon, to_wkb

from dagster_hifld.native_verification import (
    FEATURE_ID, NativeReadLimits, compare_native_inputs, normalize_native_input,
)
from dagster_hifld.validation import sha256_file
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.release_approval import ReleaseApprovalError
from test_full_verification import accepted_fixture


def policy(spatial=True):
    return {"schema_version": 1, "status": "proposed", "geometry": spatial,
            "crs": "EPSG:4326" if spatial else None, "geometry_nullable": True,
            "id_fields": ["id"], "coverage_fields": ["state"]}


def spec_for(root, fmt, *, layer=None, geometry_column=None, partitions=None):
    suffix = {"geopackage": ".gpkg", "geoparquet": ".parquet", "parquet": ".parquet", "arrow_ipc_file": ".arrow"}[fmt]
    objects = [{"key": path.relative_to(root).as_posix(), "sha256": sha256_file(path), "bytes": path.stat().st_size,
                "partition": (partitions or {}).get(path.relative_to(root).as_posix(), {})}
               for path in sorted(root.rglob("*" + suffix))]
    result = {"schema_version": 1, "format": fmt, "root": str(root), "objects": objects, "geometry_column": geometry_column}
    if layer:
        result["layer"] = layer
    return result


def reference(spec):
    return {"acceptance": "unapproved_candidate", "dataset_slug": "synthetic", "file_slug": "typed",
            "version": "candidate-only", "data_objects": [{"sha256": obj["sha256"], "bytes": obj["bytes"]} for obj in spec["objects"]]}


def arrow_table(*, ids=None, spatial=True, id_type=None):
    ids = ids or [2 ** 53 + 1, -(2 ** 63), 2 ** 63 - 1]
    fields = [pa.field("id", id_type or pa.int64()), pa.field("state", pa.string()),
              pa.field("nullable", pa.int64()), pa.field("decimal", pa.decimal128(30, 8)),
              pa.field("day", pa.date32()), pa.field("instant", pa.timestamp("ns", tz="UTC")),
              pa.field("binary", pa.binary()), pa.field("floating", pa.float64())]
    if spatial:
        fields.append(pa.field("geometry", pa.binary()))
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
        "encoding": "WKB", "crs": CRS.from_epsg(4326).to_json_dict(), "geometry_types": ["Point", "Polygon"]}}}
    schema = pa.schema(fields, metadata={b"geo": json.dumps(geo).encode()} if spatial else None)
    rows = []
    for index, key in enumerate(ids):
        row = {"id": key, "state": "PA" if index == 0 else "VA", "nullable": None if index == 0 else 2 ** 53 + 1,
               "decimal": Decimal("12345678901234567890.12000000"), "day": date(2026, 10, 19),
               "instant": 1792368000000000001 + index, "binary": b"\x00\xff", "floating": -0.0}
        if spatial:
            geometry = Point(1, 2) if index == 0 else Polygon([(0, 0), (0, 1), (1, 1), (0, 0)]) if index == 1 else None
            row["geometry"] = to_wkb(geometry) if geometry is not None else None
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def native_gate_fixture(root):
    source, target, registry = accepted_fixture(root)
    prefix = "synthetic/points/vtest"
    candidate_root = Path(source.local_dir) / prefix / "geoparquet"
    baseline_root = root / "accepted-synthetic-baseline"
    candidate_root.mkdir(parents=True)
    baseline_root.mkdir()
    pq.write_table(arrow_table(spatial=False), candidate_root / "data.parquet")
    shutil.copyfile(candidate_root / "data.parquet", baseline_root / "data.parquet")
    left, right = spec_for(baseline_root, "parquet"), spec_for(candidate_root, "parquet")
    right["storage_prefix"] = "geoparquet"
    proposed = {**policy(False), "status": "reviewed"}  # SYNTHETIC ONLY.
    metadata_root = Path(source.local_dir) / prefix / "metadata"
    research = compare_native_inputs(left, right, proposed, metadata_root, baseline_reference=reference(left))
    value = json.loads(registry.read_text())
    entry = value["releases"][0]
    baseline = {**entry["baseline"], **research["baseline"], "acceptance": "synthetic_fixture_only",
                "decision": {"id": "SYNTHETIC-ONLY", "approved_by": "test-fixture", "approved_at": "2026-10-19"},
                "data_objects": [{"url": "https://example.invalid/synthetic.parquet", "sha256": left["objects"][0]["sha256"], "bytes": left["objects"][0]["bytes"]}]}
    result = compare_native_inputs(left, right, proposed, metadata_root, baseline_reference=baseline)
    if result["status"] != "pass":
        raise AssertionError(result)
    objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "vtest")}
    candidate = result["native_inputs"]["candidate"]["receipt"]
    entry.update(baseline=result["baseline"], contract_sha256=result["contract_sha256"],
                 staged_objects=objects, published_objects=objects,
                 report_sha256=objects[f"{prefix}/metadata/baseline_comparison.json"],
                 native_policy_sha256=candidate["policy_sha256"],
                 candidate_comparison_input={"key": f"{prefix}/metadata/candidate/normalized_records.jsonl",
                    "sha256": result["candidate_sha256"], "bytes": result["candidate_input_bytes"], "adapter": result["adapter"],
                    "derivation": {"code_sha256": candidate["code_sha256"], "parameters": {"receipt_sha256": result["native_inputs"]["candidate"]["sha256"]},
                                   "source_objects": {f"{prefix}/geoparquet/data.parquet": right["objects"][0]["sha256"]}}})
    registry.write_text(json.dumps(value))
    return source, target, registry


class NativeVerificationTests(unittest.TestCase):
    def test_actual_native_full_graph_rejects_missing_approval_then_preserves_release_on_drift(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "synthetic"
            result = subprocess.run([sys.executable, "scripts/verification/native_synthetic_local.py", "--work-dir", str(root)],
                                    text=True, capture_output=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stderr[-16000:] + result.stdout[-5000:])
            evidence = json.loads((root / "integration_result.json").read_text())
            self.assertTrue(evidence["accepted_objects_unchanged"])
            self.assertGreater(evidence["published_objects"], 10)
            self.assertEqual(len({evidence[name] for name in ["unapproved_run_id", "success_run_id", "failed_drift_run_id"]}), 3)

    def test_nested_nullability_and_embedded_srid_do_not_silently_coerce(self):
        from shapely import set_srid
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            table = arrow_table()
            rows = table.to_pylist()
            rows[0]["geometry"] = to_wkb(set_srid(Point(1, 2), 3857), include_srid=True)
            pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), inputs / "data.parquet")
            receipt = normalize_native_input(spec_for(inputs, "geoparquet", geometry_column="geometry"), policy(), root / "out")
            self.assertEqual(receipt["status"], "blocked", receipt)
            self.assertIn("EWKB SRID", receipt["reason_codes"][0])
            schema = pa.schema([pa.field("id", pa.int64()), pa.field("state", pa.string()),
                                pa.field("nested", pa.struct([pa.field("required", pa.int64(), nullable=False)]))])
            invalid = pa.Table.from_pylist([{"id": 1, "state": "PA", "nested": {"required": None}}], schema=schema)
            with pa.OSFile(str(inputs / "invalid.arrow"), "wb") as sink, ipc.new_file(sink, schema) as writer:
                writer.write_table(invalid)
            receipt = normalize_native_input(spec_for(inputs, "arrow_ipc_file"), policy(False), root / "out")
            self.assertEqual(receipt["status"], "blocked", receipt)

    def test_output_inside_preserved_capture_is_refused_before_any_write(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / "capture"
            root.mkdir()
            pq.write_table(arrow_table(spatial=False), root / "data.parquet")
            spec = spec_for(root, "parquet")
            before = {path.name: sha256_file(path) for path in root.iterdir()}
            with self.assertRaisesRegex(ValueError, "outside pinned"):
                normalize_native_input(spec, policy(False), root / "evidence")
            with self.assertRaisesRegex(ValueError, "outside pinned"):
                compare_native_inputs(spec, spec, policy(False), root / "evidence", baseline_reference=reference(spec))
            self.assertEqual(before, {path.name: sha256_file(path) for path in root.iterdir()})

    def test_native_receipts_lineage_and_accepted_policy_bind_release_without_staging_self_approval(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = native_gate_fixture(Path(d))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                copied = _copy_version_files(source, target, "synthetic", "points", "vtest")
                self.assertIn("synthetic/points/vtest/metadata/candidate/normalization_lineage.jsonl", copied)
                before = {key: target.object_snapshot(key) for key in target.list_prefix()}
                source.write("synthetic", "points", "vtest", "geoparquet/extra.parquet", b"unapproved shard")
                with self.assertRaises(ReleaseApprovalError):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})

    def test_proposed_or_missing_native_policy_approval_cannot_publish_even_repinned(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = native_gate_fixture(Path(d))
            value = json.loads(registry.read_text())
            value["releases"][0].pop("native_policy_sha256")
            registry.write_text(json.dumps(value))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "native_comparison_policy"):
                    _copy_version_files(source, target, "synthetic", "points", "vtest")
            self.assertEqual(target.list_prefix(), [])

    def test_nonnullable_map_items_and_nested_map_children_block(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            types_and_values = [
                (pa.map_(pa.string(), pa.field("value", pa.int64(), nullable=False)), [("key", None)]),
                (pa.map_(pa.string(), pa.struct([pa.field("required", pa.int64(), nullable=False)])), [("key", {"required": None})]),
                (pa.map_(pa.string(), pa.list_(pa.field("item", pa.int64(), nullable=False))), [("key", [None])]),
            ]
            for kind, value in types_and_values:
                schema = pa.schema([("id", pa.int64()), ("state", pa.string()), ("map", kind)])
                table = pa.Table.from_pylist([{"id": 1, "state": "PA", "map": value}], schema=schema)
                with pa.OSFile(str(inputs / "invalid.arrow"), "wb") as sink, ipc.new_file(sink, schema) as writer:
                    writer.write_table(table)
                spec = spec_for(inputs, "arrow_ipc_file")
                receipt = normalize_native_input(spec, policy(False), root / "out")
                self.assertEqual(receipt["status"], "blocked", receipt)
                comparison = compare_native_inputs(spec, spec, policy(False), root / "comparison", baseline_reference=reference(spec))
                self.assertEqual(comparison["status"], "blocked", comparison)

    def test_nested_bbox_lists_maps_and_temporal_values_preserve_schema_and_nulls(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            table = arrow_table(spatial=False)
            bbox_type = pa.struct([pa.field(name, pa.float64(), metadata={b"unit": b"degrees"}) for name in ("xmin", "ymin", "xmax", "ymax")])
            table = table.append_column("bbox", pa.array([{"xmin": -0.0, "ymin": 2, "xmax": 3, "ymax": 4}, None, {"xmin": None, "ymin": None, "xmax": None, "ymax": None}], type=bbox_type))
            table = table.append_column("nested", pa.array([[2 ** 53 + 1, None], [], None], type=pa.list_(pa.int64())))
            table = table.append_column("map", pa.array([[('same', 1), ('same', 2)], [], None], type=pa.map_(pa.string(), pa.int64())))
            pq.write_table(table, inputs / "data.parquet")
            spec = spec_for(inputs, "parquet")
            receipt = normalize_native_input(spec, policy(False), root / "out")
            self.assertEqual(receipt["status"], "complete", receipt)
            records = [json.loads(line)["properties"] for line in (root / "out/normalized_records.jsonl").read_text().splitlines()]
            self.assertEqual(records[0]["bbox"][1][0], ["xmin", ["binary_float", "-0x0.0p+0"]])
            self.assertIsNone(records[1]["bbox"])
            self.assertNotEqual(records[1]["bbox"], records[2]["bbox"])
            self.assertEqual(records[0]["nested"], ["list", [["integer", "9007199254740993"], None]])
            self.assertEqual(len(records[0]["map"][1]), 2)
            self.assertIn('ZGVncmVlcw==', json.dumps(receipt["physical_schema"]))
            self.assertEqual(normalize_native_input(spec, policy(False), root / "limited", limits=NativeReadLimits(max_nested_values_per_record=1))["status"], "blocked")

    def test_real_geoparquet_preserves_int_decimal_nanosecond_null_mixed_geometry(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            pq.write_table(arrow_table(), inputs / "typed.parquet", row_group_size=1)
            spec = spec_for(inputs, "geoparquet", geometry_column="geometry")
            receipt = normalize_native_input(spec, policy(), root / "out", limits=NativeReadLimits(batch_rows=1))
            self.assertEqual(receipt["status"], "complete", receipt)
            self.assertEqual(receipt["rows"], 3)
            records = [json.loads(line) for line in (root / "out/normalized_records.jsonl").read_text().splitlines()]
            self.assertEqual(records[0]["properties"]["id"], ["integer", "9007199254740993"])
            self.assertIsNone(records[0]["properties"]["nullable"])
            self.assertEqual(records[0]["properties"]["decimal"], ["decimal", "12345678901234567890.12000000"])
            self.assertEqual(records[0]["properties"]["instant"][1]["ticks"], "1792368000000000001")
            self.assertEqual(records[0]["properties"]["floating"], ["binary_float", "-0x0.0p+0"])
            self.assertIsNone(records[2]["geometry"])
            self.assertIn("decimal128(30, 8)", json.dumps(receipt["physical_schema"]))
            result = compare_native_inputs(spec, spec, policy(), root / "compare", baseline_reference=reference(spec))
            self.assertEqual(result["status"], "pass", result)

    def test_actual_geopackage_uses_original_sqlite_nullable_ints_and_preserves_feature_id(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            path = inputs / "typed.gpkg"
            schema = {"geometry": "Unknown", "properties": {"id": "int64", "state": "str", "large": "int64", "day": "date"}}
            with fiona.open(path, "w", driver="GPKG", layer="typed", schema=schema, crs="EPSG:4326") as output:
                for i, geometry in enumerate(({"type": "Point", "coordinates": [1, 2]}, None)):
                    output.write({"geometry": geometry, "properties": {"id": 2 ** 53 + 1 + i, "state": "PA", "large": None if i else -(2 ** 63), "day": "2026-10-19"}})
            spec = spec_for(inputs, "geopackage", layer="typed", geometry_column="geom")
            receipt = normalize_native_input(spec, policy(), root / "out")
            self.assertEqual(receipt["status"], "complete", receipt)
            records = [json.loads(line) for line in (root / "out/normalized_records.jsonl").read_text().splitlines()]
            self.assertEqual(records[0]["properties"]["large"], ["integer", "-9223372036854775808"])
            self.assertEqual(records[0]["properties"][FEATURE_ID][1]["fiona"], "1")
            self.assertEqual(records[1]["properties"][FEATURE_ID][1]["physical"], ["integer", "2"])
            self.assertIn("DATE", json.dumps(receipt["physical_schema"]))
            result = compare_native_inputs(spec, spec, policy(), root / "compare", baseline_reference=reference(spec))
            self.assertEqual(result["status"], "pass", result)

    def test_partitioned_files_reconcile_full_roster_ids_duplicates_and_schema(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            table = arrow_table(spatial=False)
            pq.write_table(table.slice(0, 1), inputs / "one.parquet")
            pq.write_table(table.slice(1), inputs / "two.parquet")
            spec = spec_for(inputs, "parquet")
            result = compare_native_inputs(spec, spec, policy(False), root / "equal", baseline_reference=reference(spec))
            self.assertEqual(result["status"], "pass", result)
            pq.write_table(table.slice(0, 1), inputs / "unexpected.parquet")
            failure = normalize_native_input(spec, policy(False), root / "out")
            self.assertEqual(failure["status"], "blocked")
            spec = spec_for(inputs, "parquet")
            result = compare_native_inputs(spec, spec, policy(False), root / "duplicates", baseline_reference=reference(spec))
            self.assertEqual(result["status"], "fail", result)
            self.assertEqual(result["populations"]["candidate"]["duplicate_keys"], 1)

    def test_full_value_coverage_geometry_and_physical_type_drift(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            left, right = root / "left", root / "right"
            left.mkdir(); right.mkdir()
            pq.write_table(arrow_table(ids=[1, 2, 3]), left / "one.parquet")
            candidate = arrow_table(ids=[1, 2, 4])
            candidate = candidate.set_column(candidate.schema.get_field_index("state"), candidate.schema.field("state"), pa.array(["PA", "PA", "PA"]))
            pq.write_table(candidate, right / "one.parquet")
            a, b = spec_for(left, "geoparquet", geometry_column="geometry"), spec_for(right, "geoparquet", geometry_column="geometry")
            result = compare_native_inputs(a, b, policy(), root / "compare", baseline_reference=reference(a))
            self.assertEqual(result["status"], "needs_decision", result)
            self.assertEqual(result["differences"]["removed"], 1)
            self.assertEqual(result["differences"]["added"], 1)
            self.assertEqual(result["differences"]["coverage_buckets"], 2)
            pq.write_table(arrow_table(ids=[1, 2, 3], id_type=pa.int32()), right / "one.parquet")
            b = spec_for(right, "geoparquet", geometry_column="geometry")
            result = compare_native_inputs(a, b, policy(), root / "width", baseline_reference=reference(a))
            self.assertFalse(result["physical_schema"]["equal"])
            self.assertEqual(result["status"], "needs_decision", result)

    def test_arrow_ipc_and_reproducible_receipt_across_roots(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            a, b = root / "a", root / "b"
            a.mkdir(); b.mkdir()
            table = arrow_table(spatial=False)
            with pa.OSFile(str(a / "data.arrow"), "wb") as sink, ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table, max_chunksize=1)
            shutil.copyfile(a / "data.arrow", b / "data.arrow")
            one = normalize_native_input(spec_for(a, "arrow_ipc_file"), policy(False), root / "out-a")
            two = normalize_native_input(spec_for(b, "arrow_ipc_file"), policy(False), root / "out-b")
            self.assertEqual(one["status"], "complete", one)
            self.assertEqual(one, two)

    def test_corrupt_partial_schema_missing_crs_and_resource_limits_are_durable(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            pq.write_table(arrow_table(), inputs / "data.parquet")
            spec = spec_for(inputs, "geoparquet", geometry_column="geometry")
            first = normalize_native_input(spec, policy(), root / "out")
            self.assertEqual(first["status"], "complete", first)
            for limits in (NativeReadLimits(max_output_bytes=1), NativeReadLimits(max_record_bytes=1),
                           NativeReadLimits(max_batch_bytes=1), NativeReadLimits(max_row_group_bytes=1)):
                failure = normalize_native_input(spec, policy(), root / "out", limits=limits)
                self.assertEqual(failure["status"], "blocked", failure)
                self.assertEqual(json.loads((root / "out/normalization_receipt.json").read_text())["status"], "blocked")
            invalid_policy = {**policy(), "crs": "EPSG:3857"}
            self.assertEqual(normalize_native_input(spec, invalid_policy, root / "out")["status"], "blocked")
            path = inputs / "data.parquet"
            path.write_bytes(path.read_bytes()[:-7])
            spec = spec_for(inputs, "geoparquet", geometry_column="geometry")
            self.assertEqual(normalize_native_input(spec, policy(), root / "out")["status"], "blocked")

    def test_native_input_mutation_is_not_receipted_as_complete(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            pq.write_table(arrow_table(spatial=False), inputs / "data.parquet")
            spec = spec_for(inputs, "parquet")
            from dagster_hifld import native_verification as module
            real_reader = module._read_arrow
            def changed(*args, **kwargs):
                for record in real_reader(*args, **kwargs):
                    yield record
                with (inputs / "data.parquet").open("ab") as changed_file:
                    changed_file.write(b"changed after reading")
            with patch.object(module, "_read_arrow", side_effect=changed):
                receipt = normalize_native_input(spec, policy(False), root / "out")
            self.assertEqual(receipt["status"], "blocked", receipt)

    def test_cli_compares_complete_unapproved_inputs_without_approving_them(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            inputs = root / "inputs"
            inputs.mkdir()
            pq.write_table(arrow_table(spatial=False), inputs / "data.parquet")
            spec = spec_for(inputs, "parquet")
            for name, value in (("spec", spec), ("policy", policy(False)), ("reference", reference(spec))):
                (root / (name + ".json")).write_text(json.dumps(value))
            result = subprocess.run([sys.executable, "-m", "dagster_hifld.native_verification", "--policy", str(root / "policy.json"),
                                     "--output", str(root / "out"), "compare", "--baseline-spec", str(root / "spec.json"),
                                     "--candidate-spec", str(root / "spec.json"), "--baseline-reference", str(root / "reference.json")],
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
            self.assertEqual(json.loads(result.stdout)["acceptance"], "not_established_by_comparison")


if __name__ == "__main__":
    unittest.main()
