import copy
import json
import os
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from dagster import DagsterInstance, Definitions, define_asset_job

from dagster_hifld.assets.publish import (
    _copy_or_generate_geopackage, _copy_version_files,
    _write_and_publish_geoparquet, _write_and_publish_pmtiles,
    _write_and_publish_shapefile_zip, publish_version, verify_release,
)
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.promotion import promote_immutable_objects, write_immutable_object
from dagster_hifld.release_approval import (
    ReleaseApprovalError, _direct_jsonl_adapter_code_sha256, _storage_identity,
    require_release_approval, verifier_implementation_sha256,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import _merge_manifest_layers, load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.transformation_accounting import (
    ADAPTER as ACCOUNTING_ADAPTER, _code_digest as _accounting_code_digest,
    _digest as _accounting_digest, account_construction,
)
from dagster_hifld.validation import ComparisonLimits, compare_full_rows, sha256_file


CONTRACT = {
    "id_fields": ["id"], "coverage_fields": ["state"], "geometry": True, "crs": "EPSG:4326",
    "columns": {"id": {"type": "string", "nullable": False},
                "state": {"type": "string", "nullable": False},
                "name": {"type": "string", "nullable": True}},
}
BASELINE = {"dataset_slug": "synthetic", "file_slug": "points", "version": "accepted-fixture",
            "catalog_url": "https://example.invalid/catalog.json", "catalog_sha256": "0" * 64,
            "data_objects": [{"url": "https://example.invalid/data", "sha256": "0" * 64, "bytes": 0}],
            "mapping_evidence": "SYNTHETIC ONLY; no dataset approval",
            "decision": {"id": "SYNTHETIC-BASELINE", "approved_by": "test-fixture", "approved_at": "2026-10-19"}}


def feature(key="001", state="PA", name="A", x=1):
    return {"type": "Feature", "properties": {"id": key, "state": state, "name": name},
            "geometry": {"type": "Point", "coordinates": [x, 2]}}


def stream(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def baseline_reference(path):
    value = copy.deepcopy(BASELINE)
    digest = sha256_file(path)
    value["data_objects"][0].update(sha256=digest, bytes=path.stat().st_size)
    value["comparison_input"] = {"adapter": "typed-geojson-feature-jsonl-v1", "sha256": digest, "bytes": path.stat().st_size,
                                 "derivation": {"source_sha256": [digest], "code_sha256": "identity-jsonl-fixture",
                                                "parameters": {"mode": "identity"}}}
    return value


def comparison(root, left, right, contract=None, limits=ComparisonLimits()):
    baseline, candidate = root / "baseline.jsonl", root / "candidate.jsonl"
    stream(baseline, left)
    stream(candidate, right)
    return compare_full_rows(baseline, candidate, contract or CONTRACT, root / "metadata",
                             baseline_sha256=sha256_file(baseline), candidate_sha256=sha256_file(candidate),
                             baseline_reference=baseline_reference(baseline), limits=limits)


def accepted_fixture(root, candidate_version="v1.0.0"):
    """Explicit synthetic acceptance owned by the test, outside storage roots."""
    source = StagingStorageResource(local_dir=str(root / "staging"), use_local=True)
    target = PublishedStorageResource(local_dir=str(root / "published"), use_local=True)
    prefix = f"synthetic/points/{candidate_version}"
    version_dir = root / "staging" / prefix
    version_dir.mkdir(parents=True)
    stream(root / "baseline.jsonl", [feature()])
    stream(version_dir / "metadata/candidate.records.jsonl", [feature()])
    candidate = version_dir / "metadata/candidate.records.jsonl"
    report = compare_full_rows(root / "baseline.jsonl", candidate, CONTRACT, version_dir / "metadata",
                               baseline_sha256=sha256_file(root / "baseline.jsonl"), candidate_sha256=sha256_file(candidate),
                               baseline_reference=baseline_reference(root / "baseline.jsonl"))
    source.write("synthetic", "points", candidate_version, "geojson/source.geojson",
                 json.dumps({"type": "FeatureCollection", "features": [feature()]}).encode())
    raw_source = source._ensure_prefixed(f"{prefix}/geojson/source.geojson")
    raw_source_sha = source.sha256_key(raw_source)
    raw_source_bytes = source.key_size(raw_source)
    adapter_code = _direct_jsonl_adapter_code_sha256()
    input_order = [f"{prefix}/geojson/source.geojson"]
    manifest = {"schema_version": 1, "processor": "HIFLD Next", "code_commit": "synthetic-only",
                "transformations": [], "lineage": {"schema_version": 1,
                "adapter": "direct_geojson_feature_collection_to_jsonl_v1", "code_sha256": adapter_code,
                "input_order": input_order, "source_objects": {input_order[0]: raw_source_sha},
                "output": {"key": f"{prefix}/metadata/candidate.records.jsonl",
                           "sha256": sha256_file(candidate), "bytes": candidate.stat().st_size}},
                "inputs": [{"input_id": "fixture", "producer": "Synthetic Agency",
                "host": "example.invalid", "role": "producer_publication", "canonical_url": "https://example.invalid/data",
                "retrieval_url": "https://example.invalid/data", "release_date_unknown_reason": "synthetic",
                "retrieved_at": "2026-10-19T00:00:00Z", "sha256": raw_source_sha, "bytes": raw_source_bytes,
                "license": "synthetic", "attribution": "Synthetic Agency"}]}
    for name, value in (("source_manifest.json", manifest), ("quality_manifest.json", {"quality_check_passed": True}),
                        ("data_dictionary.json", {"agency": "Synthetic Agency", **manifest}),
                        ("resolved_provenance.json", manifest)):
        source.write("synthetic", "points", candidate_version, f"metadata/{name}", json.dumps(value).encode())
    snapshot_source_metadata(source, "synthetic", "points", candidate_version)
    projection = provenance_projection(load_resolved_source_manifest(source, "synthetic", "points", candidate_version).metadata)
    source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
    source.write_key(f"{prefix}/metadata/data_dictionary.json", json.dumps({"agency": "Synthetic Agency", **projection}).encode())
    source.write_key(f"{prefix}/collection.json", json.dumps({
        "type": "Collection", "stac_version": "1.1.0", "version": "1.0.0",
        "id": "hifld/synthetic/points/v1.0.0", "description": "Synthetic fixture only",
        "license": "other", "links": [], "assets": {},
        "extent": {"spatial": {"bbox": [[-180, -90, 180, 90]]}, "temporal": {"interval": [[None, None]]}},
        "stac_extensions": ["https://stac-extensions.github.io/version/v1.2.0/schema.json"],
    }).encode())
    objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", candidate_version)}
    entry = {"destination": _storage_identity(target), "staged_version_prefix": prefix,
             "implementation_sha256": verifier_implementation_sha256(),
             "decision": {"id": "SYNTHETIC-ONLY", "approved_by": "test-fixture", "approved_at": "2026-10-19"},
             "baseline": report["baseline"], "contract_sha256": report["contract_sha256"],
             "report_sha256": objects[f"{prefix}/metadata/baseline_comparison.json"],
             "provenance_sha256": objects[f"{prefix}/metadata/source_manifest.json"],
             "staged_objects": objects, "published_objects": objects,
             "candidate_comparison_input": {"key": f"{prefix}/metadata/candidate.records.jsonl",
                 "sha256": sha256_file(candidate), "bytes": candidate.stat().st_size, "adapter": report["adapter"],
                 "derivation": {"code_sha256": adapter_code,
                                "parameters": {"mode": "direct_geojson_feature_collection_to_jsonl_v1",
                                               "input_order": [f"{prefix}/geojson/source.geojson"]},
                                "source_objects": {f"{prefix}/geojson/source.geojson": objects[f"{prefix}/geojson/source.geojson"]}}}}
    registry = root / "approvals.json"
    registry.write_text(json.dumps({"schema_version": 1, "releases": [entry]}))
    return source, target, registry


class FullPopulationTests(unittest.TestCase):
    def test_linked_report_and_sidecar_outputs_preserve_original_input_bytes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            baseline, candidate = root / "baseline.jsonl", root / "candidate.jsonl"
            stream(baseline, [feature()]); stream(candidate, [feature()])
            original = baseline.read_bytes()
            for name in ["baseline_comparison.json", "baseline_comparison.md", "baseline_differences.jsonl", "baseline_field_changes.jsonl"]:
                for hardlink in [False, True]:
                    output = root / (name + str(hardlink))
                    output.mkdir()
                    link = output / name
                    if hardlink:
                        os.link(baseline, link)
                    else:
                        link.symlink_to(baseline)
                    with self.assertRaisesRegex(ValueError, "link"):
                        compare_full_rows(baseline, candidate, CONTRACT, output,
                                          baseline_sha256=sha256_file(baseline), candidate_sha256=sha256_file(candidate),
                                          baseline_reference=baseline_reference(baseline))
                    self.assertEqual(baseline.read_bytes(), original)
                    self.assertEqual(list(output.iterdir()), [link])
                    link.unlink()
            protected = root / "baseline_comparison.json"
            stream(protected, [feature()])
            with self.assertRaisesRegex(ValueError, "protected comparison input"):
                compare_full_rows(protected, candidate, CONTRACT, root,
                                  baseline_sha256=sha256_file(protected), candidate_sha256=sha256_file(candidate),
                                  baseline_reference=baseline_reference(protected))
            self.assertEqual(protected.read_bytes(), original)

    def test_unmatched_baseline_input_pin_and_unknown_crs_block(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            left, right = root / "left", root / "right"
            stream(left, [feature()])
            stream(right, [feature()])
            reference = baseline_reference(left)
            reference["comparison_input"]["sha256"] = "0" * 64
            report = compare_full_rows(left, right, CONTRACT, root / "out", baseline_sha256=sha256_file(left),
                                       candidate_sha256=sha256_file(right), baseline_reference=reference)
            self.assertEqual(report["status"], "blocked")
            reference = baseline_reference(left)
            reference["comparison_input"]["bytes"] += 1
            report = compare_full_rows(left, right, CONTRACT, root / "out", baseline_sha256=sha256_file(left),
                                       candidate_sha256=sha256_file(right), baseline_reference=reference)
            self.assertEqual(report["status"], "blocked")
            contract = {**CONTRACT, "crs": "unrecognized-crs-with-no-authority"}
            self.assertEqual(comparison(root, [feature()], [feature()], contract)["status"], "blocked")

    def test_geometry_coercion_foreign_members_invalid_type_and_overflow_fail(self):
        geometries = [
            {"type": "Point", "coordinates": ["1", "2"]},
            {"type": "Point", "coordinates": [True, False]},
            {"type": "Point", "coordinates": [1, 2], "crs": "EPSG:3857"},
            {"type": "UnknownGeometry", "coordinates": [1, 2]},
            {"type": "Point", "coordinates": [10 ** 400, 2]},
            {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [1, 2], "bbox": [1, 2, 1, 2]}]},
            {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0]]]},
        ]
        for geom in geometries:
            with self.subTest(geometry=geom), tempfile.TemporaryDirectory() as d:
                row = {**feature(), "geometry": geom}
                self.assertEqual(comparison(Path(d), [row], [row])["status"], "fail")

    def test_crs84_coordinates_outside_geographic_domain_fail(self):
        contract = {**CONTRACT, "crs": "OGC:CRS84"}
        outside = feature(x=200)
        with tempfile.TemporaryDirectory() as d:
            report = comparison(Path(d), [outside], [outside], contract)
            self.assertEqual(report["status"], "fail")
            self.assertGreater(report["populations"]["baseline"]["invalid_records"], 0)
        with tempfile.TemporaryDirectory() as d:
            valid = feature(x=180)
            report = comparison(Path(d), [valid], [valid], contract)
            self.assertEqual(report["status"], "pass")

    def test_deep_json_parser_error_replaces_stale_pass_report(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            comparison(root, [feature()], [feature()])
            nested = root / "nested"
            nested.write_text("[" * 1100 + "0" + "]" * 1100 + "\n")
            candidate = root / "candidate.jsonl"
            report = compare_full_rows(nested, candidate, CONTRACT, root / "metadata", baseline_sha256=sha256_file(nested),
                                       candidate_sha256=sha256_file(candidate), baseline_reference=baseline_reference(nested))
            self.assertEqual(report["status"], "blocked")
            self.assertEqual(json.loads((root / "metadata/baseline_comparison.json").read_text())["status"], "blocked")

    def test_input_mutation_during_comparison_is_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            baseline, candidate = root / "baseline.jsonl", root / "candidate.jsonl"
            stream(baseline, [feature()])
            stream(candidate, [feature()])
            original = sha256_file
            calls = 0
            def mutate(path):
                nonlocal calls
                calls += 1
                result = original(path)
                if calls == 2:
                    stream(candidate, [feature(name="changed during read")])
                return result
            with patch("dagster_hifld.validation.sha256_file", side_effect=mutate):
                report = compare_full_rows(baseline, candidate, CONTRACT, root / "out",
                                           baseline_sha256=original(baseline), candidate_sha256=original(candidate),
                                           baseline_reference=baseline_reference(baseline))
            self.assertEqual(report["status"], "blocked")

    def test_multi_input_provenance_merge_preserves_identity_and_rejects_conflicts(self):
        one, two = {"input_id": "a", "producer": "Agency A"}, {"input_id": "b", "producer": "Agency B"}
        merged = _merge_manifest_layers([("dataset", "a", {"inputs": [one]}), ("version", "b", {"inputs": [two]})])
        self.assertEqual(merged.metadata["inputs"], [one, two])
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            _merge_manifest_layers([("dataset", "a", {"inputs": [one]}),
                                    ("version", "b", {"inputs": [{**one, "producer": "Mirror"}]})])

    def test_top_level_source_identifier_cannot_be_silently_dropped(self):
        with tempfile.TemporaryDirectory() as d:
            report = comparison(Path(d), [{**feature(), "id": "producer-id"}], [feature()])
            self.assertEqual(report["status"], "fail")

    def test_equal_rows_distinct_ids_and_hidden_coverage_loss(self):
        with tempfile.TemporaryDirectory() as d:
            report = comparison(Path(d), [feature("001", "PA"), feature("002", "VA")],
                                [feature("001", "PA"), feature("003", "PA")])
            self.assertEqual(report["status"], "needs_decision")
            self.assertEqual(report["differences"]["added"], 1)
            self.assertEqual(report["differences"]["removed"], 1)
            self.assertEqual(report["differences"]["coverage_buckets"], 2)

    def test_complete_geometry_and_attributes_after_sample_limit(self):
        with tempfile.TemporaryDirectory() as d:
            left = [feature(str(i)) for i in range(1200)]
            right = copy.deepcopy(left)
            right[-1] = feature("1199", name="changed", x=8)
            report = comparison(Path(d), left, right)
            self.assertEqual(report["differences"]["attributes"], 1)
            self.assertEqual(report["differences"]["geometry"], 1)

    def test_schema_extra_column_null_and_duplicate_keys_fail(self):
        for rows in ([feature(None)], [feature(), feature()], [{**feature(), "properties": {**feature()["properties"], "extra": "x"}}]):
            with self.subTest(rows=rows), tempfile.TemporaryDirectory() as d:
                self.assertEqual(comparison(Path(d), [feature()], rows)["status"], "fail")

    def test_leading_zero_whitespace_and_integer_type_preserved(self):
        for key in ("1", "001 ", 1, 9007199254740993):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as d:
                report = comparison(Path(d), [feature()], [feature(key)])
                self.assertNotEqual(report["status"], "pass")

    def test_decimal_precision_is_not_rounded_to_float(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            left, right = root / "left", root / "right"
            text = json.dumps(feature()).replace('"name": "A"', '"name": 1.00000000000000000001')
            left.write_text(text + "\n")
            right.write_text(text.replace("00001", "00002") + "\n")
            contract = copy.deepcopy(CONTRACT)
            contract["columns"]["name"]["type"] = "decimal"
            report = compare_full_rows(left, right, contract, root / "out", baseline_sha256=sha256_file(left),
                                       candidate_sha256=sha256_file(right), baseline_reference=baseline_reference(left))
            self.assertEqual(report["differences"]["attributes"], 1)

    def test_resource_exhaustion_and_missing_baseline_are_durable_blockers(self):
        for limits in (ComparisonLimits(max_record_bytes=10), ComparisonLimits(max_scratch_bytes=1)):
            with tempfile.TemporaryDirectory() as d:
                root = Path(d)
                report = comparison(root, [feature()], [feature()], limits=limits)
                self.assertEqual(report["status"], "blocked")
                self.assertTrue((root / "metadata/baseline_comparison.json").exists())
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            report = compare_full_rows(root / "missing", root / "missing2", CONTRACT, root / "out",
                                       baseline_sha256="0" * 64, candidate_sha256="0" * 64, baseline_reference=BASELINE)
            self.assertEqual(report["status"], "blocked")


class ReleaseBoundaryTests(unittest.TestCase):
    def test_all_excluded_derivatives_skip_before_reuse_and_cannot_promote(self):
        cases = (
            ("geopackage", "geopackage/source.gpkg", _copy_or_generate_geopackage),
            ("geoparquet", "geoparquet/source.parquet", _write_and_publish_geoparquet),
            ("pmtiles", "pmtiles/source.pmtiles", _write_and_publish_pmtiles),
        )
        for format_name, relative_path, generate in cases:
            with self.subTest(format=format_name), tempfile.TemporaryDirectory() as directory:
                source, target, registry = accepted_fixture(Path(directory))
                prefix = "synthetic/points/v1.0.0"
                manifest_key = f"{prefix}/metadata/source_manifest.json"
                manifest = json.loads(source.read_key(manifest_key))
                reason = "full source audit shows an unapproved representation change"
                manifest["unsupported_output_formats"] = {format_name: reason}
                source.write_key(manifest_key, json.dumps(manifest).encode())
                stale_key = source.write("synthetic", "points", "v1.0.0", relative_path, b"retained excluded evidence")
                with patch("dagster_hifld.assets.publish._prepare_format_publish") as prepare, patch("dagster_hifld.assets.publish.select_processing_input") as select:
                    result = generate(source, "synthetic", "points", "v1.0.0")
                prepare.assert_not_called()
                select.assert_not_called()
                self.assertFalse(result[0].path)
                self.assertIn(reason, result[0].source_metadata["skip_reason"])
                self.assertEqual(source.read_key(stale_key), b"retained excluded evidence")
                value = json.loads(registry.read_text())
                entry = value["releases"][0]
                objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
                entry.update(staged_objects=objects, published_objects=objects, provenance_sha256=objects[manifest_key])
                registry.write_text(json.dumps(value))
                with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                    with self.assertRaisesRegex(ReleaseApprovalError, "unsupported_output_present"):
                        _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                self.assertEqual(target.list_prefix(), [])

    def test_malformed_exclusion_policy_blocks_generation_and_release_even_when_repinned(self):
        for policy in (None, [], {"shapefile_zip": None}, {"geoparquet": None}, {"unknown_format": "loss"}):
            with self.subTest(policy=policy), tempfile.TemporaryDirectory() as d:
                source, target, registry = accepted_fixture(Path(d))
                manifest_key = "synthetic/points/v1.0.0/metadata/source_manifest.json"
                manifest = json.loads(source.read_key(manifest_key))
                manifest["unsupported_output_formats"] = policy
                source.write_key(manifest_key, json.dumps(manifest).encode())
                with patch("dagster_hifld.assets.publish._read_layer") as conversion:
                    with self.assertRaises(ValueError):
                        _write_and_publish_shapefile_zip(source, "synthetic", "points", "v1.0.0")
                conversion.assert_not_called()
                value = json.loads(registry.read_text())
                entry = value["releases"][0]
                digest = source.sha256_key(manifest_key)
                entry["staged_objects"][manifest_key] = digest
                entry["published_objects"][manifest_key] = digest
                entry["provenance_sha256"] = digest
                registry.write_text(json.dumps(value))
                with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                    with self.assertRaises(ValueError):
                        _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                self.assertEqual(target.list_prefix(), [])

    def test_evidenced_unsupported_shapefile_is_skipped_and_stale_artifact_cannot_promote(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d))
            prefix = "synthetic/points/v1.0.0"
            manifest_key = f"{prefix}/metadata/source_manifest.json"
            manifest = json.loads(source.read_key(manifest_key))
            manifest["unsupported_output_formats"] = {"shapefile_zip": "full audit found long-text truncation and whitespace loss"}
            source.write_key(manifest_key, json.dumps(manifest).encode())
            stale_key = source.write("synthetic", "points", "v1.0.0", "shapefile/source.zip", b"lossy derivative")
            with patch("dagster_hifld.assets.publish._read_layer") as conversion:
                result = _write_and_publish_shapefile_zip(source, "synthetic", "points", "v1.0.0")
            conversion.assert_not_called()
            self.assertFalse(result[0].path)
            self.assertIn("long-text truncation", result[0].source_metadata["skip_reason"])
            self.assertTrue(source.object_exists(stale_key))
            value = json.loads(registry.read_text())
            entry = value["releases"][0]
            objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
            entry.update(staged_objects=objects, published_objects=objects, provenance_sha256=objects[manifest_key])
            registry.write_text(json.dumps(value))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "unsupported_output_present"):
                    _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
            self.assertEqual(target.list_prefix(), [])

    def test_candidate_comparison_input_pin_and_derivation_cannot_drift(self):
        for mutation in ("sha256", "source"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as d:
                source, target, registry = accepted_fixture(Path(d))
                value = json.loads(registry.read_text())
                candidate = value["releases"][0]["candidate_comparison_input"]
                if mutation == "sha256":
                    candidate["sha256"] = "0" * 64
                else:
                    candidate["derivation"]["source_objects"] = {"unreviewed-object": "0" * 64}
                registry.write_text(json.dumps(value))
                with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                    with self.assertRaises(ReleaseApprovalError):
                        _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                self.assertEqual(target.list_prefix(), [])

    def test_jsonl_pass_cannot_claim_unaccounted_source_to_output_custody(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root)
            prior = target.write_key("previous/accepted.bin", b"accepted bytes")
            before = target.sha256_key(prior)
            prefix = "synthetic/points/v1.0.0"
            candidate = root / "staging" / prefix / "metadata/candidate.records.jsonl"
            baseline = root / "baseline.jsonl"
            unrelated = [feature("101"), feature("102"), feature("103")]
            stream(candidate, unrelated)
            stream(baseline, unrelated)
            report = compare_full_rows(
                baseline, candidate, CONTRACT, candidate.parent,
                baseline_sha256=sha256_file(baseline), candidate_sha256=sha256_file(candidate),
                baseline_reference=baseline_reference(baseline),
            )
            self.assertEqual(report["status"], "pass")

            manifest_key = f"{prefix}/metadata/source_manifest.json"
            manifest = json.loads(source.read_key(manifest_key))
            manifest["lineage"]["output"] = {
                "key": f"{prefix}/metadata/candidate.records.jsonl",
                "sha256": sha256_file(candidate), "bytes": candidate.stat().st_size,
            }
            source.write_key(manifest_key, json.dumps(manifest).encode())
            snapshot_source_metadata(source, "synthetic", "points", "v1.0.0")
            projection = provenance_projection(
                load_resolved_source_manifest(source, "synthetic", "points", "v1.0.0").metadata
            )
            source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
            source.write_key(f"{prefix}/metadata/data_dictionary.json",
                             json.dumps({"agency": "Synthetic Agency", **projection}).encode())

            value = json.loads(registry.read_text())
            entry = value["releases"][0]
            objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
            candidate_key = f"{prefix}/metadata/candidate.records.jsonl"
            entry.update(
                baseline=report["baseline"], contract_sha256=report["contract_sha256"],
                report_sha256=objects[f"{prefix}/metadata/baseline_comparison.json"],
                provenance_sha256=objects[f"{prefix}/metadata/source_manifest.json"],
                staged_objects=objects, published_objects=objects,
            )
            entry["candidate_comparison_input"].update(
                sha256=objects[candidate_key], bytes=candidate.stat().st_size,
            )
            registry.write_text(json.dumps(value))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "jsonl_candidate_feature_differs_from_source"):
                    require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertEqual(target.sha256_key(prior), before)
            self.assertEqual(target.list_prefix(), [prior])

    def test_storage_resource_subclass_cannot_bypass_release_gate(self):
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
            source, target, _ = accepted_fixture(Path(d))
            target_alias = StagingStorageResource(local_dir=target.local_dir, use_local=True)
            key = "synthetic/points/v1.0.0/geojson/source.geojson"
            with self.assertRaises(ReleaseApprovalError):
                promote_immutable_objects(source, target_alias, [(key, key)])

    def test_approval_inside_staging_and_stale_code_decisions_block(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root)
            candidate_registry = root / "staging/forged-approval.json"
            candidate_registry.write_bytes(registry.read_bytes())
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(candidate_registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "outside_candidate"):
                    _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
            value = json.loads(registry.read_text())
            value["releases"][0]["implementation_sha256"] = "stale"
            registry.write_text(json.dumps(value))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "implementation_changed"):
                    _copy_version_files(source, target, "synthetic", "points", "v1.0.0")

    def test_absent_registry_blocks_direct_local_and_low_level_writes(self):
        with tempfile.TemporaryDirectory() as d, patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
            source, target, _ = accepted_fixture(Path(d))
            key = "synthetic/points/v1.0.0/geojson/source.geojson"
            for action in (lambda: _copy_version_files(source, target, "synthetic", "points", "v1.0.0"),
                           lambda: promote_immutable_objects(source, target, [(key, key)]),
                           lambda: write_immutable_object(target, key, b"forged")):
                with self.assertRaises(ReleaseApprovalError):
                    action()
            self.assertEqual(target.list_prefix(), [])

    def test_exact_approval_then_new_shard_or_mutated_bytes_preserves_previous_release(self):
        with tempfile.TemporaryDirectory() as d:
            source, target, registry = accepted_fixture(Path(d))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                before = {key: target.object_snapshot(key) for key in target.list_prefix()}
                source.write("synthetic", "points", "v1.0.0", "geojson/added.geojson", b"new shard")
                with self.assertRaises(ReleaseApprovalError):
                    _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})
                source.delete_prefix("synthetic/points/v1.0.0/geojson/added.geojson")
                source.write("synthetic", "points", "v1.0.0", "geojson/source.geojson", b"changed bytes")
                with self.assertRaisesRegex(ReleaseApprovalError, "bytes_changed"):
                    _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})

    def test_dagster_synthetic_verify_promote_and_failed_promotion_preservation(self):
        preserved = os.environ.get("HIFLD_VERIFICATION_EVIDENCE_DIR")
        with (nullcontext(preserved) if preserved else tempfile.TemporaryDirectory()) as d:
            root = Path(d)
            source, target, registry = accepted_fixture(root)
            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            partition = "synthetic/points/v1.0.0"
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry), "DAGSTER_HOME": str(instance_dir)}), DagsterInstance.get() as instance:
                instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [partition])
                resources = {"staging_storage": source, "published_storage": target}
                assets = [verify_release, publish_version]
                checks = [check for check in hifld_asset_checks if next(iter(check.check_specs)).asset_key.path == ["publish", "verify"]]
                defs = Definitions(assets=assets, asset_checks=checks, resources=resources,
                                   jobs=[define_asset_job("synthetic_verification")])
                job = defs.resolve_job_def("synthetic_verification")
                success = job.execute_in_process(instance=instance, partition_key=partition)
                self.assertTrue(success.success)
                before = {key: target.object_snapshot(key) for key in target.list_prefix()}
                source.write("synthetic", "points", "v1.0.0", "geojson/source.geojson", b"unapproved drift")
                failure = job.execute_in_process(instance=instance, partition_key=partition, raise_on_error=False)
                self.assertFalse(failure.success)
                self.assertEqual(before, {key: target.object_snapshot(key) for key in target.list_prefix()})
                self.assertTrue(source.object_exists("synthetic/points/v1.0.0/metadata/baseline_comparison.json"))
                (root / "integration_result.json").write_text(json.dumps({
                    "classification": "synthetic integration; no real acceptance", "success_run_id": success.run_id,
                    "failed_drift_run_id": failure.run_id, "success": success.success, "drift_success": failure.success,
                    "accepted_objects_unchanged": True, "candidate_evidence_preserved": True,
                    "published_object_count": len(before), "partition": partition,
                    "dagster_home": str(instance_dir),
                }, indent=2) + "\n")


class AccountingModeGateTests(unittest.TestCase):
    """Constructed candidates need replayed executable accounting to release."""

    def _accounted_fixture(self, root):
        """Extend the accepted fixture with a constructed candidate custody chain."""
        source, target, registry = accepted_fixture(root)
        prefix = "synthetic/points/v1.0.0"
        version_dir = root / "staging" / prefix
        shard_a = (json.dumps(feature()) + "\n").encode()
        shard_b = (json.dumps(feature("002", "VA", "B")) + "\n").encode()
        regions = (json.dumps({"state": "PA", "region": "East"}) + "\n").encode()
        ledger = (json.dumps({"row": 2, "reason": "unmatched_region"}) + "\n").encode()
        for name, body in (("streams/events-a.jsonl", shard_a), ("streams/events-b.jsonl", shard_b),
                           ("streams/regions.jsonl", regions), ("streams/dropped.jsonl", ledger)):
            source.write("synthetic", "points", "v1.0.0", name, body)
        plan = {
            "schema_version": 1,
            "steps": [
                {"op": "union", "input_streams": ["events-a", "events-b"], "output_stream": "events"},
                {"op": "join", "left_stream": "events", "right_stream": "regions", "join_type": "left",
                 "left_keys": [["properties", "state"]], "right_keys": [["state"]], "output_stream": "joined"},
                {"op": "filter", "input_stream": "joined", "output_stream": "kept",
                 "ledger": {"path": "streams/dropped.jsonl", "sha256": sha256_file(version_dir / "streams/dropped.jsonl"),
                            "bytes": len(ledger)}},
                {"op": "project", "input_stream": "kept", "output_stream": "candidate",
                 "properties": [{"name": "id", "path": ["left", "properties", "id"]},
                                {"name": "state", "path": ["left", "properties", "state"]},
                                {"name": "name", "path": ["left", "properties", "name"]}],
                 "geometry_path": ["left", "geometry"]},
            ],
            "output": "candidate",
        }
        spec = {"schema_version": 1, "root": str(version_dir), "roster_prefix": "streams", "streams": [
            {"name": "events-a", "path": "streams/events-a.jsonl", "sha256": sha256_file(version_dir / "streams/events-a.jsonl"),
             "bytes": len(shard_a), "rows": 1},
            {"name": "events-b", "path": "streams/events-b.jsonl", "sha256": sha256_file(version_dir / "streams/events-b.jsonl"),
             "bytes": len(shard_b), "rows": 1},
            {"name": "regions", "path": "streams/regions.jsonl", "sha256": sha256_file(version_dir / "streams/regions.jsonl"),
             "bytes": len(regions), "rows": 1},
        ]}
        candidate = version_dir / "metadata/candidate.records.jsonl"
        candidate_pin = {"path": "metadata/candidate.records.jsonl", "sha256": sha256_file(candidate),
                         "bytes": candidate.stat().st_size, "rows": 1}
        receipt = account_construction(spec, plan, candidate_pin, root / "accounting-evidence")
        assert receipt["status"] == "pass"
        source.write("synthetic", "points", "v1.0.0", "metadata/construction_plan.json",
                     (json.dumps(plan, sort_keys=True, indent=2) + "\n").encode())
        source.write("synthetic", "points", "v1.0.0", "metadata/transformation_accounting.json",
                     (root / "accounting-evidence/transformation_accounting.json").read_bytes())
        source.write("synthetic", "points", "v1.0.0", "metadata/transformation_lineage.jsonl",
                     (root / "accounting-evidence/transformation_lineage.jsonl").read_bytes())

        descriptor = {"adapter": ACCOUNTING_ADAPTER,
                      "plan": {"path": "construction_plan.json",
                               "sha256": sha256_file(version_dir / "metadata/construction_plan.json")},
                      "receipt": {"path": "transformation_accounting.json",
                                  "sha256": sha256_file(version_dir / "metadata/transformation_accounting.json")},
                      "lineage": {"path": "transformation_lineage.jsonl",
                                  "sha256": sha256_file(version_dir / "metadata/transformation_lineage.jsonl")}}
        report = json.loads(source.read_key(f"{prefix}/metadata/baseline_comparison.json"))
        report["construction_accounting"] = descriptor
        source.write_key(f"{prefix}/metadata/baseline_comparison.json", json.dumps(report, sort_keys=True).encode())

        manifest_key = f"{prefix}/metadata/source_manifest.json"
        manifest = json.loads(source.read_key(manifest_key))
        manifest["transformations"] = [{"name": "union_paged_shards"}, {"name": "left_join_regions"},
                                       {"name": "filter_unmatched_regions"}, {"name": "project_published_schema"}]
        objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
        manifest["lineage"] = {
            "schema_version": 1, "adapter": ACCOUNTING_ADAPTER,
            "code_sha256": _accounting_code_digest(),
            "input_streams": {f"{prefix}/streams/{name}": objects[f"{prefix}/streams/{name}"]
                              for name in ("events-a.jsonl", "events-b.jsonl", "regions.jsonl")},
            "plan": {"key": f"{prefix}/metadata/construction_plan.json", "sha256": descriptor["plan"]["sha256"]},
            "receipt": {"key": f"{prefix}/metadata/transformation_accounting.json", "sha256": descriptor["receipt"]["sha256"]},
            "lineage": {"key": f"{prefix}/metadata/transformation_lineage.jsonl", "sha256": descriptor["lineage"]["sha256"]},
            "output": {"key": f"{prefix}/metadata/candidate.records.jsonl",
                       "sha256": objects[f"{prefix}/metadata/candidate.records.jsonl"],
                       "bytes": candidate.stat().st_size},
        }
        source.write_key(manifest_key, json.dumps(manifest).encode())
        snapshot_source_metadata(source, "synthetic", "points", "v1.0.0")
        projection = provenance_projection(load_resolved_source_manifest(source, "synthetic", "points", "v1.0.0").metadata)
        source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
        source.write_key(f"{prefix}/metadata/data_dictionary.json",
                         json.dumps({"agency": "Synthetic Agency", **projection}).encode())

        objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
        value = json.loads(registry.read_text())
        entry = value["releases"][0]
        entry.update(
            report_sha256=objects[f"{prefix}/metadata/baseline_comparison.json"],
            provenance_sha256=objects[manifest_key],
            staged_objects=objects, published_objects=objects,
        )
        entry["candidate_comparison_input"]["derivation"] = {
            "code_sha256": _accounting_code_digest(),
            "parameters": {"mode": "transformation_accounting_v1",
                           "plan_path": "construction_plan.json",
                           "receipt_path": "transformation_accounting.json"},
            "source_objects": {f"{prefix}/streams/{name}": objects[f"{prefix}/streams/{name}"]
                               for name in ("events-a.jsonl", "events-b.jsonl", "regions.jsonl")},
        }
        registry.write_text(json.dumps(value))
        return source, target, registry, prefix, objects

    def test_accounted_construction_replays_and_promotes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry, prefix, objects = self._accounted_fixture(root)
            candidate_key = f"{prefix}/metadata/candidate.records.jsonl"
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                snapshots = require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertIn(candidate_key, snapshots)
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                _copy_version_files(source, target, "synthetic", "points", "v1.0.0")
            published = set(target.list_prefix())
            self.assertTrue(any(key.endswith("transformation_accounting.json") for key in published))
            self.assertTrue(any(key.endswith("transformation_lineage.jsonl") for key in published))
            self.assertEqual(target.sha256_key(f"{prefix}/streams/events-a.jsonl"), objects[f"{prefix}/streams/events-a.jsonl"])

    def test_forged_self_consistent_receipt_is_caught_by_replay(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry, prefix, objects = self._accounted_fixture(root)
            source, target = source, target
            receipt_key = f"{prefix}/metadata/transformation_accounting.json"
            receipt = json.loads(source.read_key(receipt_key))
            receipt["steps"][0]["output_rows"] += 100
            receipt["receipt_id"] = _accounting_digest({k: v for k, v in receipt.items() if k != "receipt_id"})
            source.write_key(receipt_key, json.dumps(receipt, sort_keys=True).encode())
            objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
            report = json.loads(source.read_key(f"{prefix}/metadata/baseline_comparison.json"))
            report["construction_accounting"]["receipt"]["sha256"] = objects[receipt_key]
            source.write_key(f"{prefix}/metadata/baseline_comparison.json", json.dumps(report, sort_keys=True).encode())
            value = json.loads(registry.read_text())
            entry = value["releases"][0]
            entry["staged_objects"] = objects
            entry["published_objects"] = objects
            entry["report_sha256"] = objects[f"{prefix}/metadata/baseline_comparison.json"]
            entry["candidate_comparison_input"]["derivation"]["source_objects"] = {
                f"{prefix}/streams/{name}": objects[f"{prefix}/streams/{name}"]
                for name in ("events-a.jsonl", "events-b.jsonl", "regions.jsonl")}
            manifest_key = f"{prefix}/metadata/source_manifest.json"
            manifest = json.loads(source.read_key(manifest_key))
            manifest["lineage"]["receipt"]["sha256"] = objects[receipt_key]
            source.write_key(manifest_key, json.dumps(manifest).encode())
            snapshot_source_metadata(source, "synthetic", "points", "v1.0.0")
            projection = provenance_projection(load_resolved_source_manifest(source, "synthetic", "points", "v1.0.0").metadata)
            source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
            source.write_key(f"{prefix}/metadata/data_dictionary.json",
                             json.dumps({"agency": "Synthetic Agency", **projection}).encode())
            objects = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
            entry["staged_objects"] = objects
            entry["published_objects"] = objects
            entry["provenance_sha256"] = objects[manifest_key]
            entry["report_sha256"] = objects[f"{prefix}/metadata/baseline_comparison.json"]
            registry.write_text(json.dumps(value))
            candidate_key = f"{prefix}/metadata/candidate.records.jsonl"
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaisesRegex(ReleaseApprovalError, "construction_accounting_replay_mismatch"):
                    require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertEqual(target.list_prefix(), [])

    def test_accounted_mode_requires_declared_transformations_and_matched_candidate_pin(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source, target, registry, prefix, objects = self._accounted_fixture(root)
            manifest_key = f"{prefix}/metadata/source_manifest.json"
            candidate_key = f"{prefix}/metadata/candidate.records.jsonl"
            with tempfile.TemporaryDirectory() as mutation:
                manifest = json.loads(source.read_key(manifest_key))
                manifest["transformations"] = []
                source.write_key(manifest_key, json.dumps(manifest).encode())
                snapshot_source_metadata(source, "synthetic", "points", "v1.0.0")
                projection = provenance_projection(load_resolved_source_manifest(source, "synthetic", "points", "v1.0.0").metadata)
                source.write_key(f"{prefix}/metadata/resolved_provenance.json", json.dumps(projection).encode())
                source.write_key(f"{prefix}/metadata/data_dictionary.json",
                                 json.dumps({"agency": "Synthetic Agency", **projection}).encode())
                objects_now = {key: source.sha256_key(key) for key in source.list_keys("synthetic", "points", "v1.0.0")}
                value = json.loads(registry.read_text())
                value["releases"][0].update(staged_objects=objects_now, published_objects=objects_now,
                                            provenance_sha256=objects_now[manifest_key])
                registry.write_text(json.dumps(value))
                with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                    with self.assertRaisesRegex(ReleaseApprovalError, "declared_transformations"):
                        require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertEqual(target.list_prefix(), [])
            value = json.loads(registry.read_text())
            value["releases"][0]["candidate_comparison_input"]["sha256"] = "0" * 64
            registry.write_text(json.dumps(value))
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": str(registry)}):
                with self.assertRaises(ReleaseApprovalError):
                    require_release_approval(source, target, [(candidate_key, candidate_key)])
            self.assertEqual(target.list_prefix(), [])


if __name__ == "__main__":
    unittest.main()
