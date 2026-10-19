"""Actual local Dagster native comparison/promotion fixture; never real acceptance.

All storage and decisions are synthetic and confined to a NEW empty work folder.
Runs source -> catalog -> archival formats -> native comparison -> checked publish,
first without acceptance, then with a synthetic-only exact decision, then drifted.
"""

import argparse
import csv
import json
import os
import shutil
from datetime import date
from pathlib import Path

import fiona
import pyarrow as pa
import pyarrow.parquet as pq
from pyproj import CRS
from shapely.geometry import shape
from shapely import to_wkb
from dagster import DagsterInstance, Definitions, Output, asset, define_asset_job

from dagster_hifld.assets.catalog import publish_catalog
from dagster_hifld.assets.publish import (
    _copy_version_files, publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip,
    publish_version, verify_release,
)
from dagster_hifld.checks import hifld_asset_checks
from dagster_hifld.native_verification import _digest, compare_native_inputs
from dagster_hifld.partitions import PUBLISH_PARTITIONS, PUBLISH_PARTITION_NAME
from dagster_hifld.release_approval import ReleaseApprovalError, _storage_identity, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import snapshot_source_metadata
from dagster_hifld.validation import sha256_file


def input_spec(root):
    return {"schema_version": 1, "format": "geoparquet", "root": str(root), "geometry_column": "geometry",
            "objects": [{"key": path.relative_to(root).as_posix(), "sha256": sha256_file(path), "bytes": path.stat().st_size}
                        for path in sorted(root.rglob("*.parquet"))]}


def forge_synthetic_candidate_labels(source, registry: Path, prefix: str) -> None:
    """Adversarial test only: repin labels without recomputing normalized rows."""
    value = json.loads(registry.read_text())
    entry = value["releases"][0]
    native_key = next(iter(entry["candidate_comparison_input"]["derivation"]["source_objects"]))
    physical = Path(source.local_dir) / source._ensure_prefixed(native_key)
    table = pq.ParquetFile(physical).read()
    table = table.set_column(table.schema.get_field_index("state"), table.schema.field("state"), pa.array(["NJ"] * table.num_rows))
    pq.write_table(table, physical)
    receipt_key = f"{prefix}/metadata/candidate/normalization_receipt.json"
    receipt = json.loads(source.read_key(receipt_key))
    receipt["objects"][0].update(sha256=sha256_file(physical), bytes=physical.stat().st_size)
    receipt["receipt_id"] = _digest({key: val for key, val in receipt.items() if key != "receipt_id"})
    source.write_key(receipt_key, json.dumps(receipt, sort_keys=True, indent=2).encode())
    report_key = f"{prefix}/metadata/baseline_comparison.json"
    report = json.loads(source.read_key(report_key))
    report["native_inputs"]["candidate"].update(sha256=source.sha256_key(receipt_key), receipt=receipt)
    source.write_key(report_key, json.dumps(report, sort_keys=True, indent=2).encode())
    derivation = entry["candidate_comparison_input"]["derivation"]
    derivation["source_objects"][native_key] = source.sha256_key(native_key)
    derivation["parameters"]["receipt_sha256"] = source.sha256_key(receipt_key)
    for key in [native_key, receipt_key, report_key]:
        entry["staged_objects"][key] = source.sha256_key(key)
        entry["published_objects"][key] = source.sha256_key(key)
    entry["report_sha256"] = source.sha256_key(report_key)
    registry.write_text(json.dumps(value))


def run(work, *, prebuilt_date32=False):
    if work.exists() and any(work.iterdir()):
        raise ValueError("Use a new empty local synthetic work folder; preserved evidence is never removed")
    work.mkdir(parents=True, exist_ok=True)
    (work / "SYNTHETIC_ONLY.json").write_text('{"real_acceptance": false, "cloud_writes": false}\n')
    home = work / "dagster"
    home.mkdir()
    (home / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
    os.environ["DAGSTER_HOME"] = str(home)
    os.environ["HIFLD_RELEASE_APPROVALS"] = ""
    staging = StagingStorageResource(use_local=True, local_dir=str(work / "staging"))
    published = PublishedStorageResource(use_local=True, local_dir=str(work / "published"))
    prefix = "synthetic-native/typed/v0.0.0-fixture"
    version_root = work / "staging" / prefix
    baseline_root = work / "baseline-archive"
    baseline_root.mkdir()
    policy = {"schema_version": 1, "status": "reviewed", "geometry": True, "crs": "EPSG:4326",
              "geometry_nullable": False, "id_fields": ["id"], "coverage_fields": ["state"]}
    state = {}

    @asset(key=["synthetic", "source"], partitions_def=PUBLISH_PARTITIONS)
    def fixture_source():
        path = version_root / "geopackage" / "typed.gpkg"
        if not path.exists():
            path.parent.mkdir(parents=True)
            auxiliary = version_root / "raw" / "regions.csv"
            auxiliary.parent.mkdir()
            auxiliary.write_text("state,label\nPA,Synthetic region\n")
            with auxiliary.open() as stream:
                region = next(csv.DictReader(stream))["state"]
            schema = {"geometry": "Point", "properties": {"id": "int64", "nullable_id": "int64", "state": "str", "name": "str"}}
            if prebuilt_date32:
                schema["properties"]["capture_date"] = "date"
            with fiona.open(path, "w", driver="GPKG", layer="typed", crs="EPSG:4326", schema=schema) as output:
                for index, value in enumerate([2 ** 53 + 1, 2 ** 63 - 1]):
                    properties = {"id": value, "nullable_id": None if index == 0 else -(2 ** 63), "state": region, "name": " "}
                    if prebuilt_date32:
                        properties["capture_date"] = "2026-10-19" if index == 0 else None
                    output.write({"geometry": {"type": "Point", "coordinates": [index, index]},
                                  "properties": properties})
            if prebuilt_date32:
                geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {
                    "encoding": "WKB", "crs": CRS.from_epsg(4326).to_json_dict(), "geometry_types": ["Point"]}}}
                typed = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("nullable_id", pa.int64()),
                    pa.field("state", pa.string(), nullable=False), pa.field("name", pa.string(), nullable=False),
                    pa.field("capture_date", pa.date32()), pa.field("__source_fid", pa.string(), nullable=False),
                    pa.field("geometry", pa.binary(), nullable=False)],
                    metadata={b"geo": json.dumps(geo).encode()})
                with fiona.open(path, layer="typed") as original:
                    rows = [{**dict(row["properties"]), "__source_fid": str(row["id"]), "capture_date": date.fromisoformat(row["properties"]["capture_date"]) if row["properties"]["capture_date"] else None,
                             "geometry": to_wkb(shape(row["geometry"]))} for row in original]
                prebuilt = version_root / "geoparquet/typed.parquet"
                prebuilt.parent.mkdir()
                pq.write_table(pa.Table.from_pylist(rows, schema=typed), prebuilt)
            manifest = {"schema_version": 1, "processor": "HIFLD Next", "code_commit": "synthetic-fixture-only",
                        "unsupported_output_formats": {"shapefile_zip": "Synthetic int64/null/text has no verified lossless DBF rule",
                                                       "pmtiles": "Synthetic archival fixture has no approved visualization comparison rule"},
                        "inputs": [{"input_id": "synthetic", "producer": "Synthetic fixture", "host": "example.invalid",
                                    "role": "producer_publication", "canonical_url": "https://example.invalid/synthetic",
                                    "retrieval_url": "https://example.invalid/synthetic", "requested_url": "https://example.invalid/synthetic",
                                    "observed_final_url": "https://example.invalid/synthetic", "release_date_unknown_reason": "synthetic fixture",
                                    "retrieved_at": "2026-10-19T00:00:00Z", "sha256": sha256_file(path), "bytes": path.stat().st_size,
                                    "license": "synthetic", "attribution": "Synthetic fixture"},
                                   {"input_id": "regions", "producer": "Synthetic Region Agency", "host": "example.invalid", "role": "producer_publication",
                                    "canonical_url": "https://example.invalid/regions", "retrieval_url": "https://example.invalid/regions.csv",
                                    "request_url": "https://example.invalid/regions-latest", "final_url": "https://example.invalid/regions.csv",
                                    "release_date_unknown_reason": "synthetic fixture; no producer release date",
                                    "retrieved_at": "2026-10-19T00:00:00Z", "sha256": sha256_file(auxiliary), "bytes": auxiliary.stat().st_size,
                                    "license": "Synthetic auxiliary notice", "attribution": "Synthetic Region Agency"}],
                        "transformations": [{"name": "archival format conversion", "parameters": {"no_real_acceptance": True}}]}
            if prebuilt_date32:
                manifest["prebuilt_output_policy"] = {"schema_version": 1, "formats": {"geoparquet": {
                    "reason": "Synthetic GPKG generic conversion loses native date32. Preserve exact explicitly typed output; no real approval.",
                    "objects": [{"path": "geoparquet/typed.parquet", "sha256": sha256_file(prebuilt), "bytes": prebuilt.stat().st_size}]}}}
            staging.write_key(f"{prefix}/metadata/source_manifest.json", json.dumps(manifest).encode())
        return {"classification": "synthetic source", "rows": 2}

    formats = [publish_geopackage, publish_geoparquet, publish_pmtiles, publish_shapefile_zip]

    @asset(key=["publish", "native_comparison_fixture"], partitions_def=PUBLISH_PARTITIONS,
           deps=[definition.key for definition in formats])
    def native_comparison():
        if not list(baseline_root.rglob("*.parquet")):
            for path in (version_root / "geoparquet").rglob("*.parquet"):
                destination = baseline_root / path.relative_to(version_root)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(path, destination)
        if state.get("inject_drift"):
            first = next((version_root / "geoparquet").rglob("*.parquet"))
            shutil.copyfile(first, first.with_name("unapproved-extra.parquet"))
        baseline = input_spec(baseline_root)
        candidate = input_spec(version_root / "geoparquet")
        candidate["storage_prefix"] = "geoparquet"
        reference = state.get("reference", {"acceptance": "unapproved_candidate", "dataset_slug": "synthetic-native", "file_slug": "typed",
                      "version": "synthetic-archive", "data_objects": [{"url": "https://example.invalid/" + obj["key"],
                                            "sha256": obj["sha256"], "bytes": obj["bytes"]} for obj in baseline["objects"]]})
        result = compare_native_inputs(baseline, candidate, policy, version_root / "metadata", baseline_reference=reference)
        state.update(report=result, baseline=baseline, candidate=candidate)
        return Output({"status": result["status"], "classification": "synthetic; no real acceptance"})

    # Synthetic source is not registered in the production graph. Explicit op
    # dependencies are necessary here; changing AssetSpecs alone does not add
    # execution ordering to existing Dagster op definitions.
    @asset(key=publish_catalog.key, partitions_def=PUBLISH_PARTITIONS, deps=[fixture_source.key])
    def catalog(context, staging_storage: StagingStorageResource):
        output = publish_catalog.op.compute_fn.decorated_fn(context, staging_storage)
        if state.get("inject_quality"):
            key = f"{prefix}/metadata/quality_manifest.json"
            quality = json.loads(staging.read_key(key)); quality["quality_check_passed"] = False
            staging.write_key(key, json.dumps(quality).encode())
            decision = json.loads(registry.read_text())
            for name in ["staged_objects", "published_objects"]:
                decision["releases"][0][name][key] = staging.sha256_key(key)
            registry.write_text(json.dumps(decision))
        return output

    @asset(key=verify_release.key, partitions_def=PUBLISH_PARTITIONS, deps=[native_comparison.key])
    def checked(context, staging_storage: StagingStorageResource, published_storage: PublishedStorageResource):
        return verify_release.op.compute_fn.decorated_fn(context, staging_storage, published_storage)
    defs = Definitions(assets=[fixture_source, catalog, *formats, native_comparison, checked, publish_version],
                       asset_checks=hifld_asset_checks, resources={"staging_storage": staging, "published_storage": published},
                       jobs=[define_asset_job("native_synthetic_full_graph")])
    job = defs.resolve_job_def("native_synthetic_full_graph")
    with DagsterInstance.get() as instance:
        instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME, [prefix])
        unapproved = job.execute_in_process(instance=instance, partition_key=prefix, raise_on_error=False)
        assert not unapproved.success and not published.list_prefix()
        assert state["report"]["status"] == "pass", state["report"]
        # Freeze generated catalog snapshots before constructing the exact test
        # decision, so reruns exercise deterministic original evidence retention.
        snapshot_source_metadata(staging, "synthetic-native", "typed", "v0.0.0-fixture")
        reference = dict(state["report"]["baseline"])
        reference.update(acceptance="synthetic_fixture_only", mapping_evidence="SYNTHETIC ONLY", catalog_url="https://example.invalid/catalog.json",
                         catalog_sha256="0" * 64, decision={"id": "SYNTHETIC-ONLY", "approved_by": "synthetic-test-only", "approved_at": "2026-10-19"})
        state["reference"] = reference
        result = compare_native_inputs(state["baseline"], state["candidate"], policy, version_root / "metadata", baseline_reference=reference)
        assert result["status"] == "pass", result
        objects = {key: staging.sha256_key(key) for key in staging.list_prefix(prefix)}
        native = result["native_inputs"]["candidate"]["receipt"]
        entry = {"destination": _storage_identity(published), "staged_version_prefix": prefix,
                 "implementation_sha256": verifier_implementation_sha256(),
                 "decision": {"id": "SYNTHETIC-ONLY", "approved_by": "synthetic-test-only", "approved_at": "2026-10-19"},
                 "baseline": result["baseline"], "native_baseline_input": state["baseline"], "contract_sha256": result["contract_sha256"],
                 "report_sha256": objects[f"{prefix}/metadata/baseline_comparison.json"],
                 "provenance_sha256": objects[f"{prefix}/metadata/source_manifest.json"],
                 "staged_objects": objects, "published_objects": objects, "native_policy_sha256": native["policy_sha256"],
                 "candidate_comparison_input": {"key": f"{prefix}/metadata/candidate/normalized_records.jsonl", "sha256": result["candidate_sha256"],
                    "bytes": result["candidate_input_bytes"], "adapter": result["adapter"],
                    "derivation": {"code_sha256": native["code_sha256"], "parameters": {"receipt_sha256": result["native_inputs"]["candidate"]["sha256"]},
                                   "source_objects": {f"{prefix}/geoparquet/{obj['key']}": obj["sha256"] for obj in state["candidate"]["objects"]}}}}
        registry = work / "synthetic-approvals.json"
        registry.write_text(json.dumps({"schema_version": 1, "releases": [entry]}, indent=2) + "\n")
        os.environ["HIFLD_RELEASE_APPROVALS"] = str(registry)
        success = job.execute_in_process(instance=instance, partition_key=prefix, raise_on_error=False)
        assert success.success
        before = {key: published.object_snapshot(key) for key in published.list_prefix()}
        if prebuilt_date32:
            # Actual registered format/verify/promotion assets: exact policy does
            # not certify quality, registry acceptance or an unchanged source.
            key = f"{prefix}/geoparquet/typed.parquet"
            declared = json.loads(published.read_key(f"{prefix}/metadata/source_manifest.json"))["prebuilt_output_policy"]
            for filename in ["data_dictionary.json", "resolved_provenance.json"]:
                assert json.loads(published.read_key(f"{prefix}/metadata/{filename}"))["prebuilt_output_policy"] == declared
            assert pq.ParquetFile(Path(published.local_dir) / key).schema_arrow.field("capture_date").type == pa.date32()
            assert pq.ParquetFile(Path(published.local_dir) / key).read(columns=["capture_date"]).to_pylist() == [
                {"capture_date": date(2026, 10, 19)}, {"capture_date": None}]
            registry_before = registry.read_bytes()
            staged_before = {name: staging.read_key(name) for name in staging.list_prefix(prefix)}
            state["inject_quality"] = True
            quality_failure = job.execute_in_process(instance=instance, partition_key=prefix, raise_on_error=False)
            assert not quality_failure.success
            assert before == {name: published.object_snapshot(name) for name in published.list_prefix()}
            try:
                _copy_version_files(staging, published, "synthetic-native", "typed", "v0.0.0-fixture")
            except ReleaseApprovalError as exc:
                assert "catalog_quality_failed" in str(exc), str(exc)
            else:
                raise AssertionError("Failed quality promoted prebuilt output")
            state["inject_quality"] = False
            for name, data in staged_before.items():
                staging.write_key(name, data)
            registry.write_bytes(registry_before)
            (version_root / "geoparquet/typed.parquet").write_bytes(b"changed pinned native bytes")
            drift = job.execute_in_process(instance=instance, partition_key=prefix, raise_on_error=False)
            assert not drift.success
            assert before == {name: published.object_snapshot(name) for name in published.list_prefix()}
            for name, data in staged_before.items():
                staging.write_key(name, data)
            # A wholly unsupported format remains forbidden even if its exact
            # bytes were independently included in the synthetic object map.
            manifest_key = f"{prefix}/metadata/source_manifest.json"
            banned = json.loads(staging.read_key(manifest_key)); banned.pop("prebuilt_output_policy")
            banned["unsupported_output_formats"]["geoparquet"] = "Synthetic whole-format prohibition"
            staging.write_key(manifest_key, json.dumps(banned).encode())
            decision = json.loads(registry_before)
            for name in ["staged_objects", "published_objects"]:
                decision["releases"][0][name][manifest_key] = staging.sha256_key(manifest_key)
            decision["releases"][0]["provenance_sha256"] = staging.sha256_key(manifest_key)
            registry.write_text(json.dumps(decision))
            try:
                _copy_version_files(staging, published, "synthetic-native", "typed", "v0.0.0-fixture")
            except ReleaseApprovalError as exc:
                assert "unsupported_output_present:geoparquet" in str(exc), str(exc)
            else:
                raise AssertionError("Whole-format ban unexpectedly promoted")
            assert before == {name: published.object_snapshot(name) for name in published.list_prefix()}
            outcome = {"classification": "actual synthetic Date32 prebuilt graph; no real acceptance", "unapproved_run_id": unapproved.run_id,
                "success_run_id": success.run_id, "quality_failure_run_id": quality_failure.run_id, "staged_mutation_run_id": drift.run_id,
                "date32_and_null_preserved": True, "failed_quality_blocked": True, "staged_mutation_blocked": True,
                "whole_format_ban_blocked": True, "published_objects_preserved": len(before), "dagster_home": str(home)}
            (work / "integration_result.json").write_text(json.dumps(outcome, indent=2) + "\n")
            return outcome
        # Independent exact approval labels are deliberately repinned here to
        # reproduce the review defect: only native re-derivation can disprove it.
        staged_before = {key: staging.read_key(key) for key in staging.list_prefix(prefix)}
        registry_before = registry.read_bytes()
        forge_synthetic_candidate_labels(staging, registry, prefix)
        try:
            _copy_version_files(staging, published, "synthetic-native", "typed", "v0.0.0-fixture")
        except ReleaseApprovalError as exc:
            assert "replay_mismatch" in str(exc), str(exc)
            forged_derivation_blocker = str(exc)
        else:
            raise AssertionError("Repinned false native derivation unexpectedly promoted")
        assert before == {key: published.object_snapshot(key) for key in published.list_prefix()}
        (work / "forged-derivation-result.json").write_text(json.dumps({"synthetic_only": True, "independent_registry_deliberately_repinned": True,
            "blocked": True, "reason": forged_derivation_blocker, "previous_published_objects_preserved": len(before)}, indent=2) + "\n")
        for key, data in staged_before.items():
            staging.write_key(key, data)
        registry.write_bytes(registry_before)
        invalid_manifest = json.loads(staging.read_key(f"{prefix}/metadata/source_manifest.json"))
        invalid_manifest["inputs"][0]["retrieved_at"] = "not-a-date"
        staging.write_key(f"{prefix}/metadata/source_manifest.json", json.dumps(invalid_manifest).encode())
        invalid_registry = json.loads(registry_before)
        invalid_entry = invalid_registry["releases"][0]
        manifest_key = f"{prefix}/metadata/source_manifest.json"
        for inventory in ["staged_objects", "published_objects"]:
            invalid_entry[inventory][manifest_key] = staging.sha256_key(manifest_key)
        invalid_entry["provenance_sha256"] = staging.sha256_key(manifest_key)
        registry.write_text(json.dumps(invalid_registry))
        try:
            _copy_version_files(staging, published, "synthetic-native", "typed", "v0.0.0-fixture")
        except ReleaseApprovalError as exc:
            assert "retrieved_at" in str(exc), str(exc)
        else:
            raise AssertionError("Malformed provenance unexpectedly promoted")
        assert before == {key: published.object_snapshot(key) for key in published.list_prefix()}
        for key, data in staged_before.items():
            staging.write_key(key, data)
        registry.write_bytes(registry_before)
        state["inject_drift"] = True
        failure = job.execute_in_process(instance=instance, partition_key=prefix, raise_on_error=False)
        assert not failure.success
        assert before == {key: published.object_snapshot(key) for key in published.list_prefix()}
        assert state["report"]["status"] in {"fail", "blocked"}
        outcome = {"classification": "actual local synthetic full graph; no real baseline acceptance", "unapproved_run_id": unapproved.run_id,
                   "success_run_id": success.run_id, "failed_drift_run_id": failure.run_id, "published_objects": len(before),
                   "accepted_objects_unchanged": True, "candidate_evidence_preserved": True, "repinned_false_derivation_blocked": True, "invalid_provenance_blocked": True, "source_input_count": 2, "dagster_home": str(home),
                   "archival_formats": ["geopackage", "geoparquet"], "evidenced_skips": ["pmtiles", "shapefile_zip"]}
        (work / "integration_result.json").write_text(json.dumps(outcome, indent=2) + "\n")
        return outcome


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    print(json.dumps(run(parser.parse_args().work_dir.resolve())))
