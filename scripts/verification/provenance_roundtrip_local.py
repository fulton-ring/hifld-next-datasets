"""Actual isolated multi-input metadata/STAC/SQLite publication fixture.

Uses synthetic-only external decisions after rendering exact bytes. No approval
mocks, network fetch, production storage or real source acceptance is involved.
"""
import argparse
import copy
import hashlib
import json
import os
import sqlite3
from pathlib import Path

from dagster_hifld.portolan.catalog import CatalogRecord
from dagster_hifld.portolan.provenance import attach_published_provenance
from dagster_hifld.portolan.release import ReleasePointer
from dagster_hifld.portolan.workflow import (
    RELEASE_POINTER_KEY, PortolanPublishRequest, _record_assets, apply_portolan_release_bundle,
    inspect_geoparquet, prepare_portolan_release_bundle,
)
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource
from dagster_hifld.validation import sha256_file
from scripts.verification.native_synthetic_local import run as run_native


def approve_synthetic_plan(registry: Path, plan: dict) -> None:
    value = json.loads(registry.read_text())
    assert value["releases"][0]["decision"]["id"] == "SYNTHETIC-ONLY"
    value["releases"][0]["published_objects"].update({obj["key"]: obj["sha256"] for obj in plan["objects"]})
    registry.write_text(json.dumps(value, indent=2) + "\n")


def _repin_synthetic_plan(directory: Path, plan: dict) -> None:
    """Adversarial fixtures intentionally repin exact bytes, never authority."""
    pointer_path = directory / "pointer.json"
    pointer = json.loads(pointer_path.read_text())
    database = directory / "tree/_catalog/catalog.sqlite"
    pointer.update(sha256=sha256_file(database), size_bytes=database.stat().st_size)
    pointer_path.write_text(json.dumps(pointer))
    for descriptor in plan["objects"]:
        path = directory / descriptor["path"]
        descriptor.update(sha256=sha256_file(path), bytes=path.stat().st_size)
    (directory / "bundle_plan.json").write_text(json.dumps(plan))


class CommitRaceStorage(PublishedStorageResource):
    """Inject a real data replacement after release-object writes, before CAS."""

    def write_key_with_dependencies_if_unchanged(self, key, data, expected_snapshot, dependencies):
        dependency_key = next(name for name in dependencies if "/geoparquet/" in name)
        (Path(self.local_dir) / dependency_key).write_bytes(b"injected actual dependency mutation at pointer commit")
        return super().write_key_with_dependencies_if_unchanged(key, data, expected_snapshot, dependencies)


def exercise_dependency_negatives(work, published, record, request, registry):
    results = {}
    data_key = next(key for key in published.list_prefix() if "/geoparquet/" in key)
    original_data = published.read_key(data_key)
    for mode in ["wrong_metadata_href", "changed_data_after_plan", "sqlite_metadata_omission", "sqlite_object_bounds", "commit_boundary_dependency_race"]:
        directory = work / mode
        plan = prepare_portolan_release_bundle((record,), request, published, directory)
        pointer_before = published.read_key(RELEASE_POINTER_KEY)
        if mode == "wrong_metadata_href":
            path = directory / "tree" / record.version_path / "collection.json"
            doc = json.loads(path.read_text())
            key, asset = next((key, value) for key, value in doc["assets"].items() if value.get("title") == "metadata/resolved_provenance.json")
            asset["href"] = "https://unrelated.example.invalid/wrong/metadata/resolved_provenance.json"
            path.write_text(json.dumps(doc))
            # Keep SQLite consistent too, so URL→actual object reconciliation
            # itself (rather than a projection mismatch) must reject this case.
            with sqlite3.connect(directory / "tree/_catalog/catalog.sqlite") as connection:
                asset_path = record.version_path + "/" + key
                connection.execute("UPDATE assets SET href=? WHERE asset_path=?", (asset["href"], asset_path))
                connection.execute("UPDATE asset_locations SET href=? WHERE asset_path=?", (asset["href"], asset_path))
            _repin_synthetic_plan(directory, plan)
        elif mode == "changed_data_after_plan":
            (Path(published.local_dir) / data_key).write_bytes(b"actual changed data after planning")
        elif mode == "sqlite_metadata_omission":
            with sqlite3.connect(directory / "tree/_catalog/catalog.sqlite") as connection:
                connection.execute("DELETE FROM assets WHERE format_key='metadata'")
            _repin_synthetic_plan(directory, plan)
        elif mode == "sqlite_object_bounds":
            with sqlite3.connect(directory / "tree/_catalog/catalog.sqlite") as connection:
                connection.execute("UPDATE asset_objects SET native_bbox_json='[0,0,0,0]', crs84_bbox_json='[0,0,0,0]'")
            _repin_synthetic_plan(directory, plan)
        approve_synthetic_plan(registry, plan)
        before = {key: published.object_snapshot(key) for key in published.list_prefix()}
        storage = CommitRaceStorage(use_local=True, local_dir=published.local_dir) if mode == "commit_boundary_dependency_race" else published
        try:
            apply_portolan_release_bundle(directory, storage)
        except (ValueError, RuntimeError) as exc:
            expected = {"wrong_metadata_href": "href", "changed_data_after_plan": "asset dependency", "sqlite_metadata_omission": "SQLite asset inventory", "sqlite_object_bounds": "object bounds", "commit_boundary_dependency_race": "dependency changed"}[mode]
            assert expected in str(exc), (mode, str(exc))
            results[mode] = {"blocked": True, "reason": str(exc)}
        else:
            raise AssertionError(mode + " unexpectedly published")
        after = {key: published.object_snapshot(key) for key in published.list_prefix()}
        assert set(before) == set(after), mode
        assert all(snapshot == after[key] for key, snapshot in before.items() if mode != "commit_boundary_dependency_race" or key != data_key), mode
        assert published.read_key(RELEASE_POINTER_KEY) == pointer_before, mode
        assert (directory / "bundle_plan.json").exists(), mode
        results[mode].update(pointer_preserved=True, prior_object_roster_preserved=len(after), plan_evidence_preserved=True)
        if mode in {"changed_data_after_plan", "commit_boundary_dependency_race"}:
            # The injected external writer changed this dependency. Apply must
            # never repair it itself or alter any of the other accepted objects.
            (Path(published.local_dir) / data_key).write_bytes(original_data)
    return results


def run(work: Path) -> dict:
    if work.exists() and any(work.iterdir()):
        raise ValueError("Use a new empty local fixture directory")
    native = run_native(work / "native")
    published = PublishedStorageResource(use_local=True, local_dir=str(work / "native/published"))
    version_prefix = "synthetic-native/typed/v0.0.0-fixture"
    request = PortolanPublishRequest("synthetic", "synthetic-native", "typed", "v0.0.0-fixture", "Synthetic typed fixture",
                                     "Two captured inputs; fixture only", "Original source", public_root="https://example.invalid/published")
    facts = inspect_geoparquet(Path(published.local_dir) / version_prefix)
    record = CatalogRecord("synthetic", "synthetic-native", "typed", request.version, request.title, request.description, "spatial", facts.feature_count,
                           _record_assets(published, request), columns=facts.columns, native_crs=facts.native_crs,
                           geometry_column=facts.geometry_column, geometry_type=facts.geometry_type, native_bbox=facts.native_bbox,
                           crs84_bbox=facts.crs84_bbox, provider="Original source", license_id="other")
    record = attach_published_provenance(record, published, version_prefix, request.public_root)
    expected = json.loads(published.read_key(f"{version_prefix}/metadata/resolved_provenance.json"))
    assert len(expected["inputs"]) == 2
    registry = work / "native/synthetic-approvals.json"
    os.environ["HIFLD_RELEASE_APPROVALS"] = str(registry)
    plan_dir = work / "catalog-plan"
    plan = prepare_portolan_release_bundle((record,), request, published, plan_dir)
    before = {key: published.object_snapshot(key) for key in published.list_prefix()}
    try:
        apply_portolan_release_bundle(plan_dir, published)
    except ReleaseApprovalError:
        pass
    else:
        raise AssertionError("Unapproved catalog plan unexpectedly published")
    assert before == {key: published.object_snapshot(key) for key in published.list_prefix()}
    approve_synthetic_plan(registry, plan)
    generation = apply_portolan_release_bundle(plan_dir, published)
    pointer = ReleasePointer.parse(published.read_key(RELEASE_POINTER_KEY))
    assert pointer.generation == generation
    collection_key = f"releases/{generation}/{record.version_path}/collection.json"
    collection = json.loads(published.read_key(collection_key))
    assert collection["hifld:provenance"] == expected
    assert collection["hifld:processor"] == "HIFLD Next"
    assert {item["name"] for item in collection["providers"] if "producer" in item["roles"]} == {item["producer"] for item in expected["inputs"]}
    assert next(item for item in collection["providers"] if item["name"] == "HIFLD Next")["roles"] == ["processor", "host"]
    metadata_assets = [asset for asset in collection["assets"].values() if "metadata" in asset["roles"]]
    for asset in metadata_assets:
        contents = published.read_key(asset["hifld:object_key"])
        assert len(contents) == asset["file:size"]
        assert "1220" + hashlib.sha256(contents).hexdigest() == asset["file:checksum"]
    actual_db = work / "published-catalog.sqlite"
    actual_db.write_bytes(published.read_key(pointer.catalog_key))
    with sqlite3.connect(actual_db) as connection:
        metadata_rows = connection.execute("SELECT count(*) FROM assets WHERE format_key='metadata'").fetchone()[0]
        assert metadata_rows == len(metadata_assets)
    accepted = {key: published.object_snapshot(key) for key in published.list_prefix()}
    # The second prepared release proves that removing dependency labels cannot
    # bypass actual metadata reconciliation, even with exact plan approvals.
    next_dir = work / "next-catalog-plan"
    next_plan = prepare_portolan_release_bundle((record,), request, published, next_dir)
    original_plan = copy.deepcopy(next_plan)
    next_plan["metadata_dependencies"] = {}
    (next_dir / "bundle_plan.json").write_text(json.dumps(next_plan))
    approve_synthetic_plan(registry, next_plan)
    try:
        apply_portolan_release_bundle(next_dir, published)
    except ValueError as exc:
        assert "dependency inventory" in str(exc), str(exc)
    else:
        raise AssertionError("Removed dependency bindings unexpectedly published")
    (next_dir / "bundle_plan.json").write_text(json.dumps(original_plan))
    relative_collection = "tree/" + record.version_path + "/collection.json"
    altered = json.loads((next_dir / relative_collection).read_text())
    altered["hifld:provenance"]["inputs"] = altered["hifld:provenance"]["inputs"][:1]
    retained_ids = {item["input_id"] for item in altered["hifld:provenance"]["inputs"]}
    for layer in altered["hifld:provenance"].get("manifest_layers", []):
        layer["input_ids"] = [identity for identity in layer["input_ids"] if identity in retained_ids]
    (next_dir / relative_collection).write_text(json.dumps(altered))
    descriptor = next(obj for obj in original_plan["objects"] if obj["path"] == relative_collection)
    descriptor.update(sha256=sha256_file(next_dir / relative_collection), bytes=(next_dir / relative_collection).stat().st_size)
    (next_dir / "bundle_plan.json").write_text(json.dumps(original_plan))
    approve_synthetic_plan(registry, original_plan)
    try:
        apply_portolan_release_bundle(next_dir, published)
    except ValueError as exc:
        assert "complete input provenance" in str(exc), str(exc)
    else:
        raise AssertionError("Repinned omitted-input STAC unexpectedly published")
    assert accepted == {key: published.object_snapshot(key) for key in published.list_prefix()}
    dependency_negatives = exercise_dependency_negatives(work, published, record, request, registry)
    outcome = {"classification": "actual isolated synthetic publication; no real acceptance", "source_input_count": len(expected["inputs"]),
               "generation": generation, "release_pointer_key": RELEASE_POINTER_KEY, "collection_key": collection_key,
               "metadata_assets_roundtripped": len(metadata_assets), "sqlite_metadata_rows": metadata_rows,
               "missing_approval_blocked": True, "removed_dependency_labels_blocked": True, "repinned_omitted_input_blocked": True,
               "prior_published_objects_preserved": len(accepted), "native_graph": native,
               "asset_dependency_negative_probes": dependency_negatives}
    (work / "roundtrip_result.json").write_text(json.dumps(outcome, indent=2) + "\n")
    return outcome


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    print(json.dumps(run(parser.parse_args().work_dir.resolve())))
