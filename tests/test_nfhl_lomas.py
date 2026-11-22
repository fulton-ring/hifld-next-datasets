"""Focused contract checks for the FEMA NFHL LOMAs source adapter."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.sources import nfhl_lomas as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")


def _fixture():
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
        {"name": "CASENUMBER", "type": "esriFieldTypeString", "alias": "CASENUMBER", "length": 16, "domain": None},
        {"name": "STATUS", "type": "esriFieldTypeString", "alias": "STATUS", "length": 16, "domain": None},
        {"name": "CID", "type": "esriFieldTypeString", "alias": "CID", "length": 6, "domain": None},
        {"name": "DATEENDED", "type": "esriFieldTypeDate", "alias": "DATEENDED", "length": 8, "domain": None},
        {"name": "LAT", "type": "esriFieldTypeDouble", "alias": "LAT", "domain": None},
        {"name": "LON", "type": "esriFieldTypeDouble", "alias": "LON", "domain": None},
        {"name": "SHAPE", "type": "esriFieldTypeGeometry", "alias": "SHAPE", "domain": None},
        {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
    ]
    crs = {"wkid": 4269, "latestWkid": 4269}
    layer = {"id": 34, "name": "LOMAs", "type": "Feature Layer", "geometryType": "esriGeometryPoint",
             "maxRecordCount": 2000, "advancedQueryCapabilities": {"supportsPagination": True},
             "sourceSpatialReference": crs,
             "extent": {"xmin": -100.0, "ymin": 20.0, "xmax": -60.0, "ymax": 45.0,
                        "spatialReference": crs},
             "fields": fields}
    service = {"mapName": "NFHL",
               "layers": [{"id": 34, "name": "LOMAs", "type": "Feature Layer"}]}

    def attrs(oid):
        return {"OBJECTID": oid, "CASENUMBER": "22-01-2345A", "STATUS": "Processed", "CID": "010099",
                "DATEENDED": 1704067200000, "LAT": 34.1, "LON": -86.4,
                "GlobalID": "{9585E72B-E15E-4949-8B7A-6D7D29569B64}"}

    features = [
        {"attributes": attrs(10), "geometry": {"x": -86.41197568433415, "y": 34.148463868949534}},
        {"attributes": attrs(11), "geometry": None},
        {"attributes": attrs(12), "geometry": {"x": "NaN", "y": "NaN"}},
    ]
    ids = [10, 11, 12]
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:], "spatialReference": crs}
    docs = {"service_before": service, "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 3}, "count_after": {"count": 3},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000": page0, "page_001": page1}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    return fields, features, captures


def _contract(fields):
    """The module's remaining producer contracts, patched to the fixture's values.

    Only the schema pin and the service layer roster are code pins; the
    population is witnessed at capture time, and the PAGE_SIZE patch matches
    the fixture's two-record pages.
    """
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
              for f in fields]
    return dict(PAGE_SIZE=2,
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(34, "LOMAs", "Feature Layer")])


class _Attempt:
    """Materialize fixture capture bodies as a streaming attempt directory."""

    def __init__(self, captures):
        self._temporary = tempfile.TemporaryDirectory(prefix="lomas-validate-")
        self.capture_root = Path(self._temporary.name) / "attempt"
        self.capture_root.mkdir()
        self.receipts = {}
        for name, (body, receipt) in captures.items():
            (self.capture_root / f"{name}.body").write_bytes(body)
            (self.capture_root / f"{name}.wire").write_bytes(body)
            self.receipts[name] = receipt

    def validate(self, parquet_name="candidate.parquet"):
        return source._validate_and_extract(self.capture_root, self.receipts,
                                            parquet_path=self.capture_root / parquet_name)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self._temporary.cleanup()
        return False


class NfhlLomasValidationTests(unittest.TestCase):
    def test_native_fields_and_null_and_nan_geometry_preserved_in_parquet(self):
        fields, features, captures = _fixture()
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            original_fields, extracted = attempt.validate(parquet_name="candidate.parquet")
            table = pq.read_table(attempt.capture_root / "candidate.parquet")
        self.assertEqual(original_fields[0]["name"], "OBJECTID")
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12])
        self.assertEqual(table.column("CASENUMBER").to_pylist(), ["22-01-2345A"] * 3)
        self.assertEqual(table.column("DATEENDED").type.__str__(), "int64")
        self.assertEqual(table.column("DATEENDED").to_pylist(), [1704067200000] * 3)
        self.assertEqual(table.column("LAT").type.__str__(), "double")
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        self.assertEqual(table.column("SHAPE").null_count, 2)
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["SHAPE"]["crs"]["id"]["code"], 4269)
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)
        self.assertEqual(extracted["audit"]["nan_string_geometry_count"], 1)

    def test_streamed_candidate_keeps_one_row_group_per_page(self):
        fields, _, captures = _fixture()
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            attempt.validate(parquet_name="candidate.parquet")
            parquet = pq.ParquetFile(attempt.capture_root / "candidate.parquet")
        self.assertEqual(parquet.num_row_groups, 2)
        self.assertEqual(parquet.metadata.num_rows, 3)

    def test_id_roster_response_order_is_not_population_drift(self):
        fields, _, captures = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11]), ("ids_before", [11, 12, 10]), ("ids_after", [10, 11, 12])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            _fields, extracted = attempt.validate()
        self.assertEqual(extracted["ids"], [10, 11, 12])

    def test_id_roster_change_between_witnesses_is_rejected(self):
        fields, _, captures = _fixture()
        doc = json.loads(captures["ids_after"][0])
        doc["objectIds"] = [10, 11]
        captures["ids_after"] = (json.dumps(doc, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
                attempt.validate()

    def test_missing_service_documentation_link_is_rejected(self):
        fields, _, captures = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
                attempt.validate()

    def test_crs_drift_is_rejected(self):
        fields, _, captures = _fixture()
        page = json.loads(captures["page_000"][0])
        page["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            with self.assertRaisesRegex(ValueError, "source_page_000_crs_drift"):
                attempt.validate()

    def test_layer_geometry_type_drift_is_rejected(self):
        fields, _, captures = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPolyline"
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            with self.assertRaisesRegex(ValueError, "exact_nfhl_lomas_layer_metadata_drift"):
                attempt.validate()

    def test_unexpected_field_is_quarantined_by_contract(self):
        fields, _, captures = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["layer_before"] = (json.dumps(layer, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                attempt.validate()

    def test_missing_page_id_is_not_accepted(self):
        fields, _, captures = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"] = page["features"][:1]
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                attempt.validate()

    def test_final_page_cannot_claim_transfer_truncation(self):
        fields, _, captures = _fixture()
        final = json.loads(captures["page_001"][0])
        final["exceededTransferLimit"] = True
        captures["page_001"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        with patch.multiple(source, **_contract(fields)), _Attempt(captures) as attempt:
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                attempt.validate()


def _fake_capture_get(bodies):
    requested = []

    def fake(_client, url, capture_root, name, *, max_bytes):
        requested.append(name)
        body = bodies[name]
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-12T00:00:00+00:00",
                   "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                   "bytes": len(body), "sha256": digest, "content_encoding": None,
                   "content_length": str(len(body)), "error_code": None}
        (capture_root / f"{name}.body").write_bytes(body)
        (capture_root / f"{name}.wire").write_bytes(body)
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        return body, receipt

    return fake, requested


class EmptyClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _capture_patches(fields):
    return dict(_contract(fields), DOC_CAPTURE_BASE_DELAY_SECONDS=0.01,
                PAGE_CAPTURE_BASE_DELAY_SECONDS=0.01)


class NfhlLomasCaptureTests(unittest.TestCase):
    def test_capture_rejects_incomplete_page_and_quarantines(self):
        fields, _, fixture_captures = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        page = json.loads(bodies["page_001"])
        page["features"] = page["features"][:0]
        bodies["page_001"] = json.dumps(page, separators=(",", ":")).encode()
        fake, _ = _fake_capture_get(bodies)
        with tempfile.TemporaryDirectory(prefix="lomas-drift-") as temporary, \
                patch.multiple(source, **_capture_patches(fields)), \
                patch.object(source, "capture_get", side_effect=fake), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source.capture_nfhl_lomas(storage, version="vfixture", capture_directory=str(root / "captures"))
            attempt = next((root / "captures").iterdir())
            manifest = json.loads((attempt / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])

    def test_upstream_added_feature_passes_completeness_and_warns_on_drift(self):
        from dagster import AssetCheckSeverity

        from dagster_hifld.assets.nfhl_lomas import (
            fema_nfhl_lomas_approved_baseline_drift,
            fema_nfhl_lomas_complete,
        )

        fields, features, fixture_captures = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        # Upstream edit: the producer adds one feature (OBJECTID 13) between
        # captures; every capture-time witness reflects the new population.
        added = {"attributes": {"OBJECTID": 13, "CASENUMBER": "22-01-2346B", "STATUS": "Processed",
                                "CID": "010099", "DATEENDED": 1704153600000, "LAT": 34.2, "LON": -86.3,
                                "GlobalID": "{9585E72B-E15E-4949-8B7A-6D7D29569B65}"},
                 "geometry": {"x": -86.3, "y": 34.2}}
        ids = [10, 11, 12, 13]
        crs = {"wkid": 4269, "latestWkid": 4269}
        for name in ("ids_seed", "ids_before", "ids_after"):
            body = json.loads(bodies[name])
            body["objectIds"] = ids
            bodies[name] = json.dumps(body, separators=(",", ":")).encode()
        for name in ("count_before", "count_after"):
            body = json.loads(bodies[name])
            body["count"] = 4
            bodies[name] = json.dumps(body, separators=(",", ":")).encode()
        bodies["page_000"] = json.dumps(
            {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True},
            separators=(",", ":")).encode()
        bodies["page_001"] = json.dumps(
            {"features": [features[2], added], "spatialReference": crs},
            separators=(",", ":")).encode()
        fake, _ = _fake_capture_get(bodies)
        with tempfile.TemporaryDirectory(prefix="lomas-upstream-edit-") as temporary, \
                patch.multiple(source, **_capture_patches(fields)), \
                patch.object(source, "capture_get", side_effect=fake), \
                patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
            root = Path(temporary)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            candidate = source.capture_nfhl_lomas(storage, version="vfixture",
                                                  capture_directory=str(root / "captures"))
        # Completeness: the capture passes with the full walked population.
        self.assertEqual(candidate["feature_count"], 4)
        self.assertEqual(candidate["source_reported_count"], 4)
        self.assertTrue(candidate["source_population_complete"])
        complete = fema_nfhl_lomas_complete(candidate)
        self.assertTrue(complete.passed)
        # Drift: no independently approved baseline exists, so the check warns
        # (non-blocking) instead of failing the run.
        drift = fema_nfhl_lomas_approved_baseline_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, _, fixture_captures = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        fake, _ = _fake_capture_get(bodies)

        with tempfile.TemporaryDirectory(prefix="lomas-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_capture_patches(fields)), \
                 patch.object(source, "capture_get", side_effect=fake), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_lomas(storage, version="vfixture",
                                                      capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 3)
                self.assertEqual(candidate["source_reported_count"], 3)
                self.assertEqual(candidate["source_null_geometry_count"], 1)
                self.assertEqual(candidate["source_nan_string_geometry_count"], 1)
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 2)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_nfhl_lomas(storage, version="vfixture",
                                                   capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])
                self.assertEqual(reused["feature_count"], 3)
                self.assertEqual(reused["source_reported_count"], 3)
                self.assertTrue(reused["source_population_complete"])

    def test_generated_source_manifest_identifies_the_lomas_inventory(self):
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest

        fields, _, fixture_captures = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        fake, _ = _fake_capture_get(bodies)

        with tempfile.TemporaryDirectory(prefix="lomas-identity-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_capture_patches(fields)), \
                 patch.object(source, "capture_get", side_effect=fake), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_lomas(storage, version="vfixture",
                                                      capture_directory=str(root / "captures"))
            manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                     candidate["version"]).metadata
            serialized = _canonical(manifest).decode()
            self.assertIn("LOMAs", manifest["title"])
            self.assertIn("layer 34 'LOMAs'", manifest["lineage"])
            self.assertIn("nfhl/lomas-1", manifest["lineage"])
            self.assertIn("DataLumos project 239978", manifest["lineage"])
            self.assertNotIn("coastal-gages", serialized)
            self.assertNotIn("239797", serialized)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"geoparquet/{source.DATASET_SLUG}.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           f"geoparquet/{source.DATASET_SLUG}.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


class NfhlLomasWiringTests(unittest.TestCase):
    def test_module_lists_and_definitions_are_registered(self):
        from dagster_hifld.assets import nfhl_lomas as wiring

        self.assertEqual([asset.key.to_user_string() for asset in wiring.INGEST_ASSETS],
                         [f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}"])
        self.assertEqual(len(wiring.ASSET_CHECKS), 2)
        self.assertIs(wiring.ASSET_CHECKS[0], wiring.fema_nfhl_lomas_complete)
        self.assertIs(wiring.ASSET_CHECKS[1], wiring.fema_nfhl_lomas_approved_baseline_drift)
        self.assertEqual(wiring.nfhl_lomas_draft_schedule.default_status, wiring.DefaultScheduleStatus.STOPPED)

    def test_discovery_collects_the_dataset(self):
        from dagster_hifld.assets.discovery import discovered

        names = [asset.key.to_user_string() for asset in discovered("INGEST_ASSETS")]
        self.assertIn(f"ingest/{source.DATASET_SLUG}/{source.FILE_SLUG}", names)

    def test_blocking_check_uses_capture_reported_count(self):
        from dagster_hifld.assets.nfhl_lomas import fema_nfhl_lomas_complete

        passed = fema_nfhl_lomas_complete({"feature_count": 3, "source_reported_count": 3,
                                           "source_population_complete": True,
                                           "candidate_ready": "some/path"})
        self.assertTrue(passed.passed)
        self.assertEqual(passed.metadata["source_reported_count"].value, 3)
        missing = fema_nfhl_lomas_complete({"feature_count": 3, "source_reported_count": 3,
                                            "source_population_complete": False,
                                            "candidate_ready": "some/path"})
        self.assertFalse(missing.passed)
        mismatch = fema_nfhl_lomas_complete({"feature_count": 2, "source_reported_count": 3,
                                             "source_population_complete": True,
                                             "candidate_ready": "some/path"})
        self.assertFalse(mismatch.passed)


if __name__ == "__main__":
    unittest.main()
