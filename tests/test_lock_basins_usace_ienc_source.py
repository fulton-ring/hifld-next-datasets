"""Source contract tests for the complete official IENC LOCK_BASIN_AREA layer."""

from __future__ import annotations

import copy
import io
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely
import httpx

from dagster_hifld.sources.lock_basins_usace_ienc import (
    BTS_ITEM_ID,
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_FIELDS,
    EXPECTED_ID_SHA256,
    EXPECTED_LAYER_EXTENT,
    EXPECTED_IDS,
    FILE_SLUG,
    LAYER_URL,
    MIN_FREE_BYTES,
    PAGE_SIZE,
    _esri_polygon,
    _parquet_bytes,
    _validate,
    processing_identity,
    validate_processing_identity,
)
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import lock_basins_usace_ienc as source


def _ring(x: float, y: float, size: float = 0.001) -> list[list[float]]:
    # ArcGIS exterior rings are clockwise.
    return [
        [x, y], [x, y + size], [x + size, y + size], [x + size, y], [x, y],
    ]


def _features(ids: list[int]) -> list[dict]:
    features = []
    for index, oid in enumerate(ids):
        x = EXPECTED_LAYER_EXTENT["xmin"] + (index % 1000) * 0.005
        features.append({
            "attributes": {
                "OBJECTID": oid,
                "Horizontal_Clearance_Length": "56 ft",
                "Horizontal_Clearance_Width": "360 ft",
                "Horizontal_Length": "56 ft",
                "Horizontal_Width": "360 ft",
                "Object_Name": "Test Lock Basin",
                "Status": "Operational",
                "Information": "Synthetic fixture",
                "Scale_Minimum": 60000,
                "Text_Description": "Test description",
                "Source_Date": "20210701",
                "Source_Indication": "USACE IENC",
                "Source_Dataset": "IENC lock basins",
                "Feature_ID": f"LOCK-{oid:04d}",
            },
            "geometry": {"rings": [_ring(x, EXPECTED_LAYER_EXTENT["ymin"])]},
        })
    return features


def _docs() -> dict[str, dict]:
    item = {
        "id": BTS_ITEM_ID,
        "owner": "USDOT_BTS",
        "title": "Inland Electronic Navigational Charts",
        "description": (
            "The Inland Electronic Navigational Charts (IENC) dataset is updated "
            "bi-monthly by the US Army Corps (USACE)/Army Geospatial Center (AGC), "
            "and part of U.S. Department of Transportation (USDOT)/Bureau of "
            "Transportation Statistics (BTS) National Transportation Atlas Database "
            "(NTAD). These vector data, that make up the charts, can be downloaded "
            "as a geodatabase here: https://ienccloud.us/ienc/products/files/U37/"
            "ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip. In addition, "
            "web mapping services of the feature classes/datasets can be found here: "
            "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes."
        ),
    }
    directory = {
        "services": [
            {"name": "IENC_Feature_Classes/LOCK_BASIN_AREA", "type": "MapServer"},
            {"name": "IENC_Feature_Classes/LOCK_BASIN_POINT", "type": "MapServer"},
        ],
    }
    service = {
        "mapName": "LOCK_BASIN_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 77, "name": "LOCK_BASIN_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    layer = {
        "id": 77,
        "name": "LOCK_BASIN_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**EXPECTED_LAYER_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "maxRecordCount": 2000,
        "fields": copy.deepcopy(EXPECTED_FIELD_DECLARATIONS),
    }
    crs = {"wkid": 4326, "latestWkid": 4326}
    docs: dict[str, dict] = {
        "bts_item": item,
        "service_directory": directory,
        "service_before": service,
        "service_after": copy.deepcopy(service),
        "layer_before": layer,
        "layer_after": copy.deepcopy(layer),
        "count_before": {"count": EXPECTED_COUNT},
        "count_after": {"count": EXPECTED_COUNT},
        "ids_before": {"objectIds": list(EXPECTED_IDS)},
        "ids_after": {"objectIds": list(EXPECTED_IDS)},
        "ids_seed": {"objectIds": list(EXPECTED_IDS)},
    }
    features = _features(EXPECTED_IDS)
    for index, start in enumerate(range(0, EXPECTED_COUNT, PAGE_SIZE)):
        page_features = features[start:start + PAGE_SIZE]
        docs[f"page_{index:03d}"] = {
            "spatialReference": crs,
            "features": copy.deepcopy(page_features),
        }
    return docs


def _captures() -> dict[str, tuple[bytes, dict]]:
    docs = _docs()
    return {
        name: (json.dumps(doc).encode("utf-8"),
               {"complete_body": True, "status": 200})
        for name, doc in docs.items()
    }


class CapturePreflight(unittest.TestCase):
    def test_disk_floor_refuses_before_constructing_http_client(self):
        with tempfile.TemporaryDirectory(prefix="lock-basins-disk-floor-") as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            with patch.object(
                source.shutil, "disk_usage", return_value=SimpleNamespace(free=MIN_FREE_BYTES - 1)
            ), patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP must not start")):
                with self.assertRaisesRegex(ValueError, "disk_floor_before_acquisition"):
                    source.capture_lock_basins_usace_ienc(
                        storage, version="disk-floor", capture_directory=str(Path(temporary) / "capture")
                    )

    def test_operator_capture_limit_stops_before_http(self):
        with tempfile.TemporaryDirectory(prefix="lock-basins-capture-limit-") as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            capture = Path(temporary) / "capture"
            (capture / "prior").mkdir(parents=True)
            (capture / "prior" / "old.body").write_bytes(b"12345")
            with patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=MIN_FREE_BYTES)), \
                    patch.object(source.httpx, "Client", side_effect=AssertionError("HTTP must not start")):
                with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded_before_request"):
                    source.capture_lock_basins_usace_ienc(
                        storage, version="lower-limit", capture_directory=str(capture),
                        max_capture_bytes=4,
                    )

    def test_operator_capture_limit_cannot_exceed_profile_ceiling(self):
        with tempfile.TemporaryDirectory(prefix="lock-basins-invalid-limit-") as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            with self.assertRaisesRegex(ValueError, "invalid_capture_byte_limit"):
                source.capture_lock_basins_usace_ienc(
                    storage, version="invalid-limit", max_capture_bytes=source.MAX_CAPTURE_BYTES + 1,
                )

    def test_early_bad_replay_does_not_read_oversized_receipt(self):
        with tempfile.TemporaryDirectory(prefix="lock-basins-bad-replay-") as temporary:
            root = Path(temporary) / "sealed"
            root.mkdir()
            manifest = root / "capture_manifest.json"
            manifest.write_bytes(b"x" * (source.MAX_MANIFEST_BYTES + 1))
            receipt_path = root / "malformed.receipt.json"
            with receipt_path.open("wb") as stream:
                stream.truncate(source.MAX_RECEIPT_BYTES + 1)
            storage = StagingStorageResource(
                use_local=True, local_dir=str(Path(temporary) / "staging"), prefix="")
            original_read_bytes = Path.read_bytes

            def forbid_receipt_read(path):
                if path.name.endswith(".receipt.json"):
                    raise AssertionError("replay error handler must not read receipt files")
                return original_read_bytes(path)

            with patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=MIN_FREE_BYTES)), \
                    patch.object(Path, "read_bytes", forbid_receipt_read), \
                    patch.object(source.httpx, "Client", side_effect=AssertionError("replay must not use HTTP")):
                with self.assertRaisesRegex(ValueError, "retained_capture_manifest_too_large"):
                    source.capture_lock_basins_usace_ienc(
                        storage, version="bad-replay", capture_directory=str(root))
            self.assertEqual(manifest.stat().st_size, source.MAX_MANIFEST_BYTES + 1)
            self.assertEqual(receipt_path.stat().st_size, source.MAX_RECEIPT_BYTES + 1)


class CaptureResponseBudget(unittest.TestCase):
    def test_response_limit_failure_is_not_retried(self):
        class Client:
            calls = 0

            def stream(self, method, url, **kwargs):
                self.calls += 1
                request = httpx.Request(method, url)
                response = httpx.Response(200, stream=httpx.ByteStream(b"payload"), request=request)

                class StreamContext:
                    def __enter__(self):
                        return response

                    def __exit__(self, *exc):
                        response.close()

                return StreamContext()

        client = Client()
        limited = SimpleNamespace(
            complete=False,
            status_code=200,
            content_encoding="identity",
            content_length=7,
            error_code="wire_limit_exceeded",
            error_type=None,
        )
        with tempfile.TemporaryDirectory() as directory, patch.object(
            source, "capture_http_response", return_value=limited
        ), patch.object(source.time, "sleep", return_value=None):
            with self.assertRaisesRegex(ValueError, "source_response_limit_exceeded:wire_limit_exceeded"):
                source._fetch_retry(client, "https://example.test/layer", Path(directory), "page", [0],
                                    schedule=(0.0, 0.0, 0.0))
        self.assertEqual(client.calls, 1)

    def test_actual_oversized_wire_block_is_rejected_before_persistence(self):
        payload = b"x" * (2_000_001)
        request = httpx.Request("GET", "https://example.test/layer")
        response = httpx.Response(200, stream=httpx.ByteStream(payload), request=request)

        class Client:
            def stream(self, method, url, **kwargs):
                class StreamContext:
                    def __enter__(self):
                        return response

                    def __exit__(self, *exc):
                        response.close()

                return StreamContext()

        with tempfile.TemporaryDirectory(prefix="lock-basins-wire-oversize-") as directory:
            root = Path(directory)
            with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded"):
                source._fetch(Client(), "https://example.test/layer", root, "page", [0],
                              max_capture_bytes=2_000_000)
            self.assertFalse((root / "page.wire").exists())
            self.assertFalse((root / "page.body").exists())
            self.assertTrue((root / "page.receipt.json").is_file())

    def test_sealed_replay_rejects_oversized_artifact_before_opening_it(self):
        with tempfile.TemporaryDirectory(prefix="lock-basins-replay-size-") as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            capture = Path(temporary) / "capture"
            capture.mkdir()
            (capture / "ids_seed.body").write_bytes(b"123456")
            (capture / "ids_seed.wire").write_bytes(b"x")
            (capture / "ids_seed.receipt.json").write_bytes(b"{}")
            roster = {
                "ids_seed.body": {"bytes": 6, "sha256": "0" * 64},
                "ids_seed.wire": {"bytes": 1, "sha256": "0" * 64},
                "ids_seed.receipt.json": {"bytes": 2, "sha256": "0" * 64},
            }
            (capture / "capture_manifest.json").write_bytes(json.dumps({
                "complete": True, "receipts": {}, "attempts": {}, "artifact_roster": roster,
            }).encode())
            opened = []
            real_open = source.os.open

            def tracked_open(path, *args, **kwargs):
                opened.append(Path(path).name)
                return real_open(path, *args, **kwargs)

            with patch.object(source.shutil, "disk_usage", return_value=SimpleNamespace(free=MIN_FREE_BYTES)), \
                    patch.object(source, "MAX_BODY_BYTES", 5), patch.object(source.os, "open", side_effect=tracked_open):
                with self.assertRaisesRegex(ValueError, "retained_capture_artifact_too_large"):
                    source.capture_lock_basins_usace_ienc(
                        storage, version="oversized-replay", capture_directory=str(capture),
                        max_capture_bytes=100,
                    )
            self.assertEqual(opened, ["capture_manifest.json"])


class ValidateCompleteCapture(unittest.TestCase):
    def test_complete_capture_passes(self):
        fields, features, audit = _validate(_captures())
        self.assertEqual(len(features), EXPECTED_COUNT)
        self.assertEqual(audit["source_feature_count"], EXPECTED_COUNT)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], EXPECTED_COUNT)
        self.assertEqual(audit["feature_id_distinct_count"], EXPECTED_COUNT)
        self.assertEqual(audit["geometry_valid_count"], EXPECTED_COUNT)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(fields, EXPECTED_FIELD_DECLARATIONS)

    def test_native_null_and_duplicate_feature_ids_are_preserved(self):
        docs = _docs()
        features = docs["page_000"]["features"]
        duplicate_value = features[1]["attributes"]["Feature_ID"]
        features[0]["attributes"]["Feature_ID"] = None
        features[2]["attributes"]["Feature_ID"] = duplicate_value
        captures = {
            name: (json.dumps(doc).encode("utf-8"), {"complete_body": True, "status": 200})
            for name, doc in docs.items()
        }
        _fields, validated, audit = _validate(captures)
        self.assertEqual(audit["feature_id_null_count"], 1)
        self.assertEqual(audit["feature_id_duplicate_count"], 1)
        self.assertEqual(validated[0]["attributes"]["Feature_ID"], None)
        self.assertEqual(validated[1]["attributes"]["Feature_ID"], duplicate_value)
        self.assertEqual(validated[2]["attributes"]["Feature_ID"], duplicate_value)
        table = pq.read_table(io.BytesIO(_parquet_bytes(
            EXPECTED_FIELD_DECLARATIONS, validated, 4326
        )))
        self.assertEqual(table.column("Feature_ID").null_count, 1)
        self.assertEqual(table.column("Feature_ID").to_pylist()[:2], [None, duplicate_value])

    def test_expected_schema_pins_every_declared_field_and_type(self):
        expected = [
            ("OBJECTID", "esriFieldTypeOID"),
            ("SHAPE", "esriFieldTypeGeometry"),
            ("Horizontal_Clearance_Length", "esriFieldTypeString"),
            ("Horizontal_Clearance_Width", "esriFieldTypeString"),
            ("Horizontal_Length", "esriFieldTypeString"),
            ("Horizontal_Width", "esriFieldTypeString"),
            ("Object_Name", "esriFieldTypeString"),
            ("Status", "esriFieldTypeString"),
            ("Information", "esriFieldTypeString"),
            ("Scale_Minimum", "esriFieldTypeInteger"),
            ("Text_Description", "esriFieldTypeString"),
            ("Source_Date", "esriFieldTypeString"),
            ("Source_Indication", "esriFieldTypeString"),
            ("Source_Dataset", "esriFieldTypeString"),
            ("Feature_ID", "esriFieldTypeString"),
        ]
        self.assertEqual(EXPECTED_FIELDS, expected)
        self.assertEqual(
            EXPECTED_FIELD_DECLARATIONS,
            [
                {"name": name, "type": kind, "alias": name, "domain": None,
                 **({"length": 254} if kind == "esriFieldTypeString" else {})}
                for name, kind in expected
            ],
        )

    def test_expected_population_pins(self):
        self.assertEqual(EXPECTED_COUNT, 174)
        self.assertEqual(EXPECTED_IDS[0], 1)
        self.assertEqual(EXPECTED_IDS[-1], 174)
        self.assertEqual(len(EXPECTED_IDS), EXPECTED_COUNT)
        self.assertEqual(
            EXPECTED_ID_SHA256,
            "0cf671f214459975e17fcdd41bcc2f42b4fe07060f864c73745e50e9c769856c",
        )
        self.assertTrue(LAYER_URL.endswith("LOCK_BASIN_AREA/MapServer/77"))


class PackagedRuntimeIdentity(unittest.TestCase):
    def test_no_git_packaged_runtime_uses_verified_installed_code_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "src/dagster_hifld"
            source.mkdir(parents=True)
            (source / "pipeline.py").write_text("processor = 'lock_basins'\n", encoding="utf-8")
            (root / "pyproject.toml").write_text("[project]\nname='runtime-fixture'\n", encoding="utf-8")
            identity = processing_identity(root)
            self.assertEqual(identity["code_commit_kind"], "installed_processing_fingerprint_not_vcs_commit")
            self.assertTrue(str(identity["code_commit"]).startswith("installed-processing-sha256:"))
            self.assertNotIn("uv.lock", identity["files"])
            self.assertEqual(identity["missing_processing_inputs"], ["uv.lock"])
            validate_processing_identity(root, identity)
            (root / "uv.lock").write_text("# installed after initial capture\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "installed_processing_code_identity_changed"):
                validate_processing_identity(root, identity)
            packaged_identity = processing_identity(root)
            self.assertEqual(packaged_identity["missing_processing_inputs"], [])
            self.assertIn("uv.lock", packaged_identity["files"])
            validate_processing_identity(root, packaged_identity)
            (root / "uv.lock").write_text("# changed lock\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "installed_processing_code_identity_changed"):
                validate_processing_identity(root, packaged_identity)
            (source / "pipeline.py").write_text("processor = 'changed'\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "installed_processing_code_identity_changed"):
                validate_processing_identity(root, identity)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(DATASET_SLUG, FILE_SLUG, "candidate-v1", "geoparquet/lock-basins-usace-ienc.parquet", b"candidate")
            accepted_key = published.write(DATASET_SLUG, FILE_SLUG, "accepted-v1", "geoparquet/lock-basins-usace-ienc.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError, "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, DATASET_SLUG, FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)


class ValidateDriftRejections(unittest.TestCase):
    def _mutate(self, name: str, mutation) -> None:
        docs = _docs()
        mutation(docs)
        captures = {
            key: (json.dumps(doc).encode("utf-8"), {"complete_body": True, "status": 200})
            for key, doc in docs.items()
        }
        with self.assertRaises(ValueError) as raised:
            _validate(captures)
        return raised.exception

    def test_bts_owner_drift(self):
        code = self._mutate("bts_item", lambda d: d["bts_item"].update(owner="SOMEONE_ELSE"))
        self.assertEqual(str(code), "bts_ienc_item_identity_drift")

    def test_bts_distribution_link_drift(self):
        code = self._mutate(
            "bts_item", lambda d: d["bts_item"].update(description="unrelated text"))
        self.assertEqual(str(code), "bts_ienc_official_distribution_link_drift")

    def test_service_directory_membership_drift(self):
        code = self._mutate(
            "service_directory",
            lambda d: d["service_directory"].update(services=[]))
        self.assertEqual(str(code), "official_ienc_service_directory_drift")

    def test_service_identity_drift(self):
        code = self._mutate(
            "service_before",
            lambda d: d["service_before"].update(mapName="SOMETHING_ELSE"))
        self.assertEqual(str(code), "official_lock_basin_area_service_drift")

    def test_service_change_during_capture(self):
        code = self._mutate(
            "service_after",
            lambda d: d["service_after"].update(currentVersion="9.9"))
        self.assertEqual(str(code), "source_service_changed_during_capture")

    def test_layer_change_during_capture(self):
        code = self._mutate(
            "layer_after", lambda d: d["layer_after"].update(id=70))
        self.assertEqual(str(code), "source_layer_changed_during_capture")

    def test_field_schema_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["fields"].pop(5)
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_field_schema_drift")

    def test_field_type_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["fields"][5]["type"] = "esriFieldTypeInteger"
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_field_schema_drift")

    def test_crs_drift(self):
        def mutate(docs):
            docs["layer_before"]["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
            docs["layer_after"]["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_spatial_reference_or_page_limit_drift")

    def test_extent_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["extent"] = {**EXPECTED_LAYER_EXTENT, "xmax": -70.0,
                                       "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_coverage_extent_drift")

    def test_page_limit_drift(self):
        def mutate(docs):
            docs["layer_before"]["maxRecordCount"] = 100
            docs["layer_after"]["maxRecordCount"] = 100
        code = self._mutate("layer_before", mutate)
        self.assertEqual(str(code), "source_spatial_reference_or_page_limit_drift")

    def test_count_drift(self):
        code = self._mutate(
            "count_after", lambda d: d["count_after"].update(count=92))
        self.assertEqual(str(code), "source_count_drift")

    def test_id_roster_drift(self):
        code = self._mutate(
            "ids_after", lambda d: d["ids_after"].update(objectIds=list(range(1, EXPECTED_COUNT))))
        self.assertEqual(str(code), "source_id_population_drift")

    def test_seed_roster_drift(self):
        code = self._mutate(
            "ids_seed", lambda d: d["ids_seed"].update(objectIds=[2, *EXPECTED_IDS]))
        self.assertEqual(str(code), "source_seed_id_population_drift")

    def test_page_roster_incomplete(self):
        docs = _docs()
        docs.pop("page_000")
        captures = {
            key: (json.dumps(doc).encode("utf-8"), {"complete_body": True, "status": 200})
            for key, doc in docs.items()
        }
        with self.assertRaises(ValueError) as raised:
            _validate(captures)
        self.assertIn("source_capture_roster_incomplete", str(raised.exception))

    def test_page_error_rejected(self):
        code = self._mutate("page_000", lambda d: d["page_000"].update(
            error={"code": 500, "message": "internal"}))
        self.assertEqual(str(code), "source_page_error_or_truncation")

    def test_page_truncation_rejected(self):
        code = self._mutate(
            "page_000", lambda d: d["page_000"].update(exceededTransferLimit=True))
        self.assertEqual(str(code), "source_page_error_or_truncation")

    def test_feature_population_incomplete(self):
        code = self._mutate(
            "page_000", lambda d: d["page_000"].update(features=d["page_000"]["features"][1:]))
        self.assertEqual(str(code), "source_feature_population_incomplete")

    def test_unexpected_attribute_column_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Unmapped"] = "value"
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_record_schema_or_geometry_drift")

    def test_missing_attribute_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"].pop("Information")
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_record_schema_or_geometry_drift")

    def test_integer_value_type_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Scale_Minimum"] = "30000"
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_integer_value_type_drift:Scale_Minimum")

    def test_oid_type_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["OBJECTID"] = "1"
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_oid_value_type_drift")

    def test_feature_id_type_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Feature_ID"] = 12
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_string_value_type_drift:Feature_ID")

    def test_unexpected_geometry_ordinates_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["geometry"]["curveRings"] = []
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "unexpected_source_geometry_ordinates")

    def test_geometry_outside_declared_coverage(self):
        def mutate(docs):
            feature = docs["page_000"]["features"][0]
            feature["geometry"]["rings"][0] = [
                [-70.0, 30.0], [-69.9, 30.0], [-69.9, 30.1], [-70.0, 30.1], [-70.0, 30.0],
            ]
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_geometry_outside_declared_coverage")

    def test_unclosed_ring_rejected(self):
        def mutate(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            docs["page_000"]["features"][0]["geometry"]["rings"][0] = ring[:-1]
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_polygon_ring_not_closed")

    def test_counter_clockwise_ring_rejected(self):
        def mutate(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            docs["page_000"]["features"][0]["geometry"]["rings"][0] = list(reversed(ring))
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_polygon_shell_missing")

    def test_zm_flag_mismatch_rejected(self):
        def mutate(docs):
            feature = docs["page_000"]["features"][0]
            feature["geometry"]["hasZ"] = True
        code = self._mutate("page_000", mutate)
        self.assertEqual(str(code), "source_polygon_zm_flags_do_not_match_coordinates")


class ParquetCandidateContract(unittest.TestCase):
    def _sample(self):
        _fields, features, _audit = _validate(_captures())
        fields = copy.deepcopy(EXPECTED_FIELD_DECLARATIONS)
        return fields, features

    def test_round_trip_preserves_schema_values_and_geometry(self):
        fields, features = self._sample()
        parquet = _parquet_bytes(fields, features, 4326)
        import io

        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, EXPECTED_COUNT)
        self.assertEqual(
            [field.name for field in table.schema],
            ["OBJECTID", "Horizontal_Clearance_Length", "Horizontal_Clearance_Width",
             "Horizontal_Length", "Horizontal_Width", "Object_Name", "Status", "Information",
             "Scale_Minimum", "Text_Description", "Source_Date", "Source_Indication",
             "Source_Dataset", "Feature_ID", "geometry"],
        )
        for name, kind in EXPECTED_FIELDS:
            if name == "SHAPE":
                continue
            self.assertEqual(
                table.column(name).to_pylist(),
                [feature["attributes"][name] for feature in features],
                name,
            )
        types = {field.name: str(field.type) for field in table.schema}
        self.assertEqual(types["OBJECTID"], "int64")
        self.assertEqual(types["Scale_Minimum"], "int32")
        self.assertEqual(types["Feature_ID"], "string")
        self.assertEqual(types["geometry"], "binary")
        column = json.loads(
            table.schema.metadata[b"geo"].decode("utf-8"))["columns"]["geometry"]
        self.assertEqual(column["encoding"], "WKB")
        self.assertEqual(column["crs"]["id"]["code"], 4326)
        first = features[0]
        self.assertEqual(table.column("OBJECTID").to_pylist()[0], 1)
        self.assertEqual(
            table.column("Feature_ID").to_pylist()[0], first["attributes"]["Feature_ID"])
        self.assertEqual(
            table.column("Horizontal_Clearance_Length").to_pylist()[0],
            first["attributes"]["Horizontal_Clearance_Length"])
        self.assertEqual(
            table.column("Scale_Minimum").to_pylist()[0], 60000)
        geometry = shapely.from_wkb(table.column("geometry").to_pylist()[0])
        self.assertEqual(geometry.geom_type, "Polygon")
        self.assertTrue(geometry.is_valid)
        x, y = geometry.representative_point().x, geometry.representative_point().y
        self.assertGreaterEqual(x, EXPECTED_LAYER_EXTENT["xmin"])
        self.assertLessEqual(x, EXPECTED_LAYER_EXTENT["xmax"])
        self.assertGreaterEqual(y, EXPECTED_LAYER_EXTENT["ymin"])
        self.assertLessEqual(y, EXPECTED_LAYER_EXTENT["ymax"])
        self.assertEqual(
            shapely.from_wkb(_esri_polygon(first["geometry"])).wkb,
            table.column("geometry").to_pylist()[0],
        )

    def test_wkb_bytes_are_stable_for_identical_geometry(self):
        _fields, features, _audit = _validate(_captures())
        first = features[0]
        self.assertEqual(
            _esri_polygon(first["geometry"]), _esri_polygon(first["geometry"]))

    def test_declared_fields_cover_all_non_geometry_sources(self):
        names = [name for name, _kind in EXPECTED_FIELDS if name != "SHAPE"]
        self.assertEqual(len(names), 14)
        self.assertIn("Scale_Minimum", names)
        self.assertIn("Feature_ID", names)
        self.assertIn("Horizontal_Clearance_Length", names)


class RegisteredDraftRefresh(unittest.TestCase):
    def test_candidate_is_in_ingest_registry_and_registered_job(self):
        from dagster import AssetKey, DefaultScheduleStatus
        from dagster_hifld.assets.lock_basins_usace_ienc import (
            lock_basins_usace_ienc_candidate,
            lock_basins_usace_ienc_draft_schedule,
        )
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.definitions import defs

        self.assertIn(lock_basins_usace_ienc_candidate, hifld_ingest_assets)
        repository = defs.get_repository_def()
        candidate_key = AssetKey(["ingest", DATASET_SLUG, FILE_SLUG])
        self.assertIn(candidate_key, repository.asset_graph.get_all_asset_keys())

        job = repository.get_job("lock_basins_usace_ienc_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {candidate_key})
        self.assertEqual(
            repository.get_schedule_def(lock_basins_usace_ienc_draft_schedule.name)
            .default_status,
            DefaultScheduleStatus.STOPPED,
        )


class CachedCatalogSidecarValidation(unittest.TestCase):
    def test_offline_cache_replay_rejects_catalog_tamper_and_missing_manifest_snapshot(self):
        with tempfile.TemporaryDirectory(prefix="hf-areas-cache-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root), prefix="")
            docs = _docs()
            responses = [docs["ids_seed"]] + [
                docs[name] for name, _ in source._capture_roster(EXPECTED_IDS)
            ]
            response_index = 0

            def handler(request):
                nonlocal response_index
                body = responses[response_index]
                response_index += 1
                return httpx.Response(
                    200,
                    stream=httpx.ByteStream(json.dumps(body).encode("utf-8")),
                    request=request,
                    headers={"content-type": "application/json"},
                )

            transport = httpx.MockTransport(
                handler
            )
            real_client = httpx.Client

            def fixture_client(**kwargs):
                kwargs.pop("follow_redirects", None)
                kwargs.pop("timeout", None)
                kwargs.pop("headers", None)
                return real_client(transport=transport, **kwargs)

            capture_root = root / "capture"
            with patch.object(source.httpx, "Client", fixture_client), patch.object(
                source.time, "sleep", return_value=None
            ), patch.object(
                source.shutil, "disk_usage", return_value=SimpleNamespace(free=400_000_000)
            ):
                first = source.capture_lock_basins_usace_ienc(
                    storage, version="fixture-source", capture_directory=str(capture_root)
                )
            prefix = f"{first['dataset_slug']}/{first['file_slug']}/{first['version']}"
            snapshot_source_metadata(storage, first["dataset_slug"], first["file_slug"], first["version"])
            resolved = load_resolved_source_manifest(
                storage, first["dataset_slug"], first["file_slug"], first["version"]
            )
            summary = summarize_staged_catalog(
                storage, first["dataset_slug"], first["file_slug"], first["version"],
                first["dataset_slug"], source_metadata=resolved.metadata,
            )
            write_catalog_metadata(
                storage, first["dataset_slug"], first["file_slug"], first["version"],
                summary.quality_manifest, summary.data_dictionary,
            )

            def replay():
                with patch.object(source.httpx, "Client", side_effect=AssertionError("offline replay attempted HTTP")):
                    with patch.object(
                        source.shutil, "disk_usage", return_value=SimpleNamespace(free=400_000_000)
                    ):
                        return source.capture_lock_basins_usace_ienc(
                            storage, version="offline-replay", capture_directory=str(first["capture_directory"])
                        )

            self.assertTrue(replay()["reused"])

            for name, field in (
                ("quality_manifest.json", "feature_count"),
                ("data_dictionary.json", "title"),
            ):
                key = f"{prefix}/metadata/{name}"
                original = storage.read_key(key)
                changed = json.loads(original)
                changed[field] = "tampered" if field == "title" else changed[field] + 1
                storage.write_key(key, json.dumps(changed).encode("utf-8"))
                with self.assertRaisesRegex(ValueError, "cached_catalog_metadata_mismatch"):
                    replay()
                storage.write_key(key, original)

            provenance_key = f"{prefix}/metadata/resolved_provenance.json"
            original_provenance = storage.read_key(provenance_key)
            storage.write_key(provenance_key, b"{}")
            with self.assertRaisesRegex(ValueError, "cached_catalog_resolved_provenance_mismatch"):
                replay()
            storage.write_key(provenance_key, original_provenance)

            snapshot_key = f"{prefix}/metadata/source/source_manifest.json"
            snapshot_path = Path(storage.local_dir) / snapshot_key
            original_snapshot = snapshot_path.read_bytes()
            snapshot_path.unlink()
            with self.assertRaisesRegex(ValueError, "cached_catalog_source_snapshot_incomplete"):
                replay()
            snapshot_path.write_bytes(original_snapshot)


if __name__ == "__main__":
    unittest.main()
