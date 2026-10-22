"""Source contract tests for the complete official IENC FLOATING_DOCK_AREA layer."""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import tempfile
import unittest
import copy
from pathlib import Path

import pyarrow.parquet as pq
import shapely

from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources.usace_floating_docks import (
    BTS_ITEM_ID,
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_FIELDS,
    EXPECTED_ID_SHA256,
    EXPECTED_LAYER_EXTENT,
    FILE_SLUG,
    PAGE_SIZE,
    _capture_roster,
    _parquet_bytes,
    _query,
    _validate,
    capture_usace_floating_docks,
)


def _docs(ids: list[int] | None = None) -> dict[str, dict]:
    object_ids = ids or list(range(1, EXPECTED_COUNT + 1))
    fields = EXPECTED_FIELD_DECLARATIONS
    item = {
        "id": BTS_ITEM_ID,
        "owner": "USDOT_BTS",
        "type": "PDF",
        "access": "public",
        "title": "Inland Electronic Navigational Charts",
        "description": (
            "IENC dataset is updated bi-monthly by USACE. "
            "Charts can be downloaded here: https://ienccloud.us/ienc/products/files/"
            "U37/ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip. "
            "Services: https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes"
        ),
    }
    service = {
        "mapName": "FLOATING_DOCK_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 86, "name": "FLOATING_DOCK_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    layer = {
        "id": 86,
        "name": "FLOATING_DOCK_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {"xmin": -95.29531899999995, "ymin": 33.25191700000005,
                   "xmax": -79.88356719999996, "ymax": 41.82842300000004,
                   "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "maxRecordCount": 2000,
        "fields": fields,
    }
    features = []
    for oid in object_ids:
        attrs = {name: None for name, _kind in EXPECTED_FIELDS if name != "SHAPE"}
        attrs.update({"OBJECTID": oid, "Object_Name": f"DOCK-{oid:03d}", "Feature_ID": f"U3 {oid:010d}"})
        features.append(
            {
                "attributes": attrs,
                "geometry": {
                    "rings": [[[-90, 38], [-90, 39], [-89, 39], [-89, 38], [-90, 38]]],
                    "spatialReference": {"wkid": 4326},
                },
            }
        )
    docs = {
        "bts_item": item,
        "service_directory": {"services": [{"name": "IENC_Feature_Classes/FLOATING_DOCK_AREA", "type": "MapServer"}]},
        "service_before": service,
        "service_after": service,
        "layer_before": layer,
        "layer_after": layer,
        "count_before": {"count": EXPECTED_COUNT},
        "count_after": {"count": EXPECTED_COUNT},
        "ids_seed": {"objectIds": object_ids},
        "ids_before": {"objectIds": object_ids},
        "ids_after": {"objectIds": object_ids},
    }
    for page_index, start in enumerate(range(0, len(features), PAGE_SIZE)):
        docs[f"page_{page_index:03d}"] = {
            "spatialReference": {"wkid": 4326, "latestWkid": 4326},
            "features": features[start:start + PAGE_SIZE],
        }
    return docs


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode("utf-8"), {}) for name, doc in docs.items()}


class UsaceFloatingDocksSourceTest(unittest.TestCase):
    def test_current_public_bts_pdf_is_required_prior_denied_item_rejected(self) -> None:
        captures = _captures(_docs())
        item = json.loads(captures["bts_item"][0])
        item["id"] = "3607d485a9854d6fa458bf507058ceae"
        captures["bts_item"] = (json.dumps(item).encode("utf-8"), {})
        with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
            _validate(captures)

    def test_complete_id_roster_and_geometry_are_checked(self) -> None:
        fields, features, audit = _validate(_captures(_docs()))
        self.assertEqual(fields, EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], EXPECTED_ID_SHA256)
        self.assertEqual(audit["geometry_valid_count"], EXPECTED_COUNT)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["source_vertex_count"], EXPECTED_COUNT * 5)

    def test_changed_ids_fail_closed(self) -> None:
        docs = _docs()
        docs["ids_after"] = {"objectIds": list(range(1, EXPECTED_COUNT)) + [113]}
        with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
            _validate(_captures(docs))

    def test_type_loss_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["Horizontal_Length"] = 21.6
        with self.assertRaisesRegex(ValueError, "source_string_value_type_drift"):
            _validate(_captures(docs))

    def test_duplicate_feature_ids_are_retained_without_synthesizing_replacements(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][1]["attributes"]["Feature_ID"] = \
            docs["page_000"]["features"][0]["attributes"]["Feature_ID"]
        _fields, features, audit = _validate(_captures(docs))
        self.assertEqual(audit["feature_id_duplicate_count"], 1)
        self.assertEqual(
            features[1]["attributes"]["Feature_ID"],
            features[0]["attributes"]["Feature_ID"],
        )

    def test_unexpected_column_fails_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["fields"] = docs["layer_before"]["fields"] + [
            {"name": "NEW_FIELD", "type": "esriFieldTypeString", "alias": "NEW_FIELD", "length": 254, "domain": None}
        ]
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            _validate(_captures(docs))

    def test_incomplete_page_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["exceededTransferLimit"] = True
        with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
            _validate(_captures(docs))

    def test_extent_and_geometry_coverage_drift_fail_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["extent"]["xmax"] += 1
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_coverage_extent_drift"):
            _validate(_captures(docs))
        docs = _docs()
        docs["page_000"]["features"][0]["geometry"]["rings"][0][0][0] = -110
        with self.assertRaisesRegex(ValueError, "source_geometry_outside_declared_coverage"):
            _validate(_captures(docs))

    def test_native_identifier_values_and_polygon_coordinates_survive_geoparquet(self) -> None:
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID"},
            {"name": "SHAPE", "type": "esriFieldTypeGeometry"},
            {"name": "Object_Name", "type": "esriFieldTypeString"},
            {"name": "Horizontal_Length", "type": "esriFieldTypeString"},
            {"name": "Information", "type": "esriFieldTypeString"},
            {"name": "Feature_ID", "type": "esriFieldTypeString"},
        ]
        feature = {
            "attributes": {"OBJECTID": 17, "Object_Name": "00107", "Horizontal_Length": "21.6",
                           "Information": "raw text", "Feature_ID": "U3 1393130429 00001"},
            "geometry": {"rings": [[[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]]},
        }
        body = _parquet_bytes(fields, [feature], 4326)
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [17])
        self.assertEqual(table.column("Object_Name").to_pylist(), ["00107"])
        self.assertEqual(table.column("Horizontal_Length").to_pylist(), ["21.6"])
        self.assertEqual(table.column("Information").to_pylist(), ["raw text"])
        self.assertEqual(table.column("Feature_ID").to_pylist(), ["U3 1393130429 00001"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("Horizontal_Length").type), "string")
        primary, _, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 4326)
        geom = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(list(geom.exterior.coords), [(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)])
    def test_bts_linkage_is_required(self) -> None:
        docs = _docs()
        docs["bts_item"]["owner"] = "unverified"
        with self.assertRaisesRegex(ValueError, "bts_ienc_item_identity_drift"):
            _validate(_captures(docs))

    def test_conflicting_layer_wkids_fail_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["spatialReference"] = {"wkid": 3857, "latestWkid": 4326}
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_layer_crs_drift"):
            _validate(_captures(docs))

    def test_extent_and_feature_crs_overrides_fail_closed(self) -> None:
        for location, label in (("extent", "source_layer_extent"), ("feature", "source_feature")):
            with self.subTest(location=location):
                docs = _docs()
                if location == "extent":
                    docs["layer_before"]["extent"] = {
                        **EXPECTED_LAYER_EXTENT,
                        "spatialReference": {"wkid": 4269},
                    }
                    docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                else:
                    docs["page_000"]["features"][0]["geometry"]["spatialReference"] = {
                        "wkid": 3857,
                        "latestWkid": 4326,
                    }
                with self.assertRaisesRegex(ValueError, label + "_crs_drift"):
                    _validate(_captures(docs))

    def test_page_crs_conflict_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["spatialReference"] = {"wkid": 4269, "latestWkid": 4326}
        with self.assertRaisesRegex(ValueError, "source_page_crs_drift"):
            _validate(_captures(docs))

    def test_layer_after_crs_or_extent_change_fails_closed(self) -> None:
        for key, value in (
            ("spatialReference", {"latestWkid": 4269}),
            ("extent", {"xmin": -1, "ymin": -1, "xmax": 2, "ymax": 2}),
        ):
            with self.subTest(key=key):
                docs = _docs()
                docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                docs["layer_after"][key] = value
                with self.assertRaisesRegex(ValueError, "source_layer_changed_during_capture"):
                    _validate(_captures(docs))


def _write_capture_directory(root: Path, docs: dict[str, dict]) -> Path:
    """Synthesize a complete sealed replay capture directory for offline cache tests."""
    root.mkdir(parents=True, exist_ok=True)
    ids = docs["ids_seed"]["objectIds"]
    urls = {"ids_seed": _query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})}
    urls.update(dict(_capture_roster(ids)))
    receipts = {}
    for name, url in urls.items():
        body = json.dumps(docs[name]).encode("utf-8")
        receipt = {
            "method": "GET", "url": url, "final_url": url,
            "observed_at": "2026-10-21T00:00:00+00:00", "status": 200,
            "complete_body": True,
            "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
            "content_encoding": None, "content_length": None,
            "error_code": None, "error_type": None,
        }
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.wire").write_bytes(body)
        (root / f"{name}.receipt.json").write_bytes(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
        receipts[name] = receipt
    manifest = {
        "captured_at": "2026-10-21T00:00:00+00:00", "complete": True,
        "source_feature_count": len(ids), "receipts": receipts, "attempts": {},
        "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"}),
    }
    (root / "capture_manifest.json").write_bytes(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n")
    return root


def _catalog_candidate(storage: StagingStorageResource, version: str) -> None:
    """Run the registered catalog stage exactly as the shared publish_catalog asset does."""
    snapshot_source_metadata(storage, DATASET_SLUG, FILE_SLUG, version)
    resolved = load_resolved_source_manifest(storage, DATASET_SLUG, FILE_SLUG, version)
    summary = summarize_staged_catalog(storage, DATASET_SLUG, FILE_SLUG, version, DATASET_SLUG,
                                       source_metadata=resolved.metadata)
    write_catalog_metadata(storage, DATASET_SLUG, FILE_SLUG, version,
                           summary.quality_manifest, summary.data_dictionary)


class UsaceFloatingDocksCacheIntegrityTest(unittest.TestCase):
    """Cache reuse must fail closed when catalog evidence is missing or corrupted.

    Like the other storage-level source suites, these tests bind candidates to
    the committed Git code identity, so the working tree must be clean.
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        base = Path(self._tmp.name)
        self.storage = StagingStorageResource(local_dir=str(base / "staging"))
        self.captures = _write_capture_directory(base / "captures", _docs())
        self.version = "vunittest92"

    def _stage(self) -> dict:
        return capture_usace_floating_docks(self.storage, version=self.version,
                                            capture_directory=str(self.captures))

    def _root(self, version: str) -> Path:
        return Path(self.storage.local_dir) / DATASET_SLUG / FILE_SLUG / version

    def _edit_document(self, version: str, relative: str, mutate) -> None:
        path = self._root(version) / relative
        document = json.loads(path.read_bytes())
        mutate(document)
        path.write_bytes(json.dumps(document, sort_keys=True, indent=2).encode("utf-8"))

    def test_reuse_before_catalog_stays_idempotent(self) -> None:
        first = self._stage()
        second = self._stage()
        self.assertIs(second["reused"], True)
        self.assertEqual(second["version"], first["version"])
        self.assertEqual(second["feature_count"], EXPECTED_COUNT)

    def test_reuse_after_registered_catalog_recomputes_evidence(self) -> None:
        first = self._stage()
        _catalog_candidate(self.storage, first["version"])
        root = self._root(first["version"])
        for relative in ("metadata/quality_manifest.json", "metadata/data_dictionary.json",
                         "metadata/resolved_provenance.json", "metadata/source/source_manifest.json"):
            self.assertTrue((root / relative).is_file(), relative)
        digest = hashlib.sha256((root / "metadata/source_manifest.json").read_bytes()).hexdigest()
        self.assertTrue((root / "metadata/source" / f"version_{digest}_source_manifest.json").is_file())
        second = self._stage()
        self.assertIs(second["reused"], True)
        self.assertEqual(second["version"], first["version"])

    def test_tampered_quality_manifest_blocks_reuse(self) -> None:
        version = self._stage()["version"]
        _catalog_candidate(self.storage, version)

        def corrupt(quality: dict) -> None:
            quality["feature_count"] = 999
            quality["invalid_geometry_count"] = 999

        self._edit_document(version, "metadata/quality_manifest.json", corrupt)
        with self.assertRaisesRegex(ValueError, "cached_catalog_quality_manifest_drift"):
            self._stage()

    def test_emptied_data_dictionary_blocks_reuse(self) -> None:
        version = self._stage()["version"]
        _catalog_candidate(self.storage, version)

        def corrupt(dictionary: dict) -> None:
            dictionary["columns"] = []

        self._edit_document(version, "metadata/data_dictionary.json", corrupt)
        with self.assertRaisesRegex(ValueError, "cached_catalog_data_dictionary_drift"):
            self._stage()

    def test_deleted_catalog_provenance_blocks_reuse(self) -> None:
        version = self._stage()["version"]
        _catalog_candidate(self.storage, version)
        digest = hashlib.sha256((self._root(version) / "metadata/source_manifest.json").read_bytes()).hexdigest()
        for relative in ("metadata/resolved_provenance.json", "metadata/source/source_manifest.json",
                         f"metadata/source/version_{digest}_source_manifest.json"):
            (self._root(version) / relative).unlink()
        with self.assertRaisesRegex(ValueError, "cached_catalog_provenance_missing"):
            self._stage()

    def test_rewritten_source_manifest_snapshot_blocks_reuse(self) -> None:
        version = self._stage()["version"]
        _catalog_candidate(self.storage, version)
        (self._root(version) / "metadata/source/source_manifest.json").write_bytes(b"{}")
        with self.assertRaisesRegex(ValueError, "cached_catalog_source_snapshot_changed"):
            self._stage()


if __name__ == "__main__":
    unittest.main()
