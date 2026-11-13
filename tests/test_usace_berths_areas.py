import json
import shutil
import struct
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb, get_coordinates

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_berths_areas import (
    usace_berths_areas_complete, usace_berths_areas_draft_schedule,
    usace_berths_areas_usace_ienc,
)
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.sources.epa_frs_common import processing_identity
from dagster_hifld.sources import usace_berths_areas as source


class UsaceBerthsAreasTests(unittest.TestCase):
    def test_asset_is_registered_and_refresh_is_stopped_with_acceptance_false(self):
        self.assertIn(usace_berths_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_berths_areas_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        result = usace_berths_areas_complete({"feature_count": 1108, "source_population_complete": True})
        self.assertTrue(result.passed)
        self.assertFalse(result.metadata["release_accepted"].value)
        self.assertFalse(result.metadata["rights_accepted"].value)

    def test_native_polygon_wkb_preserves_ring_and_vertex_order(self):
        exterior = [[0, 0], [0, 4], [4, 4], [4, 0], [0, 0]]
        interior = [[1, 1], [2, 1], [2, 2], [1, 2], [1, 1]]
        wkb = source._polygon_wkb({"rings": [exterior, interior]})
        self.assertEqual(struct.unpack_from("<I", wkb, 5)[0], 2)
        polygon = from_wkb(wkb)
        self.assertEqual(get_coordinates(polygon.exterior).tolist(), exterior)
        self.assertEqual(get_coordinates(polygon.interiors[0]).tolist(), interior)

    def test_polygon_rejects_non_xy_nonfinite_open_and_rewound_rings(self):
        ring = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
        cases = [
            ({"rings": [[*ring[:-1], [0, 0, 5], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [[*ring[:-1], [float("nan"), 0], ring[-1]]]}, "source_polygon_coordinate_not_finite_xy"),
            ({"rings": [ring[:-1]]}, "source_polygon_ring_invalid_or_open"),
            ({"rings": [list(reversed(ring))]}, "source_exterior_ring_winding_drift"),
            ({"rings": [ring], "hasM": True}, "source_polygon_not_xy"),
        ]
        for geometry, error in cases:
            with self.subTest(error=error):
                with self.assertRaisesRegex(ValueError, error):
                    source._polygon_wkb(geometry)

    def test_parquet_retains_all_attributes_duplicate_feature_ids_and_polygon_wkb(self):
        first = {field["name"]: None for field in source.ATTRIBUTE_FIELDS}
        first.update(OBJECTID=1, Feature_ID="duplicate")
        second = dict(first, OBJECTID=2)
        ring = [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]
        features = [
            {"attributes": first, "geometry": {"rings": [ring]}},
            {"attributes": second, "geometry": {"rings": [ring]}},
        ]
        data = source._parquet_bytes(source.EXPECTED_FIELD_DECLARATIONS, features)
        table = pq.read_table(pa.BufferReader(data))
        self.assertEqual(table.column_names, [field["name"] for field in source.ATTRIBUTE_FIELDS] + ["geometry"])
        self.assertEqual(table["Feature_ID"].to_pylist(), ["duplicate", "duplicate"])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2])
        self.assertEqual(table["geometry"][0].as_py(), source._polygon_wkb({"rings": [ring]}))

    def test_capture_root_version_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = source.StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_usace_berths_areas(storage, version="../outside")

    def test_packaged_code_identity_validates_without_git_metadata(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as tmp:
            packaged_root = Path(tmp) / "runtime"
            packaged_root.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged_root / "src/dagster_hifld")
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged_root / name)
            self.assertFalse((packaged_root / ".git").exists())
            identity = processing_identity(packaged_root)
            self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
            self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
            validate_construction_code_identity(
                packaged_root, identity["code_commit"], identity["files"], identity=identity,
            )


_BTS_ITEM = {
    "id": "7ec34864c8804468a9cf52d437fe4b6c",
    "owner": "USDOT_BTS",
    "title": "Inland Electronic Navigational Charts",
    "description": (
        "Inland Electronic Navigational Charts (IENC) web services: "
        "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes and master "
        "geodatabase USACE_IENC_Master_Service_gdb.zip."
    ),
}


class BtsSourceChainContractTests(unittest.TestCase):
    """Fail-closed delegation guard for the repaired BTS source chain (#21)."""

    def _bts_error(self, item_doc: dict) -> str:
        body = json.dumps(item_doc).encode("utf-8")
        with self.assertRaises(ValueError) as raised:
            source._validate({"bts_item": (body, {"complete_body": True, "status": 200})})
        return str(raised.exception)

    def test_pins_current_item_and_never_requests_prior_denied_item(self):
        self.assertEqual(source.BTS_ITEM_ID, "7ec34864c8804468a9cf52d437fe4b6c")
        self.assertEqual(source.PRIOR_BTS_ITEM_ID, "3607d485a9854d6fa458bf507058ceae")
        self.assertEqual(
            source.BTS_ITEM_URL,
            "https://www.arcgis.com/sharing/rest/content/items/"
            "7ec34864c8804468a9cf52d437fe4b6c?f=json",
        )
        requested = [url for _name, url in source._capture_urls(source.EXPECTED_IDS)]
        self.assertIn(source.BTS_ITEM_URL, requested)
        self.assertTrue(all(source.PRIOR_BTS_ITEM_ID not in url for url in requested))

    def test_denial_body_and_identity_drift_fail_closed(self):
        denial = {"error": {"code": 403, "messageCode": "GWM_0003",
                            "message": "Access denied", "details": []}}
        cases = [
            ("anonymous arcgis denial body", denial),
            ("prior denied item id", {**_BTS_ITEM, "id": source.PRIOR_BTS_ITEM_ID}),
            ("foreign owner", {**_BTS_ITEM, "owner": "SOMEONE_ELSE"}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                self.assertEqual(self._bts_error(doc), "bts_ienc_item_identity_drift")

    def test_delegation_marker_drift_fails_closed(self):
        cases = [
            ("missing ienccloud service link", {**_BTS_ITEM, "description": (
                "Inland Electronic Navigational Charts master geodatabase "
                "USACE_IENC_Master_Service_gdb.zip.")}),
            ("missing master gdb link", {**_BTS_ITEM, "description": (
                "Inland Electronic Navigational Charts web services "
                "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes.")}),
            ("missing ienc program title", {**_BTS_ITEM, "title": "Unrelated item",
                                             "description": (
                                                 "Web services "
                                                 "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes "
                                                 "and USACE_IENC_Master_Service_gdb.zip.")}),
        ]
        for label, doc in cases:
            with self.subTest(label=label):
                self.assertEqual(self._bts_error(doc), "bts_ienc_official_distribution_link_drift")

    def test_capture_receipt_for_prior_item_is_rejected_on_replay(self):
        prior_url = ("https://www.arcgis.com/sharing/rest/content/items/"
                     + source.PRIOR_BTS_ITEM_ID + "?f=json")
        with self.assertRaisesRegex(
                ValueError, "retained_capture_request_identity_mismatch:bts_item"):
            source._validate_request_identity(
                "bts_item", {"method": "GET", "url": prior_url, "final_url": prior_url},
                source.BTS_ITEM_URL)

    def test_prior_item_is_retained_evidence_only_without_succession_claim(self):
        module_text = Path(source.__file__).read_text(encoding="utf-8")
        self.assertIn('"bts_item_succession_claimed": False', module_text)
        self.assertIn('"prior_bts_item_id": PRIOR_BTS_ITEM_ID', module_text)
        self.assertIn("no succession or historical equivalence between the two items is claimed",
                      module_text)
        self.assertIn("recorded in docs, not validated here", module_text)


if __name__ == "__main__":
    unittest.main()
