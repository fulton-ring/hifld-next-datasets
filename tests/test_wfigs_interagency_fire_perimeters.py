"""Synthetic coherent-capture checks and adversarial drift gates; no network."""
import copy
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import shapely
from pyarrow import parquet as pq

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import wfigs_interagency_fire_perimeters as source
from dagster_hifld.sources.esri_geometry import to_wkb

SERVICE_ITEM_ID = "5e72b1699bf74eefb3f3aff6f4ba5511"
ORG_ID = "T4QMspbfLg3qTGWY"

FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "sqlType": "sqlTypeOther",
     "nullable": False, "editable": False, "domain": None, "defaultValue": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "sqlType": "sqlTypeOther",
     "length": 38, "nullable": False, "editable": False, "domain": None, "defaultValue": None},
    {"name": "attr_UniqueFireIdentifier", "type": "esriFieldTypeString", "alias": "UniqueFireIdentifier",
     "sqlType": "sqlTypeOther", "length": 30, "nullable": True, "editable": True, "domain": None,
     "defaultValue": None},
    {"name": "poly_FeatureCategory", "type": "esriFieldTypeString", "alias": "FeatureCategory",
     "sqlType": "sqlTypeOther", "length": 32, "nullable": True, "editable": True, "domain": None,
     "defaultValue": None},
    {"name": "poly_GISAcres", "type": "esriFieldTypeDouble", "alias": "GISAcres",
     "sqlType": "sqlTypeOther", "nullable": True, "editable": True, "domain": None, "defaultValue": None},
    {"name": "poly_FireYear", "type": "esriFieldTypeInteger", "alias": "FireYear",
     "sqlType": "sqlTypeOther", "nullable": True, "editable": True, "domain": None, "defaultValue": None},
    {"name": "poly_CreateDate", "type": "esriFieldTypeDate", "alias": "CreateDate",
     "sqlType": "sqlTypeOther", "nullable": True, "editable": True, "domain": None, "defaultValue": None},
]

WITNESS = {"lastEditDate": 1794708330310, "schemaLastEditDate": 1794703502898,
           "dataLastEditDate": 1794703928228}


def _ring(points):
    return [list(point) for point in points] + [list(points[0])]


def _cw(x0, y0, x1, y1):
    return _ring([(x0, y0), (x0, y1), (x1, y1), (x1, y0)])


def _ccw(x0, y0, x1, y1):
    return _ring([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])


def _features():
    return [
        {"attributes": {"OBJECTID": 11, "GlobalID": "11111111-1111-1111-1111-111111111111",
                        "attr_UniqueFireIdentifier": "2021-USCA-011122", "poly_FeatureCategory":
                        "Wildfire Daily Fire Perimeter", "poly_GISAcres": 10.5, "poly_FireYear": 2021,
                        "poly_CreateDate": 1700000000000},
         "geometry": {"rings": [_ring([(-120.0, 40.0), (-120.0, 41.0), (-119.0, 41.0), (-119.0, 40.0)])]}},
        {"attributes": {"OBJECTID": 22, "GlobalID": "22222222-2222-2222-2222-222222222222",
                        "attr_UniqueFireIdentifier": "2021-USCA-011122", "poly_FeatureCategory":
                        "Wildfire Final Fire Perimeter", "poly_GISAcres": 12, "poly_FireYear": 2021,
                        "poly_CreateDate": 1700000100000},
         "geometry": {"rings": [_ring([(-121.0, 40.0), (-121.0, 41.0), (-120.0, 41.0), (-120.0, 40.0)]),
                                _ring([(-120.6, 40.3), (-120.6, 40.7), (-120.4, 40.7), (-120.4, 40.3)])]}},
        {"attributes": {"OBJECTID": 33, "GlobalID": "33333333-3333-3333-3333-333333333333",
                        "attr_UniqueFireIdentifier": None, "poly_FeatureCategory":
                        "Prescribed Fire", "poly_GISAcres": 0.25, "poly_FireYear": 2022,
                        "poly_CreateDate": None},
         "geometry": {"rings": [_ring([(-118.0, 44.0), (-118.0, 44.5), (-117.5, 44.5), (-117.5, 44.0)]),
                                _ring([(-118.0, 44.0), (-117.9, 44.0), (-117.9, 44.1), (-118.0, 44.1)])]}},
        {"attributes": {"OBJECTID": 44, "GlobalID": "44444444-4444-4444-4444-444444444444",
                        "attr_UniqueFireIdentifier": "2022-USOR-022233", "poly_FeatureCategory":
                        "Wildfire Daily Fire Perimeter", "poly_GISAcres": 3.75, "poly_FireYear": 2022,
                        "poly_CreateDate": 1700000200000},
         "geometry": None},
        {"attributes": {"OBJECTID": 55, "GlobalID": "55555555-5555-5555-5555-555555555555",
                        "attr_UniqueFireIdentifier": "2023-USMT-033344", "poly_FeatureCategory":
                        "Wildfire Final Fire Perimeter", "poly_GISAcres": 1.5, "poly_FireYear": 2023,
                        "poly_CreateDate": 1700000300000},
         "geometry": {"rings": [_ring([(-114.0, 46.0), (-114.0, 46.5), (-113.5, 46.5), (-113.5, 46.0)])]}},
    ]


def _layer(editing_info=None):
    return {"id": 0, "name": "Perimeters", "type": "Feature Layer",
            "geometryType": "esriGeometryPolygon",
            "spatialReference": {"wkid": 4326, "latestWkid": 4326},
            "extent": {"spatialReference": {"wkid": 4326, "latestWkid": 4326}},
            "hasZ": False, "hasM": False, "maxRecordCount": 2000,
            "objectIdField": "OBJECTID", "globalIdField": "GlobalID",
            "supportedQueryFormats": "JSON, geoJSON, PBF",
            "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
            "editingInfo": copy.deepcopy(editing_info) if editing_info is not None else copy.deepcopy(WITNESS),
            "fields": copy.deepcopy(FIELDS)}


def _item():
    return {"id": SERVICE_ITEM_ID, "owner": "NIFC_Authoritative", "orgId": ORG_ID,
            "type": "Feature Service", "name": "WFIGS_Interagency_Perimeters", "size": 0,
            "snippet": "Best available perimeters for all known wildland fires in the United States.",
            "licenseInfo": "The Nature Conservancy WFIGS distribution statement (retained verbatim in raw capture)."}


def _service():
    return {"serviceItemId": SERVICE_ITEM_ID, "currentVersion": 12,
            "capabilities": "Query,Extract", "supportedQueryFormats": "JSON, geoJSON, PBF",
            "layers": [{"id": 0, "name": "Perimeters"}]}


def _capture_docs(page_size=2, features=None, witness=None, count_override=None,
                  ids_override=None):
    features = _features() if features is None else features
    ordered = sorted(features, key=lambda feature: feature["attributes"]["OBJECTID"])
    ids = [feature["attributes"]["OBJECTID"] for feature in ordered]
    docs = {"item": _item(),
            "service_before": _service(), "service_after": _service(),
            "layer_before": _layer(witness), "layer_after": _layer(witness),
            "count_before": {"count": len(ids) if count_override is None else count_override},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids if ids_override is None else ids_override}}
    for start in range(0, len(ordered), page_size):
        page_features = ordered[start:start + page_size]
        page = {"objectIdFieldName": "OBJECTID",
                "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                "geometryType": "esriGeometryPolygon", "features": copy.deepcopy(page_features)}
        if start + page_size < len(ordered):
            page["exceededTransferLimit"] = True
        docs[f"page_{start:05d}"] = page
    docs["count_after"] = {"count": len(ids) if count_override is None else count_override}
    docs["ids_after"] = {"objectIdFieldName": "OBJECTID", "objectIds": ids if ids_override is None else ids_override}
    return docs


def _write_capture(root: Path, docs: dict, page_size: int) -> None:
    root.mkdir(parents=True, exist_ok=True)
    observed = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    receipts = {}
    for name, doc in docs.items():
        body = (json.dumps(doc, ensure_ascii=False, allow_nan=False) + "\n").encode()
        (root / f"{name}.body").write_bytes(body)
        (root / f"{name}.wire").write_bytes(body)
        url = source.ITEM_URL if name == "item" else source._scope_query(name, page_size)
        receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": observed,
                   "status": 200, "complete_body": True, "wire_bytes": len(body),
                   "wire_sha256": source._sha(body), "bytes": len(body), "sha256": source._sha(body),
                   "content_encoding": None, "content_length": None, "error_code": None,
                   "error_type": None}
        (root / f"{name}.receipt.json").write_bytes(source._canonical(receipt) + b"\n")
        receipts[name] = receipt
    (root / "capture_manifest.json").write_bytes(source._canonical(
        {"complete": True, "captured_at": observed, "artifact_names": sorted(docs),
         "receipts": receipts}) + b"\n")


class WfigsSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = StagingStorageResource(local_dir=self.tmp.name, use_local=True)

    def stage(self, docs=None, page_size=2, **kwargs):
        root = Path(self.tmp.name) / "capture"
        _write_capture(root, _capture_docs(page_size=page_size, **kwargs) if docs is None else docs, page_size)
        return source.capture_wfigs_interagency_fire_perimeters(
            self.storage, capture_directory=str(root), page_size=page_size)

    def test_complete_snapshot_candidates_and_readback_parity(self):
        result = self.stage()
        self.assertTrue(result["source_population_complete"])
        self.assertEqual(result["feature_count"], 5)
        self.assertFalse(result["release_accepted"])
        root = Path(self.storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
        table = pq.read_table(root / "geoparquet" / f"{source.FILE_SLUG}.parquet")
        self.assertEqual(table.num_rows, 5)
        self.assertEqual(table.schema.names,
                         [field["name"] for field in FIELDS] + ["geometry"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("poly_CreateDate").type), "timestamp[ms, tz=UTC]")
        self.assertEqual(table.column("OBJECTID").to_pylist(), [11, 22, 33, 44, 55])
        manifest = json.loads((root / "metadata" / "source_manifest.json").read_bytes())
        self.assertFalse(manifest["approved"])
        self.assertEqual(manifest["lineage"]["item_owner"], "NIFC_Authoritative")
        audit = json.loads((root / "metadata" / "source_audit.json").read_bytes())
        self.assertEqual(audit["geometry_census"]["holes"], 2)
        self.assertEqual(audit["null_geometry_objectids"], [44])
        ready = json.loads((root / "metadata" / "candidate_ready.json").read_bytes())
        self.assertFalse(ready["release_accepted"])

    def test_replay_is_idempotent_and_immutable(self):
        root = Path(self.tmp.name) / "capture"
        _write_capture(root, _capture_docs(), 2)
        first = source.capture_wfigs_interagency_fire_perimeters(
            self.storage, capture_directory=str(root), page_size=2)
        second = source.capture_wfigs_interagency_fire_perimeters(
            self.storage, capture_directory=str(root), page_size=2)
        self.assertTrue(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(first["version"], second["version"])
        first_parquet = (Path(self.storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG
                         / first["version"] / "geoparquet" / f"{source.FILE_SLUG}.parquet").read_bytes()
        second_parquet = (Path(self.storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG
                          / second["version"] / "geoparquet" / f"{source.FILE_SLUG}.parquet").read_bytes()
        self.assertEqual(first_parquet, second_parquet)

    def test_page_order_ids_and_geometry_drifts_fail_closed(self):
        docs = _capture_docs()
        def mutate(change):
            altered = copy.deepcopy(docs)
            change(altered)
            with self.assertRaises(ValueError) as raised:
                self.stage(docs=altered)
            return str(raised.exception)
        def drop_feature(current):
            current["page_00002"]["features"].pop()
        def duplicate_oid(current):
            current["page_00002"]["features"][0]["attributes"]["OBJECTID"] = 11
        def out_of_order(current):
            rows = current["page_00000"]["features"]
            rows[0], rows[1] = rows[1], rows[0]
        def extra_attribute(current):
            current["page_00000"]["features"][0]["attributes"]["NEW"] = 1
        def drop_attribute(current):
            del current["page_00000"]["features"][0]["attributes"]["poly_FireYear"]
        def unclosed(current):
            rings = current["page_00000"]["features"][0]["geometry"]["rings"]
            rings[0].pop()
        def out_of_range(current):
            current["page_00000"]["features"][0]["geometry"]["rings"][0][0][0] = 200.0
        def nonfinite(current):
            current["page_00000"]["features"][0]["geometry"]["rings"][0][0][1] = None
        def type_drift(current):
            current["layer_before"]["fields"][4]["type"] = "esriFieldTypeString"
        def schema_changed(current):
            current["layer_after"]["fields"].pop()
        def witness_drift(current):
            current["layer_after"]["editingInfo"]["lastEditDate"] += 1
        def count_drift(current):
            current["count_after"]["count"] = 3
        def ids_drift(current):
            current["ids_after"]["objectIds"].pop()
        def globalid_duplicate(current):
            current["page_00002"]["features"][0]["attributes"]["GlobalID"] = \
                current["page_00000"]["features"][0]["attributes"]["GlobalID"]
        def item_drift(current):
            current["item"]["owner"] = "SomeMirror"
        def service_changed(current):
            current["service_after"]["capabilities"] = "Query"
        def short_page_transfer_flag(current):
            current["page_00004"]["exceededTransferLimit"] = True
        for change in (drop_feature, duplicate_oid, out_of_order, extra_attribute, drop_attribute,
                       unclosed, out_of_range, nonfinite, type_drift, schema_changed, witness_drift,
                       count_drift, ids_drift, globalid_duplicate, item_drift, service_changed,
                       short_page_transfer_flag):
            with self.subTest(change=change.__name__):
                mutate(change)


    def test_nested_island_feature_flows_to_geoparquet(self):
        feature = {"attributes": {
            "OBJECTID": 66, "GlobalID": "66666666-6666-6666-6666-666666666666",
            "attr_UniqueFireIdentifier": "2023-USMT-044455", "poly_FeatureCategory":
            "Wildfire Daily Fire Perimeter", "poly_GISAcres": 6.8, "poly_FireYear": 2023,
            "poly_CreateDate": 1700000400000},
            "geometry": {"rings": [_cw(-114.0, 46.0, -113.0, 47.0), _ccw(-113.8, 46.2, -113.2, 46.8),
                                   _cw(-113.6, 46.4, -113.4, 46.6)]}}
        result = self.stage(features=[feature], page_size=2)
        self.assertEqual(result["feature_count"], 1)
        root = Path(self.storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
        table = pq.read_table(root / "geoparquet" / f"{source.FILE_SLUG}.parquet")
        geometry = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertIsInstance(geometry, shapely.MultiPolygon)
        self.assertAlmostEqual(geometry.area, 0.68)
        self.assertTrue(geometry.is_valid)
        audit = json.loads((root / "metadata" / "source_audit.json").read_bytes())
        self.assertEqual(audit["geometry_census"]["parts"], 2)
        self.assertEqual(audit["geometry_census"]["holes"], 1)
        self.assertEqual(audit["geometry_census"]["invalid"], 0)


class WfigsRingAssemblyTests(unittest.TestCase):
    """Regression: ring membership is whole-ring containment with hole-aware nesting depth."""

    def test_nested_island_becomes_separate_part(self):
        rings = [_cw(0, 0, 10, 10), _ccw(2, 2, 8, 8), _cw(4, 4, 6, 6)]
        geometry = shapely.from_wkb(to_wkb({"rings": rings}, source.GEOMETRY_TYPE,
                                           has_z=False, has_m=False))
        census = source._census(rings, geometry)
        self.assertIsInstance(geometry, shapely.MultiPolygon)
        self.assertAlmostEqual(geometry.area, 68.0)
        self.assertTrue(geometry.is_valid)
        self.assertEqual(census, {"rings": 3, "parts": 2, "holes": 1, "degenerate_rings": 0,
                                  "invalid": 0, "empty": 0, "vertices": 15,
                                  "noncanonical_orientation": 0})
        self.assertEqual(list(geometry.geoms[0].exterior.coords), [tuple(point) for point in rings[0]])
        self.assertEqual([list(interior.coords) for interior in geometry.geoms[0].interiors],
                         [[tuple(point) for point in rings[1]]])
        self.assertEqual(list(geometry.geoms[1].exterior.coords), [tuple(point) for point in rings[2]])

    def test_noncanonical_island_orientation_stays_shell(self):
        rings = [_cw(0, 0, 10, 10), _ccw(2, 2, 8, 8), _ccw(4, 4, 6, 6)]
        geometry = shapely.from_wkb(to_wkb({"rings": rings}, source.GEOMETRY_TYPE,
                                           has_z=False, has_m=False))
        census = source._census(rings, geometry)
        self.assertAlmostEqual(geometry.area, 68.0)
        self.assertTrue(geometry.is_valid)
        self.assertEqual(census["parts"], 2)
        self.assertEqual(census["holes"], 1)
        self.assertEqual(census["noncanonical_orientation"], 1)

    def test_deep_nesting_alternates_shell_hole_island_hole(self):
        rings = [_cw(0, 0, 10, 10), _ccw(2, 2, 8, 8), _cw(3, 3, 7, 7), _ccw(4, 4, 6, 6)]
        geometry = shapely.from_wkb(to_wkb({"rings": rings}, source.GEOMETRY_TYPE,
                                           has_z=False, has_m=False))
        census = source._census(rings, geometry)
        self.assertIsInstance(geometry, shapely.MultiPolygon)
        self.assertAlmostEqual(geometry.area, 76.0)
        self.assertTrue(geometry.is_valid)
        self.assertEqual(census["parts"], 2)
        self.assertEqual(census["holes"], 2)
        self.assertEqual(len(geometry.geoms[0].interiors), 1)
        self.assertEqual(len(geometry.geoms[1].interiors), 1)

    def test_partially_overlapping_exteriors_stay_parts(self):
        rings = [_cw(0, 0, 10, 10), _cw(0, 0, 11, 4)]
        geometry = shapely.from_wkb(to_wkb({"rings": rings}, source.GEOMETRY_TYPE,
                                           has_z=False, has_m=False))
        census = source._census(rings, geometry)
        self.assertIsInstance(geometry, shapely.MultiPolygon)
        self.assertAlmostEqual(geometry.area, 144.0)
        self.assertEqual(census["parts"], 2)
        self.assertEqual(census["holes"], 0)
        self.assertEqual(census["invalid"], 1)

    def test_identical_rings_stay_parts(self):
        rings = [_cw(0, 0, 10, 10), _cw(0, 0, 10, 10)]
        geometry = shapely.from_wkb(to_wkb({"rings": rings}, source.GEOMETRY_TYPE,
                                           has_z=False, has_m=False))
        census = source._census(rings, geometry)
        self.assertEqual(census["parts"], 2)
        self.assertEqual(census["holes"], 0)
        self.assertEqual(census["invalid"], 1)

    def test_degenerate_rings_follow_containment_rule(self):
        contained = [_cw(0, 0, 10, 10), _ring([(5, 5), (6, 5), (5, 5)])]
        geometry = shapely.from_wkb(to_wkb({"rings": contained}, source.GEOMETRY_TYPE,
                                           has_z=False, has_m=False))
        census = source._census(contained, geometry)
        self.assertEqual(census["parts"], 1)
        self.assertEqual(census["holes"], 1)
        self.assertEqual(census["degenerate_rings"], 1)
        self.assertEqual(census["invalid"], 1)
        self.assertAlmostEqual(geometry.area, 100.0)
        standalone = [_ring([(5, 5), (6, 5), (5, 5)])]
        geometry = shapely.from_wkb(to_wkb({"rings": standalone}, source.GEOMETRY_TYPE,
                                           has_z=False, has_m=False))
        census = source._census(standalone, geometry)
        self.assertEqual(census["parts"], 1)
        self.assertEqual(census["holes"], 0)
        self.assertEqual(census["degenerate_rings"], 1)
        self.assertEqual(census["invalid"], 1)


if __name__ == "__main__":
    unittest.main()
