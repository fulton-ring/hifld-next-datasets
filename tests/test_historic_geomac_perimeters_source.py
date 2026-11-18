"""Synthetic coherent-capture checks and adversarial drift gates; no network."""
import copy
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import shapely
from pyarrow import parquet as pq

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import historic_geomac_perimeters as source

ITEM_ID = "e02b85c0ea784ce7bd8add7ae3d293d0"
ORG_ID = "T4QMspbfLg3qTGWY"
SERVICE_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "InterAgencyFirePerimeterHistory_All_Years_View/FeatureServer"
)
LAYER_URL = SERVICE_URL + "/0"
WITNESS = {"lastEditDate": 1784758459952, "schemaLastEditDate": 1784758459952,
           "dataLastEditDate": 1782422738160}

FIELDS = [{"name": name, "type": kind, "length": length, "nullable": nullable}
          for name, kind, length, nullable in source.EXPECTED_FIELDS]


def _ring(points):
    return [list(point) for point in points] + [list(points[0])]


def _cw(x0, y0, x1, y1):
    return _ring([(x0, y0), (x0, y1), (x1, y1), (x1, y0)])


def _ccw(x0, y0, x1, y1):
    return _ring([(x0, y0), (x1, y0), (x1, y1), (x0, y1)])


def _attributes(oid, **overrides):
    values = {
        "OBJECTID": oid, "IRWINID": None, "FORID": None, "INCIDENT": "Test Fire",
        "GIS_ACRES": 10.5, "UNQE_FIRE_ID": "2013-NMLNF-000015", "DATE_CUR": "20140201000000",
        "FIRE_YEAR_INT": 2013, "UNIT_ID": "NMLNF", "POO_RESP_I": "NMLNF", "LOCAL_NUM": "000015",
        "FEATURE_CA": "Wildfire Final Fire Perimeter", "MAP_METHOD": "GPS - Uncorrected Data",
        "COMMENTS": "Data received from district fire personnel",
        "GEO_ID": "{29F9DCCC-6429-4CB7-9797-0A1FDE5339B7}", "SOURCE": "USFS", "AGENCY": "USFS",
        "FIRE_YEAR": "2013", "GlobalID": f"{oid:08d}-1111-1111-1111-111111111111",
    }
    values.update(overrides)
    return values


def _features():
    return [
        {"attributes": _attributes(10),
         "geometry": {"rings": [_cw(-12000000, 4000000, -11900000, 4100000)]}},
        {"attributes": _attributes(20, SOURCE="BLM", AGENCY="BLM", GIS_ACRES=12.25),
         "geometry": {"rings": [_cw(-12100000, 4000000, -12000000, 4100000),
                                _ccw(-12060000, 4030000, -12040000, 4070000)]}},
        {"attributes": _attributes(30, INCIDENT=None, COMMENTS=None, FIRE_YEAR_INT=9999),
         "geometry": {"rings": [_cw(-11800000, 4400000, -11750000, 4450000),
                                _ccw(-11790000, 4410000, -11760000, 4440000)]}},
        {"attributes": _attributes(40, SOURCE="CALFIRE", AGENCY="CALFIRE", GIS_ACRES=None),
         "geometry": {"rings": [_cw(-11400000, 4600000, -11350000, 4650000)]}},
        {"attributes": _attributes(50, UNQE_FIRE_ID=None),
         "geometry": {"rings": [_cw(-13000000, 3000000, -12900000, 3100000),
                                _cw(-12800000, 3000000, -12700000, 3100000)]}},
    ]


def _layer(editing_info=None, fields=None):
    return {"id": 0, "name": "InteragencyFirePerimeterHistory", "type": "Feature Layer",
            "geometryType": "esriGeometryPolygon",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "hasZ": False, "hasM": False, "maxRecordCount": 2000,
            "objectIdField": "OBJECTID", "globalIdField": "GlobalID",
            "geometryField": None, "supportedQueryFormats": "JSON, geoJSON, PBF",
            "editingInfo": copy.deepcopy(editing_info) if editing_info is not None else copy.deepcopy(WITNESS),
            "fields": copy.deepcopy(fields if fields is not None else FIELDS)}


def _item():
    return {"id": ITEM_ID, "owner": source.OWNER, "orgId": ORG_ID,
            "type": "Feature Service", "url": SERVICE_URL,
            "title": "InterAgencyFirePerimeterHistory_AllYears_View"}


def _service():
    return {"serviceItemId": ITEM_ID, "currentVersion": 12,
            "capabilities": "Query,Extract", "supportedQueryFormats": "JSON, geoJSON, PBF",
            "layers": [{"id": 0, "name": "InteragencyFirePerimeterHistory"}]}


def _capture_docs(page_size=2, features=None, witness=None, count_override=None,
                  ids_override=None):
    features = _features() if features is None else features
    ordered = sorted(features, key=lambda feature: feature["attributes"]["OBJECTID"])
    ids = [feature["attributes"]["OBJECTID"] for feature in ordered]
    docs = {"item": _item(), "service_before": _service(), "service_after": _service(),
            "layer_before": _layer(witness), "layer_after": _layer(witness),
            "count_before": {"count": len(ids) if count_override is None else count_override},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids if ids_override is None else ids_override},
            "count_after": {"count": len(ids) if count_override is None else count_override},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids if ids_override is None else ids_override}}
    for start in range(0, len(ordered), page_size):
        page = {"objectIdFieldName": "OBJECTID",
                "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                "geometryType": "esriGeometryPolygon",
                "features": copy.deepcopy(ordered[start:start + page_size])}
        if start + page_size < len(ordered):
            page["exceededTransferLimit"] = True
        docs[f"page_{start:05d}"] = page
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


class HistoricPerimetersSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = StagingStorageResource(local_dir=self.tmp.name, use_local=True)

    def stage(self, docs=None, page_size=2, **kwargs):
        root = Path(self.tmp.name) / "capture"
        _write_capture(root, _capture_docs(page_size=page_size, **kwargs) if docs is None else docs, page_size)
        return source.capture_historic_perimeters(
            self.storage, capture_directory=str(root), page_size=page_size)

    def test_complete_snapshot_candidate_and_readback_parity(self):
        result = self.stage()
        self.assertTrue(result["source_population_complete"])
        self.assertEqual(result["feature_count"], 5)
        self.assertEqual(result["source_invalid_geometry_count"], 0)
        self.assertFalse(result["release_accepted"])
        root = Path(self.storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
        table = pq.read_table(root / "geoparquet" / f"{source.FILE_SLUG}.parquet")
        self.assertEqual(table.num_rows, 5)
        self.assertEqual(table.schema.names, source.FIELD_NAMES + ["geometry"])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("FIRE_YEAR_INT").type), "int16")
        self.assertEqual(str(table.schema.field("GIS_ACRES").type), "float")
        self.assertEqual(str(table.schema.field("GlobalID").type), "string")
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 20, 30, 40, 50])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 3857)
        self.assertTrue(all(shapely.from_wkb(value).is_valid for value in table.column("geometry").to_pylist()))
        manifest = json.loads((root / "metadata" / "source_manifest.json").read_bytes())
        self.assertFalse(manifest["approved"])
        self.assertEqual(manifest["lineage"]["item_owner"], source.OWNER)
        self.assertEqual(manifest["lineage"]["supersedes"],
                         "Historic Perimeters Combined 2000-2018 GeoMAC (retired)")
        self.assertEqual({urlsplit(entry["retrieval_url"]).hostname for entry in manifest["inputs"]},
                         {"services3.arcgis.com", "www.arcgis.com"})
        audit = json.loads((root / "metadata" / "source_audit.json").read_bytes())
        self.assertEqual(audit["feature_count"], 5)
        self.assertEqual(audit["null_counts"]["INCIDENT"], 1)
        self.assertEqual(audit["fire_year_counts"]["9999"], 1)
        ready = json.loads((root / "metadata" / "candidate_ready.json").read_bytes())
        self.assertFalse(ready["release_accepted"])
        self.assertEqual(ready["source_invalid_geometry_count"], 0)

    def test_holes_and_multipart_polygons_are_preserved(self):
        result = self.stage()
        root = Path(self.storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG / result["version"]
        table = pq.read_table(root / "geoparquet" / f"{source.FILE_SLUG}.parquet")
        geometries = [shapely.from_wkb(value) for value in table.column("geometry").to_pylist()]
        self.assertIsInstance(geometries[1], shapely.Polygon)
        self.assertEqual(len(geometries[1].interiors), 1)
        self.assertIsInstance(geometries[4], shapely.MultiPolygon)
        self.assertEqual(len(geometries[4].geoms), 2)

    def test_replay_is_idempotent_and_immutable(self):
        root = Path(self.tmp.name) / "capture"
        _write_capture(root, _capture_docs(), 2)
        first = source.capture_historic_perimeters(self.storage, capture_directory=str(root), page_size=2)
        second = source.capture_historic_perimeters(self.storage, capture_directory=str(root), page_size=2)
        self.assertTrue(first["reused"])
        self.assertTrue(second["reused"])
        self.assertEqual(first["version"], second["version"])
        base = Path(self.storage.local_dir) / source.DATASET_SLUG / source.FILE_SLUG
        first_parquet = (base / first["version"] / "geoparquet" / f"{source.FILE_SLUG}.parquet").read_bytes()
        second_parquet = (base / second["version"] / "geoparquet" / f"{source.FILE_SLUG}.parquet").read_bytes()
        self.assertEqual(first_parquet, second_parquet)

    def test_page_order_ids_and_metadata_drifts_fail_closed(self):
        docs = _capture_docs()

        def mutate(change):
            altered = copy.deepcopy(docs)
            change(altered)
            with self.assertRaises(ValueError):
                self.stage(docs=altered)

        def drop_feature(current):
            current["page_00002"]["features"].pop()

        def duplicate_oid(current):
            current["page_00002"]["features"][0]["attributes"]["OBJECTID"] = 10

        def out_of_order(current):
            rows = current["page_00000"]["features"]
            rows[0], rows[1] = rows[1], rows[0]

        def extra_attribute(current):
            current["page_00000"]["features"][0]["attributes"]["NEW"] = 1

        def drop_attribute(current):
            del current["page_00000"]["features"][0]["attributes"]["SOURCE"]

        def string_length_drift(current):
            current["page_00000"]["features"][0]["attributes"]["AGENCY"] = "TOO-LONG-AGENCY"

        def count_drift(current):
            current["count_after"]["count"] = 3

        def ids_drift(current):
            current["ids_after"]["objectIds"].pop()

        def field_type_drift(current):
            current["layer_before"]["fields"][4]["type"] = "esriFieldTypeString"

        def field_added(current):
            current["layer_after"]["fields"].append({"name": "NEW", "type": "esriFieldTypeString",
                                                     "length": 5, "nullable": True})

        def witness_drift(current):
            current["layer_after"]["editingInfo"]["lastEditDate"] += 1

        def item_drift(current):
            current["item"]["owner"] = "SomeMirror"

        def service_changed(current):
            current["service_after"]["capabilities"] = "Query"

        def crs_drift(current):
            current["layer_before"]["spatialReference"] = {"wkid": 4326, "latestWkid": 4326}

        def short_page_transfer_flag(current):
            current["page_00004"]["exceededTransferLimit"] = True

        def null_geometry(current):
            current["page_00000"]["features"][0]["geometry"] = None

        def invalid_geometry(current):
            ring = _ring([(-12000000, 4000000), (-11900000, 4100000),
                          (-11900000, 4000000), (-12000000, 4100000)])
            current["page_00000"]["features"][0]["geometry"] = {"rings": [ring]}

        for change in (drop_feature, duplicate_oid, out_of_order, extra_attribute, drop_attribute,
                       string_length_drift, count_drift, ids_drift, field_type_drift, field_added,
                       witness_drift, item_drift, service_changed, crs_drift,
                       short_page_transfer_flag, null_geometry, invalid_geometry):
            with self.subTest(change=change.__name__):
                mutate(change)

    def test_page_receipt_mismatch_fails_closed(self):
        root = Path(self.tmp.name) / "capture"
        _write_capture(root, _capture_docs(), 2)
        receipt_path = root / "page_00000.receipt.json"
        receipt = json.loads(receipt_path.read_bytes())
        receipt["bytes"] += 1
        receipt_path.write_bytes(source._canonical(receipt) + b"\n")
        manifest_path = root / "capture_manifest.json"
        manifest = json.loads(manifest_path.read_bytes())
        manifest["receipts"]["page_00000"] = receipt
        manifest_path.write_bytes(source._canonical(manifest) + b"\n")
        with self.assertRaises(ValueError):
            source.capture_historic_perimeters(self.storage, capture_directory=str(root), page_size=2)


if __name__ == "__main__":
    unittest.main()
