"""Focused native-drift tests for the Red Cross headquarters adapter."""
import hashlib
import json
import unittest
from pathlib import Path
from urllib.parse import urlencode

import pyarrow.parquet as pq
import pyarrow as pa

from dagster_hifld.sources import red_cross_headquarters as source


FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
    *[{"name": name, "type": "esriFieldTypeString", "nullable": True}
      for name in ("ECODE", "Chapter", "Region_Type", "RCODE", "Region", "DCODE", "Division",
                   "Chapter_Address", "Chapter_Address_2", "Chapter_City", "Chapter_State", "Chapter_Zip",
                   "Chapter_Phone", "Time_Zone", "FEMA_Region", "Status")],
    {"name": "Score", "type": "esriFieldTypeDouble", "nullable": True},
    {"name": "Match_addr", "type": "esriFieldTypeString", "nullable": True},
]


def _json(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def _fixture():
    layer = {"id": 0, "name": source.EXPECTED_LAYER, "geometryType": "esriGeometryPoint",
             "objectIdField": "OBJECTID", "hasZ": False, "hasM": False,
             "spatialReference": {"wkid": 102100, "latestWkid": 3857}, "fields": FIELDS}
    ids = list(range(1, source.EXPECTED_COUNT + 1))
    features = []
    for identity in ids:
        attrs = {field["name"]: None for field in FIELDS}
        attrs.update({"OBJECTID": identity, "ECODE": "55004" if identity == 226 else f"E{identity:05}",
                      "Status": "U" if identity == 226 else "A", "Score": float(identity)})
        geometry = None if identity == 226 else {"x": float(identity), "y": float(-identity)}
        feature = {"attributes": attrs}
        if geometry is not None:
            feature["geometry"] = geometry
        features.append(feature)
    bodies = {
        "layer_before": _json(layer), "count_before": _json({"count": len(ids)}),
        "ids_before": _json({"objectIdFieldName": "OBJECTID", "objectIds": ids}),
        "count_after": _json({"count": len(ids)}),
        "ids_after": _json({"objectIdFieldName": "OBJECTID", "objectIds": ids}),
        "layer_after": _json(layer),
    }
    for offset in range(0, len(features), 100):
        bodies[f"page_{offset:04d}"] = _json({"features": features[offset:offset + 100], "exceededTransferLimit": False})
    receipts = {}
    for name, body in bodies.items():
        if name.startswith("layer_"):
            params = {"f": "pjson"}
            url = source.SERVICE_URL
        elif name.startswith("count_"):
            params = {"f": "json", "where": "1=1", "returnCountOnly": "true"}
            url = source.SERVICE_URL + "/query"
        elif name.startswith("ids_"):
            params = {"f": "json", "where": "1=1", "returnIdsOnly": "true"}
            url = source.SERVICE_URL + "/query"
        else:
            offset = int(name.removeprefix("page_"))
            params = {"f": "json", "objectIds": ",".join(map(str, ids[offset:offset + 100])),
                      "outFields": "*", "returnGeometry": "true", "returnZ": "true",
                      "returnM": "true", "returnTrueCurves": "true", "orderByFields": "OBJECTID ASC"}
            url = source.SERVICE_URL + "/query"
        query = urlencode(params)
        if name.startswith("page_"):
            query = query.replace("%2C", "%2C")
        url += ("?" + query)
        receipts[name] = {"status": 200, "url": url, "final_url": url,
                          "fetched_at": "2026-10-19T11:20:02.072195+00:00",
                          "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                          "complete_body": True}
    return {name: {"body": body, "receipt": receipts[name], "receipt_body": _json(receipts[name]),
                   "retrieved_at": receipts[name]["fetched_at"],
                   "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
            for name, body in bodies.items()}


class RedCrossHeadquartersNativeTests(unittest.TestCase):
    def test_complete_typed_candidate_keeps_null_source_geometry_and_native_crs(self):
        captures = _fixture()
        layer, features, ids = source._validate_population(captures)
        parquet, geo = source._geoparquet(source._schema(layer), features)
        table = pq.read_table(pa.BufferReader(parquet))
        self.assertEqual(table.num_rows, 226)
        self.assertEqual(table.column("OBJECTID").to_pylist(), ids)
        self.assertEqual(table.column("geometry").null_count, 1)
        self.assertEqual(table.column("geometry").to_pylist()[-1], None)
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 3857)
        self.assertFalse(source._manifest(captures, layer, source._schema(layer), parquet, geo, captures,
            b'{"schema_version":1,"rows":[]}')["promotion_allowed"])

    def test_page_identity_and_request_roster_drift_fail_closed(self):
        captures = _fixture()
        page = json.loads(captures["page_0000"]["body"])
        page["features"][0]["attributes"]["OBJECTID"] = 999
        body = _json(page)
        captures["page_0000"]["body"] = body
        captures["page_0000"]["sha256"] = hashlib.sha256(body).hexdigest()
        captures["page_0000"]["bytes"] = len(body)
        captures["page_0000"]["receipt"]["sha256"] = captures["page_0000"]["sha256"]
        captures["page_0000"]["receipt"]["bytes"] = len(body)
        with self.assertRaisesRegex(ValueError, "feature_page_id_roster"):
            source._validate_population(captures)

        captures = _fixture()
        url = source.SERVICE_URL + "/query?f=json"
        captures["page_0100"]["receipt"]["url"] = url
        captures["page_0100"]["receipt"]["final_url"] = url
        with self.assertRaisesRegex(ValueError, "request_scope_drift"):
            source._validate_population(captures)

    def test_missing_geometry_cannot_move_to_another_record(self):
        captures = _fixture()
        page = json.loads(captures["page_0200"]["body"])
        page["features"][0]["geometry"] = None
        page["features"][25]["geometry"] = {"x": 1.0, "y": 1.0}
        body = _json(page)
        captures["page_0200"]["body"] = body
        captures["page_0200"]["sha256"] = hashlib.sha256(body).hexdigest()
        captures["page_0200"]["bytes"] = len(body)
        captures["page_0200"]["receipt"]["sha256"] = captures["page_0200"]["sha256"]
        captures["page_0200"]["receipt"]["bytes"] = len(body)
        with self.assertRaisesRegex(ValueError, "missing_geometry_roster_changed"):
            source._validate_population(captures)


if __name__ == "__main__":
    unittest.main()
