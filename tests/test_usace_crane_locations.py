"""Focused fidelity and drift checks for USACE IENC crane locations."""
import json
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.assets.usace_crane_locations import (
    usace_crane_locations_complete,
    usace_crane_locations_quality,
    usace_crane_locations_release,
)
from dagster_hifld.sources.usace_crane_locations import (
    BTS_ITEM_ID, EXPECTED_FIELDS, EXPECTED_IDS, _parquet_bytes, _validate,
)


def _capture(value):
    return json.dumps(value, separators=(",", ":")).encode(), {}


def _valid_capture():
    fields = [
        {"name": name, "type": kind, "alias": name, "domain": None,
         **({"length": 254} if kind == "esriFieldTypeString" else {})}
        for name, kind in EXPECTED_FIELDS
    ]
    layer = {
        "name": "CRANES_POINT", "id": 8, "fields": fields,
        "geometryType": "esriGeometryPoint",
        "spatialReference": {"latestWkid": 4326},
        "extent": {"xmin": -91.74366579999997, "ymin": 30.528435600000023,
                   "xmax": -79.51109279999997, "ymax": 42.72636520000003,
                   "spatialReference": {"latestWkid": 4326}},
        "maxRecordCount": 2000, "hasZ": False, "hasM": False,
    }
    attribute_fields = [name for name, _kind in EXPECTED_FIELDS if name != "SHAPE"]
    features = []
    for oid in EXPECTED_IDS:
        attributes = {name: None for name in attribute_fields}
        attributes.update(OBJECTID=oid, Scale_Minimum=0, Feature_ID=f"C-{oid}")
        features.append({"attributes": attributes, "geometry": {
            "x": -90.0 + oid / 1000, "y": 31.0 + oid / 1000,
            "spatialReference": {"latestWkid": 4326},
        }})
    page_fields = [{"name": n, "type": t} for n, t in EXPECTED_FIELDS if n != "SHAPE"]
    directory = {"services": [{"name": "IENC_Feature_Classes/CRANES_POINT", "type": "MapServer"}]}
    service = {"mapName": "CRANES_POINT", "layers": [{"id": 8, "name": "CRANES_POINT"}]}
    captures = {
        "ids_seed": _capture({"objectIds": EXPECTED_IDS}),
        "bts_item": _capture({"id": BTS_ITEM_ID, "owner": "USDOT_BTS", "description": "Bi-monthly USACE/AGC IENC updates"}),
        "service_directory": _capture(directory),
        "service_before": _capture(service), "service_after": _capture(service),
        "layer_before": _capture(layer), "layer_after": _capture(layer),
        "count_before": _capture({"count": len(EXPECTED_IDS)}),
        "count_after": _capture({"count": len(EXPECTED_IDS)}),
        "ids_before": _capture({"objectIdFieldName": "OBJECTID", "objectIds": EXPECTED_IDS}),
        "ids_after": _capture({"objectIdFieldName": "OBJECTID", "objectIds": EXPECTED_IDS}),
        "page_000": _capture({"fields": page_fields, "geometryType": "esriGeometryPoint",
                              "spatialReference": {"latestWkid": 4326}, "features": features}),
    }
    return captures, fields, features


class UsaceCraneLocationsTests(unittest.TestCase):
    def test_complete_native_population_and_typed_parquet(self):
        captures, fields, features = _valid_capture()
        parsed_fields, parsed_features, audit = _validate(captures)
        self.assertEqual(parsed_fields, fields)
        self.assertEqual(len(parsed_features), 148)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 148)
        table = pq.read_table(pa.BufferReader(_parquet_bytes(parsed_fields, parsed_features, 4326)))
        self.assertEqual(table.column_names, [name for name, _ in EXPECTED_FIELDS if name != "SHAPE"] + ["geometry"])
        self.assertEqual(table.schema.field("OBJECTID").type, pa.int64())
        self.assertEqual(table.schema.field("Scale_Minimum").type, pa.int32())
        self.assertEqual(table.schema.field("Feature_ID").type, pa.string())
        self.assertEqual(table.num_rows, 148)

    def test_rejects_page_crs_change_and_extra_ordinate(self):
        captures, _fields, _features = _valid_capture()
        page = json.loads(captures["page_000"][0])
        page["spatialReference"] = {"latestWkid": 3857}
        captures["page_000"] = _capture(page)
        with self.assertRaisesRegex(ValueError, "crs"):
            _validate(captures)

        captures, _fields, _features = _valid_capture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"]["z"] = 0
        captures["page_000"] = _capture(page)
        with self.assertRaisesRegex(ValueError, "dimensions|ordinates"):
            _validate(captures)

    def test_registered_checks_keep_release_acceptance_pending(self):
        candidate = {"feature_count": 148, "source_population_complete": True,
                     "source_null_geometry_count": 0, "source_empty_geometry_count": 0,
                     "source_invalid_geometry_count": 0, "source_schema_sha256": "x",
                     "release_accepted": False}
        self.assertTrue(usace_crane_locations_complete.node_def.compute_fn.decorated_fn(candidate).passed)
        self.assertTrue(usace_crane_locations_quality.node_def.compute_fn.decorated_fn(candidate).passed)
        release = usace_crane_locations_release.node_def.compute_fn.decorated_fn(candidate)
        self.assertFalse(release.passed)
        self.assertEqual(release.metadata["approval_status"].value, "pending")


if __name__ == "__main__":
    unittest.main()
