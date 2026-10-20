"""Native fidelity, drift, and approval tests for the USGS plate capture."""
import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.sources import plate_boundaries as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


class PlateBoundariesSourceTests(unittest.TestCase):
    def fixture(self):
        ids = [1, 3]
        layer = {"id": 1, "name": "Plates", "geometryType": "esriGeometryPolyline",
                 "fields": source.EXPECTED_FIELDS, "maxRecordCount": 1000,
                 "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "extent": {"xmin": 2.0, "ymin": 0.0, "xmax": 5.0, "ymax": 1.0,
                            "spatialReference": {"wkid": 102100, "latestWkid": 3857}}}
        feats = []
        for oid, name, x in ((1, "alpha", 2.0), (3, None, 4.0)):
            feats.append({"attributes": {"OBJECTID": oid, "NAME": name, "LABEL": "Other", "Shape_Length": 10.0},
                "geometry": {"paths": [[[x, 0.0], [x + 1, 1.0]]], "spatialReference": {"wkid": 3857}}})
        docs = {"service_before": {"serviceDescription": "USGS Tectonic Plate Boundaries",
                    "layers": [{"id": 0, "name": "Microplates"}, {"id": 1, "name": "Plates"}]},
                "service_after": {"serviceDescription": "USGS Tectonic Plate Boundaries",
                    "layers": [{"id": 0, "name": "Microplates"}, {"id": 1, "name": "Plates"}]},
                "layer_before": layer, "layer_after": layer,
                "count_before": {"count": 2}, "count_after": {"count": 2},
                "ids_before": {"objectIds": ids}, "ids_after": {"objectIds": ids},
                "page_000": {"spatialReference": {"wkid": 3857}, "features": feats}}
        return ids, docs

    def validate_fixture(self, docs):
        captures = {name: (canonical(doc), {}) for name, doc in docs.items()}
        with patch.object(source, "COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA", hashlib.sha256(canonical([1, 3])).hexdigest()), \
             patch.object(source, "EXPECTED_NULL_COUNTS", {"NAME": 1, "LABEL": 0, "Shape_Length": 0}), \
             patch.object(source, "EXPECTED_EXTENT", {"xmin": 2.0, "ymin": 0.0, "xmax": 5.0, "ymax": 1.0}), \
             patch.object(source, "EXPECTED_BOUNDS", (2.0, 0.0, 5.0, 1.0)), \
             patch.object(source, "EXPECTED_CAPTURE_NAMES", set(docs)):
            return source._validate(captures)

    def test_native_xy_and_source_attributes_survive_typed_geoparquet(self):
        _ids, docs = self.fixture()
        fields, features, audit = self.validate_fixture(docs)
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 3])
        self.assertEqual(table.schema.field("OBJECTID").type.__str__(), "int64")
        self.assertEqual(table.schema.field("NAME").type.__str__(), "string")
        self.assertEqual(table.schema.field("Shape_Length").type.__str__(), "double")
        geoms = [from_wkb(x) for x in table.column("geometry").to_pylist()]
        self.assertEqual([list(g.geoms[0].coords) for g in geoms], [[(2.0, 0.0), (3.0, 1.0)], [(4.0, 0.0), (5.0, 1.0)]])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})
        self.assertEqual(audit["attribute_null_counts"]["NAME"], 1)

    def test_missing_id_fails_closed(self):
        _ids, docs = self.fixture()
        docs["page_000"]["features"].pop()
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            self.validate_fixture(docs)

    def test_schema_drift_fails_closed(self):
        _ids, docs = self.fixture()
        docs["layer_before"]["fields"] = docs["layer_before"]["fields"][:-1]
        docs["layer_after"] = docs["layer_before"]
        with self.assertRaisesRegex(ValueError, "source_layer_schema_or_limit_drift"):
            self.validate_fixture(docs)

    def test_conflicting_wkid_declarations_fail_closed(self):
        _ids, docs = self.fixture()
        docs["page_000"]["spatialReference"] = {"wkid": 4326, "latestWkid": 3857}
        with self.assertRaisesRegex(ValueError, "page_000_response_crs_drift"):
            self.validate_fixture(docs)

if __name__ == "__main__":
    unittest.main()
