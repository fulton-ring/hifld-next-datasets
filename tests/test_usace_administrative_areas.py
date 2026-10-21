import io
import hashlib
import json
import tempfile
import unittest
import math
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock

import httpx
import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.usace_administrative_areas import (
    EXPECTED_FIELD_DECLARATIONS, EXPECTED_FIELDS, PAGE_SIZE, _esri_polygon, _fetch_retry, _parquet_bytes, _validate,
)
from dagster_hifld.http_capture import capture_roster_digests, validate_capture_roster


class UsaceAdministrativeAreasTests(unittest.TestCase):
    def test_midstream_transport_failure_is_receipted_and_retry_roster_is_sealed(self):
        class BrokenStream(httpx.SyncByteStream):
            def __iter__(self):
                yield b"partial-"
                raise httpx.ReadError("connection reset")
            def close(self):
                pass

        class CompleteStream(httpx.SyncByteStream):
            def __iter__(self):
                yield b"complete"
            def close(self):
                pass

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            failed = httpx.Response(200, headers={"content-length":"20"}, stream=BrokenStream(),
                                    request=httpx.Request("GET", "https://example.test/data"))
            succeeded = httpx.Response(200, stream=CompleteStream(), request=httpx.Request("GET", "https://example.test/data"))
            client = Mock()
            client.stream.side_effect = [nullcontext(failed), nullcontext(succeeded)]
            budget = [0]
            body, receipt = _fetch_retry(client, "https://example.test/data", root, "page_000", budget)
            self.assertEqual(body, b"complete")
            self.assertTrue(receipt["complete_body"])
            failure = json.loads((root / "page_000.attempt1.receipt.json").read_bytes())
            self.assertFalse(failure["complete_body"])
            self.assertEqual(failure["bytes"], len(b"partial-"))
            self.assertEqual(failure["sha256"], hashlib.sha256(b"partial-").hexdigest())
            self.assertEqual(failure["error_type"], "ReadError")
            roster = capture_roster_digests(root)
            validate_capture_roster(root, set(roster))
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                validate_capture_roster(root, set(roster) - {"page_000.wire"})
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                validate_capture_roster(root, set(roster) | {"unbound.extra"})
            (root / "unbound.extra").write_bytes(b"extra")
            with self.assertRaisesRegex(ValueError, "capture_roster_mismatch"):
                validate_capture_roster(root, set(roster))
            (root / "unbound.extra").unlink()
            (root / "page_000.attempt1.body").write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "capture_roster_artifact_mismatch"):
                validate_capture_roster(root, roster)

    def test_native_physical_types_ids_and_polygon_coordinates_are_retained(self):
        fields = [
            {"name":"OBJECTID","type":"esriFieldTypeOID","nullable":False},
            {"name":"SHAPE","type":"esriFieldTypeGeometry"},
            {"name":"Jurisdiction","type":"esriFieldTypeString","length":254},
            {"name":"Scale_Minimum","type":"esriFieldTypeInteger"},
            {"name":"Feature_ID","type":"esriFieldTypeString","length":254},
        ]
        feature = {"attributes":{"OBJECTID":17,"Jurisdiction":"00107","Scale_Minimum":0,"Feature_ID":"X-17"},
                   "geometry":{"rings":[[[0,0],[0,4],[4,4],[4,0],[0,0]]],"spatialReference":{"wkid":4326}}}
        body = _parquet_bytes(fields,[feature],4326)
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(),[17])
        self.assertEqual(table.column("Jurisdiction").to_pylist(),["00107"])
        self.assertEqual(table.column("Scale_Minimum").to_pylist(),[0])
        self.assertEqual(table.column("Feature_ID").to_pylist(),["X-17"])
        self.assertEqual(str(table.schema.field("OBJECTID").type),"int64")
        self.assertEqual(str(table.schema.field("Scale_Minimum").type),"int32")
        primary,_,crs=geometry_metadata(table.schema)
        self.assertEqual(primary,"geometry"); self.assertEqual(crs.to_epsg(),4326)
        geom=shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(list(geom.exterior.coords),[(0.,0.),(0.,4.),(4.,4.),(4.,0.),(0.,0.)])

    def test_polygon_z_and_m_ordinates_are_encoded_without_loss(self):
        for geometry, expected in [
            ({"hasZ":True,"rings":[[[0,0,7],[0,1,8],[1,1,9],[1,0,10],[0,0,7]]]},(True,False)),
            ({"hasM":True,"rings":[[[0,0,17],[0,1,18],[1,1,19],[1,0,20],[0,0,17]]]},(False,True)),
            ({"hasZ":True,"hasM":True,"rings":[[[0,0,7,17],[0,1,8,18],[1,1,9,19],[1,0,10,20],[0,0,7,17]]]},(True,True)),
        ]:
            geom=shapely.from_wkb(_esri_polygon(geometry))
            self.assertEqual((geom.has_z,geom.has_m),expected)
            coords=shapely.get_coordinates(geom,include_z=True,include_m=True)
            self.assertEqual(coords[0,:2].tolist(),[0.,0.])
            if expected[0]: self.assertEqual(coords[0,2],7.)
            else: self.assertTrue(math.isnan(coords[0,2]))
            if expected[1]: self.assertEqual(coords[0,3],17.)
            else: self.assertTrue(math.isnan(coords[0,3]))

    def test_native_schema_profile_is_explicit_and_captures_complete_population(self):
        fields=EXPECTED_FIELD_DECLARATIONS
        ids=[3,8]
        docs={
            "layer_before":{"name":"ADMINISTRATIVE_AREA","id":56,"geometryType":"esriGeometryPolygon","spatialReference":{"latestWkid":4326},"maxRecordCount":2000,"fields":fields},
            "layer_after":{"fields":fields,"geometryType":"esriGeometryPolygon"},
            "count_before":{"count":2966},"count_after":{"count":2966},
            "ids_before":{"objectIds":ids},"ids_after":{"objectIds":ids},
        }
        # Population and pages are represented by valid complete capture rows.
        attrs={name:None for name,_ in EXPECTED_FIELDS if name!="SHAPE"}
        features=[]
        for oid in range(2966):
            row=dict(attrs); row["OBJECTID"]=oid
            features.append({"attributes":row,"geometry":{"rings":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}})
        docs["ids_before"]["objectIds"]=list(range(2966));docs["ids_after"]["objectIds"]=list(range(2966))
        captures={name:(__import__("json").dumps(doc).encode(),{}) for name,doc in docs.items()}
        for i, start in enumerate(range(0, len(features), PAGE_SIZE)):
            captures[f"page_{i:03d}"]=(__import__("json").dumps({"spatialReference":{"latestWkid":4326},"features":features[start:start+PAGE_SIZE]}).encode(),{})
        parsed_fields,parsed_features,audit=_validate(captures)
        self.assertEqual(parsed_fields,fields);self.assertEqual(len(parsed_features),2966)
        self.assertEqual(audit["source_feature_count"],2966)


if __name__ == "__main__":
    unittest.main()
