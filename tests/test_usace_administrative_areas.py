import io
import unittest
import math

import pyarrow.parquet as pq
import shapely

from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.sources.usace_administrative_areas import (
    EXPECTED_FIELD_DECLARATIONS, EXPECTED_FIELDS, _esri_polygon, _parquet_bytes, _validate,
)


class UsaceAdministrativeAreasTests(unittest.TestCase):
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
        for i in range(3):
            captures[f"page_{i:03d}"]=(__import__("json").dumps({"spatialReference":{"latestWkid":4326},"features":features[i*1000:(i+1)*1000]}).encode(),{})
        parsed_fields,parsed_features,audit=_validate(captures)
        self.assertEqual(parsed_fields,fields);self.assertEqual(len(parsed_features),2966)
        self.assertEqual(audit["source_feature_count"],2966)


if __name__ == "__main__":
    unittest.main()
