"""Tiny, bounded tests for FHWA NTI source-contract and release blocking."""
from __future__ import annotations

import unittest
import json
from unittest.mock import patch

from dagster_hifld.assets.fhwa_tunnel_inventory import fhwa_national_tunnel_inventory_release_gate
from dagster_hifld.sources import fhwa_tunnel_inventory as source


class FhwaTunnelInventoryTests(unittest.TestCase):
    def _captures(self):
        fields = [
            {"name":"OBJECTID","type":"esriFieldTypeOID","alias":"OBJECTID","nullable":False,"domain":None},
            {"name":"tunnel_name_i2","type":"esriFieldTypeString","alias":"tunnel_name_i2","length":10,"nullable":True,"domain":None},
        ]
        service = {"serviceItemId":source.ITEM_ID,"isView":True,"description":(
            "FHWA Federal Highway Administration (FHWA); compiled on September 02, 2025; "
            "published on August 26, 2025"),"layers":[{"id":0,"name":source.LAYER_NAME,"geometryType":"esriGeometryPoint"}]}
        layer = {"id":0,"name":source.LAYER_NAME,"geometryType":"esriGeometryPoint","objectIdField":"OBJECTID",
            "hasZ":False,"hasM":False,"extent":{"spatialReference":{"latestWkid":4326}},"fields":fields}
        portal = {"id":source.ITEM_ID,"owner":"USDOT_BTS","title":"National Tunnel Inventory",
            "url":source.SERVICE_URL,"access":"public","modified":1}
        features = [
            {"attributes":{"OBJECTID":1,"tunnel_name_i2":"Alpha"},"geometry":{"x":-100.0,"y":40.0}},
            {"attributes":{"OBJECTID":2,"tunnel_name_i2":None},"geometry":{"x":-101.0,"y":41.0}},
        ]
        ids = [1,2]
        geometry_rows = [[1,-100.0,40.0],[2,-101.0,41.0]]
        with patch.object(source, "EXPECTED_COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA256", source._sha(source._canonical(ids))), \
             patch.object(source, "EXPECTED_RECORD_SHA256", source._sha(source._canonical(features))), \
             patch.object(source, "EXPECTED_GEOMETRY_SHA256", source._sha(source._canonical(geometry_rows))), \
             patch.object(source, "EXPECTED_SCHEMA_SHA256", source._sha(source._canonical(fields))):
            count = {"count":2}
            id_doc = {"objectIdFieldName":"OBJECTID","objectIds":ids}
            page = {"geometryType":"esriGeometryPoint","spatialReference":{"latestWkid":4326},
                    "fields":fields,"features":features}
            docs = {"portal_item":portal,"service_before":service,"layer_before":layer,
                    "count_before":count,"ids_before":id_doc,"page_000":page,
                    "layer_after":layer,"service_after":service,"count_after":count,"ids_after":id_doc}
            return {name:(source._canonical(doc),{}) for name,doc in docs.items()}

    def test_source_contract_accepts_complete_native_ids_and_point_geometry(self):
        captures=self._captures()
        docs={name:json.loads(body) for name,(body,_) in captures.items()}
        fields=[source._plain_field(row) for row in docs["layer_before"]["fields"]]
        features=docs["page_000"]["features"]
        geometries=sorted([[f["attributes"]["OBJECTID"],f["geometry"]["x"],f["geometry"]["y"]] for f in features])
        with patch.object(source, "EXPECTED_COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA256", source._sha(source._canonical([1,2]))), \
             patch.object(source, "EXPECTED_RECORD_SHA256", source._sha(source._canonical(features))), \
             patch.object(source, "EXPECTED_GEOMETRY_SHA256", source._sha(source._canonical(geometries))), \
             patch.object(source, "EXPECTED_SCHEMA_SHA256", source._sha(source._canonical(fields))):
            layer,fields,extracted=source._validate(captures)
        self.assertEqual(layer["name"],source.LAYER_NAME)
        self.assertEqual(len(fields),2)
        self.assertEqual([row["attributes"]["OBJECTID"] for row in extracted["features"]],[1,2])
        self.assertEqual(extracted["audit"]["source_crs_epsg"],4326)

    def test_drifted_integer_value_is_rejected_without_coercion(self):
        captures=self._captures()
        docs={name:json.loads(body) for name,(body,_) in captures.items()}
        docs["page_000"]["features"][0]["attributes"]["OBJECTID"]="1"
        captures={name:(source._canonical(doc),{}) for name,doc in docs.items()}
        with patch.object(source, "EXPECTED_COUNT", 2), \
             patch.object(source, "EXPECTED_ID_SHA256", source._sha(source._canonical([1,2]))), \
             patch.object(source, "EXPECTED_SCHEMA_SHA256", source._sha(source._canonical([
                 source._plain_field(row) for row in docs["layer_before"]["fields"]]))):
            with self.assertRaisesRegex(ValueError,"source_objectid_physical_type_drift"):
                source._validate(captures)

    def test_release_gate_stays_closed_when_population_is_complete(self):
        result=fhwa_national_tunnel_inventory_release_gate({"feature_count":580})
        self.assertFalse(result.passed)
        self.assertFalse(result.metadata["source_edition_accepted"].value)
        self.assertFalse(result.metadata["release_accepted"].value)


if __name__ == "__main__":
    unittest.main()
