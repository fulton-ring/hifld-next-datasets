"""BTS NTAD Intermodal Freight Facilities shared family coverage.

The per-layer capture/seal/catalog/drift coverage lives in the thin test
modules (``BtsIntermodalLayerTests`` in ``bts_intermodal_family_support``);
this module pins the shared spec, URL and page-planning behavior to the
pre-refactor byte output.
"""
from __future__ import annotations

import dataclasses
import unittest

from dagster_hifld.sources import bts_air_truck as air_source
from dagster_hifld.sources import bts_marine_roro as marine_source
from dagster_hifld.sources import bts_rail_tofc_cofc as rail_source
from dagster_hifld.sources.bts_intermodal_family import (
    AIR_TO_TRUCK_SPEC, MARINE_RORO_SPEC, RAIL_TOFC_COFC_SPEC,
    object_id_chunks, page_plan, query_url,
    source_after_urls, source_metadata_urls,
)

import httpx

_SERVICE = {
    "air_truck": ("https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
                  "NTAD_Intermodal_Freight_Facilities_Air_to_Truck/FeatureServer"),
    "marine_roro": ("https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
                    "NTAD_Intermodal_Freight_Facilities_Marine_Roll_on_Roll_off/FeatureServer"),
    "rail_tofc_cofc": ("https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/"
                       "NTAD_Intermodal_Freight_Facilities_Rail_TOFC_COFC/FeatureServer"),
}

_SEED_QUERY = "where=1%3D1&returnIdsOnly=true&f=json"
_COUNT_QUERY = "where=1%3D1&returnCountOnly=true&f=json"
_ORDERED_PAGE_QUERY = ("where=1%3D1&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true"
                       "&outSR=4326&orderByFields=OBJECTID+ASC&resultOffset=0&resultRecordCount=1000&f=json")
_BOUNDED_PAGE_QUERY = ("objectIds=1%2C2&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true"
                       "&outSR=4326&orderByFields=OBJECTID+ASC&f=json")

_CHUNK_SIZES = {
    "air_truck": [276, 258, 258, 251, 221, 221, 221, 221, 221, 221, 221, 221, 221, 221, 221,
                  221, 221, 221, 221, 221, 221, 200],
    "marine_roro": [274, 256, 256, 250, 220, 220, 220, 220, 220, 220, 220, 220, 220, 220, 220,
                    220, 220, 220, 220, 220, 220, 220, 4],
    "rail_tofc_cofc": [276, 258, 258, 250, 221, 221, 221, 221, 221, 221, 221, 221, 221, 221, 221,
                       221, 221, 221, 221, 221, 221, 201],
}

_ORDERED_LAYER = {"advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True}}
_BOUNDED_LAYER = {"advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": False}}


class BtsIntermodalFamilyTests(unittest.TestCase):
    LAYERS = (("air_truck", AIR_TO_TRUCK_SPEC),
              ("marine_roro", MARINE_RORO_SPEC),
              ("rail_tofc_cofc", RAIL_TOFC_COFC_SPEC))

    def test_specs_are_frozen_and_distinct(self):
        specs = [spec for _layer, spec in self.LAYERS]
        for spec in specs:
            with self.assertRaises(dataclasses.FrozenInstanceError):
                spec.page_size = 1
        self.assertEqual({spec.item_id for spec in specs},
                         {"e73c3b5df80f42e79e2212a478392544",
                          "ff5f787f987c4686b3a79d3dc5b3daa2",
                          "e6332597fcd546d3899f60ec9b9157b9"})
        self.assertEqual({spec.expected_source_count for spec in specs}, {404, 84, 241})
        for spec in specs:
            self.assertEqual(len(spec.expected_field_names), len(spec.expected_field_types))
            self.assertEqual(spec.expected_field_names[0], "OBJECTID")

    def test_seed_count_and_page_urls_match_pre_refactor_bytes(self):
        for layer, spec in self.LAYERS:
            with self.subTest(layer=layer):
                self.assertEqual(spec.service_url, _SERVICE[layer])
                self.assertEqual(spec.layer_url, _SERVICE[layer] + "/0")
                self.assertEqual(query_url(spec.layer_url, where="1=1", returnIdsOnly="true", f="json"),
                                 _SERVICE[layer] + "/0/query?" + _SEED_QUERY)
                self.assertEqual(query_url(spec.layer_url, where="1=1", returnCountOnly="true", f="json"),
                                 _SERVICE[layer] + "/0/query?" + _COUNT_QUERY)
                self.assertEqual(source_metadata_urls(spec), [
                    ("portal_item", f"https://www.arcgis.com/sharing/rest/content/items/{spec.item_id}?f=json"),
                    ("service_before", spec.service_url + "?f=pjson"),
                    ("layer_before", spec.layer_url + "?f=pjson"),
                    ("count_before", _SERVICE[layer] + "/0/query?" + _COUNT_QUERY),
                    ("ids_before", _SERVICE[layer] + "/0/query?" + _SEED_QUERY),
                ])
                self.assertEqual(source_after_urls(spec), [
                    ("layer_after", spec.layer_url + "?f=pjson"),
                    ("service_after", spec.service_url + "?f=pjson"),
                    ("count_after", _SERVICE[layer] + "/0/query?" + _COUNT_QUERY),
                    ("ids_after", _SERVICE[layer] + "/0/query?" + _SEED_QUERY),
                ])

    def test_page_plans_match_pre_refactor_bytes(self):
        for layer, spec in self.LAYERS:
            with self.subTest(layer=layer):
                kwargs = {"layer_url": spec.layer_url, "out_sr": 4326,
                          "page_size": spec.page_size, "max_url_length": spec.max_object_id_url_length}
                mode, pages = page_plan([1, 2], _ORDERED_LAYER, **kwargs)
                self.assertEqual(mode, "ordered_offset")
                self.assertEqual(pages,
                                 [("page_000", _SERVICE[layer] + "/0/query?" + _ORDERED_PAGE_QUERY, [1, 2])])
                mode, pages = page_plan([1, 2], _BOUNDED_LAYER, **kwargs)
                self.assertEqual(mode, "bounded_object_ids")
                self.assertEqual(pages,
                                 [("page_000", _SERVICE[layer] + "/0/query?" + _BOUNDED_PAGE_QUERY, [1, 2])])

    def test_object_id_chunks_match_pre_refactor_bytes(self):
        ids = list(range(1, 5001))
        for layer, spec in self.LAYERS:
            with self.subTest(layer=layer):
                chunks = object_id_chunks(ids, layer_url=spec.layer_url, out_sr=4326,
                                          max_url_length=spec.max_object_id_url_length)
                self.assertEqual([len(chunk) for chunk in chunks], _CHUNK_SIZES[layer])
                self.assertEqual(sum(len(chunk) for chunk in chunks), 5000)
                for index, chunk in enumerate(chunks):
                    def chunk_url(object_ids):
                        return query_url(spec.layer_url, objectIds=",".join(map(str, object_ids)),
                                         outFields="*", returnGeometry="true", returnZ="true",
                                         returnM="true", outSR="4326", orderByFields="OBJECTID ASC",
                                         f="json")
                    self.assertLess(len(chunk_url(chunk)), spec.max_object_id_url_length)
                    if index + 1 < len(chunks):
                        self.assertGreaterEqual(
                            len(chunk_url([*chunk, chunks[index + 1][0]])), spec.max_object_id_url_length)

    def test_ordered_plan_pages_full_roster(self):
        for layer, spec in self.LAYERS:
            with self.subTest(layer=layer):
                mode, pages = page_plan(list(range(1, 2501)), _ORDERED_LAYER,
                                        layer_url=spec.layer_url, out_sr=4326,
                                        page_size=spec.page_size,
                                        max_url_length=spec.max_object_id_url_length)
                self.assertEqual(mode, "ordered_offset")
                self.assertEqual(len(pages), 3)
                self.assertEqual([len(page_ids) for _name, _url, page_ids in pages], [1000, 1000, 500])
                self.assertIn("resultOffset=0&resultRecordCount=1000", pages[0][1])
                self.assertIn("resultOffset=2000&resultRecordCount=1000", pages[2][1])
                self.assertEqual(pages[2][2], list(range(2001, 2501)))

    def test_thin_modules_expose_frozen_spec_and_test_seams(self):
        for module, spec in ((air_source, AIR_TO_TRUCK_SPEC),
                             (marine_source, MARINE_RORO_SPEC),
                             (rail_source, RAIL_TOFC_COFC_SPEC)):
            with self.subTest(module=module.__name__):
                self.assertIs(module.SPEC, spec)
                self.assertEqual(module.ITEM_ID, spec.item_id)
                self.assertEqual(module.SERVICE_URL, spec.service_url)
                self.assertEqual(module.LAYER_ID, spec.layer_id)
                self.assertEqual(module.LAYER_URL, spec.layer_url)
                self.assertEqual(module.LAYER_NAME, spec.layer_name)
                self.assertEqual(module.DATASET_SLUG, spec.dataset_slug)
                self.assertEqual(module.FILE_SLUG, spec.file_slug)
                self.assertEqual(module.EXPECTED_SOURCE_COUNT, spec.expected_source_count)
                self.assertEqual(module.EXPECTED_SOURCE_ID_SHA256, spec.expected_source_id_sha256)
                self.assertEqual(module.EXPECTED_SOURCE_SCHEMA_SHA256, spec.expected_source_schema_sha256)
                self.assertIs(module.httpx, httpx)


if __name__ == "__main__":
    unittest.main()
