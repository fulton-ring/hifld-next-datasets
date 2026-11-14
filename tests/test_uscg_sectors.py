import json
import os
import struct
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus
from shapely import from_wkb
from shapely.geometry import Polygon

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.uscg_sectors import (
    uscg_sectors, uscg_sectors_complete, uscg_sectors_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import uscg_sectors as source
from dagster_hifld.sources.epa_frs_common import _canonical

SECTOR_NAMES = [
    "BOSTON", "CHARLESTON", "COLUMBIA RIVER", "CORPUS CHRISTI", "DELAWARE BAY",
    "DETROIT", "EASTERN GREAT LAKES", "GUAM", "HONOLULU", "HOUSTON-GALVESTON",
    "HUMBOLDT BAY", "JACKSONVILLE", "KEY WEST", "LAKE MICHIGAN",
    "LONG ISLAND SOUND", "LOS ANGELES-LONG BEACH",
    "LOWER MISSISSIPPI RIVER (MEMPHIS)", "MARYLAND-NCR", "MIAMI", "MOBILE",
    "NEW ORLEANS", "NEW YORK", "NORTH CAROLINA", "NORTHERN GREAT LAKES",
    "NORTHERN NEW ENGLAND (PORTLAND, MAINE)", "OHIO VALLEY", "SAN DIEGO",
    "SAN FRANCISCO", "SAN JUAN", "SEAK - SOUTHEAST ALASKA (JUNEAU)",
    "SEATTLE (PUGET SOUND)", "SOUTHEASTERN NEW ENGLAND (PROVIDENCE)",
    "ST. PETERSBURG", "UPPER MISSISSIPPI RIVER (ST. LOUIS)", "VIRGINIA",
    "WESTERN ALASKA (ANCHORAGE)",
]


class UscgSectorsTests(unittest.TestCase):
    def _shell(self, oid, lon0=-93.5, lat0=35.0):
        x0 = lon0 + (oid % 100) * 0.003
        y0 = lat0 + (oid // 100) * 0.003
        return [[x0, y0], [x0, y0 + 0.001], [x0 + 0.001, y0 + 0.001], [x0 + 0.001, y0], [x0, y0]]

    def _hole(self, shell):
        inset = 0.0002
        return [[shell[0][0] + inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[0][1] + inset],
                [shell[2][0] - inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[2][1] - inset],
                [shell[0][0] + inset, shell[0][1] + inset]]

    def _rings_for(self, oid):
        if oid == 7:
            # Honolulu-like: three clockwise shells, one pair split at the dateline.
            west = [[-180.0, 19.0], [-180.0, 20.0], [-179.9, 20.0], [-179.9, 19.0], [-180.0, 19.0]]
            east = [[179.9, 19.0], [179.9, 20.0], [179.9999, 20.0],
                    [179.9999, 19.0], [179.9, 19.0]]
            mid = [[-156.0, 20.5], [-156.0, 21.5], [-155.0, 21.5], [-155.0, 20.5], [-156.0, 20.5]]
            return [west, east, mid]
        if oid == 31:
            shell = self._shell(31)
            return [shell, self._hole(shell)]
        if oid == 16:
            # A clockwise zero-area spike: OGC-invalid like the source's
            # Sector New York ring self-intersection, and minimally repairable.
            return [[[0.0, 0.0], [0.0, 4.0], [2.0, 4.0], [2.0, 2.0],
                     [2.0, 4.0], [4.0, 4.0], [4.0, 0.0], [0.0, 0.0]]]
        return [self._shell(oid)]

    def _sector_names(self):
        return dict(zip(source.EXPECTED_IDS, sorted(SECTOR_NAMES)))

    def _set_rings(self, feature, rings):
        """Replace a feature's rings and resync the shape parity attributes."""
        feature["geometry"]["rings"] = rings
        decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
        feature["attributes"]["Shape__Area"] = decoded.area
        feature["attributes"]["Shape__Length"] = decoded.length

    def _documents(self):
        sr = {"wkid": 4326, "latestWkid": 4326}
        service = {"serviceItemId": source.CATALOG_ITEM_ID, "description": "",
                   "serviceDescription": "", "capabilities": "Query,Extract",
                   "maxRecordCount": source.SERVICE_MAX_RECORD_COUNT,
                   "hasVersionedData": False, "isView": True, "spatialReference": sr,
                   "fullExtent": {**source.SERVICE_FULL_EXTENT, "spatialReference": sr},
                   "layers": [{"id": 0, "name": source.LAYER_NAME, "parentLayerId": -1,
                               "subLayerIds": None, "type": "Feature Layer",
                               "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 0, "name": source.LAYER_NAME, "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": source.LAYER_MAX_RECORD_COUNT, "objectIdField": "FID",
                 "globalIdField": "", "typeIdField": "SectorName", "displayField": "NAME",
                 "hasZ": False, "hasM": False, "isView": True, "enableNullGeometry": False,
                 "copyrightText": source.LAYER_COPYRIGHT,
                 "editingInfo": dict(source.EXPECTED_EDITING_INFO),
                 "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
                 "spatialReference": sr,
                 "extent": {**source.EXPECTED_EXTENT, "spatialReference": sr},
                 "advancedQueryCapabilities": {"supportsPagination": True},
                 "fields": source.EXPECTED_FIELD_DECLARATIONS}
        docs = {
            "catalog_item": {"id": source.CATALOG_ITEM_ID, "owner": source.CATALOG_ITEM_OWNER,
                             "orgId": source.CATALOG_ORG_ID, "type": "Feature Service",
                             "title": source.ITEM_TITLE, "access": "public",
                             "accessInformation": source.ITEM_ACCESS_INFORMATION,
                             "spatialReference": "4326", "snippet": source.ITEM_SNIPPET,
                             "categories": [source.ITEM_CATEGORY],
                             "description": source.ITEM_DESCRIPTION,
                             "licenseInfo": source.ITEM_LICENSE,
                             "extent": [list(source.ITEM_EXTENT[0]), list(source.ITEM_EXTENT[1])]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        page_fields = [{key: value for key, value in field.items()
                        if key not in ("nullable", "editable", "isViewOverride")}
                       for field in source.EXPECTED_FIELD_DECLARATIONS]
        sector_names = self._sector_names()
        for page_index in range(source.PAGE_COUNT):
            chunk = source.EXPECTED_IDS[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]
            rows = []
            for oid in chunk:
                rings = self._rings_for(oid)
                decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
                attrs = {}
                for field in source.ATTRIBUTE_FIELDS:
                    name, kind = field["name"], field["type"]
                    if name == "FID":
                        attrs[name] = oid
                    elif name == "SectorName":
                        attrs[name] = sector_names[oid]
                    elif kind == "esriFieldTypeString":
                        attrs[name] = f"{name}-{oid}"
                    elif name == "SectorName":
                        attrs[name] = SECTOR_NAMES[oid - 1]
                    elif name == "Shape__Area":
                        attrs[name] = decoded.area
                    elif name == "Shape__Length":
                        attrs[name] = decoded.length
                    else:
                        attrs[name] = float(oid)
                rows.append({"attributes": attrs, "geometry": {"rings": rings}})
            docs[f"page_{page_index:03d}"] = {"objectIdFieldName": "FID",
                                              "geometryType": "esriGeometryPolygon",
                                              "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
                                              "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                                              "fields": page_fields, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (json.dumps(value, ensure_ascii=False, allow_nan=False).encode(), {"status": 200})
                for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(uscg_sectors, hifld_ingest_assets)
        self.assertEqual(uscg_sectors_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = uscg_sectors_complete({"feature_count": 36, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = uscg_sectors_complete({"feature_count": 35, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        from urllib.parse import parse_qs, urlsplit
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         ["page_000", "page_001", "page_002", "page_003"])
        covered = []
        for index in range(source.PAGE_COUNT):
            params = parse_qs(urlsplit(urls[f"page_{index:03d}"]).query)
            page_ids = [int(value) for value in params["objectIds"][0].split(",")]
            self.assertEqual(page_ids,
                             source.EXPECTED_IDS[index * source.PAGE_SIZE:(index + 1) * source.PAGE_SIZE])
            self.assertEqual(params["outSR"], ["4326"])
            self.assertEqual(params["orderByFields"], ["FID ASC"])
            self.assertEqual(params["returnZ"], ["true"])
            self.assertEqual(params["returnM"], ["true"])
            self.assertEqual(params["outFields"], ["*"])
            covered.extend(page_ids)
        self.assertEqual(sorted(covered), source.EXPECTED_IDS)
        self.assertNotIn(30, covered)
        self.assertEqual(source.CATALOG_ITEM_URL, "https://www.arcgis.com/sharing/rest/content/items/"
                         "ec3a1f03ec4548bb9d4435ea7e580c1a?f=json")
        self.assertEqual(source.LAYER_URL, "https://services9.arcgis.com/Fr5UUTVwNRJlXAkf/"
                         "arcgis/rest/services/USCG_Sectors_view/FeatureServer/0")

    def test_source_pins_match_researched_producer_publication(self):
        self.assertEqual(source.DATASET_SLUG, "uscg-sectors")
        self.assertEqual(source.PUBLISHER, "United States Coast Guard (USCG)")
        self.assertEqual(source.CATALOG_ORG_NAME, "USCG FSLTP Portal")
        self.assertIn("Operation Systems Center (OSC), United States Coast Guard (USCG)",
                      source.LAYER_COPYRIGHT)
        self.assertEqual(source.EXPECTED_COUNT, 36)
        self.assertEqual(source.EXPECTED_IDS, sorted([*range(1, 30), *range(31, 38)]))
        self.assertEqual(source.EXPECTED_LAST_DATA_EDIT_UTC, "2026-01-30T18:29:41.779000Z")
        self.assertEqual(source.EXPECTED_GEOMETRY_INVALID_COUNT, 1)
        self.assertEqual(source._sha(_canonical(source.EXPECTED_IDS)), source.EXPECTED_ID_SHA256)
        self.assertEqual(source._sha(_canonical(sorted(SECTOR_NAMES))),
                         source.EXPECTED_SECTOR_NAME_SHA256)
        self.assertIn("ec3a1f03ec4548bb9d4435ea7e580c1a", source.LANDING_URL)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 36)
        self.assertEqual(sorted(row["attributes"]["FID"] for row in features), source.EXPECTED_IDS)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 37)
        self.assertEqual(audit["geometry_invalid_count"], 1)
        self.assertEqual(audit["geometry_valid_count"], 35)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["source_vertex_count"], 198)
        self.assertEqual(audit["source_last_data_edit_utc"], "2026-01-30T18:29:41.779000Z")
        self.assertEqual(audit["dhs_crosswalk_row"]["status"], "Migrated")
        self.assertIn("USCG-owned", audit["source_coverage_scope"])
        self.assertIn("Fr5UUTVwNRJlXAkf", audit["catalog_item_org"])
        self.assertEqual(audit["geometry_invalid_ids"], [16])

    def test_polygon_wkb_is_xy_only_and_preserves_ring_structure(self):
        shell = self._shell(1)
        single = source._esri_polygon_wkb({"rings": [shell]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        west = [[-180.0, 19.0], [-180.0, 20.0], [-179.9, 20.0], [-179.9, 19.0], [-180.0, 19.0]]
        east = [[179.9, 19.0], [179.9, 20.0], [179.9999, 20.0], [179.9999, 19.0], [179.9, 19.0]]
        dateline = source._esri_polygon_wkb({"rings": [west, east]})
        self.assertEqual(struct.unpack("<BI", dateline[:5]), (1, 6))
        with_hole = source._esri_polygon_wkb({"rings": [shell, self._hole(shell)]})
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [shell], **flags})
        unflagged = [[0.0, 0.0, 1.0], [0.0, 1.0, 1.0], [1.0, 1.0, 1.0], [1.0, 0.0, 1.0], [0.0, 0.0, 1.0]]
        with self.assertRaisesRegex(ValueError, "source_polygon_zm_flags_do_not_match_coordinates"):
            source._esri_polygon_wkb({"rings": [unflagged]})

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population after", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_id_population_drift:after"),
            ("id roster gap filled", lambda d: d["ids_before"].update(objectIds=[*range(1, 37)]),
             "source_id_population_drift:before"),
            ("count before", lambda d: d["count_before"].update(count=35),
             "source_count_population_drift:before"),
            ("schema", lambda d: d["layer_before"]["fields"][2].update(name="renamed"),
             "source_field_schema_drift"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}),
             "source_page_000_page_crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                FID=d["page_000"]["features"][0]["attributes"]["FID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer freshness", lambda d: d["layer_before"]["editingInfo"].update(dataLastEditDate=1769797781780),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer extent", lambda d: d["layer_before"]["extent"].update(xmax=0.0),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer copyright", lambda d: d["layer_before"].update(copyrightText="someone else"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer view flag", lambda d: d["layer_before"].update(isView=False),
             "source_layer_identity_extent_or_freshness_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("page identity", lambda d: d["page_000"].update(objectIdFieldName="OBJECTID"),
             "source_page_identity_drift"),
            ("page geometry properties", lambda d: d["page_000"].update(geometryProperties={"units": "esriMeters"}),
             "source_page_geometry_properties_drift"),
            ("page field drift", lambda d: d["page_000"]["fields"][2].update(name="renamed"),
             "source_page_physical_schema_changed"),
            ("geometry dimensional", lambda d: d["page_000"]["features"][0]["geometry"].update(hasZ=True),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-185.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
            ("hole without shell", lambda d: d["page_001"]["features"][0]["geometry"]["rings"].append(
                self._hole(self._shell(99))),
             "source_hole_ownership_ambiguous"),
            ("shell missing", lambda d: d["page_001"]["features"][0]["geometry"].update(
                rings=[self._hole(self._shell(99))]),
             "source_polygon_shell_missing"),
            ("item identity", lambda d: d["catalog_item"].update(owner="someone_else"),
             "uscg_catalog_item_identity_drift"),
            ("item org", lambda d: d["catalog_item"].update(orgId="otherorg"),
             "uscg_catalog_item_identity_drift"),
            ("item access", lambda d: d["catalog_item"].update(access="private"),
             "uscg_catalog_item_identity_drift"),
            ("item accessInformation", lambda d: d["catalog_item"].update(accessInformation="Esri"),
             "uscg_catalog_item_identity_drift"),
            ("item license text", lambda d: d["catalog_item"].update(licenseInfo="changed"),
             "uscg_catalog_item_text_drift"),
            ("item description text", lambda d: d["catalog_item"].update(description="changed"),
             "uscg_catalog_item_text_drift"),
            ("item snippet", lambda d: d["catalog_item"].update(snippet="changed"),
             "uscg_catalog_item_text_drift"),
            ("item category", lambda d: d["catalog_item"].update(categories=["/Categories/Other"]),
             "uscg_catalog_item_text_drift"),
            ("item extent", lambda d: d["catalog_item"].update(extent=[[0.0, 0.0], [1.0, 1.0]]),
             "uscg_catalog_item_extent_drift"),
            ("service identity", lambda d: d["service_before"].update(serviceItemId="other"),
             "service_before_service_identity_drift"),
            ("service view flag", lambda d: d["service_before"].update(isView=False),
             "service_before_service_identity_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].append(
                {"id": 1, "name": "Extra", "parentLayerId": -1, "subLayerIds": None,
                 "type": "Feature Layer", "geometryType": "esriGeometryPolygon"}),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", lambda d: d["service_after"].update(maxRecordCount=2000),
             "source_service_or_layer_changed_during_capture"),
            ("string value type", lambda d: d["page_000"]["features"][0]["attributes"].update(SectorName=5),
             "source_string_value_type_drift:SectorName"),
            ("double value type", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area="x"),
             "source_double_value_type_drift:Shape__Area"),
            ("area parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area=1.5),
             "source_shape_area_parity_drift"),
            ("length parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Length=1.5),
             "source_shape_length_parity_drift"),
            ("attribute order", lambda d: d["page_000"]["features"][0]["attributes"].pop("Atu"),
             "source_attribute_roster_or_order_drift"),
            ("oid value", lambda d: d["page_000"]["features"][0]["attributes"].update(FID=0),
             "source_oid_value_type_drift"),
            ("oid roster", lambda d: d["page_000"]["features"][0]["attributes"].update(FID=99),
             "source_feature_id_roster_drift"),
            ("sector name roster", lambda d: d["page_003"]["features"][0]["attributes"].update(SectorName="RENAMED"),
             "source_sector_name_roster_drift"),
            ("geometry invalid count", lambda d: self._set_rings(
                d["page_001"]["features"][5], [self._shell(16)]),
             "source_geometry_invalid_count_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_repair_of_pinned_self_touching_ring_is_minimal_and_fail_closed(self):
        from shapely.validation import explain_validity
        # A clockwise zero-area spike, invalid under OGC exactly like the
        # source's Sector New York ring self-intersection.
        spike = {"rings": [[[0.0, 0.0], [0.0, 4.0], [2.0, 4.0], [2.0, 2.0],
                            [2.0, 4.0], [4.0, 4.0], [4.0, 0.0], [0.0, 0.0]]]}
        wkb, record = source._candidate_geometry_wkb(16, spike)
        fixed = from_wkb(wkb)
        raw = from_wkb(source._esri_polygon_wkb(spike))
        self.assertTrue(fixed.is_valid)
        self.assertIn(fixed.geom_type, ("Polygon", "MultiPolygon"))
        self.assertAlmostEqual(fixed.area, raw.area, places=12)
        self.assertEqual(fixed.bounds, raw.bounds)
        self.assertEqual(record["feature_id"], 16)
        self.assertEqual(record["operation"], "make_valid(method=structure)")
        self.assertEqual(record["validity_before"], explain_validity(raw))
        self.assertEqual(source._candidate_geometry_wkb(1, {"rings": [self._shell(1)]}),
                         (source._esri_polygon_wkb({"rings": [self._shell(1)]}), None))
        # A crossing bowtie would repair into lobes whose area changes and
        # must fail closed.
        overlapping = {"rings": [[[-74.66, 42.0], [-74.64, 42.02], [-74.64, 41.99],
                                  [-74.66, 42.01], [-74.66, 42.0]]]}
        with self.assertRaisesRegex(ValueError, "source_geometry_repair_area_drift"):
            source._candidate_geometry_wkb(16, overlapping)

    def test_real_capture_repair_contract_when_retained_evidence_is_available(self):
        capture_dir = os.environ.get("USCG_REAL_CAPTURE")
        if not capture_dir:
            self.skipTest("opt-in real-capture evidence via USCG_REAL_CAPTURE")
        page = json.loads((Path(capture_dir) / "page_001.body").read_bytes())
        fid16 = next(row for row in page["features"]
                     if row["attributes"]["FID"] == source.EXPECTED_REPAIRED_IDS[0])
        raw = from_wkb(source._esri_polygon_wkb(fid16["geometry"]))
        self.assertFalse(raw.is_valid)
        wkb, record = source._candidate_geometry_wkb(fid16["attributes"]["FID"], fid16["geometry"])
        fixed = from_wkb(wkb)
        self.assertTrue(fixed.is_valid)
        self.assertAlmostEqual(fixed.area, raw.area, places=12)
        self.assertEqual(fixed.bounds, raw.bounds)
        self.assertEqual(record["validity_before"],
                         "Ring Self-intersection[-74.6500029470783 42.0000067959769]")

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_uscg_sectors(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
