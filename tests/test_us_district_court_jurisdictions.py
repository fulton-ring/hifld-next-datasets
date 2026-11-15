import json
import struct
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.us_district_court_jurisdictions import (
    us_district_court_jurisdictions, us_district_court_jurisdictions_complete,
    us_district_court_jurisdictions_draft_schedule,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import us_district_court_jurisdictions as source
from dagster_hifld.sources.epa_frs_common import _canonical

# Real rosters as observed on 2026-11-14: 94 federal judicial districts plus the
# unassigned null-geometry 'XXX' placeholder (OBJECTID 95, Circuit 'XX'). The
# producer's own 'Lousiana' spelling in DistName is preserved exactly.
DISTRICT_NAMES = ['Central District of California', 'Central District of Illinois', 'District of Alaska', 'District of Arizona', 'District of Colorado', 'District of Columbia', 'District of Connecticut', 'District of Delaware', 'District of Guam', 'District of Hawaii', 'District of Idaho', 'District of Kansas', 'District of Maine', 'District of Maryland', 'District of Massachusetts', 'District of Minnesota', 'District of Montana', 'District of Nebraska', 'District of Nevada', 'District of New Hampshire', 'District of New Jersey', 'District of New Mexico', 'District of North Dakota', 'District of Oregon', 'District of Puerto Rico', 'District of Rhode Island', 'District of South Carolina', 'District of South Dakota', 'District of Utah', 'District of Vermont', 'District of Wyoming', 'District of the Northern Mariana Islands', 'District of the Virgin Islands', 'Eastern District of Arkansas', 'Eastern District of California', 'Eastern District of Kentucky', 'Eastern District of Louisiana', 'Eastern District of Michigan', 'Eastern District of Missouri', 'Eastern District of New York', 'Eastern District of North Carolina', 'Eastern District of Oklahoma', 'Eastern District of Pennsylvania', 'Eastern District of Tennessee', 'Eastern District of Texas', 'Eastern District of Virginia', 'Eastern District of Washington', 'Eastern District of Wisconsin', 'Middle District of Alabama', 'Middle District of Florida', 'Middle District of Georgia', 'Middle District of Louisiana', 'Middle District of North Carolina', 'Middle District of Pennsylvania', 'Middle District of Tennessee', 'Northern District of Alabama', 'Northern District of California', 'Northern District of Florida', 'Northern District of Georgia', 'Northern District of Illinois', 'Northern District of Indiana', 'Northern District of Iowa', 'Northern District of Mississippi', 'Northern District of New York', 'Northern District of Ohio', 'Northern District of Oklahoma', 'Northern District of Texas', 'Northern District of West Virginia', 'Southern District of Alabama', 'Southern District of California', 'Southern District of Florida', 'Southern District of Georgia', 'Southern District of Illinois', 'Southern District of Indiana', 'Southern District of Iowa', 'Southern District of Mississippi', 'Southern District of New York', 'Southern District of Ohio', 'Southern District of Texas', 'Southern District of West Virginia', 'Western District of Arkansas', 'Western District of Kentucky', 'Western District of Louisiana', 'Western District of Michigan', 'Western District of Missouri', 'Western District of New York', 'Western District of North Carolina', 'Western District of Oklahoma', 'Western District of Pennsylvania', 'Western District of Tennessee', 'Western District of Texas', 'Western District of Virginia', 'Western District of Washington', 'Western District of Wisconsin', 'XXX']
ABBREVIATIONS = ['AKX', 'ALM', 'ALN', 'ALS', 'ARE', 'ARW', 'AZX', 'CAC', 'CAE', 'CAN', 'CAS', 'COX', 'CTX', 'DCX', 'DEX', 'FLM', 'FLN', 'FLS', 'GAM', 'GAN', 'GAS', 'GUX', 'HIX', 'IAN', 'IAS', 'IDX', 'ILC', 'ILN', 'ILS', 'INN', 'INS', 'KSX', 'KYE', 'KYW', 'LAE', 'LAM', 'LAW', 'MAX', 'MDX', 'MEX', 'MIE', 'MIW', 'MNX', 'MOE', 'MOW', 'MPX', 'MSN', 'MSS', 'MTX', 'NCE', 'NCM', 'NCW', 'NDX', 'NEX', 'NHX', 'NJX', 'NMX', 'NVX', 'NYE', 'NYN', 'NYS', 'NYW', 'OHN', 'OHS', 'OKE', 'OKN', 'OKW', 'ORX', 'PAE', 'PAM', 'PAW', 'PRX', 'RIX', 'SCX', 'SDX', 'TNE', 'TNM', 'TNW', 'TXE', 'TXN', 'TXS', 'TXW', 'UTX', 'VAE', 'VAW', 'VIX', 'VTX', 'WAE', 'WAW', 'WIE', 'WIW', 'WVN', 'WVS', 'WYX', 'XXX']
SHORT_NAMES = ['Alabama (Middle)', 'Alabama (Northern)', 'Alabama (Southern)', 'Alaska', 'Arizona', 'Arkansas (Eastern)', 'Arkansas (Western)', 'California (Central)', 'California (Eastern)', 'California (Northern)', 'California (Southern)', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida (Middle)', 'Florida (Northern)', 'Florida (Southern)', 'Georgia (Middle)', 'Georgia (Northern)', 'Georgia (Southern)', 'Guam', 'Hawaii', 'Idaho', 'Illinois (Central)', 'Illinois (Northern)', 'Illinois (Southern)', 'Indiana (Northern)', 'Indiana (Southern)', 'Iowa (Northern)', 'Iowa (Southern)', 'Kansas', 'Kentucky (Eastern)', 'Kentucky (Western)', 'Louisiana (Middle)', 'Lousiana (Eastern)', 'Lousiana (Western)', 'Maine', 'Maryland', 'Massachusetts', 'Michigan (Eastern)', 'Michigan (Western)', 'Minnesota', 'Mississippi (Northern)', 'Mississippi (Southern)', 'Missouri (Eastern)', 'Missouri (Western)', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York (Eastern)', 'New York (Northern)', 'New York (Southern)', 'New York (Western)', 'North Carolina (Eastern)', 'North Carolina (Middle)', 'North Carolina (Western)', 'North Dakota', 'Northern Mariana Islands', 'Ohio (Northern)', 'Ohio (Southern)', 'Oklahoma (Eastern)', 'Oklahoma (Northern)', 'Oklahoma (Western)', 'Oregon', 'Pennsylvania (Eastern)', 'Pennsylvania (Middle)', 'Pennsylvania (Western)', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee (Eastern)', 'Tennessee (Middle)', 'Tennessee (Western)', 'Texas (Eastern)', 'Texas (Northern)', 'Texas (Southern)', 'Texas (Western)', 'Utah', 'Vermont', 'Virgin Islands', 'Virginia (Eastern)', 'Virginia (Western)', 'Washington (Eastern)', 'Washington (Western)', 'West Virginia (Northern)', 'West Virginia (Southern)', 'Wisconsin (Eastern)', 'Wisconsin (Western)', 'Wyoming', 'XXX']
STATES = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Columbia', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Guam', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Northern Mariana Islands', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Puerto Rico', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virgin Islands', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
CIRCUITS = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "DC", "XX"]


class UsDistrictCourtJurisdictionsTests(unittest.TestCase):
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

    def _bowtie(self, oid):
        # A crossing ring: closed and inside coverage but invalid under OGC,
        # mirroring the producer-published self-intersections (OIDs 24, 86, 92).
        x0 = -93.5 + (oid % 100) * 0.003
        y0 = 35.0 + (oid // 100) * 0.003
        return [[x0, y0], [x0, y0 + 0.004], [x0 + 0.002, y0 + 0.004],
                [x0 + 0.002, y0 + 0.002], [x0 + 0.002, y0 + 0.004],
                [x0 + 0.004, y0 + 0.004], [x0 + 0.004, y0], [x0, y0]]

    def _rings_for(self, oid):
        if oid in source.EXPECTED_GEOMETRY_INVALID_IDS:
            return [self._bowtie(oid)]
        if oid == 9:
            # A shell with one hole exercises winding and hole ownership.
            shell = self._shell(oid)
            return [shell, self._hole(shell)]
        if oid == 11:
            # Two shells model a multi-part district polygon.
            return [self._shell(oid), self._shell(oid + 100, lon0=-100.0)]
        return [self._shell(oid)]

    def _features(self):
        rows = []
        for index, oid in enumerate(source.EXPECTED_IDS):
            attrs = {"OBJECTID": oid,
                     "NAME": DISTRICT_NAMES[index % len(DISTRICT_NAMES)],
                     "ABV": ABBREVIATIONS[index % len(ABBREVIATIONS)],
                     "DistName": SHORT_NAMES[index % len(SHORT_NAMES)],
                     "State": STATES[index % len(STATES)] if oid != 95 else None,
                     "Circuit": "XX" if oid == 95 else CIRCUITS[index % 12],
                     "Shape_Leng": 12.5 if oid not in (94, 95) else None,
                     "sqmi": 1000 + oid if oid != 95 else None,
                     "Shape__Area": None, "Shape__Length": None}
            feature = {"attributes": attrs}
            if oid in source.EXPECTED_NULL_GEOMETRY_IDS:
                rows.append(feature)
                continue
            rings = self._rings_for(oid)
            decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
            attrs["Shape__Area"] = decoded.area
            attrs["Shape__Length"] = decoded.length
            feature["geometry"] = {"rings": rings}
            rows.append(feature)
        return rows

    def _documents(self):
        sr = {"wkid": 4326, "latestWkid": 4326}
        service = {"serviceItemId": source.CATALOG_ITEM_ID,
                   "serviceDescription": source.SERVICE_DESCRIPTION,
                   "capabilities": source.SERVICE_CAPABILITIES,
                   "maxRecordCount": source.SERVICE_MAX_RECORD_COUNT,
                   "hasVersionedData": False, "supportedQueryFormats": "JSON",
                   "spatialReference": sr,
                   "fullExtent": {**source.SERVICE_FULL_EXTENT, "spatialReference": sr},
                   "layers": [{"id": 5, "name": source.LAYER_NAME, "parentLayerId": -1,
                               "subLayerIds": None, "type": "Feature Layer",
                               "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 5, "name": source.LAYER_NAME, "type": "Feature Layer",
                 "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": source.LAYER_MAX_RECORD_COUNT,
                 "objectIdField": "OBJECTID", "globalIdField": "", "typeIdField": "",
                 "displayField": source.LAYER_DISPLAY_FIELD,
                 "hasZ": False, "hasM": False, "enableNullGeometry": False,
                 "isDataVersioned": False, "copyrightText": source.LAYER_COPYRIGHT,
                 "serviceItemId": source.CATALOG_ITEM_ID,
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
                             "tags": list(source.ITEM_TAGS),
                             "categories": list(source.ITEM_CATEGORIES),
                             "description": source.ITEM_DESCRIPTION,
                             "licenseInfo": source.ITEM_LICENSE,
                             "extent": [list(source.ITEM_EXTENT[0]), list(source.ITEM_EXTENT[1])]},
            "service_before": service, "layer_before": layer,
            "count_before": {"count": source.EXPECTED_COUNT},
            "ids_before": {"objectIds": source.EXPECTED_IDS},
            "service_after": json.loads(json.dumps(service)),
            "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": source.EXPECTED_COUNT},
            "ids_after": {"objectIds": source.EXPECTED_IDS},
        }
        page_fields = [{key: value for key, value in field.items()
                        if key not in ("nullable", "editable")}
                       for field in source.EXPECTED_FIELD_DECLARATIONS]
        for page_index in range(source.PAGE_COUNT):
            chunk = source.EXPECTED_IDS[page_index * source.PAGE_SIZE:
                                        (page_index + 1) * source.PAGE_SIZE]
            rows = [row for row in self._features() if row["attributes"]["OBJECTID"] in chunk]
            docs[f"page_{page_index:03d}"] = {"objectIdFieldName": "OBJECTID",
                                              "globalIdFieldName": "",
                                              "uniqueIdField": {"name": "OBJECTID",
                                                                "isSystemMaintained": True},
                                              "geometryType": "esriGeometryPolygon",
                                              "geometryProperties": dict(source.EXPECTED_GEOMETRY_PROPERTIES),
                                              "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                                              "fields": page_fields, "features": rows}
        return docs

    def _captures(self, docs):
        return {name: (json.dumps(value, ensure_ascii=False, allow_nan=False).encode(), {"status": 200})
                for name, value in docs.items()}

    def _set_ring(self, docs, oid, rings):
        """Replace one feature's rings and resync its shape-measure parity."""
        feature = next(row for row in docs["page_000"]["features"]
                       if row["attributes"]["OBJECTID"] == oid)
        feature["geometry"] = {"rings": rings}
        decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
        feature["attributes"]["Shape__Area"] = decoded.area
        feature["attributes"]["Shape__Length"] = decoded.length

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(us_district_court_jurisdictions, hifld_ingest_assets)
        self.assertEqual(us_district_court_jurisdictions_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)
        check = us_district_court_jurisdictions_complete(
            {"feature_count": 95, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = us_district_court_jurisdictions_complete(
            {"feature_count": 94, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_capture_roster_single_page_covers_source_ids(self):
        from urllib.parse import parse_qs, urlsplit
        urls = dict(source._capture_roster(source.EXPECTED_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         ["page_000"])
        params = parse_qs(urlsplit(urls["page_000"]).query)
        self.assertEqual([int(value) for value in params["objectIds"][0].split(",")],
                         source.EXPECTED_IDS)
        self.assertEqual(params["outSR"], ["4326"])
        self.assertEqual(params["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(params["returnZ"], ["false"])
        self.assertEqual(params["returnM"], ["false"])
        self.assertEqual(params["outFields"], ["*"])
        self.assertEqual(source.CATALOG_ITEM_URL, "https://www.arcgis.com/sharing/rest/content/"
                         "items/dca3851a95ba4cc2a1fb19ab5dff1f5d?f=json")
        self.assertEqual(source.LAYER_URL, "https://services9.arcgis.com/LJKafwRzLHEkKeOL/"
                         "arcgis/rest/services/USDistricts_Project/FeatureServer/5")

    def test_source_pins_match_researched_producer_publication(self):
        self.assertEqual(source.DATASET_SLUG, "us-district-court-jurisdictions")
        self.assertEqual(source.FILE_SLUG, "us-district-court-jurisdictions")
        self.assertEqual(source.PUBLISHER, "Administrative Office of the U.S. Courts (AOUSC)")
        self.assertEqual(source.CATALOG_ORG_NAME, "Judicial Branch (AOUSC)")
        self.assertEqual(source.CATALOG_ORG_ID, "LJKafwRzLHEkKeOL")
        self.assertEqual(source.CATALOG_ITEM_OWNER, "Courts_GIS_AOUSC")
        self.assertEqual(source.EXPECTED_COUNT, 95)
        self.assertEqual(source.EXPECTED_IDS, list(range(1, 96)))
        self.assertEqual(source.LAYER_ID, 5)
        self.assertEqual(source.LAYER_NAME, "TMB U.S. Courts Districts Final")
        self.assertEqual(source.ITEM_TITLE, "TMB U.S. Courts Districts Final")
        self.assertEqual(source.LAYER_DISPLAY_FIELD, "NAME")
        self.assertEqual(source.SERVICE_CAPABILITIES, "Query,Extract")
        self.assertEqual(source.EXPECTED_CIRCUIT_ROSTER, CIRCUITS)
        self.assertEqual(source.EXPECTED_NULL_GEOMETRY_IDS, [95])
        self.assertEqual(source.EXPECTED_GEOMETRY_INVALID_IDS, [24, 86, 92])
        self.assertEqual(source.EXPECTED_NULL_ATTRIBUTE_IDS,
                         {"NAME": [], "ABV": [], "DistName": [], "State": [95],
                          "Circuit": [], "Shape_Leng": [94, 95], "sqmi": [95],
                          "Shape__Area": [95], "Shape__Length": [95]})
        self.assertEqual(source.EXPECTED_LAST_DATA_EDIT_UTC, "2026-03-24T16:54:26.262000Z")
        self.assertEqual(source.EXPECTED_LAST_EDIT_UTC, "2026-04-16T15:16:10.444000Z")
        self.assertEqual(source.EXPECTED_ID_SHA256,
                         "04fd325cb6bf5cbb4e33c02b3f5297be13f8acc9a4ab0508290b482ba51bc14f")
        self.assertEqual(source._sha(_canonical(sorted(DISTRICT_NAMES))),
                         source.EXPECTED_DISTRICT_NAME_SHA256)
        self.assertEqual(source._sha(_canonical(sorted(ABBREVIATIONS))),
                         source.EXPECTED_ABBREVIATION_SHA256)
        self.assertEqual(source._sha(_canonical(sorted(SHORT_NAMES))),
                         source.EXPECTED_DISTRICT_SHORT_NAME_SHA256)
        self.assertEqual(source._sha(_canonical(sorted(STATES))),
                         source.EXPECTED_STATE_SHA256)
        self.assertEqual(source._sha(_canonical(sorted(CIRCUITS))),
                         source.EXPECTED_CIRCUIT_SHA256)
        self.assertEqual(source._sha(_canonical(source.EXPECTED_FIELD_DECLARATIONS)),
                         "47595a72bd3ef39fb36b13f93d9ad9fd9e1a7b51e7ca6ec31efd5fe0fcce75dd")
        self.assertIn("dca3851a95ba4cc2a1fb19ab5dff1f5d", source.LANDING_URL)
        self.assertIn("XXX", DISTRICT_NAMES)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), 95)
        self.assertEqual(sorted(row["attributes"]["OBJECTID"] for row in features),
                         source.EXPECTED_IDS)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 95)
        self.assertEqual(audit["geometry_valid_count"], 91)
        self.assertEqual(audit["geometry_null_count"], 1)
        self.assertEqual(audit["geometry_null_ids"], [95])
        self.assertEqual(audit["geometry_invalid_count"], 3)
        self.assertEqual(audit["geometry_invalid_ids"], [24, 86, 92])
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["source_last_data_edit_utc"], "2026-03-24T16:54:26.262000Z")
        self.assertEqual(audit["dhs_crosswalk_row"]["status"], "Deprecate")
        self.assertEqual(audit["dhs_crosswalk_row"]["agency"], "Federal Judicial Center")
        self.assertIn("Administrative Office of the U.S. Courts", audit["source_coverage_scope"])
        self.assertIn("LJKafwRzLHEkKeOL", audit["catalog_item_org"])
        self.assertEqual(audit["source_record_sha256"],
                         source._sha(_canonical(
                             [[row["attributes"]["OBJECTID"], row["attributes"]] for row in features])))

    def test_candidate_parquet_preserves_fields_ids_and_null_geometry(self):
        fields, features, _audit = source._validate(self._captures(self._documents()))
        parquet = source._parquet_bytes(fields, features)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertEqual(table.column_names,
                         ["OBJECTID", "NAME", "ABV", "DistName", "State", "Circuit",
                          "Shape_Leng", "sqmi", "Shape__Area", "Shape__Length", "geometry"])
        self.assertEqual(str(table.schema.field("sqmi").type), "int64")
        self.assertEqual(str(table.schema.field("State").type), "string")
        self.assertEqual(table.num_rows, 95)
        geometry_column = table.column("geometry").to_pylist()
        self.assertIsNone(geometry_column[94])
        self.assertEqual(len([value for value in geometry_column if value is not None]), 94)
        oid_column = table.column("OBJECTID").to_pylist()
        self.assertEqual(oid_column, source.EXPECTED_IDS)
        state_column = table.column("State").to_pylist()
        self.assertIsNone(state_column[94])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertEqual(geo["columns"]["geometry"]["encoding"], "WKB")
        for value in geometry_column:
            if value is None:
                continue
            geom = from_wkb(value)
            self.assertIn(geom.geom_type, ("Polygon", "MultiPolygon"))

    def test_wkb_is_xy_only_polygon_or_multipolygon(self):
        single = source._esri_polygon_wkb({"rings": [self._shell(1)]})
        self.assertEqual(struct.unpack("<BI", single[:5]), (1, 3))
        west = [[-180.0, 19.0], [-180.0, 20.0], [-179.9, 20.0], [-179.9, 19.0], [-180.0, 19.0]]
        east = [[179.9, 19.0], [179.9, 20.0], [179.9999, 20.0], [179.9999, 19.0], [179.9, 19.0]]
        dateline = source._esri_polygon_wkb({"rings": [west, east]})
        self.assertEqual(struct.unpack("<BI", dateline[:5]), (1, 6))
        with_hole = source._esri_polygon_wkb({"rings": [self._shell(1), self._hole(self._shell(1))]})
        self.assertEqual(struct.unpack("<BI", with_hole[:5]), (1, 3))
        for flags in ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True}):
            with self.assertRaisesRegex(ValueError, "source_polygon_dimensional_flags_rejected"):
                source._esri_polygon_wkb({"rings": [self._shell(1)], **flags})

    def test_population_schema_id_roster_geometry_and_contract_drift_fail_closed(self):
        cases = [
            ("population after", lambda d: d["ids_after"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_id_population_drift:after"),
            ("id roster gap", lambda d: d["ids_before"].update(objectIds=source.EXPECTED_IDS[:-1]),
             "source_id_population_drift:before"),
            ("count before", lambda d: d["count_before"].update(count=94),
             "source_count_population_drift:before"),
            ("schema rename", lambda d: d["layer_before"]["fields"][1].update(name="renamed"),
             "source_field_schema_drift"),
            ("schema type change", lambda d: d["layer_before"]["fields"][1].update(type="esriFieldTypeInteger"),
             "source_field_schema_drift"),
            ("unexpected column", lambda d: d["layer_before"]["fields"].append(
                 {"name": "EXTRA", "type": "esriFieldTypeString", "alias": "EXTRA",
                  "sqlType": "sqlTypeOther", "length": 5, "nullable": True, "editable": True,
                  "domain": None, "defaultValue": None}),
             "source_field_schema_drift"),
            ("page unexpected column", lambda d: d["page_000"]["fields"].append(
                 {"name": "EXTRA", "type": "esriFieldTypeString", "alias": "EXTRA",
                  "sqlType": "sqlTypeOther", "domain": None, "defaultValue": None}),
             "source_page_physical_schema_changed"),
            ("transfer", lambda d: d["page_000"].update(exceededTransferLimit=True),
             "source_page_error_or_transfer_limit"),
            ("page error", lambda d: d["page_000"].update(error={"code": 400, "message": "x"}),
             "source_page_error_or_transfer_limit"),
            ("CRS", lambda d: d["page_000"].update(spatialReference={"wkid": 3857}),
             "source_page_000_page_crs_drift"),
            ("page ids", lambda d: d["page_000"]["features"][1]["attributes"].update(
                OBJECTID=d["page_000"]["features"][0]["attributes"]["OBJECTID"]),
             "source_page_id_population_drift"),
            ("layer dimensional", lambda d: d["layer_before"].update(hasZ=True),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer freshness", lambda d: d["layer_before"]["editingInfo"].update(dataLastEditDate=1774371266264),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer extent", lambda d: d["layer_before"]["extent"].update(xmax=0.0),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer copyright", lambda d: d["layer_before"].update(copyrightText="someone else"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer display field", lambda d: d["layer_before"].update(displayField="Circuit"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("layer service item", lambda d: d["layer_before"].update(serviceItemId="other"),
             "source_layer_identity_extent_or_freshness_drift"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("page identity", lambda d: d["page_000"].update(objectIdFieldName="FID"),
             "source_page_identity_drift"),
            ("page unique field", lambda d: d["page_000"].update(
                uniqueIdField={"name": "OBJECTID", "isSystemMaintained": False}),
             "source_page_identity_drift"),
            ("page geometry properties", lambda d: d["page_000"].update(
                geometryProperties={"units": "esriMeters"}),
             "source_page_geometry_properties_drift"),
            ("page field drift", lambda d: d["page_000"]["fields"][1].update(name="renamed"),
             "source_page_physical_schema_changed"),
            ("geometry dimensional", lambda d: (
                d["page_000"]["features"][0]["geometry"].update(hasZ=True),
                d["page_000"]["features"][0]["attributes"].update(Shape__Area=None, Shape__Length=None)),
             "source_geometry_dimensional_drift"),
            ("open ring", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].pop(),
             "source_polygon_ring_not_closed"),
            ("outside coverage", lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                2, [-185.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
            ("hole without shell", lambda d: d["page_000"]["features"][8]["geometry"].update(
                rings=[self._hole(self._shell(99))]),
             "source_polygon_shell_missing"),
            ("item identity", lambda d: d["catalog_item"].update(owner="someone_else"),
             "aousc_catalog_item_identity_drift"),
            ("item org", lambda d: d["catalog_item"].update(orgId="otherorg"),
             "aousc_catalog_item_identity_drift"),
            ("item access", lambda d: d["catalog_item"].update(access="private"),
             "aousc_catalog_item_identity_drift"),
            ("item accessInformation", lambda d: d["catalog_item"].update(accessInformation="Esri"),
             "aousc_catalog_item_identity_drift"),
            ("item license text", lambda d: d["catalog_item"].update(licenseInfo="changed"),
             "aousc_catalog_item_text_drift"),
            ("item description text", lambda d: d["catalog_item"].update(description="changed"),
             "aousc_catalog_item_text_drift"),
            ("item snippet", lambda d: d["catalog_item"].update(snippet="changed"),
             "aousc_catalog_item_text_drift"),
            ("item tags", lambda d: d["catalog_item"].update(tags=["Other"]),
             "aousc_catalog_item_text_drift"),
            ("item category", lambda d: d["catalog_item"].update(categories=["/Categories/Other"]),
             "aousc_catalog_item_text_drift"),
            ("item extent", lambda d: d["catalog_item"].update(extent=[[0.0, 0.0], [1.0, 1.0]]),
             "aousc_catalog_item_extent_drift"),
            ("service identity", lambda d: d["service_before"].update(serviceItemId="other"),
             "service_before_service_identity_drift"),
            ("service capabilities", lambda d: d["service_before"].update(capabilities="Query"),
             "service_before_service_identity_drift"),
            ("service roster", lambda d: d["service_before"]["layers"].append(
                 {"id": 1, "name": "Extra", "parentLayerId": -1, "subLayerIds": None,
                  "type": "Feature Layer", "geometryType": "esriGeometryPolygon"}),
             "service_before_service_layer_roster_drift"),
            ("service changed during capture", lambda d: d["service_after"].update(maxRecordCount=100),
             "source_service_or_layer_changed_during_capture"),
            ("layer changed during capture", lambda d: d["layer_after"]["fields"][1].update(name="renamed"),
             "source_service_or_layer_changed_during_capture"),
            ("string value type", lambda d: d["page_000"]["features"][0]["attributes"].update(Circuit=5),
             "source_string_value_type_drift:Circuit"),
            ("integer value type", lambda d: d["page_000"]["features"][0]["attributes"].update(sqmi="x"),
             "source_integer_value_type_drift:sqmi"),
            ("double value type", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area="x"),
             "source_double_value_type_drift:Shape__Area"),
            ("area parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Area=1.5),
             "source_shape_area_parity_drift"),
            ("length parity", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape__Length=1.5),
             "source_shape_length_parity_drift"),
            ("attribute order", lambda d: d["page_000"]["features"][0]["attributes"].pop("sqmi"),
             "source_attribute_roster_or_order_drift"),
            ("oid value", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=0),
             "source_oid_value_type_drift"),
            ("oid roster", lambda d: d["page_000"]["features"][0]["attributes"].update(OBJECTID=99),
             "source_feature_id_roster_drift"),
            ("district name roster", lambda d: d["page_000"]["features"][0]["attributes"].update(
                NAME=d["page_000"]["features"][1]["attributes"]["NAME"]),
             "source_district_name_roster_drift"),
            ("abbreviation roster", lambda d: d["page_000"]["features"][0]["attributes"].update(
                ABV="ZZZ"),
             "source_abbreviation_roster_drift"),
            ("short name roster", lambda d: d["page_000"]["features"][0]["attributes"].update(
                DistName="Not A District"),
             "source_district_short_name_roster_drift"),
            ("state roster", lambda d: d["page_000"]["features"][0]["attributes"].update(State="Atlantis"),
             "source_state_roster_drift"),
            ("circuit roster", lambda d: d["page_000"]["features"][0]["attributes"].update(Circuit="99"),
             "source_circuit_roster_drift"),
            ("state null on real row", lambda d: d["page_000"]["features"][0]["attributes"].update(State=None),
             "source_null_attribute_roster_drift:State"),
            ("state filled on placeholder", lambda d: d["page_000"]["features"][94]["attributes"].update(State="Guam"),
             "source_null_attribute_roster_drift:State"),
            ("shape leng null drift", lambda d: d["page_000"]["features"][0]["attributes"].update(Shape_Leng=None),
             "source_null_attribute_roster_drift:Shape_Leng"),
            ("sqmi filled on placeholder", lambda d: d["page_000"]["features"][94]["attributes"].update(sqmi=1),
             "source_null_attribute_roster_drift:sqmi"),
            ("null geometry roster", lambda d: d["page_000"]["features"][0].pop("geometry"),
             "source_null_geometry_roster_drift"),
            ("null geometry gained", lambda d: d["page_000"]["features"][94].update(
                geometry={"rings": [self._shell(95)]}),
             "source_null_geometry_roster_drift"),
            ("invalid geometry roster", lambda d: self._set_ring(
                d, 1, self._rings_for(24)),
             "source_geometry_invalid_roster_drift"),
            ("invalid geometry repaired", lambda d: self._set_ring(
                d, 24, self._rings_for(1)),
             "source_geometry_invalid_roster_drift"),
            ("capture roster", lambda d: d.pop("service_after"),
             "source_capture_name_roster_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_producer_invalid_geometry_and_placeholder_are_retained(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        by_oid = {row["attributes"]["OBJECTID"]: row for row in features}
        for oid in (24, 86, 92):
            self.assertFalse(from_wkb(source._esri_polygon_wkb(by_oid[oid]["geometry"])).is_valid)
        self.assertIsNone(by_oid[95].get("geometry"))
        self.assertEqual(by_oid[95]["attributes"]["Circuit"], "XX")
        self.assertEqual(by_oid[95]["attributes"]["NAME"], "XXX")
        self.assertIn("never repaired", audit["geometry_invalid_note"])

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_us_district_court_jurisdictions(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
