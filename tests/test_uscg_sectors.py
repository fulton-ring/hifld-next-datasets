import hashlib
import io
import json
import os
import struct
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
from dagster import AssetCheckSeverity, DefaultScheduleStatus
from shapely import from_wkb

from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.uscg_sectors import (
    uscg_sectors, uscg_sectors_complete, uscg_sectors_draft_schedule, uscg_sectors_drift,
)
from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.http_capture import capture_roster_digests
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import uscg_sectors as source
from dagster_hifld.sources.esri_common import canonical as _canonical

# Small gapped fixture roster for speed: IDs run 1..31 with 3, 6, 8..15,
# 17..23 and 25..30 absent, so page planning and coverage must tolerate gaps
# without any contiguity assumption. FID 16 carries the source-style
# OGC-invalid ring, FID 7 the dateline-split shell pair, FID 31 a hole.
FIXTURE_IDS = [1, 2, 4, 5, 7, 16, 24, 31]
FIXTURE_EDITING_INFO = {"lastEditDate": 1774374531856, "schemaLastEditDate": 1774374529592,
                        "dataLastEditDate": 1769797781779}
FIXTURE_GEOMETRY_PROPERTIES = {"shapeAreaFieldName": "Shape__Area",
                               "shapeLengthFieldName": "Shape__Length",
                               "units": "esriDecimalDegrees"}
FIXTURE_EXTENT = {"xmin": -180.0, "ymin": 0.0, "xmax": 180.0, "ymax": 36.0}
PATCH = dict(PAGE_SIZE=2)


class UscgSectorsTests(unittest.TestCase):
    def setUp(self):
        self._patches = [patch.object(source, name, value) for name, value in PATCH.items()]
        for item in self._patches:
            item.start()
            self.addCleanup(item.stop)

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

    def _spike(self):
        # A clockwise zero-area spike: OGC-invalid like the source's
        # Sector New York ring self-intersection, and minimally repairable.
        return [[[0.0, 0.0], [0.0, 4.0], [2.0, 4.0], [2.0, 2.0],
                 [2.0, 4.0], [4.0, 4.0], [4.0, 0.0], [0.0, 0.0]]]

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
            return self._spike()
        return [self._shell(oid)]

    def _set_rings(self, feature, rings):
        """Replace a feature's rings and resync the shape parity attributes."""
        feature["geometry"]["rings"] = rings
        decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
        feature["attributes"]["Shape__Area"] = decoded.area
        feature["attributes"]["Shape__Length"] = decoded.length

    def _feature(self, oid):
        rings = self._rings_for(oid)
        decoded = from_wkb(source._esri_polygon_wkb({"rings": rings}))
        attrs = {}
        for field in source.ATTRIBUTE_FIELDS:
            name, kind = field["name"], field["type"]
            if name == "FID":
                attrs[name] = oid
            elif name == "SectorName":
                attrs[name] = f"SECTOR-{oid}"
            elif name == "Shape__Area":
                attrs[name] = decoded.area
            elif name == "Shape__Length":
                attrs[name] = decoded.length
            elif kind == "esriFieldTypeString":
                attrs[name] = f"{name}-{oid}"
            else:
                attrs[name] = float(oid)
        return {"attributes": attrs, "geometry": {"rings": rings}}

    def _documents(self, ids=None):
        ids = list(FIXTURE_IDS if ids is None else ids)
        sr = {"wkid": 4326, "latestWkid": 4326}
        service = {"serviceItemId": source.CATALOG_ITEM_ID, "description": "",
                   "serviceDescription": "", "capabilities": "Query,Extract",
                   "maxRecordCount": source.SERVICE_MAX_RECORD_COUNT,
                   "hasVersionedData": False, "isView": True, "spatialReference": sr,
                   "fullExtent": {**FIXTURE_EXTENT, "spatialReference": sr},
                   "layers": [{"id": 0, "name": source.LAYER_NAME, "parentLayerId": -1,
                               "subLayerIds": None, "type": "Feature Layer",
                               "geometryType": "esriGeometryPolygon"}]}
        layer = {"id": 0, "name": source.LAYER_NAME, "geometryType": "esriGeometryPolygon",
                 "maxRecordCount": source.LAYER_MAX_RECORD_COUNT, "objectIdField": "FID",
                 "globalIdField": "", "typeIdField": "SectorName", "displayField": "NAME",
                 "hasZ": False, "hasM": False, "isView": True, "enableNullGeometry": False,
                 "copyrightText": source.LAYER_COPYRIGHT,
                 "editingInfo": dict(FIXTURE_EDITING_INFO),
                 "geometryProperties": dict(FIXTURE_GEOMETRY_PROPERTIES),
                 "spatialReference": sr,
                 "extent": {**FIXTURE_EXTENT, "spatialReference": sr},
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
            "count_before": {"count": len(ids)},
            "ids_before": {"objectIds": list(ids)},
            "service_after": json.loads(json.dumps(service)), "layer_after": json.loads(json.dumps(layer)),
            "count_after": {"count": len(ids)},
            "ids_after": {"objectIds": list(ids)},
        }
        page_fields = [{key: value for key, value in field.items()
                        if key not in ("nullable", "editable", "isViewOverride")}
                       for field in source.EXPECTED_FIELD_DECLARATIONS]
        for page_index in range((len(ids) + source.PAGE_SIZE - 1) // source.PAGE_SIZE):
            chunk = ids[page_index * source.PAGE_SIZE:(page_index + 1) * source.PAGE_SIZE]
            docs[f"page_{page_index:03d}"] = {"objectIdFieldName": "FID",
                                              "geometryType": "esriGeometryPolygon",
                                              "geometryProperties": dict(FIXTURE_GEOMETRY_PROPERTIES),
                                              "spatialReference": sr,
                                              "fields": page_fields,
                                              "features": [self._feature(oid) for oid in chunk]}
        return docs

    def _captures(self, docs):
        # Plain JSON, not canonical: the page attribute order is a checked
        # contract, so fixture bodies keep the producer's field order.
        return {name: (json.dumps(value, ensure_ascii=False, allow_nan=False).encode(),
                       {"status": 200}) for name, value in docs.items()}

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(uscg_sectors, hifld_ingest_assets)
        self.assertEqual(uscg_sectors_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = uscg_sectors_complete({"feature_count": len(FIXTURE_IDS),
                                       "source_reported_count": len(FIXTURE_IDS),
                                       "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertEqual(check.metadata["source_reported_count"].value, len(FIXTURE_IDS))
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = uscg_sectors_complete({"feature_count": len(FIXTURE_IDS) - 1,
                                        "source_reported_count": len(FIXTURE_IDS),
                                        "source_population_complete": True})
        self.assertFalse(failed.passed)
        missing = uscg_sectors_complete({"feature_count": len(FIXTURE_IDS),
                                         "source_population_complete": True})
        self.assertFalse(missing.passed)
        drift = uscg_sectors_drift({"feature_count": len(FIXTURE_IDS),
                                    "source_reported_count": len(FIXTURE_IDS),
                                    "source_population_complete": True})
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        self.assertFalse(list(uscg_sectors_drift.check_specs)[0].blocking)

    def test_capture_roster_pages_cover_source_ids_without_overlapping(self):
        from urllib.parse import parse_qs, urlsplit
        urls = dict(source._capture_roster(FIXTURE_IDS))
        self.assertEqual([name for name in sorted(urls) if name.startswith("page_")],
                         [f"page_{index:03d}" for index in range(4)])
        covered = []
        for index in range(4):
            params = parse_qs(urlsplit(urls[f"page_{index:03d}"]).query)
            page_ids = [int(value) for value in params["objectIds"][0].split(",")]
            self.assertEqual(page_ids,
                             FIXTURE_IDS[index * source.PAGE_SIZE:(index + 1) * source.PAGE_SIZE])
            self.assertEqual(params["outSR"], ["4326"])
            self.assertEqual(params["orderByFields"], ["FID ASC"])
            self.assertEqual(params["returnZ"], ["true"])
            self.assertEqual(params["returnM"], ["true"])
            self.assertEqual(params["outFields"], ["*"])
            covered.extend(page_ids)
        self.assertEqual(sorted(covered), FIXTURE_IDS)
        self.assertNotIn(3, covered)
        self.assertEqual(source.CATALOG_ITEM_URL, "https://www.arcgis.com/sharing/rest/content/items/"
                          "ec3a1f03ec4548bb9d4435ea7e580c1a?f=json")
        self.assertEqual(source.LAYER_URL, "https://services9.arcgis.com/Fr5UUTVwNRJlXAkf/"
                          "arcgis/rest/services/USCG_Sectors_view/FeatureServer/0")

    def test_source_identity_matches_researched_producer_publication(self):
        self.assertEqual(source.DATASET_SLUG, "uscg-sectors")
        self.assertEqual(source.PUBLISHER, "United States Coast Guard (USCG)")
        self.assertEqual(source.CATALOG_ITEM_OWNER, "Ian.S.Hanna_uscoastguard")
        self.assertEqual(source.CATALOG_ORG_ID, "Fr5UUTVwNRJlXAkf")
        self.assertEqual(source.CATALOG_ORG_NAME, "USCG FSLTP Portal")
        self.assertIn("Operation Systems Center (OSC), United States Coast Guard (USCG)",
                      source.LAYER_COPYRIGHT)
        self.assertIn("ec3a1f03ec4548bb9d4435ea7e580c1a", source.LANDING_URL)

    def test_complete_native_population_and_geometry_are_validated(self):
        fields, features, audit = source._validate(self._captures(self._documents()))
        self.assertEqual(fields, source.EXPECTED_FIELD_DECLARATIONS)
        self.assertEqual(len(features), len(FIXTURE_IDS))
        self.assertEqual(sorted(row["attributes"]["FID"] for row in features), FIXTURE_IDS)
        self.assertEqual(audit["source_feature_count"], len(FIXTURE_IDS))
        self.assertEqual(audit["source_id_sha256"], source._sha(_canonical(FIXTURE_IDS)))
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 31)
        self.assertEqual(audit["source_id_gaps"],
                         [3, 6, *range(8, 16), *range(17, 24), *range(25, 31)])
        self.assertEqual(audit["source_sector_name_sha256"],
                         source._sha(_canonical(sorted(f"SECTOR-{oid}" for oid in FIXTURE_IDS))))
        self.assertEqual(audit["source_sector_name_distinct_count"], len(FIXTURE_IDS))
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(audit["source_vertex_count"], 58)
        self.assertEqual(audit["geometry_valid_count"], 7)
        self.assertEqual(audit["geometry_invalid_count"], 1)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["geometry_invalid_ids"], [16])
        self.assertEqual(audit["source_extent"], {**FIXTURE_EXTENT, "spatialReference":
                                                  {"wkid": 4326, "latestWkid": 4326}})
        self.assertEqual(audit["source_editing_info"], FIXTURE_EDITING_INFO)
        self.assertEqual(audit["source_last_data_edit_utc"], "2026-01-30T18:29:41.779000Z")
        self.assertEqual(audit["source_last_schema_edit_utc"], "2026-03-24T17:48:49.592000Z")
        self.assertEqual(audit["source_geometry_properties"], FIXTURE_GEOMETRY_PROPERTIES)
        check = audit["source_identity_check"]
        self.assertEqual(check["observed_owner"], source.CATALOG_ITEM_OWNER)
        self.assertEqual(check["observed_org_id"], source.CATALOG_ORG_ID)
        self.assertEqual(check["org_name"], "USCG FSLTP Portal")
        self.assertEqual(audit["dhs_crosswalk_row"]["status"], "Migrated")
        self.assertIn("USCG-owned", audit["source_coverage_scope"])
        self.assertIn(source.CATALOG_ORG_ID, audit["catalog_item_org"])

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

    def test_upstream_added_feature_uses_capture_witness_and_warns(self):
        new_ids = [*FIXTURE_IDS, 33]
        docs = self._documents(new_ids)
        fields, features, audit = source._validate(self._captures(docs))
        self.assertEqual(len(features), len(new_ids))
        self.assertEqual(audit["source_feature_count"], len(new_ids))
        self.assertEqual(audit["source_id_max"], 33)
        self.assertEqual(audit["source_id_sha256"], source._sha(_canonical(new_ids)))
        candidate = {"dataset_slug": source.DATASET_SLUG, "file_slug": source.FILE_SLUG,
                     "feature_count": len(new_ids), "source_reported_count": len(new_ids),
                     "source_population_complete": True,
                     "source_id_sha256": source._sha(_canonical(new_ids))}
        self.assertTrue(uscg_sectors_complete(candidate).passed)
        drift = uscg_sectors_drift(candidate)
        self.assertFalse(drift.passed)
        self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
        self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
        baseline = {**candidate, "version": "v-approved", "release_accepted": True,
                    "feature_count": len(FIXTURE_IDS),
                    "source_id_sha256": source._sha(_canonical(FIXTURE_IDS))}
        compared = approved_baseline_drift(candidate, baseline)
        self.assertFalse(compared.passed)
        self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
        self.assertEqual(compared.metadata["comparison_status"].value, "compared_approved_release")
        self.assertEqual(compared.metadata["feature_count_delta"].value, 1)
        self.assertTrue(compared.metadata["native_id_roster_changed"].value)
        table = pq.read_table(io.BytesIO(source._parquet_bytes(fields, features,
                                                               {row["attributes"]["FID"]:
                                                                source._esri_polygon_wkb(
                                                                    row["geometry"])
                                                                for row in features})))
        self.assertEqual(table.num_rows, len(new_ids))

    def test_newly_broken_source_geometry_is_repaired_and_recorded_not_rejected(self):
        docs = self._documents()
        self._set_rings(next(row for row in docs["page_000"]["features"]
                             if row["attributes"]["FID"] == 1), self._spike())
        _fields, features, audit = source._validate(self._captures(docs))
        self.assertEqual(len(features), len(FIXTURE_IDS))
        self.assertEqual(audit["geometry_invalid_count"], 2)
        self.assertEqual(audit["geometry_invalid_ids"], [1, 16])
        repaired = []
        for row in features:
            wkb, record = source._candidate_geometry_wkb(row["attributes"]["FID"], row["geometry"])
            if record is not None:
                repaired.append(record["feature_id"])
                self.assertTrue(from_wkb(wkb).is_valid)
        self.assertEqual(repaired, [1, 16])

    def test_torn_duplicate_and_missing_capture_witnesses_are_refused(self):
        cases = [
            ("same count changed IDs", lambda d: d["ids_after"]["objectIds"].__setitem__(-1, 35),
             "source_count_or_id_roster_drift"),
            ("count changed", lambda d: d["count_after"].update(count=len(FIXTURE_IDS) + 1),
             "source_count_or_id_roster_drift"),
            ("duplicate IDs", lambda d: d["ids_before"]["objectIds"].__setitem__(-1, FIXTURE_IDS[0]),
             "source_count_or_id_roster_drift"),
            ("added between witnesses", lambda d: [
                d["ids_after"]["objectIds"].append(33),
                d["count_after"].update(count=len(FIXTURE_IDS) + 1)],
             "source_count_or_id_roster_drift"),
            ("gap filled upstream", lambda d: [
                d["ids_before"]["objectIds"].__setitem__(slice(None), list(range(1, 32))),
                d["count_before"].update(count=31)],
             "source_count_or_id_roster_drift"),
            ("missing witness", lambda d: d.pop("count_after"), "source_capture_witness_missing"),
            ("missing page", lambda d: d.pop("page_000"), "source_capture_name_roster_drift"),
            ("omitted feature", lambda d: d["page_000"]["features"].pop(),
             "source_page_population_incomplete"),
            ("empty population", lambda d: [
                (d[f"ids_{side}"].update(objectIds=[]), d[f"count_{side}"].update(count=0))
                for side in ("before", "after")], "source_population_empty"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_population_schema_transfer_limit_and_crs_drift_fail_closed(self):
        cases = [
            ("population after", lambda d: d["ids_after"].update(objectIds=FIXTURE_IDS[:-1]),
             "source_count_or_id_roster_drift"),
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
             "source_layer_identity_drift"),
            ("layer extent invalid", lambda d: d["layer_before"]["extent"].update(xmax=-180.0),
             "source_layer_extent_invalid"),
            ("layer editing info", lambda d: d["layer_before"].pop("editingInfo"),
             "source_layer_editing_info_invalid"),
            ("layer copyright", lambda d: d["layer_before"].update(copyrightText="someone else"),
             "source_layer_identity_drift"),
            ("layer view flag", lambda d: d["layer_before"].update(isView=False),
             "source_layer_identity_drift"),
            ("service full extent invalid", lambda d: d["service_before"]["fullExtent"].update(xmax=-180.0),
             "service_before_service_extent_invalid"),
            ("page dimensional", lambda d: d["page_000"].update(hasZ=True),
             "source_page_dimensional_drift"),
            ("page identity", lambda d: d["page_000"].update(objectIdFieldName="OBJECTID"),
             "source_page_identity_drift"),
            ("page geometry properties", lambda d: d["page_000"].update(
                geometryProperties={"units": "esriMeters"}),
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
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                docs = json.loads(json.dumps(self._documents()))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, expected):
                    source._validate(self._captures(docs))

    def test_repair_of_self_touching_ring_is_minimal_and_fail_closed(self):
        from shapely.validation import explain_validity
        spike = self._spike()
        wkb, record = source._candidate_geometry_wkb(16, {"rings": spike})
        fixed = from_wkb(wkb)
        raw = from_wkb(source._esri_polygon_wkb({"rings": spike}))
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
        invalid = []
        for path in sorted(Path(capture_dir).glob("page_*.body")):
            for row in json.loads(path.read_bytes()).get("features", []):
                raw = from_wkb(source._esri_polygon_wkb(row["geometry"]))
                if not raw.is_valid:
                    invalid.append(row)
        self.assertGreaterEqual(len(invalid), 1)
        for row in invalid:
            wkb, record = source._candidate_geometry_wkb(row["attributes"]["FID"], row["geometry"])
            fixed = from_wkb(wkb)
            raw = from_wkb(source._esri_polygon_wkb(row["geometry"]))
            self.assertTrue(fixed.is_valid)
            self.assertAlmostEqual(fixed.area, raw.area, places=12)
            self.assertEqual(fixed.bounds, raw.bounds)
            if row["attributes"]["FID"] == 16:
                self.assertEqual(record["validity_before"],
                                 "Ring Self-intersection[-74.6500029470783 42.0000067959769]")

    def _replay_directory(self, root, docs, mutate=None):
        documents = json.loads(json.dumps(docs))
        if mutate is not None:
            mutate(documents)
        ids = documents["ids_before"]["objectIds"]
        root.mkdir(parents=True)
        roster_urls = dict(source._capture_roster(ids))
        receipts = {}
        for name in sorted(source._capture_names(ids)):
            body = json.dumps(documents[name], ensure_ascii=False, allow_nan=False).encode()
            receipt = {"method": "GET", "url": roster_urls[name], "final_url": roster_urls[name],
                       "observed_at": "2026-11-20T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
                       "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "content_encoding": "identity", "content_length": len(body),
                       "error_code": None, "error_type": None}
            receipts[name] = receipt
            (root / f"{name}.body").write_bytes(body)
            (root / f"{name}.wire").write_bytes(body)
            (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        manifest = {"complete": True, "captured_at": "2026-11-20T00:00:00+00:00",
                    "source_feature_count": len(ids), "receipts": receipts,
                    "artifact_roster": capture_roster_digests(root, exclude={"capture_manifest.json"})}
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")
        return root

    def _rebind_receipt(self, root, name, mutate_url):
        receipt_path = root / f"{name}.receipt.json"
        receipt = json.loads(receipt_path.read_bytes())
        receipt["url"] = receipt["final_url"] = mutate_url(receipt["url"])
        receipt_path.write_bytes(_canonical(receipt) + b"\n")
        manifest = json.loads((root / "capture_manifest.json").read_bytes())
        manifest["receipts"][name] = receipt
        manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
        (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")

    def test_drifted_replay_seals_no_ready_candidate(self):
        cases = [
            ("roster digest", lambda d: d["ids_after"].update(objectIds=FIXTURE_IDS[:-1]),
             "source_count_or_id_roster_drift"),
            ("vertex outside coverage",
             lambda d: d["page_000"]["features"][0]["geometry"]["rings"][0].__setitem__(
                 2, [-185.0, 35.5]),
             "source_geometry_outside_declared_coverage"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                root = self._replay_directory(Path(tmp) / "capture", self._documents(), mutate)
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_uscg_sectors(storage, version="regression",
                                                capture_directory=str(root))
                self.assertEqual(storage.list_prefix(""), [])

    def test_valid_replay_seals_ready_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            root = self._replay_directory(Path(tmp) / "capture", self._documents())
            first = source.capture_uscg_sectors(storage, version="regression",
                                                capture_directory=str(root))
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], len(FIXTURE_IDS))
            self.assertEqual(first["source_reported_count"], len(FIXTURE_IDS))
            self.assertEqual(first["source_id_sha256"], source._sha(_canonical(FIXTURE_IDS)))
            self.assertEqual(first["source_id_min"], 1)
            self.assertEqual(first["source_id_max"], 31)
            self.assertTrue(first["source_population_complete"])
            self.assertFalse(first["release_accepted"])
            ready = json.loads(storage.read_key(first["candidate_ready"]))
            self.assertEqual(ready["version"], first["version"])
            self.assertFalse(ready["approved"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/raw/page_000.body", staged)
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", staged)
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["geometry_valid_count"], 7)
            self.assertEqual(audit["geometry_invalid_count"], 1)
            self.assertEqual(audit["geometry_invalid_ids"], [16])
            self.assertEqual(audit["geometry_repair"]["applied_to_feature_ids"], [16])
            self.assertEqual(audit["source_last_data_edit_utc"], "2026-01-30T18:29:41.779000Z")
            self.assertEqual(audit["source_identity_check"]["observed_owner"],
                             source.CATALOG_ITEM_OWNER)
            self.assertEqual(audit["source_identity_check"]["observed_org_id"],
                             source.CATALOG_ORG_ID)
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertEqual(manifest["source_modified"], "2026-01-30T18:29:41.779000Z")
            self.assertEqual(manifest["source_identity_check"]["observed_owner"],
                             source.CATALOG_ITEM_OWNER)
            self.assertEqual(manifest["source_identity_check"]["observed_org_id"],
                             source.CATALOG_ORG_ID)
            self.assertEqual(manifest["source_identity_check"]["org_name"], "USCG FSLTP Portal")
            self.assertFalse(manifest["approved"])
            # A cached candidate must still report the verified ID hash of the
            # retained capture.
            second = source.capture_uscg_sectors(storage, version="regression",
                                                 capture_directory=str(root))
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual(second["source_id_sha256"], source._sha(_canonical(FIXTURE_IDS)))

    def _capture_queues(self, docs):
        ids = docs["ids_before"]["objectIds"]
        queues = {}
        for name, url in source._capture_roster(ids):
            queues.setdefault(url, []).append(name)
        return ids, queues

    def _healthy_handler(self, docs, queues):
        def handle(request):
            name = queues[str(request.url)].pop(0)
            body = json.dumps(docs[name], ensure_ascii=False, allow_nan=False).encode()
            return httpx.Response(200, stream=httpx.ByteStream(body),
                                  headers={"Content-Type": "application/json"})
        return handle

    def test_dynamic_acquisition_replay_and_transport_integrity(self):
        docs = self._documents()
        ids, _queues = self._capture_queues(docs)
        with tempfile.TemporaryDirectory() as temporary:
            storage = StagingStorageResource(local_dir=str(Path(temporary) / "stage"))

            def make_handler(documents, q):
                return self._healthy_handler(documents, q)

            with patch.object(source.httpx, "Client",
                              return_value=httpx.Client(transport=httpx.MockTransport(
                                  make_handler(docs, self._capture_queues(docs)[1])))):
                result = source.capture_uscg_sectors(storage, version="fixture-capture",
                                                     capture_directory=str(Path(temporary) / "captures"))
            self.assertEqual(result["feature_count"], len(ids))
            self.assertEqual(result["source_reported_count"], len(ids))
            self.assertEqual(result["source_id_max"], 31)
            self.assertTrue(uscg_sectors_complete(result).passed)
            root = Path(result["capture_directory"])
            self.assertTrue((root / "page_003.body").exists())
            # Replay must not touch the network and must reuse the sealed candidate.
            with patch.object(source.httpx, "Client", side_effect=AssertionError("network on replay")):
                reused = source.capture_uscg_sectors(storage, version="fixture-replay",
                                                     capture_directory=str(root))
            self.assertEqual(reused["version"], result["version"])
            self.assertTrue(reused["reused"])
            self.assertEqual(reused["source_id_sha256"], result["source_id_sha256"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            table = pq.read_table(io.BytesIO(
                storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")))
            self.assertEqual(table.column("FID").to_pylist(), sorted(ids))
            # A retained receipt rebound to a different publisher request is refused.
            self._rebind_receipt(root, "page_001",
                                 lambda url: source.LAYER_URL + "/query?objectIds=999")
            with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
                source._read_replay(root)
            # A torn capture (producer count moves between witnesses) quarantines
            # and seals no candidate.
            staged_before = set(storage.list_prefix(source.DATASET_SLUG))
            torn = json.loads(json.dumps(docs))
            torn["count_after"]["count"] += 1
            failed_root = Path(temporary) / "torn"
            with patch.object(source.httpx, "Client",
                              return_value=httpx.Client(transport=httpx.MockTransport(
                                  make_handler(torn, self._capture_queues(torn)[1])))):
                with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
                    source.capture_uscg_sectors(storage, version="torn-capture",
                                                capture_directory=str(failed_root))
            failure = next(failed_root.rglob("capture_manifest.json"))
            self.assertFalse(json.loads(failure.read_bytes())["complete"])
            self.assertTrue((failure.parent / "ids_after.body").exists())
            self.assertTrue(any(key.startswith(f"_quarantine/{source.DATASET_SLUG}/")
                                for key in storage.list_prefix("_quarantine/")))
            self.assertEqual(set(storage.list_prefix(source.DATASET_SLUG)), staged_before)

    def test_interrupted_capture_restarts_and_preserves_every_source_row_once(self):
        docs = self._documents()
        ids, queues = self._capture_queues(docs)
        urls = dict(source._capture_roster(ids))
        with tempfile.TemporaryDirectory() as temporary:
            storage = StagingStorageResource(local_dir=str(Path(temporary) / "stage"))
            capture_root = Path(temporary) / "captures"

            def interrupted(request):
                if str(request.url) == urls["page_001"]:
                    return httpx.Response(500, stream=httpx.ByteStream(b'{"error":{"code":500}}'))
                return self._healthy_handler(docs, queues)(request)

            with patch.object(source.httpx, "Client",
                              return_value=httpx.Client(transport=httpx.MockTransport(interrupted))):
                with self.assertRaisesRegex(ValueError, "incomplete_source_capture:page_001"):
                    source.capture_uscg_sectors(storage, version="restart",
                                                capture_directory=str(capture_root))
            attempts = sorted(capture_root.rglob("capture_manifest.json"))
            self.assertEqual(len(attempts), 1)
            failed = json.loads(attempts[0].read_bytes())
            self.assertFalse(failed["complete"])
            self.assertEqual(failed["error_code"], "ValueError")
            self.assertTrue(any(key.startswith(f"_quarantine/{source.DATASET_SLUG}/")
                                for key in storage.list_prefix("_quarantine/")))
            self.assertEqual(storage.list_prefix(source.DATASET_SLUG), [])
            # The restart runs in a fresh attempt directory and seals every
            # source row exactly once: no drop, no duplicate.
            with patch.object(source.httpx, "Client",
                              return_value=httpx.Client(transport=httpx.MockTransport(
                                  self._healthy_handler(docs, self._capture_queues(docs)[1])))):
                result = source.capture_uscg_sectors(storage, version="restart",
                                                     capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], len(ids))
            self.assertEqual(result["source_reported_count"], len(ids))
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}"
            table = pq.read_table(io.BytesIO(
                storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")))
            self.assertEqual(table.column("FID").to_pylist(), sorted(ids))
            self.assertEqual(len(list((capture_root / "restart").iterdir())), 2)

    def test_unsealed_interrupted_attempt_does_not_block_restart(self):
        docs = self._documents()
        _ids, queues = self._capture_queues(docs)
        with tempfile.TemporaryDirectory() as temporary:
            capture_root = Path(temporary) / "captures"
            leftover = capture_root / "restart" / "interrupted"
            leftover.mkdir(parents=True)
            (leftover / "page_000.body").write_bytes(b"partial")
            (leftover / "page_000.wire").write_bytes(b"partial")
            storage = StagingStorageResource(local_dir=str(Path(temporary) / "stage"))
            with patch.object(source.httpx, "Client",
                              return_value=httpx.Client(transport=httpx.MockTransport(
                                  self._healthy_handler(docs, queues)))):
                result = source.capture_uscg_sectors(storage, version="restart",
                                                     capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], len(FIXTURE_IDS))
            self.assertFalse((leftover / "capture_manifest.json").exists())

    def test_replay_rejects_foreign_or_stale_checkpoint_identity(self):
        other_layer = ("https://services9.arcgis.com/Fr5UUTVwNRJlXAkf/arcgis/rest/services/"
                       "Other_Service/FeatureServer/0")

        def stale_artifact_roster(root):
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            manifest["artifact_roster"].pop("page_001.receipt.json")
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")

        cases = [
            ("foreign layer checkpoint",
             lambda root: self._rebind_receipt(root, "page_001",
                                               lambda url: other_layer + "/query?objectIds=7%2C16"),
             "retained_capture_request_identity_mismatch"),
            ("stale page query",
             lambda root: self._rebind_receipt(root, "page_001",
                                               lambda url: source.LAYER_URL + "/query?objectIds=999"),
             "retained_capture_request_identity_mismatch"),
            ("stale artifact roster", stale_artifact_roster, "capture_roster_mismatch"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                root = self._replay_directory(Path(tmp) / "capture", self._documents())
                mutate(root)
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_uscg_sectors(storage, version="regression",
                                                capture_directory=str(root))
                self.assertEqual(storage.list_prefix(""), [])

    def test_replay_rejects_changed_or_missing_receipts(self):
        def corrupt_receipt_digest(root):
            receipt_path = root / "page_000.receipt.json"
            receipt = json.loads(receipt_path.read_bytes())
            receipt["sha256"] = "0" * 64
            receipt_path.write_bytes(_canonical(receipt) + b"\n")
            manifest = json.loads((root / "capture_manifest.json").read_bytes())
            manifest["receipts"]["page_000"] = receipt
            manifest["artifact_roster"] = capture_roster_digests(root, exclude={"capture_manifest.json"})
            (root / "capture_manifest.json").write_bytes(_canonical(manifest) + b"\n")

        def corrupt_body(root):
            body_path = root / "page_000.body"
            body_path.write_bytes(body_path.read_bytes() + b"tampered")

        cases = [
            ("missing receipt file", lambda root: (root / "page_000.receipt.json").unlink(),
             "capture_roster_mismatch"),
            ("changed receipt digest", corrupt_receipt_digest, "retained_capture_receipt_invalid"),
            ("changed body", corrupt_body, "retained_capture_artifact_digest_mismatch"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                root = self._replay_directory(Path(tmp) / "capture", self._documents())
                mutate(root)
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_uscg_sectors(storage, version="regression",
                                                capture_directory=str(root))
                self.assertEqual(storage.list_prefix(""), [])

    def test_budget_exhaustion_refuses_before_sending_a_request(self):
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(source, "CAPTURE_MAX_BYTES", 2 * source.PAGE_MAX_BYTES - 1), \
                patch.object(source, "capture_get") as capture_get:
            with self.assertRaisesRegex(ValueError, "source_capture_budget_exceeded_before_request"):
                source._capture(None, "https://example.invalid/query", Path(tmp), "page_000")
        capture_get.assert_not_called()

    def test_nonlocal_or_published_storage_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
            source.capture_uscg_sectors(
                StagingStorageResource(bucket="remote", use_local=False, local_dir="/tmp/x"),
                version="v1")

    def test_capture_version_cannot_escape_capture_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_uscg_sectors(storage, version="../outside")


if __name__ == "__main__":
    unittest.main()
