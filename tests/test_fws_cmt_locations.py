"""Native preservation, dual-coordinate fidelity and baseline drift checks for the FWS CMT layer.

All fixtures are synthetic: no real USFWS organization names, addresses or telephone
values are embedded here. Fixture counts model a development capture, not an
approved release baseline (1,816 rows; 4 null coordinate rows; 140 coordinate
disagreements).
"""
import copy
import io
import json
import math
import shutil
import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx
import unittest

import pyarrow.parquet as pq
from shapely import from_wkb
from dagster import DefaultScheduleStatus

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import fws_cmt_locations as source
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.fws_cmt_locations import (
    fws_cmt_locations_complete, fws_cmt_locations_drift, fws_cmt_locations_draft_refresh_schedule, fws_cmt_locations,
)

FIXTURE_COUNT = 1816
R = source.SPHERE_RADIUS_M
IDS = [1667 + index for index in range(FIXTURE_COUNT)]
NULL_ROWS = {FIXTURE_COUNT - 4, FIXTURE_COUNT - 3,
             FIXTURE_COUNT - 2, FIXTURE_COUNT - 1}
DISAGREE_ROWS = set(range(140))
LEADING_ZERO_ROWS = set(range(76))


def _forward(latitude: float, longitude: float) -> tuple[float, float]:
    x = longitude * math.pi * R / 180.0
    y = R * math.log(math.tan(math.pi / 4.0 + latitude * math.pi / 360.0))
    return x, y


def _feature(oid: int) -> dict:
    index = oid - 1667
    latitude = -60.0 + (index % 120)
    longitude = -170.0 + (index % 330)
    org_code = f"0{index:04d}" if index in LEADING_ZERO_ROWS else f"1{index:04d}"
    if index in DISAGREE_ROWS:
        geometry_x, geometry_y = _forward(latitude + 5e-4, longitude - 5e-4)
    else:
        geometry_x, geometry_y = _forward(latitude, longitude)
    geometry = None
    if index not in NULL_ROWS:
        geometry = {"x": geometry_x, "y": geometry_y}
    else:
        latitude = longitude = None
    attributes = {"physStateAbbr": "ST", "orgName": f"Synthetic Unit {index}",
                  "physAdd1": f"{index} Synthetic Way", "orgTypeAcro": "SYN",
                  "physCity": "Syntheticville", "mailCity": "Syntheticville",
                  "program": f"Program {index % 7}", "mailStateAbbr": "ST",
                  "telephone": "000-000-0000", "region": 1 + index % 8,
                  "mailAdd1": f"{index} Synthetic Mail Way", "orgType": "Synthetic",
                  "physAdd2": None, "mailAdd2": None, "ObjectId": oid,
                  "physZip": "00000", "mailZip": "00000", "orgCode": org_code,
                  "longitude": longitude, "latitude": latitude,
                  "CCCode": f"SYN{index:08d}", "RptToCC": None, "RptOrgCode": None,
                  "OrgNameAbbr": f"SU{index}", "ProgAcro": "SYN",
                  "RptToOrgName": None, "LIT": None}
    return {"attributes": attributes, "geometry": geometry}


def _capture_docs_plain(ids=IDS) -> dict:
    fields = [{**declaration, "actualType": "nvarchar", "sqlType": "sqlTypeNVarchar",
               "nullable": True, "editable": True, "defaultValue": None}
              for declaration in source.EXPECTED_FIELD_DECLARATIONS]
    layer = {"id": source.LAYER_ID, "name": source.LAYER_NAME,
             "geometryType": "esriGeometryPoint", "maxRecordCount": 2000, "fields": fields,
             "spatialReference": {"wkid": 102100, "latestWkid": 3857},
             "uniqueIdField": {"name": "ObjectId", "isSystemMaintained": True},
             "extent": {"xmin": -2.0e7, "ymin": -3.0e6, "xmax": 2.0e7, "ymax": 1.0e7,
                        "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
             "editingInfo": {"dataLastEditDate": 1793673009098}}
    service = {"serviceItemId": source.ITEM_ID,
               "layers": [{"id": 0, "name": source.LAYER_NAME, "parentLayerId": -1,
                           "defaultVisibility": True, "subLayerIds": None, "minScale": 1,
                           "maxScale": 0, "type": "Feature Layer",
                           "geometryType": "esriGeometryPoint"}],
               "tables": [], "spatialReference": {"wkid": 102100, "latestWkid": 3857}}
    features = [_feature(oid) for oid in ids]
    docs = {
        "org": {"id": source.ORG_ID, "name": "U.S. Fish & Wildlife Service",
                "portalName": "ArcGIS Online", "urlKey": "fws"},
        "item": {"id": source.ITEM_ID, "owner": source.OWNER_ACCOUNT,
                 "type": "Feature Service", "access": "public", "title": source.ITEM_TITLE,
                 "url": source.SERVICE_URL,
                 "accessInformation": ("FWS GIS (US Fish & Wildlife Service - Information Resources "
                                        "and Technology Management - Branch of Geospatial Data Services)  "
                                        "E-Mail: FWSGIS@fws.gov")},
        "owner": {"username": source.OWNER_ACCOUNT, "fullName": "US Fish & Wildlife Service",
                  "description": "US Fish and Wildlife Service - Information Resources and Technology "
                                  "Management - Branch of Geospatial Data Services."},
        "service_before": copy.deepcopy(service), "service_after": copy.deepcopy(service),
        "layer_before": copy.deepcopy(layer), "layer_after": copy.deepcopy(layer),
        "count_before": {"count": len(ids)},
        "count_after": {"count": len(ids)},
        "ids_before": {"objectIds": list(ids)}, "ids_after": {"objectIds": list(ids)},
    }
    page_count = (len(ids) + source.PAGE_SIZE - 1) // source.PAGE_SIZE
    for index in range(page_count):
        start = index * source.PAGE_SIZE
        rows = features[start:start + source.PAGE_SIZE]
        page = {"features": rows, "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                "geometryType": "esriGeometryPoint", "objectIdFieldName": "ObjectId",
                "globalIdFieldName": "", "uniqueIdField": {"name": "ObjectId", "isGlobalId": False}}
        if index < page_count - 1:
            page["exceededTransferLimit"] = True
        docs[f"page_{index:03d}"] = page
    return docs


def _capture_docs() -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode(), {})
            for name, doc in _capture_docs_plain().items()}


def _mutated_docs(mutator) -> dict[str, tuple[bytes, dict]]:
    docs = _capture_docs_plain()
    mutator(docs)
    return {name: (json.dumps(doc).encode(), {}) for name, doc in docs.items()}


def _edit(docs: dict, name: str) -> dict:
    return docs[name]


class FwsCmtLocationsTests(unittest.TestCase):
    def test_registered_stopped_draft_and_unapproved_release_gate(self):
        self.assertIn(fws_cmt_locations, hifld_ingest_assets)
        self.assertEqual(fws_cmt_locations_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        candidate = {"feature_count": FIXTURE_COUNT, "source_reported_count": FIXTURE_COUNT,
                     "source_population_complete": True,
                     "null_coordinate_count": 4,
                     "geometry_attribute_disagreement_count":
                         140}
        check = fws_cmt_locations_complete(candidate)
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["source_registry_accepted"].value)
        self.assertFalse(check.metadata["mapping_approved"].value)
        rejected = fws_cmt_locations_complete({**candidate, "feature_count": 1815})
        self.assertFalse(rejected.passed)

    def test_fresh_complete_captures_add_remove_and_reuse_immutable_candidate(self):
        # Exercise real acquisition, receipts and construction sealing using a
        # copied installed-package layout; no provenance verifier is bypassed.
        repository = Path(source.__file__).parents[3]
        client_type = httpx.Client
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root / "runtime"
            shutil.copytree(repository / "src/dagster_hifld", runtime / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repository / name, runtime / name)
            storage = StagingStorageResource(local_dir=str(root / "staging"), bucket=None, use_local=True)
            candidates = []
            originals = {}
            for iteration, ids in enumerate((IDS[:2], IDS[:3], IDS[1:3], IDS[1:3])):
                docs = _capture_docs_plain(ids)
                responses = {url: docs[name] for name, url in source._capture_roster(len(ids))}
                def respond(request):
                    body = json.dumps(responses[str(request.url)]).encode()
                    return httpx.Response(200, headers={"content-type": "application/json"},
                                          stream=httpx.ByteStream(body))
                transport = httpx.MockTransport(respond)
                def client(**kwargs):
                    return client_type(transport=transport, **kwargs)
                with patch.object(source, "__file__", str(runtime / "src/dagster_hifld/sources/fws_cmt_locations.py")), \
                        patch.object(source.httpx, "Client", client):
                    candidate = source.capture_fws_cmt_locations(storage, version=f"fixture-{iteration}",
                        capture_directory=str(root / f"capture-{iteration}"))
                self.assertTrue(fws_cmt_locations_complete(candidate).passed)
                self.assertFalse(fws_cmt_locations_drift(candidate).passed)
                candidates.append(candidate)
                prefix = "/".join(candidate["candidate_ready"].split("/")[:-2])
                objects = {key: storage.read_key(key) for key in storage.list_prefix(prefix)}
                if iteration == 2:
                    originals = objects
                if iteration == 3:
                    self.assertEqual(objects, originals)
            self.assertEqual(len({row["version"] for row in candidates[:3]}), 3)
            self.assertTrue(candidates[-1]["reused"])
            self.assertEqual(candidates[-1]["version"], candidates[-2]["version"])

    def test_retained_capture_is_bound_to_exact_named_get_request(self):
        for name, url in source._capture_roster():
            receipt = {"method": "GET", "url": url, "final_url": url}
            source.validate_capture_request_identity(name, receipt, url)
            receipt["final_url"] = "https://example.invalid/wrong"
            with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch"):
                source.validate_capture_request_identity(name, receipt, url)

    def test_complete_population_preserves_both_representations(self):
        features, audit = source._validate(_capture_docs())
        self.assertEqual(len(features), FIXTURE_COUNT)
        self.assertEqual(audit["source_feature_count"], FIXTURE_COUNT)
        self.assertEqual(audit["source_null_coordinate_count"], 4)
        self.assertEqual(audit["source_geometry_attribute_disagreement_count"],
                         140)
        self.assertEqual(audit["source_org_code_leading_zero_count"], len(LEADING_ZERO_ROWS))
        self.assertEqual(audit["source_org_code_duplicate_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], FIXTURE_COUNT - 4)
        self.assertEqual(audit["geometry_empty_count"], 4)
        parquet = source._parquet_bytes(features)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, FIXTURE_COUNT)
        org_codes = table["orgCode"].to_pylist()
        self.assertTrue(all(value == f"0{index:04d}" for index, value in
                            enumerate(org_codes) if index in LEADING_ZERO_ROWS))
        self.assertTrue(all(isinstance(value, str) for value in org_codes))
        self.assertEqual(sum(value is None for value in table["latitude"].to_pylist()), 4)
        self.assertEqual(sum(value is None for value in table["longitude"].to_pylist()), 4)
        self.assertEqual(sum(value is None for value in table["geometry"].to_pylist()), 4)
        self.assertTrue(table.schema.field("region").type.equals(__import__("pyarrow").int32()))
        self.assertTrue(table.schema.field("latitude").type.equals(__import__("pyarrow").float64()))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})
        from pyproj import CRS
        self.assertTrue(CRS.from_json(json.dumps(geo["columns"]["geometry"]["crs"]))
                        .equals(CRS.from_epsg(3857)))
        # Both source representations coexist in the candidate: recompute the
        # disagreement baseline from the published columns themselves.
        recomputed = 0
        for row_index, wkb in enumerate(table["geometry"].to_pylist()):
            attributes_latitude = table["latitude"][row_index].as_py()
            attributes_longitude = table["longitude"][row_index].as_py()
            if wkb is None:
                self.assertIsNone(attributes_latitude)
                continue
            point = from_wkb(wkb)
            self.assertEqual(point.geom_type, "Point")
            delta = max(abs(source._inverse_x(point.x) - attributes_longitude),
                        abs(source._inverse_y(point.y) - attributes_latitude))
            if delta > source.DISAGREEMENT_THRESHOLD_DEG:
                recomputed += 1
        self.assertEqual(recomputed, 140)

    def test_complete_addition_and_removal_pass_but_baseline_remains_unavailable(self):
        for new_ids in (IDS + [IDS[-1] + 1], IDS[:-1], IDS[:1000], IDS + list(range(4000, 4200))):
            docs = _capture_docs_plain()
            for name in list(docs):
                if name.startswith("page_"):
                    del docs[name]
            for phase in ("before", "after"):
                docs["count_" + phase] = {"count": len(new_ids)}
                docs["ids_" + phase] = {"objectIds": list(new_ids)}
            for index, start in enumerate(range(0, len(new_ids), source.PAGE_SIZE)):
                docs[f"page_{index:03d}"] = {
                    "features": [_feature(oid) for oid in new_ids[start:start + source.PAGE_SIZE]],
                    "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                    "geometryType": "esriGeometryPoint", "objectIdFieldName": "ObjectId"}
            features, audit = source._validate({key: (json.dumps(value).encode(), {}) for key, value in docs.items()})
            self.assertEqual(len(features), len(new_ids))
            candidate = {"feature_count": len(new_ids), "source_reported_count": len(new_ids),
                         "source_population_complete": True}
            self.assertTrue(fws_cmt_locations_complete(candidate).passed)
            drift = fws_cmt_locations_drift(candidate)
            self.assertFalse(drift.passed)
            self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
            missing_page = dict(docs)
            missing_page.pop("page_000")
            with self.assertRaises(ValueError):
                source._validate({key: (json.dumps(value).encode(), {}) for key, value in missing_page.items()})

    def test_rejects_identity_drift(self):
        def owner_changed(captures):
            _edit(captures, "item")["owner"] = "someone_else"
        def org_changed(captures):
            _edit(captures, "org")["name"] = "Not FWS"
        def account_changed(captures):
            _edit(captures, "owner")["fullName"] = "Not FWS"
        def service_changed(captures):
            _edit(captures, "service_before")["serviceItemId"] = "0" * 32
        def service_moved(captures):
            _edit(captures, "service_after")["layers"] = []
        for mutator, error in ((owner_changed, "source_item_identity_drift"),
                               (org_changed, "source_org_identity_drift"),
                               (account_changed, "source_publisher_account_identity_drift"),
                               (service_changed, "source_service_identity_drift"),
                               (service_moved, "source_service_or_layer_changed_during_capture")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))

    def test_rejects_schema_and_population_drift(self):
        def drop_field(captures):
            _edit(captures, "layer_before")["fields"].pop()
        def rename_field(captures):
            _edit(captures, "layer_before")["fields"][17]["alias"] = "Renamed"
        def count_drift(captures):
            _edit(captures, "count_before")["count"] = FIXTURE_COUNT + 1
        def roster_drift(captures):
            _edit(captures, "ids_after")["objectIds"][0] = 999999
        def page_short(captures):
            _edit(captures, "page_001")["features"].pop()
        def duplicate_page_id(captures):
            rows = _edit(captures, "page_000")["features"]
            rows[1]["attributes"]["ObjectId"] = rows[0]["attributes"]["ObjectId"]
        def edited_during_capture(captures):
            _edit(captures, "layer_after")["editingInfo"]["dataLastEditDate"] += 1
        def page_transfer_limit(captures):
            _edit(captures, "page_001")["exceededTransferLimit"] = True
        for mutator, error in ((drop_field, "source_field_schema_drift"),
                               (rename_field, "source_field_schema_drift"),
                               (count_drift, "source_count_or_id_roster_drift"),
                               (roster_drift, "source_count_or_id_roster_drift"),
                               (page_short, "source_page_population_incomplete"),
                               (duplicate_page_id, "source_feature_id_roster_drift"),
                               (edited_during_capture, "source_service_or_layer_changed_during_capture"),
                               (page_transfer_limit, "source_page_error_or_transfer_limit")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))

    def test_null_and_disagreement_counts_are_observations_not_baselines(self):
        def extra_null_row(captures):
            page = _edit(captures, "page_000")
            row = page["features"][10]
            row["geometry"] = None
            row["attributes"]["latitude"] = row["attributes"]["longitude"] = None
        def repaired_disagreement(captures):
            page = _edit(captures, "page_000")
            row = page["features"][0]
            latitude, longitude = row["attributes"]["latitude"], row["attributes"]["longitude"]
            row["geometry"]["x"], row["geometry"]["y"] = _forward(latitude, longitude)
        def duplicate_org_code(captures):
            page = _edit(captures, "page_000")
            page["features"][5]["attributes"]["orgCode"] = page["features"][4]["attributes"]["orgCode"]
        def null_org_code(captures):
            _edit(captures, "page_000")["features"][5]["attributes"]["orgCode"] = None
        for mutator in (extra_null_row, repaired_disagreement):
            features, audit = source._validate(_mutated_docs(mutator))
            self.assertEqual(len(features), FIXTURE_COUNT)
        for mutator, error in ((duplicate_org_code, "source_org_code_duplicate_drift"),
                               (null_org_code, "source_org_code_null_or_empty_drift")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))

    def test_rejects_coordinate_and_geometry_structure_drift(self):
        def unpaired_null_latitude(captures):
            _edit(captures, "page_000")["features"][7]["attributes"]["latitude"] = None
        def geometry_attr_null_mismatch(captures):
            _edit(captures, "page_000")["features"][7]["geometry"] = None
        def latitude_out_of_range(captures):
            _edit(captures, "page_000")["features"][7]["attributes"]["latitude"] = 91.0
        def z_geometry(captures):
            row = _edit(captures, "page_000")["features"][7]
            row["geometry"]["z"] = 0.0
        def world_bound_breach(captures):
            _edit(captures, "page_000")["features"][7]["geometry"]["x"] = -3.0e7
        def string_ordinate(captures):
            _edit(captures, "page_000")["features"][7]["geometry"]["x"] = "-13000000"
        for mutator, error in ((unpaired_null_latitude, "source_latitude_longitude_null_pairing_drift"),
                               (geometry_attr_null_mismatch, "source_geometry_attribute_null_pairing_drift"),
                               (latitude_out_of_range, "source_attribute_coordinate_range_drift"),
                               (z_geometry, "source_point_geometry_structure_drift"),
                               (world_bound_breach, "source_point_geometry_structure_drift"),
                               (string_ordinate, "source_point_geometry_structure_drift")):
            with self.assertRaisesRegex(ValueError, error):
                source._validate(_mutated_docs(mutator))


if __name__ == "__main__":
    unittest.main()
