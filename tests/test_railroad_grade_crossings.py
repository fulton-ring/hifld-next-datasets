"""Focused contract checks for the FRA Railroad Grade Crossings source adapter."""
from __future__ import annotations

import hashlib
import httpx
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import railroad_grade_crossings as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_URL = "https://services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Railroad_Grade_Crossings/FeatureServer"

PORTAL_ITEM_BODY = json.dumps({
    "id": "f51a402931a04fe890a568ef88fb81a6", "owner": "USDOT_BTS",
    "orgId": "xOi1kZaI0eWDREZv", "title": "Railroad Grade Crossings",
    "type": "Feature Service", "access": "public", "url": SERVICE_URL,
    "description": ("The Railroad Grade Crossings dataset was updated on October 09, 2026 and "
                    "was created on October 09, 2026 by the Federal Railroad Administration "
                    "(FRA), and is part of the U.S. Department of Transportation (USDOT)/Bureau "
                    "of Transportation Statistics (BTS) National Transportation Atlas Database "
                    "(NTAD). The Railroad Grade Crossings is a spatial file that originates "
                    "from the National Highway-Rail Crossing Inventory Program. Crossings are "
                    "moved to latitude longitude 0,0 if they are geospatially located outside "
                    "their designated state or located outside the United States."),
}, separators=(",", ":")).encode()

SERVICE_BODY = json.dumps({
    "serviceItemId": "f51a402931a04fe890a568ef88fb81a6",
    "layers": [{"id": 0, "name": "Railroad Grade Crossings", "type": "Feature Layer"}],
    "editingInfo": None,
}, separators=(",", ":")).encode()

EXTENT = {"xmin": -150.1233369997574, "ymin": 0.0, "xmax": 0.0, "ymax": 64.91895099993351,
          "spatialReference": {"wkid": 4326, "latestWkid": 4326}}

_FIELD_SPEC = [
    ("OBJECTID", "esriFieldTypeOID", None),
    ("CrossingID", "esriFieldTypeString", 7), ("ReasonCode", "esriFieldTypeString", 2),
    ("StateCode", "esriFieldTypeString", 2), ("STATENAME", "esriFieldTypeString", 20),
    ("CountyCode", "esriFieldTypeString", 3), ("COUNTYNAME", "esriFieldTypeString", 22),
    ("CityCode", "esriFieldTypeString", 9), ("CITYNAME", "esriFieldTypeString", 23),
    ("StateAbbreviation", "esriFieldTypeString", 2), ("RailroadCode", "esriFieldTypeString", 4),
    ("RailroadDivision", "esriFieldTypeString", 2), ("RailroadSubdivision", "esriFieldTypeString", 2),
    ("HighwayName", "esriFieldTypeString", 44), ("STREET", "esriFieldTypeString", 64),
    ("TimetableStation", "esriFieldTypeString", 6), ("RailroadMilepostNumber", "esriFieldTypeString", 10),
    ("CrossingType", "esriFieldTypeString", 1), ("CrossingPosition", "esriFieldTypeString", 1),
    ("CrossingPurpose", "esriFieldTypeString", 1), ("OperatingRailroadType", "esriFieldTypeString", 4),
    ("WDCODE", "esriFieldTypeString", 30), ("SignsOrSignals", "esriFieldTypeString", 1),
    ("ReportingAgencyName", "esriFieldTypeString", 30),
    ("Longitude", "esriFieldTypeDouble", None), ("LATITUDE", "esriFieldTypeDouble", None),
    ("LatLongSource", "esriFieldTypeString", 1), ("WhistleBan", "esriFieldTypeString", 1),
    ("RailroadContactTelephoneNumber", "esriFieldTypeString", 10),
    ("EmergencyTelephoneNumber", "esriFieldTypeString", 10), ("URL", "esriFieldTypeString", 124),
    ("AccURL", "esriFieldTypeString", 124),
    ("AnnualAverageDailyTrafficCount", "esriFieldTypeString", 6),
    ("AnnualAverageDailyTrafficYear", "esriFieldTypeString", 4),
    ("ParentRailroadCode", "esriFieldTypeString", 4),
    ("NearestTimetableStation", "esriFieldTypeString", 38),
    ("MaximumTimetableSpeed", "esriFieldTypeString", 3),
    ("TotalDaylightThruTrains", "esriFieldTypeInteger", None),
    ("TotalNighttimeThruTrains", "esriFieldTypeInteger", None),
    ("NumberOfMainTracks", "esriFieldTypeInteger", None),
    ("NumberOfSidingTracks", "esriFieldTypeInteger", None),
    ("NumberOfYardTracks", "esriFieldTypeInteger", None),
    ("NumberOfTransitTracks", "esriFieldTypeInteger", None),
    ("NumberOfIndistryTracks", "esriFieldTypeInteger", None),
    ("OTHRTRK", "esriFieldTypeInteger", None),
    ("RRCoStartDate", "esriFieldTypeString", 10), ("RRCoEndDate", "esriFieldTypeString", 10),
    ("LATDD", "esriFieldTypeDouble", None), ("LONGDD", "esriFieldTypeDouble", None),
]


def _fields():
    fields = []
    for name, field_type, length in _FIELD_SPEC:
        field = {"name": name, "type": field_type, "alias": name, "nullable": name != "OBJECTID",
                 "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _attributes(oid):
    """Producer-shaped rows: complete values, producer nulls, and the 0,0 relocation row."""
    base = {
        "CrossingID": "351464X", "ReasonCode": "01", "StateCode": "01", "STATENAME": "Alabama",
        "CountyCode": "097", "COUNTYNAME": "Mobile", "CityCode": "0009704", "CITYNAME": "Mobile",
        "StateAbbreviation": "AL", "RailroadCode": "CSRR", "RailroadDivision": "01",
        "RailroadSubdivision": "02", "HighwayName": "US 90", "STREET": "US HWY 90 W",
        "TimetableStation": "TENMSUB", "RailroadMilepostNumber": "SM 130.5",
        "CrossingType": "1", "CrossingPosition": "1", "CrossingPurpose": "1",
        "OperatingRailroadType": "4", "WDCODE": "AL", "SignsOrSignals": "3",
        "ReportingAgencyName": "CSX Transportation", "Longitude": -88.24450490024674,
        "LATITUDE": 30.50497350025622, "LatLongSource": "A", "WhistleBan": "N",
        "RailroadContactTelephoneNumber": "8002320144", "EmergencyTelephoneNumber": "8002320144",
        "URL": "https://railroads.dot.gov/crossing/351464X",
        "AccURL": "https://safetydata.fra.dot.gov/MasterWebService/WSFRA/351464X",
        "AnnualAverageDailyTrafficCount": "012450", "AnnualAverageDailyTrafficYear": "2023",
        "ParentRailroadCode": "CSRR", "NearestTimetableStation": "WHISTLER",
        "MaximumTimetableSpeed": "55", "TotalDaylightThruTrains": 12,
        "TotalNighttimeThruTrains": 8, "NumberOfMainTracks": 1, "NumberOfSidingTracks": 0,
        "NumberOfYardTracks": 0, "NumberOfTransitTracks": 0, "NumberOfIndistryTracks": 0,
        "OTHRTRK": 0, "RRCoStartDate": "1863-05-01", "RRCoEndDate": None,
        "LATDD": 30.50497350025622, "LONGDD": -88.24450490024674,
    }
    overrides = {
        10: {},
        11: {"CrossingID": "651753U", "ReasonCode": "14", "StateCode": "02", "STATENAME": "Alaska",
             "CountyCode": "020", "COUNTYNAME": "Denali", "StateAbbreviation": "AK",
             "RailroadCode": "ARR", "HighwayName": "PARKS HWY", "STREET": "GEORGE PARKS HWY",
             "Longitude": -150.1233369997574, "LATITUDE": 31.2, "LATDD": 31.2,
             "LONGDD": -150.1233369997574, "AnnualAverageDailyTrafficCount": None,
             "AnnualAverageDailyTrafficYear": None, "RailroadContactTelephoneNumber": None,
             "EmergencyTelephoneNumber": None, "URL": None, "AccURL": None,
             "RRCoStartDate": "2014-06-01"},
        12: {"CrossingID": "123456F", "ReasonCode": "01", "StateCode": "28", "STATENAME": "Mississippi",
             "CountyCode": "073", "COUNTYNAME": "Itawamba", "CityCode": None, "CITYNAME": None,
             "StateAbbreviation": "MS", "Longitude": 0.0, "LATITUDE": 0.0,
             "LATDD": None, "LONGDD": None, "RailroadDivision": None, "RailroadSubdivision": None,
             "TimetableStation": None, "RailroadMilepostNumber": None, "CrossingType": None,
             "CrossingPosition": None, "CrossingPurpose": None, "OperatingRailroadType": None,
             "WDCODE": None, "SignsOrSignals": None, "ReportingAgencyName": None,
             "LatLongSource": "P", "WhistleBan": None, "RailroadContactTelephoneNumber": None,
             "EmergencyTelephoneNumber": None, "URL": None, "AccURL": None,
             "AnnualAverageDailyTrafficCount": None, "AnnualAverageDailyTrafficYear": None,
             "ParentRailroadCode": None, "NearestTimetableStation": None,
             "MaximumTimetableSpeed": None, "TotalDaylightThruTrains": None,
             "TotalNighttimeThruTrains": None, "NumberOfMainTracks": None,
             "NumberOfSidingTracks": None, "NumberOfYardTracks": None,
             "NumberOfTransitTracks": None, "NumberOfIndistryTracks": None, "OTHRTRK": None,
             "RRCoStartDate": None},
        13: {"CrossingID": "756432E", "ReasonCode": "14", "StateCode": "02", "STATENAME": "Alaska",
             "CountyCode": "068", "COUNTYNAME": "Denali", "StateAbbreviation": "AK",
             "RailroadCode": "ARR", "Longitude": -149.9, "LATITUDE": 64.91895099993351,
             "LATDD": 64.91895099993351, "LONGDD": -149.9,
             "RailroadContactTelephoneNumber": None, "EmergencyTelephoneNumber": None,
             "URL": None, "AccURL": None, "RRCoStartDate": "2014-06-01"},
        14: {"CrossingID": "123457F", "ReasonCode": "01", "StateCode": "06", "STATENAME": "California",
             "CountyCode": "037", "COUNTYNAME": "Los Angeles", "StateAbbreviation": "CA",
             "RailroadCode": "UPRR"},
        15: {"CrossingID": "101010Y", "ReasonCode": "04", "StateCode": "35", "STATENAME": "New Mexico",
             "CountyCode": "031", "COUNTYNAME": "Hidalgo", "StateAbbreviation": "NM",
             "RailroadCode": "BNSF", "Longitude": -106.0, "LATITUDE": 28.6,
             "LATDD": 28.6, "LONGDD": -106.0},
    }
    merged = dict(base)
    merged.update(overrides[oid])
    return {"OBJECTID": oid, **merged}


def _geometries():
    return {
        10: {"x": -88.24450490024674, "y": 30.50497350025622},
        11: {"x": -150.1233369997574, "y": 31.2},
        12: {"x": 0.0, "y": 0.0},
        13: {"x": -149.9, "y": 64.91895099993351},
        14: None,
        15: {"x": -106.0, "y": 28.6},
    }


def _fixture():
    fields = _fields()
    layer = {"id": 0, "name": "Railroad Grade Crossings", "type": "Feature Layer",
             "geometryType": "esriGeometryPoint", "objectIdField": "OBJECTID",
             "hasZ": False, "hasM": False, "maxRecordCount": 2000,
             "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
             "extent": EXTENT, "fields": fields}
    service = {"serviceItemId": "f51a402931a04fe890a568ef88fb81a6",
               "layers": [{"id": 0, "name": "Railroad Grade Crossings", "type": "Feature Layer"}]}
    features = [{"attributes": _attributes(oid), "geometry": _geometries()[oid]}
                for oid in (10, 11, 12, 13, 14, 15)]
    ids = [10, 11, 12, 13, 14, 15]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
              for f in fields]
    attr_names = [f["name"] for f in fields]
    geometry_records, null_count, invalid_count, bounds = source._classify_geometries(features, attr_names)
    crs = {"wkid": 4326, "latestWkid": 4326}
    page0 = {"features": features[:2], "spatialReference": crs, "exceededTransferLimit": True}
    page1 = {"features": features[2:4], "spatialReference": crs, "exceededTransferLimit": True}
    page2 = {"features": features[4:], "spatialReference": crs}
    docs = {"portal_item": json.loads(PORTAL_ITEM_BODY), "service_before": service,
            "service_after": service, "layer_before": layer, "layer_after": layer,
            "count_before": {"count": 6}, "count_after": {"count": 6},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000000_00002": page0, "page_000002_00002": page1, "page_000004_00002": page2}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    geom = geometry_records
    counts = (null_count, invalid_count)
    return fields, features, captures, schema, geom, counts, bounds


def _pins(features, schema, geom, counts, bounds):
    null_count, invalid_count = counts
    return dict(EXPECTED_COUNT=6, PAGE_START_RECORDS=2,
                EXPECTED_NULL_GEOMETRY_COUNT=null_count,
                EXPECTED_INVALID_GEOMETRY_COUNT=invalid_count,
                EXPECTED_CROSSINGID_NULL_COUNT=0, EXPECTED_CROSSINGID_DUPLICATE_COUNT=0,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical([10, 11, 12, 13, 14, 15])).hexdigest(),
                EXPECTED_RECORD_SHA256=source._list_sha(sorted(features, key=lambda row: row["attributes"]["OBJECTID"])),
                EXPECTED_GEOMETRY_SHA256=source._list_sha(geom),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_SERVICE_LAYER_ROSTER=[(0, "Railroad Grade Crossings", "Feature Layer")],
                EXPECTED_LAYER_EXTENT={"xmin": -150.1233369997574, "ymin": 0.0, "xmax": 0.0,
                                       "ymax": 64.91895099993351},
                EXPECTED_OBSERVED_BOUNDS=bounds)


def _page_features(captures):
    rows = [row for name in sorted(captures) if name.startswith("page_")
            for row in json.loads(captures[name][0])["features"]]
    return rows


def _replace(captures, name, doc):
    captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), {"status": 200})


class RailroadGradeCrossingsTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, {"a": "b", "list": [1, 2, 3]}], None, "x", 4.5]
        self.assertEqual(source._list_sha(items), hashlib.sha256(_canonical(items)).hexdigest())

    def test_native_fields_ids_values_and_zero_zero_relocation_preserved_in_parquet(self):
        fields, features, captures, schema, geom, counts, _ = _fixture()
        with patch.multiple(source, **_pins(features, schema, geom, counts, None)):
            _, original_fields, extracted = source._validate_captures(captures)
            body = source._parquet_bytes(original_fields, extracted["features"])
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [10, 11, 12, 13, 14, 15])
        self.assertEqual(table.column("CrossingID").to_pylist(),
                         ["351464X", "651753U", "123456F", "756432E", "123457F", "101010Y"])
        # Producer string codes keep their leading zeros verbatim; nulls stay null.
        self.assertEqual(table.column("StateCode").to_pylist(),
                         ["01", "02", "28", "02", "06", "35"])
        self.assertEqual(table.column("CountyCode").to_pylist(),
                         ["097", "020", "073", "068", "037", "031"])
        self.assertIsNone(table.column("CityCode").to_pylist()[2])
        self.assertEqual(table.column("AnnualAverageDailyTrafficCount").to_pylist()[0], "012450")
        self.assertEqual(table.column("TotalDaylightThruTrains").type.__str__(), "int32")
        self.assertEqual(table.column("TotalDaylightThruTrains").to_pylist()[0], 12)
        self.assertEqual(table.column("NumberOfMainTracks").to_pylist()[0], 1)
        self.assertIsNone(table.column("NumberOfMainTracks").to_pylist()[2])
        self.assertEqual(table.column("Longitude").type.__str__(), "double")
        # The 0,0 relocation row keeps the producer's reported lat/long attributes
        # verbatim while its derived LATDD/LONGDD and names stay null.
        self.assertEqual(table.column("LATITUDE").to_pylist()[2], 0.0)
        self.assertEqual(table.column("Longitude").to_pylist()[2], 0.0)
        self.assertIsNone(table.column("LATDD").to_pylist()[2])
        self.assertIsNone(table.column("LONGDD").to_pylist()[2])
        self.assertEqual(table.column("RRCoEndDate").to_pylist(),
                         [None, None, None, None, None, None])
        points = [shapely.from_wkb(wkb) if wkb is not None else None
                  for wkb in table.column("geometry").to_pylist()]
        self.assertIsInstance(points[0], shapely.Point)
        self.assertEqual((points[0].x, points[0].y), (-88.24450490024674, 30.50497350025622))
        # The producer's 0,0 relocation bucket is retained verbatim.
        self.assertEqual((points[2].x, points[2].y), (0.0, 0.0))
        self.assertIsNone(points[4])
        self.assertEqual((points[3].x, points[3].y), (-149.9, 64.91895099993351))
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])

    def test_id_roster_response_order_is_not_population_drift(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        for name, ids in (("ids_seed", [12, 10, 11, 14, 13, 15]),
                          ("ids_before", [11, 12, 10, 13, 14, 15]),
                          ("ids_after", [10, 11, 12, 13, 14, 15])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = ids
            _replace(captures, name, doc)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            _, _, extracted = source._validate_captures(captures)
        self.assertEqual(extracted["ids"], [10, 11, 12, 13, 14, 15])
        self.assertEqual(extracted["audit"]["null_geometry_count"], 1)

    def test_portal_item_owner_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["owner"] = "NOT_USDOT_BTS"
        _replace(captures, "portal_item", portal)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_portal_item_identity_drift"):
                source._validate_captures(captures)

    def test_portal_item_url_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["url"] = "https://mirror.invalid/arcgis/rest/services/Other/FeatureServer"
        _replace(captures, "portal_item", portal)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_portal_item_identity_drift"):
                source._validate_captures(captures)

    def test_portal_item_delegation_text_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        portal = json.loads(captures["portal_item"][0])
        portal["description"] = "A repackaged copy of grade crossings, no FRA lineage stated."
        _replace(captures, "portal_item", portal)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "official_FRA_NTAD_relationship_drift"):
                source._validate_captures(captures)

    def test_service_item_identity_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["serviceItemId"] = "00000000000000000000000000000000"
        _replace(captures, "service_before", service)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError,
                                        "official_BTS_railroad_grade_crossings_service_identity_drift"):
                source._validate_captures(captures)

    def test_service_layer_roster_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        service = json.loads(captures["service_before"][0])
        service["layers"].append({"id": 1, "name": "Extra Layer", "type": "Feature Layer"})
        _replace(captures, "service_before", service)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError,
                                        "official_BTS_railroad_grade_crossings_service_identity_drift"):
                source._validate_captures(captures)

    def test_layer_geometry_type_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["geometryType"] = "esriGeometryPolyline"
        _replace(captures, "layer_before", layer)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError,
                                        "exact_BTS_railroad_grade_crossings_layer_identity_drift"):
                source._validate_captures(captures)

    def test_pagination_capability_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["advancedQueryCapabilities"] = {"supportsPagination": False, "supportsOrderBy": True}
        _replace(captures, "layer_before", layer)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_pagination_capability_changed"):
                source._validate_captures(captures)

    def test_record_limit_change_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["maxRecordCount"] = 1000
        _replace(captures, "layer_before", layer)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_limit_drift"):
                source._validate_captures(captures)

    def test_layer_extent_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["extent"]["xmax"] = -50.0
        _replace(captures, "layer_before", layer)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_layer_extent_drift"):
                source._validate_captures(captures)

    def test_crs_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
        _replace(captures, "page_000000_00002", page)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_000000_crs_drift"):
                source._validate_captures(captures)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        _replace(captures, "layer_before", layer)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_renamed_field_is_quarantined_by_contract(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_before"][0])
        layer["fields"][1]["name"] = "CROSSING"
        _replace(captures, "layer_before", layer)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
                source._validate_captures(captures)

    def test_missing_page_id_is_not_accepted(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"] = page["features"][:1]
        _replace(captures, "page_000000_00002", page)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
                source._validate_captures(captures)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        final = json.loads(captures["page_000004_00002"][0])
        final["exceededTransferLimit"] = True
        _replace(captures, "page_000004_00002", final)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
                source._validate_captures(captures)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["STATENAME"] = "XX"
        _replace(captures, "page_000000_00002", page)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_geometry_change_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000004_00002"][0])
        page["features"][1]["geometry"]["x"] = -120.5
        _replace(captures, "page_000004_00002", page)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_record_value_drift"):
                source._validate_captures(captures)

    def test_coordinate_range_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["geometry"]["x"] = -200.0
        _replace(captures, "page_000000_00002", page)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_coordinate_range_drift"):
                source._validate_captures(captures)

    def test_zero_zero_relocation_row_is_coverage_pinned_not_repaired(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -150.1233369997574, "ymin": 31.2,
                                            "xmax": -88.24450490024674, "ymax": 64.91895099993351}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(captures)

    def test_non_numeric_point_geometry_fails_closed(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000004_00002"][0])
        page["features"][1]["geometry"] = {"x": "NaN", "y": 28.6}
        _replace(captures, "page_000004_00002", page)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_null_geometry_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000004_00002"][0])
        page["features"][1]["geometry"] = None
        _replace(captures, "page_000004_00002", page)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_geometry_population_drift"):
                source._validate_captures(captures)

    def test_crossingid_null_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000000_00002"][0])
        page["features"][0]["attributes"]["CrossingID"] = None
        _replace(captures, "page_000000_00002", page)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_feature_id_null_drift"):
                source._validate_captures(captures)

    def test_crossingid_duplicate_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        page = json.loads(captures["page_000002_00002"][0])
        page["features"][0]["attributes"]["CrossingID"] = "351464X"
        _replace(captures, "page_000002_00002", page)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_feature_id_duplicate_drift"):
                source._validate_captures(captures)

    def test_observed_coverage_drift_is_detected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        pins = _pins(_page_features(captures), schema, geom, counts, bounds)
        pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": -160.0, "ymin": 14.0, "xmax": -119.5, "ymax": 34.42}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                source._validate_captures(captures)

    def test_count_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        count = json.loads(captures["count_after"][0])
        count["count"] = 5
        _replace(captures, "count_after", count)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_count_drift"):
                source._validate_captures(captures)

    def test_id_roster_drift_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        ids = json.loads(captures["ids_after"][0])
        ids["objectIds"] = [10, 11, 12, 13, 14, 99]
        _replace(captures, "ids_after", ids)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._validate_captures(captures)

    def test_seed_roster_mismatch_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        seed = json.loads(captures["ids_seed"][0])
        seed["objectIds"] = [10, 11, 12, 13, 14, 99]
        _replace(captures, "ids_seed", seed)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_id_population_drift"):
                source._validate_captures(captures)

    def test_page_tiling_gap_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        del captures["page_000004_00002"]
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_page_tiling_incomplete_or_overlapped"):
                source._validate_captures(captures)

    def test_malformed_page_name_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        captures["page_xxxxx_00002"] = captures.pop("page_000004_00002")
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
                source._validate_captures(captures)

    def test_layer_metadata_change_during_capture_is_rejected(self):
        _, features, captures, schema, geom, counts, bounds = _fixture()
        layer = json.loads(captures["layer_after"][0])
        layer["maxRecordCount"] = 1000
        _replace(captures, "layer_after", layer)
        features = _page_features(captures)
        with patch.multiple(source, **_pins(features, schema, geom, counts, bounds)):
            with self.assertRaisesRegex(ValueError, "source_layer_metadata_changed_during_capture"):
                source._validate_captures(captures)

    def test_transient_producer_error_doc_is_retried_with_evidence(self):
        ok_body = b'{"objectIds": []}'
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            calls.append(name)
            body = error_body if len(calls) == 1 else ok_body
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep") as sleep_mock:
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS,
                                                            ledger=source._CaptureLedger(1 << 30))
            self.assertEqual(body, ok_body)
            self.assertEqual(calls, ["probe", "probe"])
            self.assertEqual((root / "probe.body").read_bytes(), ok_body)
            self.assertEqual((root / "probe.attempt-01.body").read_bytes(), error_body)
            self.assertTrue(sleep_mock.called)

    def test_persistent_producer_error_doc_exhausts_retries(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            calls.append(name)
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS,
                                                            ledger=source._CaptureLedger(1 << 30))
            self.assertEqual(body, error_body)
            self.assertEqual(len(calls), source.DOC_CAPTURE_ATTEMPTS)
            for attempt in range(1, source.DOC_CAPTURE_ATTEMPTS):
                self.assertEqual((root / f"probe.attempt-{attempt:02d}.body").read_bytes(), error_body)

    def test_incomplete_transport_capture_is_retried_bounded(self):
        ok_body = b'{"objectIds": []}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            calls.append(name)
            if name == "probe2":
                raise ValueError("unrelated_source_capture_failure")
            if len(calls) == 1:
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            digest = hashlib.sha256(ok_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(ok_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(ok_body)
            (capture_root / f"{name}.wire").write_bytes(ok_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return ok_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                body, _receipt = source._capture_with_retry(None, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=1024,
                                                            attempts=source.DOC_CAPTURE_ATTEMPTS,
                                                            base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS,
                                                            ledger=source._CaptureLedger(1 << 30))
                with self.assertRaisesRegex(ValueError, "unrelated_source_capture_failure"):
                    source._capture_with_retry(None, "https://producer.invalid/other",
                                               root, "probe2", max_bytes=1024,
                                               attempts=source.DOC_CAPTURE_ATTEMPTS,
                                               base_delay=source.DOC_CAPTURE_BASE_DELAY_SECONDS,
                                               ledger=source._CaptureLedger(1 << 30))
            self.assertEqual(body, ok_body)
            self.assertEqual(calls.count("probe"), 2)
            self.assertEqual(len(calls), 3)
            self.assertEqual((root / "probe.attempt-01.receipt.json").read_bytes(), b'{"status": null}\n')

    def test_adaptive_bisection_splits_producer_error_doc_failures(self):
        _, features, fixture_captures, _, _, _, _ = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            calls.append(name)
            body = error_body if name == "page_000000_00004" else bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-13T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                pages = source._capture_pages(None, root, 6, source._CaptureLedger(1 << 30))
            self.assertEqual(sorted(pages), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
            self.assertEqual((root / "page_000000_00004.body").read_bytes(), error_body)
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_adaptive_bisection_splits_persistent_transport_failures(self):
        _, features, fixture_captures, _, _, _, _ = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        calls = []

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            calls.append(name)
            if name == "page_000000_00004":
                (capture_root / f"{name}.body").write_bytes(b"")
                (capture_root / f"{name}.wire").write_bytes(b"")
                (capture_root / f"{name}.receipt.json").write_bytes(b'{"status": null}\n')
                raise ValueError(f"incomplete_source_capture:{name}")
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at":
                       "2026-11-13T00:00:00+00:00", "status": 200, "complete_body": True,
                       "wire_bytes": len(body), "wire_sha256": digest, "bytes": len(body),
                       "sha256": digest, "content_encoding": None, "content_length": str(len(body)),
                       "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                pages = source._capture_pages(None, root, 6, source._CaptureLedger(1 << 30))
            self.assertEqual(sorted(pages), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
            self.assertEqual(calls.count("page_000000_00004"), 1)

    def test_unservable_single_feature_fails_explicitly(self):
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            digest = hashlib.sha256(error_body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "status": 200,
                       "complete_body": True, "bytes": len(error_body), "sha256": digest}
            (capture_root / f"{name}.body").write_bytes(error_body)
            (capture_root / f"{name}.wire").write_bytes(error_body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return error_body, receipt

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            with patch.multiple(source, PAGE_START_RECORDS=4,
                                PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=2), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_single_feature_unservable"):
                    source._capture_pages(None, root, 6, source._CaptureLedger(1 << 30))

    def test_capture_requires_explicit_local_staging(self):
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_railroad_grade_crossings(
                    PublishedStorageResource(use_local=True, local_dir=str(root)),
                    version="v20261113")
            bucketed = StagingStorageResource(use_local=False, bucket="some-bucket")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_railroad_grade_crossings(bucketed, version="v20261113")
            prefixed = StagingStorageResource(use_local=True, local_dir=str(root), prefix="sub")
            with self.assertRaisesRegex(ValueError, "explicit_local_staging_resource_required"):
                source.capture_railroad_grade_crossings(prefixed, version="v20261113")

    def test_invalid_version_is_rejected(self):
        from dagster_hifld.resources import StagingStorageResource
        with tempfile.TemporaryDirectory() as temporary:
            storage = StagingStorageResource(use_local=True, local_dir=temporary, prefix="")
            with self.assertRaisesRegex(ValueError, "invalid_version"):
                source.capture_railroad_grade_crossings(storage, version="v/invalid")

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/railroad-grade-crossings.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/railroad-grade-crossings.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)

    def _seal_catalog_cache_flow(self, scenario="happy"):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        fields, features, fixture_captures, schema, geom, counts, bounds = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        error_once = {}

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes, before_retain=None):
            if (scenario == "retry" and name == "page_000000_00002"
                    and error_once.get(name, 0) == 0) or (scenario == "refused_parent"
                                                          and name == "page_000000_00004"):
                error_once[name] = error_once.get(name, 0) + 1
                body = error_body
            else:
                body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-13T00:00:00+00:00",
                       "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest, "content_encoding": None,
                       "content_length": str(len(body)), "error_code": None}
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            return body, receipt

        class EmptyClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        pins = _pins(features, schema, geom, counts, bounds)
        if scenario == "refused_parent":
            pins.update(PAGE_START_RECORDS=4, PAGE_PROBE_ATTEMPTS=1, DOC_CAPTURE_ATTEMPTS=1)
        with tempfile.TemporaryDirectory(prefix="railroad-grade-crossings-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **pins), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_railroad_grade_crossings(
                    storage, version="vfixture", capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 6)
                self.assertEqual(candidate["source_null_geometry_count"], counts[0])
                self.assertEqual(candidate["source_invalid_geometry_count"], counts[1])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), body)
                capture_manifest = json.loads((Path(candidate["capture_directory"])
                                               / "capture_manifest.json").read_bytes())
                expected_evidence = {"retry": "page_000000_00002.attempt-01",
                                     "refused_parent": "page_000000_00004"}.get(scenario)
                if expected_evidence is None:
                    self.assertEqual(capture_manifest["evidence"], {})
                else:
                    self.assertEqual(sorted(capture_manifest["evidence"]), [expected_evidence])
                    descriptor = capture_manifest["evidence"][expected_evidence]["files"][".body"]
                    self.assertEqual(descriptor["sha256"], hashlib.sha256(error_body).hexdigest())
                    stem = expected_evidence
                    for suffix in (".body", ".wire", ".receipt.json"):
                        sealed = storage.read_key(f"{prefix}/raw/evidence/{stem}{suffix}")
                        on_disk = (Path(candidate["capture_directory"]) / f"{stem}{suffix}").read_bytes()
                        self.assertEqual(sealed, on_disk)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/evidence/{stem}.body"), error_body)
                    self.assertEqual(capture_manifest["evidence"][expected_evidence]["kind"],
                                     "failed_attempt" if scenario == "retry" else "refused_page")
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                         candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 1)
                self.assertFalse(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG,
                                       file_slug=source.FILE_SLUG, version=candidate["version"],
                                       quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_railroad_grade_crossings(
                    storage, version="vfixture", capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])
                if expected_evidence is not None:
                    self.assertEqual(storage.read_key(
                        f"{prefix}/raw/evidence/{expected_evidence}.body"), error_body)
                    self.assertTrue(storage.object_snapshot(
                        f"{prefix}/raw/evidence/{expected_evidence}.body"))

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_retry_to_seal_preserves_failed_attempt_evidence(self):
        self._seal_catalog_cache_flow(scenario="retry")

    def test_refused_parent_page_is_sealed_as_evidence(self):
        self._seal_catalog_cache_flow(scenario="refused_parent")

    def test_unexpected_capture_evidence_file_fails_closed(self):
        _, features, fixture_captures, _, _, _, _ = _fixture()
        captures = {name: (body, {"status": 200}) for name, (body, _) in fixture_captures.items()}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "rogue.body").write_bytes(b"unexpected")
            with self.assertRaisesRegex(ValueError, "unsafe_capture_evidence_file:rogue.body"):
                source._capture_evidence(root, captures)

    def test_evidence_receipt_pin_mismatch_fails_closed(self):
        _, features, fixture_captures, _, _, _, _ = _fixture()
        captures = {name: (body, {"status": 200}) for name, (body, _) in fixture_captures.items()}
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        receipt = {"status": 200, "complete_body": True, "bytes": len(error_body),
                   "sha256": hashlib.sha256(error_body).hexdigest()}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "ids_seed.attempt-01.body").write_bytes(error_body)
            (root / "ids_seed.attempt-01.wire").write_bytes(error_body)
            (root / "ids_seed.attempt-01.receipt.json").write_bytes(
                _canonical({**receipt, "bytes": len(error_body) + 1}) + b"\n")
            with self.assertRaisesRegex(ValueError,
                                        "capture_evidence_receipt_mismatch:ids_seed.attempt-01.body"):
                source._capture_evidence(root, captures)

    def test_capture_budget_refuses_before_retaining_blocks(self):
        _, _features, fixture_captures, _schema, _geom, _counts, _bounds = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        page_names = ["page_000000_00002", "page_000002_00002", "page_000004_00002"]

        def handler(request):
            params = request.url.params
            name = source._page_name(int(params["resultOffset"]), int(params["resultRecordCount"]))
            return httpx.Response(200, request=request, stream=httpx.ByteStream(bodies[name]))

        client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
        budget = 2 * (len(bodies["page_000000_00002"]) + len(bodies["page_000002_00002"]))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            ledger = source._CaptureLedger(budget)
            with patch.multiple(source, PAGE_START_RECORDS=2), patch.object(source.time, "sleep"):
                with self.assertRaisesRegex(ValueError, "source_capture_exceeds_scratch_budget"):
                    source._capture_pages(client, root, 6, ledger)
            self.assertEqual(ledger.retained, budget)
            self.assertLessEqual(sum(path.stat().st_size for path in root.iterdir()
                                     if path.name.endswith((".body", ".wire"))), budget)
            # The refused page's delivered blocks were never retained.
            self.assertEqual((root / "page_000004_00002.body").read_bytes(), b"")

    def test_capture_budget_charges_rejected_retries(self):
        ok_body = b'{"objectIds": []}'
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'
        responses = [error_body, ok_body]

        def handler(request):
            return httpx.Response(200, request=request,
                                  stream=httpx.ByteStream(responses.pop(0)))

        client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            ledger = source._CaptureLedger(1 << 20)
            with patch.object(source.time, "sleep"):
                body, _receipt = source._capture_with_retry(client, "https://producer.invalid/page",
                                                            root, "probe", max_bytes=64 * 1024 * 1024,
                                                            attempts=2, base_delay=0.0, ledger=ledger)
            self.assertEqual(body, ok_body)
            # Wire plus decoded bytes of the rejected attempt are budgeted too.
            self.assertEqual(ledger.retained, 2 * (len(error_body) + len(ok_body)))
            self.assertEqual((root / "probe.attempt-01.body").read_bytes(), error_body)

    def test_capture_budget_charges_refused_parent_pages(self):
        _, _features, fixture_captures, _schema, _geom, _counts, _bounds = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}
        error_body = b'{"error": {"code": 500, "message": "Error performing query operation"}}'

        def handler(request):
            params = request.url.params
            name = source._page_name(int(params["resultOffset"]), int(params["resultRecordCount"]))
            body = error_body if name == "page_000000_00004" else bodies[name]
            return httpx.Response(200, request=request, stream=httpx.ByteStream(body))

        client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "captures"
            root.mkdir()
            ledger = source._CaptureLedger(1 << 30)
            with patch.multiple(source, PAGE_START_RECORDS=4, PAGE_PROBE_ATTEMPTS=1,
                                DOC_CAPTURE_ATTEMPTS=1), patch.object(source.time, "sleep"):
                pages = source._capture_pages(client, root, 6, ledger)
            self.assertEqual(sorted(pages), ["page_000000_00002", "page_000002_00002", "page_000004_00002"])
            accepted = 2 * sum(len(bodies[name]) for name in sorted(pages))
            self.assertEqual(ledger.retained, 2 * len(error_body) + accepted)
            self.assertEqual((root / "page_000000_00004.body").read_bytes(), error_body)

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        self._seal_catalog_cache_flow()

    def test_packaged_runtime_without_git_seals_then_reuses_candidate(self):
        repo_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix="railroad-grade-crossings-packaged-") as temporary:
            packaged = Path(temporary) / "runtime"
            packaged.mkdir()
            shutil.copytree(repo_root / "src/dagster_hifld", packaged / "src/dagster_hifld",
                            ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("pyproject.toml", "uv.lock"):
                shutil.copy2(repo_root / name, packaged / name)
            self.assertFalse((packaged / ".git").exists())
            packaged_source = packaged / "src/dagster_hifld/sources/railroad_grade_crossings.py"
            with patch.object(source, "__file__", str(packaged_source)):
                identity = source._code_identity()
                self.assertEqual(identity["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
                self.assertEqual(identity["working_tree_status"], "unknown")
                self.assertTrue(identity["code_commit"].startswith("packaged-sha256:"))
                self._seal_catalog_cache_flow()


if __name__ == "__main__":
    unittest.main()
