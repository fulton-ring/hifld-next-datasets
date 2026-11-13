"""Focused contract checks for the FEMA NFHL Study_Info source adapter."""
from __future__ import annotations

import hashlib
import json
import unittest
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.sources import nfhl_study_info as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


SERVICE_DOC_BODY = (b"<html>FEMA MSC GIS Web Services page: effective NFHL data at "
                    b"https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer</html>")

# Field order and types exactly as served by the producer's table metadata.
FIELDS = [
    {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None},
    {"name": "DFIRM_ID", "type": "esriFieldTypeString", "alias": "DFIRM_ID", "length": 6, "domain": None},
    {"name": "VERSION_ID", "type": "esriFieldTypeString", "alias": "VERSION_ID", "length": 11, "domain": None},
    {"name": "STD_NFO_ID", "type": "esriFieldTypeString", "alias": "STD_NFO_ID", "length": 32, "domain": None},
    {"name": "STUDY_PRE", "type": "esriFieldTypeString", "alias": "STUDY_PRE", "length": 19, "domain": None},
    {"name": "STUDY_NM", "type": "esriFieldTypeString", "alias": "STUDY_NM", "length": 128, "domain": None},
    {"name": "STATE_NM", "type": "esriFieldTypeString", "alias": "STATE_NM", "length": 24, "domain": None},
    {"name": "CNTY_NM", "type": "esriFieldTypeString", "alias": "CNTY_NM", "length": 128, "domain": None},
    {"name": "JURIS_TYP", "type": "esriFieldTypeString", "alias": "JURIS_TYP", "length": 22, "domain": None},
    {"name": "LG_PAN_NO", "type": "esriFieldTypeString", "alias": "LG_PAN_NO", "length": 4, "domain": None},
    {"name": "OPP_TF", "type": "esriFieldTypeString", "alias": "OPP_TF", "length": 1, "domain": None},
    {"name": "H_DATUM", "type": "esriFieldTypeString", "alias": "H_DATUM", "length": 30, "domain": None},
    {"name": "V_DATUM", "type": "esriFieldTypeString", "alias": "V_DATUM", "length": 17, "domain": None},
    {"name": "PROJECTION", "type": "esriFieldTypeString", "alias": "PROJECTION", "length": 75, "domain": None},
    {"name": "PROJ_ZONE", "type": "esriFieldTypeString", "alias": "PROJ_ZONE", "length": 5, "domain": None},
    {"name": "PROJ_UNIT", "type": "esriFieldTypeString", "alias": "PROJ_UNIT", "length": 18, "domain": None},
    {"name": "PROJ_SECND", "type": "esriFieldTypeString", "alias": "PROJ_SECND", "length": 75, "domain": None},
    {"name": "PROJ_SUNIT", "type": "esriFieldTypeString", "alias": "PROJ_SUNIT", "length": 18, "domain": None},
    {"name": "PROJ_SZONE", "type": "esriFieldTypeString", "alias": "PROJ_SZONE", "length": 5, "domain": None},
    {"name": "LANDWD_VAL", "type": "esriFieldTypeDouble", "alias": "LANDWD_VAL", "domain": None},
    {"name": "CW_TF", "type": "esriFieldTypeString", "alias": "CW_TF", "length": 1, "domain": None},
    {"name": "RTROFT_TF", "type": "esriFieldTypeString", "alias": "RTROFT_TF", "length": 1, "domain": None},
    {"name": "META_NM", "type": "esriFieldTypeString", "alias": "META_NM", "length": 50, "domain": None},
    {"name": "FIS_NM", "type": "esriFieldTypeString", "alias": "FIS_NM", "length": 15, "domain": None},
    {"name": "LOGO_NM", "type": "esriFieldTypeString", "alias": "LOGO_NM", "length": 50, "domain": None},
    {"name": "INDX_EFFDT", "type": "esriFieldTypeDate", "alias": "INDX_EFFDT", "length": 8, "domain": None},
    {"name": "DBREV_DT", "type": "esriFieldTypeDate", "alias": "DBREV_DT", "length": 8, "domain": None},
    {"name": "AVG_CFACTR", "type": "esriFieldTypeDouble", "alias": "AVG_CFACTR", "domain": None},
    {"name": "GFID", "type": "esriFieldTypeString", "alias": "GFID", "length": 36, "domain": None},
    {"name": "INDX_SUFFX", "type": "esriFieldTypeString", "alias": "INDX_SUFFX", "length": 1, "domain": None},
    {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None},
]
ATTR_NAMES = [f["name"] for f in FIELDS]

# Producer-observed value conventions retained verbatim: text sentinels ("NP",
# "FEMA"), numeric sentinels (-9999.0, -8888.0), and the 9999-12-31
# epoch-millisecond date sentinel, alongside producer nulls.
_SENTINEL_DATE = 253392451200000


def _attrs(oid, **overrides):
    values = {"OBJECTID": oid, "DFIRM_ID": "01075C", "VERSION_ID": "1.0.0.0",
              "STD_NFO_ID": f"01075C_{oid}", "STUDY_PRE": None, "STUDY_NM": "LAMAR COUNTY",
              "STATE_NM": "Alabama", "CNTY_NM": "LAMAR COUNTY", "JURIS_TYP": "And Incorporated Areas",
              "LG_PAN_NO": "0425", "OPP_TF": "F", "H_DATUM": "NAD83", "V_DATUM": "NAVD 88",
              "PROJECTION": "UTM", "PROJ_ZONE": "16", "PROJ_UNIT": "METERS", "PROJ_SECND": None,
              "PROJ_SUNIT": None, "PROJ_SZONE": None, "LANDWD_VAL": -8888.0, "CW_TF": "T",
              "RTROFT_TF": "T", "META_NM": "01075C_20100503_metadata.txt", "FIS_NM": "NP",
              "LOGO_NM": "NP", "INDX_EFFDT": 1601251200000, "DBREV_DT": None,
              "AVG_CFACTR": -9999.0, "GFID": None, "INDX_SUFFX": None,
              "GlobalID": "{496A16C2-39E5-48AB-A8AF-342D26EB6D81}"}
    values.update(overrides)
    return {name: values[name] for name in ATTR_NAMES}


def _fixture():
    # Fixtures pin maxRecordCount together with PAGE_SIZE (3) so page tiling
    # exercises multi-page slicing; production pins both at the producer's 2000.
    table = {"id": 41, "name": "Study_Info", "type": "Table", "geometryType": None,
             "maxRecordCount": 3,
             "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
             "fields": FIELDS}
    service = {"mapName": "NFHL",
               "layers": [{"id": 1, "name": "LOMRs", "type": "Feature Layer"}],
               "tables": [{"id": 41, "name": "Study_Info", "type": "Table"}]}
    portal_item = {"id": source.PORTAL_ITEM_ID, "owner": source.PORTAL_ITEM_OWNER,
                   "orgId": source.PORTAL_ITEM_ORG_ID, "title": source.PORTAL_ITEM_TITLE,
                   "type": "Map Service", "url": source.SERVICE_URL, "modified": 1724335999000,
                   "access": "public"}
    portal_org = {"id": source.PORTAL_ITEM_ORG_ID, "name": source.PORTAL_ITEM_ORG_NAME,
                  "url": "https://fema-agol.maps.arcgis.com", "created": 1455100000000}
    features = [
        # Full populated row with the producer's numeric sentinels.
        {"attributes": _attrs(1541)},
        # Row with producer nulls in optional second-projection and suffix fields.
        {"attributes": _attrs(1542, STD_NFO_ID=None, PROJ_ZONE=None, DBREV_DT=_SENTINEL_DATE)},
        # Row with the 9999-12-31 date sentinel and text sentinels.
        {"attributes": _attrs(1543, INDX_EFFDT=_SENTINEL_DATE, FIS_NM="NP", LOGO_NM="FEMA",
                              LANDWD_VAL=601.1, AVG_CFACTR=9.0, GFID="f798d519-5c2d-442a-bab4-6fa51eadad69")},
        {"attributes": _attrs(1544, STUDY_NM=None, META_NM=None)},
        {"attributes": _attrs(1545, INDX_SUFFX="C")},
    ]
    ids = [1541, 1542, 1543, 1544, 1545]
    schema = [{k: f[k] for k in ("name", "type", "alias", "length", "nullable", "domain") if k in f}
              for f in FIELDS]
    pages = [features[0:3], features[3:5]]
    page_docs = [{"features": page, **({"exceededTransferLimit": True} if index < len(pages) - 1 else {})}
                 for index, page in enumerate(pages)]
    docs = {"service_before": service, "service_after": service, "table_before": table,
            "table_after": table, "portal_item": portal_item, "portal_org": portal_org,
            "count_before": {"count": 5}, "count_after": {"count": 5},
            "ids_before": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_after": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "ids_seed": {"objectIdFieldName": "OBJECTID", "objectIds": ids},
            "page_000": page_docs[0], "page_001": page_docs[1]}
    captures = {name: (json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode(), {"status": 200})
                for name, doc in docs.items()}
    captures["service_doc"] = (SERVICE_DOC_BODY, {"status": 200})
    return table, features, captures, schema


def _null_pins(features):
    counts = {name: 0 for name in ATTR_NAMES}
    for feature in features:
        for name, value in feature["attributes"].items():
            if value is None:
                counts[name] += 1
    return counts, sum(counts.values())


def _pins(features, schema):
    nulls, total = _null_pins(features)
    sorted_features = sorted(features, key=lambda row: row["attributes"]["OBJECTID"])
    return dict(EXPECTED_COUNT=5, PAGE_SIZE=3,
                EXPECTED_ID_SHA256=hashlib.sha256(_canonical(_ids(features))).hexdigest(),
                EXPECTED_RECORD_SHA256=hashlib.sha256(_canonical(sorted_features)).hexdigest(),
                EXPECTED_SCHEMA_SHA256=hashlib.sha256(_canonical(schema)).hexdigest(),
                EXPECTED_NULL_VALUE_COUNTS=nulls,
                EXPECTED_NULL_VALUE_TOTAL=total,
                EXPECTED_SERVICE_LAYER_ROSTER=[(1, "LOMRs", "Feature Layer")],
                EXPECTED_SERVICE_TABLES_ROSTER=[(41, "Study_Info", "Table")],
                EXPECTED_PORTAL_ITEM={"id": source.PORTAL_ITEM_ID, "owner": source.PORTAL_ITEM_OWNER,
                                      "orgId": source.PORTAL_ITEM_ORG_ID, "title": source.PORTAL_ITEM_TITLE,
                                      "type": "Map Service", "url": source.SERVICE_URL},
                EXPECTED_PORTAL_ORG={"id": source.PORTAL_ITEM_ORG_ID, "name": source.PORTAL_ITEM_ORG_NAME})


def _ids(features):
    return [row["attributes"]["OBJECTID"] for row in features]


def _validated(features, captures, schema):
    with patch.multiple(source, **_pins(features, schema)):
        return source._validate_captures(captures)


def _page_features(captures):
    return [row for i in range(2) for row in json.loads(captures[f"page_{i:03d}"][0])["features"]]


class NfhlStudyInfoTests(unittest.TestCase):
    def test_table_values_nulls_and_sentinels_preserved_in_parquet(self):
        _, features, captures, schema = _fixture()
        _, original_fields, extracted = _validated(features, captures, schema)
        body = source._parquet_bytes(original_fields, extracted["features"])
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 5)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1541, 1542, 1543, 1544, 1545])
        self.assertEqual(table.column("STD_NFO_ID").to_pylist(),
                         ["01075C_1541", None, "01075C_1543", "01075C_1544", "01075C_1545"])
        self.assertEqual(table.column("STUDY_NM").to_pylist(),
                         ["LAMAR COUNTY", "LAMAR COUNTY", "LAMAR COUNTY", None, "LAMAR COUNTY"])
        # Epoch-millisecond date fields stay int64 with raw producer values.
        self.assertEqual(table.column("INDX_EFFDT").type.__str__(), "int64")
        self.assertEqual(table.column("INDX_EFFDT").to_pylist(),
                         [1601251200000, 1601251200000, 253392451200000, 1601251200000, 1601251200000])
        self.assertEqual(table.column("DBREV_DT").type.__str__(), "int64")
        self.assertEqual(table.column("DBREV_DT").to_pylist(),
                         [None, 253392451200000, None, None, None])
        # Numeric sentinels retained verbatim, never coerced.
        self.assertEqual(table.column("LANDWD_VAL").to_pylist(),
                         [-8888.0, -8888.0, 601.1, -8888.0, -8888.0])
        self.assertEqual(table.column("AVG_CFACTR").to_pylist(),
                         [-9999.0, -9999.0, 9.0, -9999.0, -9999.0])
        self.assertEqual(table.column("FIS_NM").to_pylist(), ["NP", "NP", "NP", "NP", "NP"])
        self.assertEqual(table.column("GlobalID").type.__str__(), "string")
        schema_metadata = table.schema.metadata or {}
        self.assertEqual(schema_metadata[b"hifld:geometry"], b"absent")
        self.assertEqual(schema_metadata[b"hifld:source_table"], b"Study_Info")
        self.assertEqual(table.column_names, ATTR_NAMES)
        original = json.loads(table.schema.field("INDX_EFFDT").metadata[b"original_esri_field"])
        self.assertEqual(original["name"], "INDX_EFFDT")
        self.assertEqual(original["type"], "esriFieldTypeDate")
        self.assertEqual(original["length"], 8)

    def test_id_roster_response_order_is_not_population_drift(self):
        _, features, captures, schema = _fixture()
        for name, order in (("ids_seed", [1543, 1541, 1542, 1545, 1544]),
                            ("ids_before", [1542, 1541, 1543, 1544, 1545]),
                            ("ids_after", [1541, 1542, 1543, 1544, 1545])):
            doc = json.loads(captures[name][0])
            doc["objectIds"] = order
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), captures[name][1])
        _, _, extracted = _validated(features, captures, schema)
        self.assertEqual(extracted["ids"], [1541, 1542, 1543, 1544, 1545])
        self.assertEqual(extracted["audit"]["null_value_total"], _null_pins(features)[1])

    def test_missing_service_documentation_link_is_rejected(self):
        _, features, captures, schema = _fixture()
        captures["service_doc"] = (b"<html>unrelated</html>", {"status": 200})
        with self.assertRaisesRegex(ValueError, "fema_service_documentation_link_drift"):
            _validated(features, captures, schema)

    def test_geometry_in_table_row_is_rejected(self):
        _, features, captures, schema = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["geometry"] = None
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_table_returned_unexpected_geometry"):
            _validated(features, captures, schema)

    def test_field_type_drift_is_rejected(self):
        _, features, captures, schema = _fixture()
        table = json.loads(captures["table_before"][0])
        table["fields"][25]["type"] = "esriFieldTypeString"
        captures["table_before"] = (json.dumps(table, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
            _validated(features, captures, schema)

    def test_unexpected_field_is_quarantined_by_contract(self):
        _, features, captures, schema = _fixture()
        table = json.loads(captures["table_before"][0])
        table["fields"].append({"name": "SURPRISE", "type": "esriFieldTypeString"})
        captures["table_before"] = (json.dumps(table, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_schema_contract_drift"):
            _validated(features, captures, schema)

    def test_count_drift_is_rejected(self):
        _, features, captures, schema = _fixture()
        for name in ("count_before", "count_after"):
            doc = json.loads(captures[name][0])
            doc["count"] = 2655
            captures[name] = (json.dumps(doc, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_count_drift"):
            _validated(features, captures, schema)

    def test_final_page_cannot_claim_transfer_truncation(self):
        _, features, captures, schema = _fixture()
        final = json.loads(captures["page_001"][0])
        final["exceededTransferLimit"] = True
        captures["page_001"] = (json.dumps(final, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_page_unexpected_exceeded_transfer_limit"):
            _validated(features, captures, schema)

    def test_missing_page_id_is_not_accepted(self):
        _, features, captures, schema = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"] = page["features"][:2]
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_page_id_slice_incomplete_or_reordered"):
            _validated(features, captures, schema)

    def test_record_value_change_is_detected(self):
        _, features, captures, schema = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["STATUS"] = "Retired"
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_record_attribute_schema_drift"):
            _validated(features, captures, schema)

    def test_null_count_change_is_detected(self):
        _, features, captures, schema = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["STUDY_NM"] = None
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        pins = _pins(features, schema)
        pins["EXPECTED_RECORD_SHA256"] = hashlib.sha256(_canonical(
            sorted(_page_features(captures), key=lambda row: row["attributes"]["OBJECTID"]))).hexdigest()
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_null_value_population_drift"):
                source._validate_captures(captures)

    def test_value_type_change_is_detected(self):
        _, features, captures, schema = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["STUDY_NM"] = 42
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_native_string_type_changed"):
            _validated(features, captures, schema)

    def test_date_value_type_change_is_detected(self):
        _, features, captures, schema = _fixture()
        page = json.loads(captures["page_000"][0])
        page["features"][0]["attributes"]["INDX_EFFDT"] = "2020-09-28"
        captures["page_000"] = (json.dumps(page, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_native_integer_type_changed"):
            _validated(features, captures, schema)

    def test_portal_item_identity_drift_is_rejected(self):
        _, features, captures, schema = _fixture()
        item = json.loads(captures["portal_item"][0])
        item["owner"] = "not_fema"
        captures["portal_item"] = (json.dumps(item, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "fema_portal_service_item_identity_drift"):
            _validated(features, captures, schema)

    def test_tables_roster_drift_is_rejected(self):
        _, features, captures, schema = _fixture()
        service = json.loads(captures["service_before"][0])
        service["tables"] = []
        captures["service_before"] = (json.dumps(service, separators=(",", ":")).encode(), {"status": 200})
        with self.assertRaisesRegex(ValueError, "fema_nfhl_service_tables_roster_drift"):
            _validated(features, captures, schema)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        import os
        import tempfile

        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          f"unknown/{source.DATASET_SLUG}.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           f"unknown/{source.DATASET_SLUG}.parquet", b"accepted")
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

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        import tempfile
        from pathlib import Path

        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        _, features, fixture_captures, schema = _fixture()
        bodies = {name: body for name, (body, _) in fixture_captures.items()}

        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = bodies[name]
            digest = hashlib.sha256(body).hexdigest()
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-12T00:00:00+00:00",
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

        with tempfile.TemporaryDirectory(prefix="nfhl-study-info-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **_pins(features, schema)), \
                 patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_nfhl_study_info(storage, version="vfixture",
                                                           capture_directory=str(root / "captures"))
                self.assertEqual(candidate["row_count"], 5)
                self.assertEqual(candidate["geometry"], "absent")
                self.assertEqual(candidate["source_null_value_total"], _null_pins(features)[1])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                for name, body in bodies.items():
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.body"), body)
                    self.assertEqual(storage.read_key(f"{prefix}/raw/{name}.wire"), body)
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                         candidate["version"]).metadata
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                   candidate["version"], source.DATASET_SLUG,
                                                   source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["feature_count"], 5)
                self.assertEqual(summary.quality_manifest["geometry_type"], None)
                self.assertEqual(summary.quality_manifest["spatial_status"], "non_spatial_source")
                self.assertTrue(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                       version=candidate["version"], quality_dict=summary.quality_manifest,
                                       dictionary_dict=summary.data_dictionary)
                reused = source.capture_nfhl_study_info(storage, version="vfixture",
                                                        capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])


if __name__ == "__main__":
    unittest.main()
