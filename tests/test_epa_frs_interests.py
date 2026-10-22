"""Focused tests for the full-universe EPA FRS INTERESTS draft pipeline."""
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.epa_frs_interests import epa_frs_interests
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import epa_frs_common
from dagster_hifld.sources import epa_frs_interests as src

FIXTURE_COUNT = 5


def _fixture_rows(count: int) -> list[dict]:
    rows = []
    for objectid in range(1, count + 1):
        attrs = {
            "OBJECTID": objectid,
            "REGISTRY_ID": f"{objectid:012d}",
            "PRIMARY_NAME": f"FIXTURE FACILITY {objectid}" if objectid != 3 else None,
            "LOCATION_ADDRESS": f"{objectid} MAIN ST",
            "CITY_NAME": "WASHINGTON",
            "COUNTY_NAME": "DISTRICT OF COLUMBIA",
            "FIPS_CODE": "11001",
            "STATE_CODE": "DC",
            "POSTAL_CODE": "20400",
            "LATITUDE83": 38.9 + objectid / 1000,
            "LONGITUDE83": -77.0 - objectid / 1000,
            "HUC8_CODE": "02070010",
            "ACCURACY_VALUE": 15,
            "COLLECT_MTH_DESC": "INTERPOLATION-MAP",
            "REF_POINT_DESC": None,
            "CREATE_DATE": 1171973343000,
            "UPDATE_DATE": 1472907416000 if objectid != 2 else None,
            "LAST_REPORTED_DATE": None,
            "FAC_URL": "https://ofmpub.epa.gov/frs_public2/fii_query_detail.disp_program_facility",
            "PGM_SYS_ID": "DC0000001",
            "PGM_SYS_ACRNM": "NPDES",
            "INTEREST_TYPE": "ICIS-NPDES NON-MAJOR",
            "PROGRAM_URL": None,
            "PGM_REPORT_URL": "no data yet",
            "PUBLIC_IND": "Y",
            "ACTIVE_STATUS": "ACTIVE",
            "FEDERAL_AGENCY_NAME": "Environmental Protection Agency",
            "HUC_12": None,
            "FEDERAL_LAND_IND": None,
            "FED_FACILITY_CODE": "N",
            "EPA_REGION_CODE": "03",
            "KEY_FIELD": f"NPDES{objectid:07d}",
        }
        rows.append({"attributes": attrs,
                     "geometry": {"x": attrs["LONGITUDE83"], "y": attrs["LATITUDE83"]}})
    return rows


def _fixture_layer_fields() -> list[dict]:
    return [{"name": name, "type": esri_type, "alias": name, "nullable": nullable,
             "length": length, "domain": None}
            for name, esri_type, nullable, length in src.NATIVE_FIELDS]


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        self.body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def make_responder(rows: list[dict], *, portal=None, layer_fields=None, count=None,
                   service=None, geometry=None) -> httpx.MockTransport:
    count = count if count is not None else len(rows)
    portal = portal or {
        "id": src.ITEM_ID, "owner": "EPA_GEO", "title": src.ITEM_TITLE,
        "type": "Feature Service", "url": src.SERVICE_URL, "access": "public",
        "modified": 1778694378000,
        "licenseInfo": ("This content item is licensed under the US EPA Data Licensing Agreement "
                        "for <b>public </b>use. <a href=\"https://edg.epa.gov/EPA_Data_License.html\">"
                        "License Summary</a>"),
        "accessInformation": ("This EPA Geospatial data set is generated from the following "
                              "national environmental programs."),
    }
    layer_fields = layer_fields if layer_fields is not None else _fixture_layer_fields()

    def layer_doc():
        return {"name": src.LAYER_NAME, "geometryType": "esriGeometryPoint",
                "extent": {"spatialReference": {"wkid": 4269, "latestWkid": 4269}},
                "fields": layer_fields, "objectIdField": "OBJECTID"}

    def service_doc():
        return service if service is not None else {
            "serviceItemId": src.ITEM_ID,
            "layers": [{"id": src.LAYER_ID, "name": src.LAYER_NAME}],
            "description": "FRS interests full universe service.",
        }

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if "/sharing/rest/content/items/" in path:
            return httpx.Response(200, stream=JSONStream(portal), headers={"content-type": "application/json"})
        if path.rstrip("/").endswith("FeatureServer"):
            return httpx.Response(200, stream=JSONStream(service_doc()), headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()), headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}), headers={"content-type": "application/json"})
        last = int(query["where"][0].split(">")[1])
        page_rows = [row for row in rows if row["attributes"]["OBJECTID"] > last][: src.PAGE_SIZE]
        if geometry is not None:
            page_rows = [dict(row, geometry=geometry(row)) for row in page_rows]
        return httpx.Response(200, stream=JSONStream(
            {"objectIdFieldName": "OBJECTID",
             "spatialReference": {"wkid": 4269, "latestWkid": 4269},
             "features": page_rows, "exceededTransferLimit": False}),
            headers={"content-type": "application/json"})

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport):
    import dagster_hifld.assets.epa_frs_interests as asset_module
    contexts = [
        patch.object(src, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(asset_module, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_code_identity",
                     return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                   "files": [], "working_tree_status": "unknown"}),
        patch.object(src, "validate_construction_code_identity"),
        patch.object(epa_frs_common, "validate_construction_code_identity"),
    ]
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


class EpaFrsInterestsTests(unittest.TestCase):
    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "frs-interests")
        self.assertEqual(src.ITEM_ID, "cdff193a3e3743a5bc770e2743f215b3")
        self.assertEqual(src.LAYER_ID, 0)
        self.assertEqual(src.LAYER_NAME, "FRS_INTERESTS")
        self.assertEqual(src.EXPECTED_COUNT, 7_097_772)
        self.assertEqual(len(src.NATIVE_FIELDS), 32)
        self.assertIsNone(src.SOURCE_VINTAGE)
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG] for key in keys))

    def test_keyset_pagination_contract_is_ordered_and_bounded(self):
        url = src._page_url(460000)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["OBJECTID>460000"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultRecordCount"], [str(src.PAGE_SIZE)])
        self.assertEqual(query["outSR"], ["4269"])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertLess(len(url), 400)

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows(FIXTURE_COUNT)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                                 prefix="", backend="local")
                first = src.capture_epa_frs_interests(storage, version="fixture-run",
                                                      capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_epa_frs_interests(storage, version="fixture-run-2",
                                                       capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("REGISTRY_ID").to_pylist(),
                             [f"{i:012d}" for i in range(1, FIXTURE_COUNT + 1)])

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [epa_frs_interests],
                        resources={"staging_storage": StagingStorageResource(
                            use_local=True, local_dir=str(root / "staging"), prefix="",
                            backend="local")},
                        instance=instance,
                        run_config={"ops": {"ingest__frs_interests__frs_interests": {"config": {
                            "capture_directory": str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    candidate = result.output_for_node("ingest__frs_interests__frs_interests")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_resume_reuses_retained_pages_and_discards_torn_evidence(self):
        rows = _fixture_rows(FIXTURE_COUNT)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                                 prefix="", backend="local")
                src.capture_epa_frs_interests(storage, version="resume-run",
                                              capture_directory=str(capture_root))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(len(state["data_pages"]), 1)
                # Simulate an interruption after the final data page: drop the tail witness.
                for suffix in ("body", "wire", "receipt.json"):
                    (capture_root / f"page_tail.{suffix}").unlink()
                state["tail"] = None
                (capture_root / "capture_state.json").write_bytes(src._canonical(state) + b"\n")
                fetches = {"count": 0}
                real_send = httpx.Client.send

                def counting_send(client, request, **kwargs):
                    fetches["count"] += 1
                    return real_send(client, request, **kwargs)

                with patch.object(httpx.Client, "send", counting_send):
                    result = src.fetch_frs_interests_pages(capture_root)
                self.assertTrue(result["complete"])
                self.assertLessEqual(fetches["count"], 6)
                self.assertTrue((capture_root / "page_tail.body").exists())
                # A torn retained response is never trusted without a fetch client.
                (capture_root / "count_after.body").write_bytes(b'{"count":999999999}')
                with self.assertRaisesRegex(src.EPAFRSInterestsError, "retained_capture_receipt_invalid"):
                    src._retained_or_fetch(None, capture_root, "count_after", src._count_url())
                # With a fetch client the torn response is discarded and refetched.
                with patch.object(httpx.Client, "send", counting_send):
                    src.fetch_frs_interests_pages(capture_root)
                self.assertTrue((capture_root / "page_tail.body").exists())

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows(FIXTURE_COUNT)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = {
            "source_count_drift": dict(count=FIXTURE_COUNT + 1),
            "source_schema_drift": dict(layer_fields=_fixture_layer_fields()
                                        + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                            "alias": "EXTRA", "nullable": True, "length": 4,
                                            "domain": None}]),
            "item_license_declaration_missing_or_changed": dict(
                portal={"id": src.ITEM_ID, "owner": "EPA_GEO", "title": src.ITEM_TITLE,
                        "type": "Feature Service", "url": src.SERVICE_URL, "access": "public",
                        "licenseInfo": "All rights reserved.", "accessInformation": "EPA Geospatial"}),
            "official_EPA_portal_ownership_or_publication_drift": dict(
                portal={"id": src.ITEM_ID, "owner": "mirror", "title": src.ITEM_TITLE,
                        "type": "Feature Service", "url": src.SERVICE_URL, "access": "public",
                        "licenseInfo": "US EPA Data Licensing Agreement for public use "
                                       "https://edg.epa.gov/EPA_Data_License.html",
                        "accessInformation": "EPA Geospatial"}),
            "source_point_coordinate_invalid": dict(geometry=lambda row: {"x": None, "y": 38.9}),
            "source_object_ids_not_strictly_increasing": dict(rows=[
                dict(rows[0]), dict(rows[0])], count=FIXTURE_COUNT),
            "source_nonnullable_value_is_null:REGISTRY_ID": dict(count=FIXTURE_COUNT, rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "REGISTRY_ID": None}},
                *rows[1:]]),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    rows_case = kwargs.pop("rows", rows)
                    transport = make_responder(rows_case, **kwargs)
                    with fixture_mode(schema_sha, transport):
                        storage = StagingStorageResource(use_local=True,
                                                         local_dir=str(root / "staging"),
                                                         prefix="", backend="local")
                        with self.assertRaisesRegex(src.EPAFRSInterestsError, expected_error):
                            src.capture_epa_frs_interests(storage, version="drift-run",
                                                          capture_directory=str(capture_root))
                    quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
                    self.assertTrue(failure["reason_code"])


if __name__ == "__main__":
    unittest.main()
