"""Focused tests for the duplicate EPA FRS Power Plants inventory entry (issue #392)."""
import io
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq
import shapely

from dagster_hifld.definitions import defs
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common
from dagster_hifld.sources import epa_power_generation as src
from dagster_hifld.sources.epa_power_generation import _parquet_bytes
from dagster_hifld.sources.esri_common import canonical as _canonical, sha as _sha

FIXTURE_COUNT = 5


def _fixture_layer_fields() -> list[dict]:
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "nullable": False, "domain": None},
        {"name": "OBJECTID_1", "type": "esriFieldTypeInteger", "alias": "OBJECTID", "nullable": False, "domain": None},
        {"name": "UTILITY_ID", "type": "esriFieldTypeInteger", "alias": "UTILITY_ID", "nullable": True, "domain": None},
        {"name": "UTILITY_NAME", "type": "esriFieldTypeString", "alias": "UTILITY_NAME", "nullable": True, "length": 40, "domain": None},
        {"name": "PLANT_CODE", "type": "esriFieldTypeInteger", "alias": "PLANT_CODE", "nullable": True, "domain": None},
        {"name": "PLANT_NAME", "type": "esriFieldTypeString", "alias": "PLANT_NAME", "nullable": False, "length": 40, "domain": None},
        {"name": "FIPS_CODE", "type": "esriFieldTypeString", "alias": "FIPS_CODE", "nullable": True, "length": 5, "domain": None},
        {"name": "STATE", "type": "esriFieldTypeString", "alias": "STATE", "nullable": True, "length": 2, "domain": None},
        {"name": "LATITUDE", "type": "esriFieldTypeDouble", "alias": "LATITUDE", "nullable": True, "domain": None},
        {"name": "LONGITUDE", "type": "esriFieldTypeDouble", "alias": "LONGITUDE", "nullable": True, "domain": None},
    ]
    return fields


def _fixture_rows(count: int) -> list[dict]:
    rows = []
    for objectid in range(1, count + 1):
        rows.append({
            "attributes": {
                "OBJECTID": objectid,
                "OBJECTID_1": objectid,
                "UTILITY_ID": 10000 + objectid,
                "UTILITY_NAME": None if objectid == 3 else f"FIXTURE UTILITY {objectid}",
                "PLANT_CODE": 2000 + objectid,
                "PLANT_NAME": f"FIXTURE PLANT {objectid}",
                "FIPS_CODE": f"{1000 + objectid:05d}",
                "STATE": "DC",
                "LATITUDE": 38.9 + objectid / 1000,
                "LONGITUDE": -77.0 - objectid / 1000,
            },
            "geometry": {"x": -8571203.0 + objectid, "y": 4709211.0 + objectid},
        })
    return rows


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        self.body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


class ByteBodyStream(httpx.SyncByteStream):
    def __init__(self, body: bytes):
        self.body = body

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def make_responder(rows: list[dict]) -> httpx.MockTransport:
    portal = {
        "id": src.ITEM_ID, "owner": "EPA_GEO", "title": "FRS US Power Generation Facilities",
        "type": "Feature Service", "url": src.SERVICE_URL, "access": "public",
        "modified": 1769190978000,
    }

    def layer_doc():
        return {"name": src.LAYER_NAME, "geometryType": "esriGeometryPoint",
                "extent": {"spatialReference": {"wkid": 102100, "latestWkid": 3857}},
                "fields": _fixture_layer_fields(), "objectIdField": "OBJECTID"}

    def service_doc():
        return {
            "serviceItemId": src.ITEM_ID, "name": "FRS_PowerPlants",
            "description": ("US Power Generation Facilities, compiled from most-current "
                            "(as of June 2014) Energy Information Administration EIA-860 "
                            "powerplant data, together with USEPA Facility Registry Service data."),
            "layers": [{"id": src.LAYER_ID, "name": src.LAYER_NAME}],
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
            return httpx.Response(200, stream=JSONStream({"count": len(rows)}), headers={"content-type": "application/json"})
        if query.get("returnIdsOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream(
                {"objectIds": [row["attributes"]["OBJECTID"] for row in rows]}),
                headers={"content-type": "application/json"})
        if "objectIds" in query:
            requested = {int(x) for x in query["objectIds"][0].split(",") if x}
            page_rows = [row for row in rows if row["attributes"]["OBJECTID"] in requested]
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID",
                 "spatialReference": {"wkid": 102100, "latestWkid": 3857},
                 "features": page_rows, "exceededTransferLimit": False}),
                headers={"content-type": "application/json"})
        html = (b"<html><body>US Power Generation Facilities, compiled from most-current "
                b"(as of June 2014) Energy Information Administration (EIA-860) Database "
                b"powerplant data...</body></html>")
        return httpx.Response(200, stream=ByteBodyStream(html), headers={"content-type": "text/html"})

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, ids_sha: str, transport: httpx.MockTransport):
    import dagster_hifld.assets.epa_facility_registry_service_frs_power_plants as asset_module
    contexts = [
        patch.object(src, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "EXPECTED_ID_SHA256", ids_sha),
        patch.object(src, "EXPECTED_RECORD_SHA256", ""),
        patch.object(src, "EXPECTED_GEOMETRY_SHA256", ""),
        patch.object(asset_module, "EXPECTED_COUNT", FIXTURE_COUNT),
        patch.object(src, "_default_client", side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_code_identity",
                     return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                   "files": [], "working_tree_status": "unknown"}),
        patch.object(src, "validate_construction_code_identity"),
        patch.object(capture_common, "validate_construction_code_identity"),
    ]
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


class RegistryEntryIdentityTests(unittest.TestCase):
    """The duplicate ``epa-facility-registry-service-frs-power-plants-2`` inventory entry.

    The HIFLD Open catalog lists the same EPA FRS/EIA-860 power generation product
    twice (identical description, agency and keywords); this entry stages its own
    candidate identity from the same maintained source and pins.
    """

    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "epa-frs-eia-860-power-generation-facilities-by-source")
        self.assertEqual(src.EPA_ITEM_DATASET_SLUG, "epa-facility-registry-service-frs-power-plants-2")
        self.assertIn(src.EPA_ITEM_DATASET_SLUG, src.DATASET_SLUGS)
        self.assertEqual(src._DEFAULT_CAPTURE_ROOTS[src.DATASET_SLUG], "69")
        self.assertEqual(src._DEFAULT_CAPTURE_ROOTS[src.EPA_ITEM_DATASET_SLUG], "392")
        self.assertEqual(src.ITEM_ID, "2a1459ac9b03469e87c2e7785cc188a9")
        self.assertEqual(src.LAYER_ID, 4)
        self.assertEqual(src.LAYER_NAME, "EPA Facility Registry Service - EIA-860 Power Generation Facilities By Source")
        self.assertEqual(src.EXPECTED_COUNT, 2326)
        self.assertEqual(src.EXPECTED_SCHEMA_SHA256,
                         "5a8de30d69b9f91343f5cd85e21c0cda33bedb7b768551a126c9f77a28ffddff")
        self.assertEqual(src.SOURCE_VINTAGE, "2014-06")

    def test_registry_entry_is_registered_with_its_own_identity(self):
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.EPA_ITEM_DATASET_SLUG, src.EPA_ITEM_DATASET_SLUG]
                            for key in keys))
        check_keys = defs.resolve_asset_graph().asset_check_keys
        self.assertTrue(any(key.to_user_string().endswith(":epa_facility_registry_service_frs_power_plants_complete")
                            for key in check_keys))
        job = defs.get_job_def("epa_facility_registry_service_frs_power_plants_draft_refresh")
        self.assertIsNotNone(job)
        schedule = defs.get_schedule_def("epa_facility_registry_service_frs_power_plants_draft_refresh_schedule")
        self.assertIsNotNone(schedule)
        self.assertEqual(schedule.default_status.value if hasattr(schedule.default_status, "value")
                         else schedule.default_status, "STOPPED")

    def test_unknown_dataset_slug_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                             prefix="", backend="local")
            with self.assertRaisesRegex(ValueError, "unknown_dataset_slug:not-a-known-inventory-entry"):
                src.capture_epa_power_generation(storage, version="slug-run",
                                                 dataset_slug="not-a-known-inventory-entry")
            self.assertEqual(sorted(p.name for p in (root / "staging").rglob("*") if p.is_file()), [])

    def test_registry_entry_fixture_capture_stages_under_its_own_identity(self):
        rows = _fixture_rows(FIXTURE_COUNT)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        ids_sha = _sha(_canonical(sorted(row["attributes"]["OBJECTID"] for row in rows)))
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            captures_root = root / "captures"
            with fixture_mode(schema_sha, ids_sha, transport):
                storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                                 prefix="", backend="local")
                first = src.capture_epa_power_generation(
                    storage, version="registry-entry-run", capture_directory=str(captures_root),
                    dataset_slug=src.EPA_ITEM_DATASET_SLUG)
                self.assertEqual(first["dataset_slug"], src.EPA_ITEM_DATASET_SLUG)
                self.assertEqual(first["file_slug"], src.EPA_ITEM_DATASET_SLUG)
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                second = src.capture_epa_power_generation(
                    storage, version="registry-entry-run-2", capture_directory=str(captures_root),
                    dataset_slug=src.EPA_ITEM_DATASET_SLUG)
            self.assertEqual(second["version"], first["version"])
            self.assertTrue(second["reused"])
            self.assertFalse(second["release_accepted"])
            prefix = f"{src.EPA_ITEM_DATASET_SLUG}/{src.EPA_ITEM_DATASET_SLUG}/{first['version']}"
            sealed = storage.read_key(f"{prefix}/geoparquet/{src.EPA_ITEM_DATASET_SLUG}.parquet")
            self.assertEqual(pq.read_table(io.BytesIO(sealed)).num_rows, FIXTURE_COUNT)
            table = pq.read_table(io.BytesIO(sealed))
            self.assertEqual(table.column("FIPS_CODE").to_pylist(),
                             [f"{1000 + i:05d}" for i in range(1, FIXTURE_COUNT + 1)])
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertEqual(manifest["title"], src.LAYER_NAME)
            self.assertEqual(manifest["agency"], "Environmental Protection Agency")
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["inputs"][0]["host"], "services.arcgis.com")
            self.assertTrue(storage.read_key(f"{prefix}/raw/ids_seed.receipt.json"))
            self.assertTrue(storage.read_key(f"{prefix}/raw/portal_item.receipt.json"))
            self.assertTrue(storage.read_key(f"{prefix}/raw/page_000.receipt.json"))
            ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["release_accepted"])

    def test_both_inventory_identities_stage_their_own_candidates(self):
        rows = _fixture_rows(FIXTURE_COUNT)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        ids_sha = _sha(_canonical(sorted(row["attributes"]["OBJECTID"] for row in rows)))
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            captures_root = root / "captures"
            with fixture_mode(schema_sha, ids_sha, transport):
                storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                                 prefix="", backend="local")
                duplicate = src.capture_epa_power_generation(
                    storage, version="both-identity-run", capture_directory=str(captures_root),
                    dataset_slug=src.EPA_ITEM_DATASET_SLUG)
                original = src.capture_epa_power_generation(
                    storage, version="both-identity-run-2", capture_directory=str(captures_root),
                    dataset_slug=src.DATASET_SLUG)
            self.assertEqual(original["dataset_slug"], src.DATASET_SLUG)
            self.assertFalse(original["reused"])
            self.assertEqual(original["version"], duplicate["version"])
            for slug in src.DATASET_SLUGS:
                prefix = f"{slug}/{slug}/{duplicate['version']}"
                sealed = storage.read_key(f"{prefix}/geoparquet/{slug}.parquet")
                self.assertEqual(pq.read_table(io.BytesIO(sealed)).num_rows, FIXTURE_COUNT)
            duplicate_parquet = storage.read_key(
                f"{src.EPA_ITEM_DATASET_SLUG}/{src.EPA_ITEM_DATASET_SLUG}/{duplicate['version']}"
                f"/geoparquet/{src.EPA_ITEM_DATASET_SLUG}.parquet")
            original_parquet = storage.read_key(
                f"{src.DATASET_SLUG}/{src.DATASET_SLUG}/{duplicate['version']}"
                f"/geoparquet/{src.DATASET_SLUG}.parquet")
            self.assertEqual(_sha(duplicate_parquet), _sha(original_parquet))

    def test_registry_entry_drift_quarantines_under_its_own_slug(self):
        rows = _fixture_rows(FIXTURE_COUNT)
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        ids_sha = _sha(_canonical(sorted(row["attributes"]["OBJECTID"] for row in rows)))
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            captures_root = root / "captures"
            with fixture_mode(schema_sha, ids_sha, transport):
                storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                                 prefix="", backend="local")
                src.capture_epa_power_generation(storage, version="registry-pin-run",
                                                 capture_directory=str(captures_root),
                                                 dataset_slug=src.EPA_ITEM_DATASET_SLUG)
                with patch.object(src, "EXPECTED_RECORD_SHA256", "0" * 64):
                    with self.assertRaisesRegex(ValueError, "source_value_or_geometry_baseline_drift"):
                        src.capture_epa_power_generation(
                            storage, version="registry-pin-drift",
                            capture_directory=str(captures_root),
                            dataset_slug=src.EPA_ITEM_DATASET_SLUG)
            quarantined = sorted((root / "staging" / "_quarantine"
                                  / src.EPA_ITEM_DATASET_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"], "ValueError")
            self.assertIn("page_000", failure["captured_names"])
            self.assertEqual(storage.list_prefix(src.DATASET_SLUG), [])

    def test_registry_entry_resolves_its_own_inventory_row(self):
        from dagster_hifld.source_manifest import _inventory_metadata_for

        metadata = _inventory_metadata_for(src.EPA_ITEM_DATASET_SLUG, src.EPA_ITEM_DATASET_SLUG, "v1.0.0")
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata["inventory_match_type"], "filename_same_root")
        self.assertEqual(metadata["title"], "Environmental Protection Agency (EPA) Facility Registry Service (FRS) Power Plants")
        self.assertEqual(metadata["agency"], "Environmental Protection Agency")
        self.assertEqual(metadata["publisher"], "Environmental Protection Agency")
        self.assertEqual(metadata["date_issued"], "2024-03-28")
        other = _inventory_metadata_for(src.DATASET_SLUG, src.DATASET_SLUG, "v1.0.0")
        self.assertIsNotNone(other)
        self.assertEqual(other["inventory_match_type"], "filename_same_root")
        self.assertEqual(other["title"], "EPA Facility Registry Service - EIA-860 Power Generation Facilities By Source")
        self.assertEqual(other["agency"], "Environmental Protection Agency")


class NativeParquetContractTests(unittest.TestCase):
    """The native candidate keeps IDs, zero-padded strings and point geometry."""

    def test_native_geoparquet_preserves_source_ids_zeroes_strings_and_point(self):
        fields = _fixture_layer_fields()
        source_rows = _fixture_rows(3)
        body = _parquet_bytes(fields, source_rows)
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 2, 3])
        self.assertEqual(table.column("FIPS_CODE").to_pylist(), ["01001", "01002", "01003"])
        self.assertIsNone(table.column("UTILITY_NAME").to_pylist()[2])
        primary, _column, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 3857)
        point = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertAlmostEqual(point.x, -8571202.0)
        self.assertAlmostEqual(point.y, 4709212.0)


if __name__ == "__main__":
    unittest.main()
