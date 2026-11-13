from __future__ import annotations

import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
from dagster import DagsterInstance, materialize

from dagster_hifld.assets import usgs_nldi as nldi
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME, build_publish_partition_key
from dagster_hifld.resources import StagingStorageResource

FIXTURE_COUNT = 7
FIXTURE_PAGE_LIMIT = 3
ASSET_NAME = "ingest__nhd__usgs_hydro_nldi_streamgage_monitoring_locations"


def _fixture_identifier(index: int) -> str:
    return f"{1000001 + index:09d}"


def _fixture_feature(index: int) -> dict:
    identifier = _fixture_identifier(index)
    return {"type": "Feature", "id": identifier,
            "properties": {"identifier": identifier,
                           "navigation": f"{nldi.NLDI_ROOT}/linked-data/ref_gage/{identifier}/navigation",
                           "name": f"GAGE {index}", "source": "ref_gage",
                           "sourceName": "geoconnex.us reference gages",
                           "comid": 17000000 + index,
                           "type": "hydrolocation",
                           "uri": f"{nldi.GAGE_URI_PREFIX}{identifier}",
                           "reachcode": str(13020205000000 + index) if index % 3 else None,
                           "measure": round(0.001 * (index + 1), 4) if index % 2 else None,
                           "mainstem": f"{nldi.MAINSTEM_URI_PREFIX}{700000 + index}" if index % 5 == 0 else None},
            "geometry": {"type": "Point", "coordinates": [-107.0 - 0.001 * index, 35.0 + 0.001 * index]}}


def _fixture_rows() -> list[dict]:
    return [_fixture_feature(index) for index in range(FIXTURE_COUNT)]


def _fixture_sources() -> dict:
    return {"sources": [
        {"source": "comid", "sourceName": "NHDPlus comid",
         "features": "https://api.water.usgs.gov/nldi/linked-data/comid"},
        dict(nldi.EXPECTED_SOURCE_ENTRY),
        {"source": "nwissite", "sourceName": "NWIS Surface Water Sites",
         "features": "https://api.water.usgs.gov/nldi/linked-data/nwissite"},
    ]}


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        self.body = payload if isinstance(payload, bytes) else json.dumps(
            payload, separators=(",", ":"), ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def make_transport(rows: list[dict] | None = None, *, sources: dict | None = None,
                   refuse_at_offset: int | None = None, extra_features: int = 0) -> httpx.MockTransport:
    rows = list(rows) if rows is not None else _fixture_rows()
    sources = sources if sources is not None else _fixture_sources()

    def respond(request: httpx.Request) -> httpx.Response:
        parsed = urlparse(str(request.url))
        query = parse_qs(parsed.query)
        if parsed.path.rstrip("/").endswith("/linked-data"):
            return httpx.Response(200, stream=JSONStream(sources),
                                  headers={"content-type": "application/json"})
        if parsed.path.rstrip("/").endswith("/linked-data/ref_gage"):
            offset = int(query.get("offset", ["0"])[0])
            limit = int(query.get("limit", ["0"])[0])
            if refuse_at_offset is not None and offset == refuse_at_offset:
                return httpx.Response(504, stream=JSONStream(b"gateway timeout"),
                                      headers={"content-type": "text/plain"})
            window = rows[offset:offset + limit] if limit else []
            if extra_features:
                window = window + [_fixture_feature(FIXTURE_COUNT + position)
                                   for position in range(extra_features)]
            return httpx.Response(200, stream=JSONStream(
                {"type": "FeatureCollection", "features": window}),
                headers={"content-type": "application/geo+json"})
        raise AssertionError(f"unrouted request {request.url}")

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(transport: httpx.MockTransport | None = None):
    contexts = [
        patch.object(nldi, "PAGE_LIMIT", FIXTURE_PAGE_LIMIT),
        patch.object(nldi, "EXPECTED_COUNT", None),
        patch.object(nldi, "EXPECTED_UNIQUE_ID_COUNT", None),
        patch.object(nldi, "EXPECTED_IDS_SHA256", ""),
        patch.object(nldi, "EXPECTED_SCHEMA_SHA256", ""),
        patch.object(nldi, "EXPECTED_ASSET_SHA256", ""),
        patch.object(nldi, "EXPECTED_FIRST_ID", None),
        patch.object(nldi, "EXPECTED_LAST_ID", None),
        patch.object(nldi, "EXPECTED_NULL_GEOMETRY_COUNT", None),
        patch.object(nldi, "EXPECTED_OBSERVED_BOUNDS", None),
        *([patch.object(nldi, "_default_client",
                        side_effect=lambda: httpx.Client(transport=transport))]
          if transport is not None else []),
    ]
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


def _staging(root: Path) -> StagingStorageResource:
    return StagingStorageResource(use_local=True, local_dir=str(root / "staging"))


def _materialize(staging: StagingStorageResource, capture_directory: str, instance) -> object:
    return materialize(
        [nldi.usgs_nldi_streamgage_monitoring_locations],
        resources={"staging_storage": staging}, instance=instance,
        run_config={"ops": {ASSET_NAME: {"config": {"capture_directory": capture_directory}}}})


class USGSNLDIAssetTests(unittest.TestCase):
    def test_registered_refresh_reuses_content_version_and_partition(self):
        transport = make_transport()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            staging = _staging(root)
            dagster_home = root / "dagster-home"
            dagster_home.mkdir()
            with fixture_mode(transport), patch.dict(os.environ, {"DAGSTER_HOME": str(dagster_home)}), \
                 DagsterInstance.local_temp() as instance:
                first = _materialize(staging, str(capture_root), instance)
                result = first.output_for_node(ASSET_NAME)
                prefix = Path(root) / "staging" / nldi.DATASET_SLUG / nldi.FILE_SLUG / result["version"]
                composed = (prefix / nldi.ASSET_PATH).read_bytes()
                second = _materialize(staging, str(capture_root), instance)
                again = second.output_for_node(ASSET_NAME)
                partition = build_publish_partition_key(nldi.DATASET_SLUG, nldi.FILE_SLUG, result["version"])
                self.assertIn(partition, instance.get_dynamic_partitions(PUBLISH_PARTITION_NAME))
            self.assertFalse(result["reused"])
            self.assertTrue(again["reused"])
            self.assertEqual(result["version"], again["version"])
            self.assertTrue(result["version"].startswith("v0.0.0-candidate."))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            audit = result["source_audit"]
            self.assertEqual(audit["first_id"], _fixture_identifier(0))
            self.assertEqual(audit["last_id"], _fixture_identifier(FIXTURE_COUNT - 1))
            self.assertEqual(audit["unique_id_count"], FIXTURE_COUNT)
            self.assertEqual(audit["null_geometry_count"], 0)
            self.assertEqual(audit["asset_sha256"], __import__("hashlib").sha256(composed).hexdigest())
            collection = json.loads(composed)
            self.assertEqual(collection["type"], "FeatureCollection")
            self.assertEqual(len(collection["features"]), FIXTURE_COUNT)
            self.assertEqual(collection["features"][0]["properties"]["measure"],
                             _fixture_rows()[0]["properties"]["measure"])
            self.assertNotIn("crs", collection)
            manifest = json.loads((prefix / "metadata/source_manifest.json").read_text())
            self.assertEqual(manifest["publisher"], "U.S. Geological Survey (NLDI)")
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            self.assertTrue(manifest["inputs"])
            for entry in manifest["inputs"]:
                self.assertEqual(entry["host"], "api.water.usgs.gov")
                self.assertEqual(entry["role"], "producer_publication")
                self.assertEqual(entry["producer"], "U.S. Geological Survey")
                self.assertIsNone(entry["release_date"])
                self.assertTrue(entry["release_date_unknown_reason"])
            self.assertEqual(manifest["literal_geojson_catalog"]["sha256"], audit["asset_sha256"])
            self.assertEqual(manifest["literal_geojson_catalog"]["path"], nldi.ASSET_PATH)
            self.assertNotIn("root_metadata", manifest["literal_geojson_catalog"])
            observation = manifest["source_observation"]
            self.assertEqual(observation["registered_source_entry"], nldi.EXPECTED_SOURCE_ENTRY)
            self.assertEqual(observation["pagination"]["mode"], "limit_offset_walk")
            self.assertEqual(observation["pagination"]["page_limit"], FIXTURE_PAGE_LIMIT)
            self.assertTrue(observation["pagination"]["resumable"])
            self.assertFalse(observation["agency_authority_accepted"])
            self.assertFalse(observation["compatibility_accepted"])
            self.assertEqual(observation["publication_status"], "STOPPED")
            self.assertTrue((prefix / "evidence" / "sources-before.decoded").is_file())
            self.assertTrue((prefix / "evidence" / "page_000000000.decoded").is_file())
            self.assertTrue((prefix / "evidence" / "page_000000003.wire").is_file())
            self.assertTrue((Path(root) / "staging" / "_construction-state" / nldi.DATASET_SLUG
                             / nldi.FILE_SLUG / f"{result['version']}.json").is_file())
            self.assertEqual(
                (capture_root / "page_000000000.decoded").read_bytes(),
                (prefix / "evidence" / "page_000000000.decoded").read_bytes())

    def test_feature_source_registration_drift_stops(self):
        sources = {"sources": [{"source": "nwissite", "sourceName": "NWIS Surface Water Sites",
                                "features": "https://api.water.usgs.gov/nldi/linked-data/nwissite"}]}
        transport = make_transport(sources=sources)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(), patch.object(nldi, "DEFAULT_CAPTURE_ROOT", str(root / "capture")):
                with self.assertRaisesRegex(ValueError, "NLDI ref_gage feature source is missing or ambiguous"):
                    nldi.fetch_and_stage_nldi(_staging(root), client=httpx.Client(transport=transport))
            quarantined = sorted((root / "staging" / "_quarantine" / nldi.DATASET_SLUG
                                 / nldi.FILE_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_text())
            self.assertEqual(failure["reason_code"], "nldi_feature_source_registration_drift")

    def test_page_drift_cases_are_refused(self):
        rows = _fixture_rows()
        cases = {
            "NLDI page at offset 3 HTTP capture incomplete": dict(refuse_at_offset=3),
            "NLDI served more features than the requested page limit": dict(extra_features=3),
            "NLDI feature property roster or order drifted": dict(rows=[
                {**rows[0], "properties": {"identifier": rows[0]["properties"]["identifier"],
                                           "name": "no roster", "source": "ref_gage",
                                           "sourceName": "geoconnex.us reference gages",
                                           "comid": None, "type": "hydrolocation",
                                           "uri": rows[0]["properties"]["uri"],
                                           "reachcode": None, "measure": None, "mainstem": None}},
                *rows[1:]]),
            "NLDI feature geometry is not a plain Point": dict(rows=[
                {**rows[0], "geometry": {"type": "MultiPoint", "coordinates": [[-107.0, 35.0]]}},
                *rows[1:]]),
            "NLDI comid is neither an integer nor null": dict(rows=[
                {**rows[0], "properties": {**rows[0]["properties"], "comid": 17789327.0}},
                *rows[1:]]),
            "NLDI feature uri drifted": dict(rows=[
                {**rows[0], "properties": {**rows[0]["properties"],
                                           "uri": "https://geoconnex.us/ref/gages/other"}},
                *rows[1:]]),
            "NLDI mainstem uri drifted": dict(rows=[
                {**rows[0], "properties": {**rows[0]["properties"],
                                           "mainstem": "https://example.com/mainstem/1"}},
                *rows[1:]]),
            "NLDI feature coordinates fall outside the geographic degree ranges": dict(rows=[
                {**rows[0], "geometry": {"type": "Point", "coordinates": [200.0, 35.0]}},
                *rows[1:]]),
            "the walked identifier order is not strictly ascending": dict(rows=[
                rows[0], rows[0], *rows[2:]]),
            "NLDI feature id/identifier do not match the walk position": dict(rows=[
                {**rows[0], "id": "000000999"},
                *rows[1:]]),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    transport = make_transport(**kwargs)
                    with fixture_mode(), patch.object(nldi, "DEFAULT_CAPTURE_ROOT", str(root / "capture")):
                        with self.assertRaisesRegex(ValueError, expected_error):
                            nldi.fetch_and_stage_nldi(_staging(root),
                                                      client=httpx.Client(transport=transport))

    def test_walk_reuses_retained_page_evidence_without_transport(self):
        transport = make_transport()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(), patch.object(nldi, "DEFAULT_CAPTURE_ROOT", str(capture_root)):
                first = nldi.fetch_and_stage_nldi(_staging(root), client=httpx.Client(transport=transport))
                second = nldi.fetch_and_stage_nldi(_staging(root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "capture_state.json").exists())
            state = json.loads((capture_root / "capture_state.json").read_text())
            self.assertTrue(state["complete"])
            self.assertEqual(len(state["pages"]), 5)

    def test_pinned_walk_drift_is_refused(self):
        transport = make_transport()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(), patch.object(nldi, "DEFAULT_CAPTURE_ROOT", str(root / "capture")):
                first = nldi.fetch_and_stage_nldi(_staging(root), client=httpx.Client(transport=transport))
                audit = first["source_audit"]
                with patch.object(nldi, "EXPECTED_COUNT", audit["feature_count"] + 1):
                    with self.assertRaisesRegex(ValueError, "population count drifted"):
                        nldi.fetch_and_stage_nldi(_staging(root), client=httpx.Client(transport=transport))
                with patch.object(nldi, "EXPECTED_IDS_SHA256", "0" * 64):
                    with self.assertRaisesRegex(ValueError, "identifier roster drifted"):
                        nldi.fetch_and_stage_nldi(_staging(root), client=httpx.Client(transport=transport))
                with patch.object(nldi, "EXPECTED_ASSET_SHA256", "0" * 64):
                    with self.assertRaisesRegex(ValueError, "candidate asset bytes drifted"):
                        nldi.fetch_and_stage_nldi(_staging(root), client=httpx.Client(transport=transport))


if __name__ == "__main__":
    unittest.main()
