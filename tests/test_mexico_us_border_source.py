"""Source contract tests for the official IBWC US–Mexico boundary layer."""

from __future__ import annotations

import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import httpx
import pyarrow.parquet as pq
import shapely
from dagster import AssetCheckSeverity, DefaultScheduleStatus

from dagster_hifld.capture_completeness import approved_baseline_drift
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import mexico_us_border as source
from dagster_hifld.sources.mexico_us_border import (
    DATASET_SLUG,
    EXPECTED_BORDER_TYPE_CODES,
    EXPECTED_COVERAGE,
    EXPECTED_COPYRIGHT,
    EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_ITEM_TAGS,
    EXPECTED_LAYER_EXTENT,
    EXPECTED_LAYER_SR,
    EXPECTED_PAGE_SR,
    PAGE_SIZE,
    PORTAL_ITEM_ID,
    SERVICE_URL,
    _capture_roster,
    _esri_polyline,
    _parquet_bytes,
    _validate,
)
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.mexico_us_border import (
    mexico_us_border,
    mexico_us_border_complete,
    mexico_us_border_drift,
    mexico_us_border_draft_job,
    mexico_us_border_draft_schedule,
)
from dagster_hifld.definitions import defs

EXPECTED_COUNT = 14
EXPECTED_ID_SHA256 = "397db166b17b198de79db0e6b31cf193aa4e77f9b695385b7f70e94455f8d01c"
EXPECTED_GLOBALIDS = [
    "{2E367C76-0FEE-4316-9140-804CD159957E}", "{3CA5AD51-ECC8-41FC-BDA4-82A5C220AAF3}",
    "{66A9F54A-4B41-4439-8945-2C34C048093F}", "{24116A62-D6FB-4A7B-83FA-8558F3970889}",
    "{1093EA66-C068-4E44-877F-FF84EC423B5F}", "{996B5DDE-A90B-480F-93A1-710174260EAD}",
    "{6FBF3B57-42D2-4159-8BE3-534D91E35B85}", "{8C94A81E-BDA2-44A8-BD5B-3730602BBECF}",
    "{E13A1DCA-0153-41B8-B619-6CE2C8291F23}", "{8558ACF7-E07D-41C0-91A8-9EA5420EFC58}",
    "{65F1D28C-EBD8-49CA-8790-B872FEA83DD6}", "{468B451C-A0F9-44BA-A09A-CF00F385CD05}",
    "{70DA3A9C-525E-4F85-BF91-45AD61A7552D}", "{44E95C90-091B-4731-927E-528EA3F340C2}",
]

FIELD_NAMES = [field["name"] for field in EXPECTED_FIELD_DECLARATIONS]
PORTAL_ITEM_URL = (
    "https://gisportal.ibwc.gov/agsportal/sharing/rest/content/items/"
    f"{PORTAL_ITEM_ID}?f=json"
)
SERVICE_DESCRIPTION = (
    "This dataset represents the international boundary between the United States of "
    "America and the United States of Mexico in accordance with the 1970 Treaty."
)


def _docs(ids: list[int] | None = None) -> dict[str, dict]:
    object_ids = list(range(1, EXPECTED_COUNT + 1)) if ids is None else ids
    item = {
        "id": PORTAL_ITEM_ID,
        "owner": "ibwcgis",
        "orgId": "0123456789ABCDEF",
        "type": "Feature Service",
        "access": "public",
        "title": "US_MexicoIB",
        "created": 1706746235861,
        "modified": 1745000282541,
        "url": SERVICE_URL,
        "snippet": "This dataset represents the international boundary between the "
                   "United States of America and the United States of Mexico.",
        "description": SERVICE_DESCRIPTION,
        "tags": EXPECTED_ITEM_TAGS,
        "accessInformation": EXPECTED_COPYRIGHT,
        "licenseInfo": "Access Constraint: None (Public Domain Information) "
                       "Use Constraint: None (Public Use)",
    }
    service = {
        "serviceDescription": SERVICE_DESCRIPTION,
        "copyrightText": EXPECTED_COPYRIGHT,
        "layers": [{"id": 0, "name": "US_MexicoIB", "geometryType": "esriGeometryPolyline"}],
        "supportedQueryFormats": "JSON",
    }
    layer = {
        "id": 0,
        "name": "US_MexicoIB",
        "geometryType": "esriGeometryPolyline",
        "spatialReference": None,
        "extent": {**EXPECTED_LAYER_EXTENT, "spatialReference": dict(EXPECTED_LAYER_SR)},
        "maxRecordCount": 2000,
        "hasZ": False,
        "hasM": False,
        "fields": EXPECTED_FIELD_DECLARATIONS,
    }
    features = []
    for index, oid in enumerate(object_ids):
        attrs = {name: None for name in FIELD_NAMES}
        attrs.update({
            "Name": f"Monument {oid} to Monument {oid + 1}",
            "DistanceMeters": 1000.5 + oid,
            "DistanceFeet": 3282.5 + oid,
            "DistanceKilometers": 1.0005 + oid,
            "DistanceMiles": 0.6215 + oid,
            "DistanceNauticalMiles": 0.5401 + oid,
            "BORDER_TYPE": 1,
            "DateUpdated": 1627603200000,
            "GlobalID": (EXPECTED_GLOBALIDS[oid - 1] if oid <= len(EXPECTED_GLOBALIDS)
                         else f"{{00000000-0000-0000-0000-{oid:012d}}}"),
            "OBJECTID": oid,
            "Label": "US / Mexico Boundary",
            "Shape__Length": 1000.25 + oid,
        })
        features.append({
            "attributes": attrs,
            "geometry": {"paths": [[[-100.0, 30.0], [-100.1, 30.1 + (oid % 15) / 10]]]},
        })
    docs = {
        "portal_item": item,
        "service_directory": {"services": [{"name": "USIBWCPublic/US_MexicoIB", "type": "FeatureServer"}]},
        "service_before": service,
        "service_after": service,
        "layer_before": layer,
        "layer_after": layer,
        "count_before": {"count": len(object_ids)},
        "count_after": {"count": len(object_ids)},
        "ids_seed": {"objectIds": list(object_ids)},
        "ids_before": {"objectIds": list(object_ids)},
        "ids_after": {"objectIds": list(object_ids)},
    }
    for page_index, start in enumerate(range(0, len(features), PAGE_SIZE)):
        docs[f"page_{page_index:03d}"] = {
            "spatialReference": dict(EXPECTED_PAGE_SR),
            "features": features[start:start + PAGE_SIZE],
        }
    return docs


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {name: (json.dumps(doc).encode("utf-8"), {}) for name, doc in docs.items()}


def _client(docs, ids, refusal=None):
    responses = {}
    for name, url in [("ids_seed", source._query({"where": "1=1", "returnIdsOnly": "true", "f": "json"})),
                      *source._capture_roster(ids)]:
        responses.setdefault(url, []).append(name)
    calls = []

    def handle(request):
        name = responses[str(request.url)][0]
        calls.append(name)
        if name == refusal:
            return httpx.Response(503, stream=httpx.ByteStream(b"unavailable"))
        responses[str(request.url)].pop(0)
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(docs[name]).encode()),
                              headers={"Content-Type": "application/json"})

    return httpx.Client(transport=httpx.MockTransport(handle)), responses, calls


class MexicoUsBorderSourceTest(unittest.TestCase):
    def test_complete_upstream_addition_and_removal_use_publisher_witnesses(self) -> None:
        for ids in (list(range(1, 15)) + [27], list(range(2, 15))):
            with self.subTest(ids=ids):
                fields, features, audit = _validate(_captures(_docs(ids)))
                self.assertEqual(audit["source_feature_count"], len(ids))
                self.assertEqual(audit["source_id_min"], min(ids))
                self.assertEqual(audit["source_id_max"], max(ids))
                table = pq.read_table(io.BytesIO(_parquet_bytes(fields, features, 4326)))
                self.assertEqual(table.column("OBJECTID").to_pylist(), ids)
                self.assertEqual(audit["global_id_distinct_count"], len(ids))

    def test_changed_global_id_is_preserved_without_a_snapshot_digest(self) -> None:
        docs = _docs()
        replacement = "{00000000-0000-0000-0000-000000000000}"
        docs["page_000"]["features"][2]["attributes"]["GlobalID"] = replacement
        fields, features, audit = _validate(_captures(docs))
        table = pq.read_table(io.BytesIO(_parquet_bytes(fields, features, 4326)))
        self.assertEqual(table.column("GlobalID")[2].as_py(), replacement)
        self.assertEqual(audit["global_id_duplicate_count"], 0)

    def test_torn_counts_ids_seed_and_exact_pages_fail_closed(self) -> None:
        cases = [
            ("count mismatch", lambda d: d["count_after"].update(count=14), "source_count_or_id_roster_drift"),
            ("same count different IDs", lambda d: d["ids_after"]["objectIds"].__setitem__(-1, 28),
             "source_count_or_id_roster_drift"),
            ("seed mismatch", lambda d: d["ids_seed"].update(objectIds=list(range(1, 16))),
             "source_seed_id_population_drift"),
            ("duplicate witness IDs", lambda d: d["ids_after"]["objectIds"].__setitem__(-1, 1),
             "source_count_or_id_roster_drift"),
            ("missing witness", lambda d: d.pop("count_after"), "source_capture_witness_missing"),
            ("missing page", lambda d: d.pop("page_000"), "source_capture_roster_incomplete"),
            ("omitted row", lambda d: d["page_000"]["features"].pop(), "source_page_id_population_drift"),
            ("duplicate captured ID", lambda d: d["page_000"]["features"][-1]["attributes"].update(OBJECTID=1),
             "source_page_id_population_drift"),
            ("extra page", lambda d: d.update(page_001=copy.deepcopy(d["page_000"])), "source_capture_roster_incomplete"),
        ]
        for label, mutate, reason in cases:
            with self.subTest(label=label):
                docs = copy.deepcopy(_docs(list(range(1, 15)) + [27]))
                mutate(docs)
                with self.assertRaisesRegex(ValueError, reason):
                    _validate(_captures(docs))
        docs = _docs([])
        with self.assertRaisesRegex(ValueError, "source_population_empty"):
            _validate(_captures(docs))

    def test_pages_must_match_their_own_observed_id_slice(self) -> None:
        docs = copy.deepcopy(_docs(list(range(1, 15)) + [27]))
        docs["page_001"] = {**docs["page_000"], "features": [docs["page_000"]["features"].pop()]}
        docs["page_000"]["features"][0], docs["page_001"]["features"][0] = (
            docs["page_001"]["features"][0], docs["page_000"]["features"][0])
        with patch.object(source, "PAGE_SIZE", 14):
            with self.assertRaisesRegex(ValueError, "source_page_id_population_drift:page_000"):
                _validate(_captures(docs))

    def test_acquisition_first_construction_and_cached_replay_use_shared_witnesses(self) -> None:
        for ids in (list(range(1, 15)) + [27], list(range(2, 15))):
            with self.subTest(ids=ids), tempfile.TemporaryDirectory() as temporary, patch.object(source, "PAGE_SIZE", 14):
                docs = copy.deepcopy(_docs(ids))
                if len(ids) > 14:
                    docs["page_001"] = {**docs["page_000"], "features": [docs["page_000"]["features"].pop()]}
                for name in ("ids_seed", "ids_before", "ids_after"):
                    docs[name]["objectIds"] = list(reversed(ids))
                storage = StagingStorageResource(local_dir=str(Path(temporary) / "stage"))
                client, responses, _calls = _client(docs, ids)
                with patch.object(source.httpx, "Client", return_value=client), \
                        patch.object(source, "capture_layer", wraps=source.capture_layer) as shared:
                    result = source.capture_mexico_us_border(storage, version="fixture-capture",
                        capture_directory=str(Path(temporary) / "captures"))
                self.assertEqual(shared.call_count, 1)
                self.assertEqual(result["feature_count"], len(ids))
                self.assertEqual(result["source_reported_count"], len(ids))
                self.assertTrue(mexico_us_border_complete(result).passed)
                self.assertTrue(all(not pending for pending in responses.values()))
                root = Path(result["capture_directory"])
                self.assertEqual(len(list(root.glob("page_*.body"))), (len(ids) + 13) // 14)
                manifest_before = (root / "capture_manifest.json").read_bytes()
                with patch.object(source.httpx, "Client", side_effect=AssertionError("network on replay")):
                    cached = source.capture_mexico_us_border(storage, version="replay", capture_directory=str(root))
                self.assertTrue(cached["reused"])
                self.assertEqual(cached["version"], result["version"])
                self.assertEqual(cached["source_reported_count"], len(ids))
                self.assertEqual((root / "capture_manifest.json").read_bytes(), manifest_before)
                drift = mexico_us_border_drift(result)
                self.assertFalse(drift.passed)
                self.assertEqual(drift.severity, AssetCheckSeverity.WARN)
                self.assertEqual(drift.metadata["comparison_status"].value, "skipped_no_approved_baseline")
                self.assertFalse(list(mexico_us_border_drift.check_specs)[0].blocking)
                baseline = {**result, "version": "approved-fixture", "release_accepted": True,
                            "feature_count": EXPECTED_COUNT, "source_id_sha256": EXPECTED_ID_SHA256}
                compared = approved_baseline_drift(result, baseline)
                self.assertFalse(compared.passed)
                self.assertEqual(compared.severity, AssetCheckSeverity.WARN)
                self.assertEqual(compared.metadata["feature_count_delta"].value, len(ids) - EXPECTED_COUNT)
                self.assertTrue(compared.metadata["native_id_roster_changed"].value)
                prefix = result["candidate_ready"].removesuffix("metadata/candidate_ready.json")
                table = pq.read_table(io.BytesIO(storage.read_key(prefix + f"geoparquet/{DATASET_SLUG}.parquet")))
                self.assertEqual(table.column("OBJECTID").to_pylist(), ids)
                for key in ("source_release_accepted", "source_freshness_accepted", "schema_contract_accepted",
                            "id_contract_accepted", "coverage_accepted", "rights_accepted", "release_accepted"):
                    self.assertIs(result[key], False)
                receipt_path = root / "page_000.receipt.json"
                receipt = json.loads(receipt_path.read_bytes())
                receipt["url"] = source.LAYER_URL + "/query?objectIds=999"
                receipt_path.write_bytes(source._canonical(receipt))
                forged_manifest = json.loads(manifest_before)
                forged_manifest["receipts"]["page_000"] = receipt
                forged_manifest["artifact_roster"] = source.capture_roster_digests(root, exclude={"capture_manifest.json"})
                (root / "capture_manifest.json").write_bytes(source._canonical(forged_manifest))
                forged_bytes = (root / "capture_manifest.json").read_bytes()
                with self.assertRaisesRegex(ValueError, "retained_capture_request_identity_mismatch:page_000"):
                    source.capture_mexico_us_border(storage, version="corrupt-replay", capture_directory=str(root))
                self.assertEqual((root / "capture_manifest.json").read_bytes(), forged_bytes)

    def test_torn_live_witness_and_refused_page_retain_quarantine_evidence(self) -> None:
        for refusal in (None, "page_000"):
            with self.subTest(refusal=refusal), tempfile.TemporaryDirectory() as temporary:
                ids = list(range(1, 15)) + [27]
                docs = copy.deepcopy(_docs(ids))
                docs["count_after"]["count"] += 1
                client, _responses, calls = _client(docs, ids, refusal)
                storage = StagingStorageResource(local_dir=str(Path(temporary) / "stage"))
                requested = Path(temporary) / "captures"
                with patch.object(source.httpx, "Client", return_value=client):
                    with self.assertRaisesRegex(ValueError, "incomplete_source_capture|source_count_or_id_roster_drift"):
                        source.capture_mexico_us_border(storage, version="failed-capture", capture_directory=str(requested))
                manifest_path = next(requested.rglob("capture_manifest.json"))
                manifest = json.loads(manifest_path.read_bytes())
                self.assertFalse(manifest["complete"])
                quarantine = storage.list_prefix(f"_quarantine/{DATASET_SLUG}")
                self.assertTrue(any(name.endswith("/raw/portal_item.body") for name in quarantine))
                self.assertFalse(any(name.endswith("candidate_ready.json") for name in storage.list_prefix(DATASET_SLUG)))
                if refusal:
                    self.assertEqual(calls.count("page_000"), 2)
                    for attempt in (1, 2):
                        self.assertEqual((manifest_path.parent / f"page_000.attempt{attempt}.body").read_bytes(), b"unavailable")
                else:
                    self.assertTrue((manifest_path.parent / "ids_after.body").exists())

    def test_shared_replay_rejects_forged_decoded_body_even_with_matching_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            body, wire = b'{"count":15}', b'{"count":14}'
            (root / "count_before.body").write_bytes(body)
            (root / "count_before.wire").write_bytes(wire)
            (root / "count_before.receipt.json").write_bytes(source._canonical({
                "complete_body": True, "status": 200, "bytes": len(body), "sha256": source._sha(body),
                "wire_bytes": len(wire), "wire_sha256": source._sha(wire), "content_encoding": "identity"}))
            with self.assertRaisesRegex(ValueError, "retained_capture_wire_body_mismatch"):
                source._load_replay(root, "count_before")

    def test_registered_stopped_draft_and_unapproved_release_gate(self) -> None:
        self.assertIn(mexico_us_border, hifld_ingest_assets)
        registered = defs.get_repository_def()
        self.assertIn(mexico_us_border.key, registered.asset_graph.get_all_asset_keys())
        self.assertIn("mexico_us_border_complete",
                      {key.name for key in registered.asset_checks_defs_by_key})
        self.assertIn(mexico_us_border_draft_job.name,
                      {job.name for job in registered.get_all_jobs()})
        self.assertIn(mexico_us_border_draft_schedule.name,
                      {schedule.name for schedule in registered.schedule_defs})
        self.assertEqual(mexico_us_border_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = mexico_us_border_complete({"feature_count": EXPECTED_COUNT, "source_reported_count": EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        self.assertFalse(check.metadata["mapping_approved"].value)
        self.assertFalse(check.metadata["schema_contract_accepted"].value)

    def test_complete_synthetic_capture_validates(self) -> None:
        fields, features, audit = _validate(_captures(_docs()))
        self.assertEqual([field["name"] for field in fields], FIELD_NAMES)
        self.assertEqual(len(features), EXPECTED_COUNT)
        self.assertEqual(audit["source_id_sha256"], EXPECTED_ID_SHA256)
        self.assertEqual(audit["source_feature_count"], EXPECTED_COUNT)
        self.assertEqual(audit["geometry_valid_count"], EXPECTED_COUNT)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["source_vertex_count"], EXPECTED_COUNT * 2)
        self.assertEqual(audit["global_id_distinct_count"], EXPECTED_COUNT)
        self.assertEqual(audit["global_id_duplicate_count"], 0)

    def test_portal_item_identity_and_rights_are_required(self) -> None:
        for mutation, error in (
            ({"owner": "someone_else"}, "ibwc_portal_item_identity_drift"),
            ({"orgId": "another_org"}, "ibwc_portal_item_identity_drift"),
            ({"type": "Map Service"}, "ibwc_portal_item_identity_drift"),
            ({"url": "https://services.arcgis.example/FeatureServer"}, "ibwc_portal_item_identity_drift"),
            ({"licenseInfo": "restricted"}, "ibwc_portal_item_rights_drift"),
        ):
            with self.subTest(mutation=mutation):
                docs = _docs()
                docs["portal_item"].update(mutation)
                with self.assertRaisesRegex(ValueError, error):
                    _validate(_captures(docs))

    def test_changed_ids_fail_closed(self) -> None:
        docs = _docs()
        docs["ids_after"] = {"objectIds": list(range(1, EXPECTED_COUNT)) + [15]}
        with self.assertRaisesRegex(ValueError, "source_count_or_id_roster_drift"):
            _validate(_captures(docs))

    def test_changed_global_ids_fail_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][2]["attributes"]["GlobalID"] = docs["page_000"]["features"][0]["attributes"]["GlobalID"]
        with self.assertRaisesRegex(ValueError, "source_global_id_duplicate"):
            _validate(_captures(docs))

    def test_type_loss_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["Name"] = 40
        with self.assertRaisesRegex(ValueError, "source_string_value_type_drift"):
            _validate(_captures(docs))

    def test_non_nullable_global_id_null_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["GlobalID"] = None
        with self.assertRaisesRegex(ValueError, "source_non_nullable_value_missing:GlobalID"):
            _validate(_captures(docs))

    def test_nullable_name_is_preserved(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["Name"] = None
        fields, features, audit = _validate(_captures(docs))
        self.assertIsNone(features[0]["attributes"]["Name"])
        parquet = pq.read_table(io.BytesIO(_parquet_bytes(fields, features, EXPECTED_PAGE_SR["latestWkid"])))
        self.assertEqual(parquet["Name"].null_count, 1)
        self.assertEqual(parquet["OBJECTID"].to_pylist(), list(range(1, EXPECTED_COUNT + 1)))

    def test_coded_domain_value_drift_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["features"][0]["attributes"]["BORDER_TYPE"] = 7
        with self.assertRaisesRegex(ValueError, "source_coded_domain_value_drift:BORDER_TYPE"):
            _validate(_captures(docs))
        for code in EXPECTED_BORDER_TYPE_CODES:
            docs = _docs()
            docs["page_000"]["features"][0]["attributes"]["BORDER_TYPE"] = code
            _validate(_captures(docs))

    def test_unexpected_column_fails_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["fields"] = docs["layer_before"]["fields"] + [
            {"name": "NEW_FIELD", "type": "esriFieldTypeString", "alias": "NEW_FIELD",
             "length": 254, "domain": None, "nullable": True}
        ]
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
            _validate(_captures(docs))

    def test_incomplete_page_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["exceededTransferLimit"] = True
        with self.assertRaisesRegex(ValueError, "source_page_error_or_truncation"):
            _validate(_captures(docs))

    def test_extent_and_geometry_coverage_drift_fail_closed(self) -> None:
        docs = _docs()
        docs["layer_before"]["extent"]["xmax"] += 1
        docs["layer_after"] = copy.deepcopy(docs["layer_before"])
        with self.assertRaisesRegex(ValueError, "source_coverage_extent_drift"):
            _validate(_captures(docs))
        docs = _docs()
        docs["page_000"]["features"][0]["geometry"]["paths"][0][0][0] = -130.0
        with self.assertRaisesRegex(ValueError, "source_geometry_outside_declared_coverage"):
            _validate(_captures(docs))

    def test_native_identifier_values_and_polyline_coordinates_survive_geoparquet(self) -> None:
        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID", "domain": None, "nullable": False},
            {"name": "GlobalID", "type": "esriFieldTypeGlobalID", "alias": "GlobalID", "length": 38, "domain": None, "nullable": False},
            {"name": "Name", "type": "esriFieldTypeString", "alias": "Name", "length": 50, "domain": None, "nullable": True},
            {"name": "DateUpdated", "type": "esriFieldTypeDate", "alias": "Date Updated", "length": 8, "domain": None, "nullable": True},
            {"name": "BORDER_TYPE", "type": "esriFieldTypeInteger", "alias": "Border Type", "domain": None, "nullable": True},
        ]
        feature = {
            "attributes": {"OBJECTID": 9, "GlobalID": EXPECTED_GLOBALIDS[8], "Name": "Monument 127",
                           "DateUpdated": 1627603200000, "BORDER_TYPE": 2},
            "geometry": {"paths": [[[-111.5, 31.25], [-111.4, 31.3]]]},
        }
        body = _parquet_bytes(fields, [feature], EXPECTED_PAGE_SR["latestWkid"])
        table = pq.ParquetFile(io.BytesIO(body)).read(use_threads=False)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [9])
        self.assertEqual(table.column("GlobalID").to_pylist(), [EXPECTED_GLOBALIDS[8]])
        self.assertEqual(table.column("Name").to_pylist(), ["Monument 127"])
        self.assertEqual(table.column("DateUpdated").to_pylist(), [1627603200000])
        self.assertEqual(table.column("BORDER_TYPE").to_pylist(), [2])
        self.assertEqual(str(table.schema.field("OBJECTID").type), "int64")
        self.assertEqual(str(table.schema.field("DateUpdated").type), "int64")
        self.assertEqual(str(table.schema.field("BORDER_TYPE").type), "int32")
        self.assertEqual(str(table.schema.field("Name").type), "string")
        primary, _geo, crs = geometry_metadata(table.schema)
        self.assertEqual(primary, "geometry")
        self.assertEqual(crs.to_epsg(), 4326)
        geom = shapely.from_wkb(table.column("geometry")[0].as_py())
        self.assertEqual(list(geom.coords), [(-111.5, 31.25), (-111.4, 31.3)])

    def test_page_queries_request_wgs84_and_zm(self) -> None:
        pages = [(name, url) for name, url in _capture_roster(list(range(1, EXPECTED_COUNT + 1)))
                 if name.startswith("page_")]
        self.assertEqual(len(pages), 1)
        for _name, url in pages:
            query = parse_qs(urlsplit(url).query)
            self.assertEqual(query["outSR"], ["4326"])
            self.assertEqual(query["returnZ"], ["true"])
            self.assertEqual(query["returnM"], ["true"])
            self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])

    def test_service_identity_and_layers_are_checked(self) -> None:
        docs = _docs()
        docs["service_before"]["layers"] = [{"id": 0, "name": "OTHER", "geometryType": "esriGeometryPolyline"}]
        docs["service_after"] = copy.deepcopy(docs["service_before"])
        with self.assertRaisesRegex(ValueError, "official_us_mexico_boundary_service_drift"):
            _validate(_captures(docs))
        docs = _docs()
        docs["service_directory"]["services"] = []
        with self.assertRaisesRegex(ValueError, "official_ibwc_service_directory_drift"):
            _validate(_captures(docs))

    def test_service_description_markers_are_required(self) -> None:
        docs = _docs()
        docs["service_before"]["serviceDescription"] = "unrelated content"
        docs["service_after"] = copy.deepcopy(docs["service_before"])
        with self.assertRaisesRegex(ValueError, "service_description_drift"):
            _validate(_captures(docs))

    def test_conflicting_layer_wkids_fail_closed(self) -> None:
        for declared in ({"wkid": 3857, "latestWkid": 4326}, {"wkid": 4269, "latestWkid": 4269}):
            with self.subTest(declared=declared):
                docs = _docs()
                docs["layer_before"]["spatialReference"] = declared
                docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                with self.assertRaisesRegex(ValueError, "source_layer_crs_drift"):
                    _validate(_captures(docs))
                docs = _docs()
                docs["layer_before"]["extent"]["spatialReference"] = {"wkid": 3857, "latestWkid": 4326}
                docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                with self.assertRaisesRegex(ValueError, "source_layer_extent_crs_drift"):
                    _validate(_captures(docs))

    def test_page_crs_conflict_fails_closed(self) -> None:
        docs = _docs()
        docs["page_000"]["spatialReference"] = {"wkid": 4269, "latestWkid": 4326}
        with self.assertRaisesRegex(ValueError, "source_page_crs_drift"):
            _validate(_captures(docs))

    def test_layer_after_crs_or_extent_change_fails_closed(self) -> None:
        for key, value in (
            ("spatialReference", {"latestWkid": 4269}),
            ("extent", {"xmin": -1, "ymin": -1, "xmax": 2, "ymax": 2}),
        ):
            with self.subTest(key=key):
                docs = _docs()
                docs["layer_after"] = copy.deepcopy(docs["layer_before"])
                docs["layer_after"][key] = value
                with self.assertRaisesRegex(ValueError, "source_layer_changed_during_capture"):
                    _validate(_captures(docs))

    def test_correctly_flagged_z_m_drift_fails_closed_at_every_boundary(self) -> None:
        flags_cases = ({"hasZ": True}, {"hasM": True}, {"hasZ": True, "hasM": True})
        for flags in flags_cases:
            with self.subTest(flags=sorted(flags), boundary="layer"):
                docs = _docs()
                for name in ("layer_before", "layer_after"):
                    docs[name] = {**docs[name], **flags}
                with self.assertRaisesRegex(ValueError, "source_layer_dimensional_drift"):
                    _validate(_captures(docs))
            with self.subTest(flags=sorted(flags), boundary="page"):
                docs = _docs()
                docs["page_000"] = {**docs["page_000"], **flags}
                with self.assertRaisesRegex(ValueError, "source_page_dimensional_drift"):
                    _validate(_captures(docs))
            with self.subTest(flags=sorted(flags), boundary="geometry_flagged"):
                docs = _docs()
                for feature in docs["page_000"]["features"]:
                    feature["geometry"] = {**feature["geometry"], **flags}
                with self.assertRaisesRegex(ValueError, "source_geometry_dimensional_drift"):
                    _validate(_captures(docs))
            with self.subTest(flags=sorted(flags), boundary="builder_direct"):
                with self.assertRaisesRegex(ValueError, "source_polyline_dimensional_flags_rejected"):
                    _esri_polyline({"paths": [[[-100.0, 30.0], [-100.1, 30.1]]], **flags})

    def test_ordinate_count_drift_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "source_polyline_zm_flags_do_not_match_coordinates"):
            _esri_polyline({"paths": [[[-100.0, 30.0, 17.0], [-100.1, 30.1, 18.0]]]})
        docs = _docs()
        docs["page_000"]["features"][0]["geometry"]["paths"][0][0] = [-100.0, 30.0, 17.0]
        with self.assertRaisesRegex(ValueError, "source_polyline_zm_flags_do_not_match_coordinates"):
            _validate(_captures(docs))

    def test_esri_polyline_wkb_shapes(self) -> None:
        single = shapely.from_wkb(_esri_polyline({"paths": [[[-100.0, 30.0], [-100.1, 30.1]]]}))
        self.assertEqual(single.wkb_hex[:10], "0102000000")
        self.assertEqual(list(single.coords), [(-100.0, 30.0), (-100.1, 30.1)])
        multi = shapely.from_wkb(_esri_polyline({"paths": [
            [[-100.0, 30.0], [-100.1, 30.1]], [[-101.0, 31.0], [-101.1, 31.1]]]}))
        self.assertEqual(multi.wkb_hex[:10], "0105000000")
        self.assertEqual(len(multi.geoms), 2)
        with self.assertRaisesRegex(ValueError, "source_polyline_paths_missing"):
            _esri_polyline({"paths": []})

    def test_coverage_box_is_pinned_to_declared_extent(self) -> None:
        self.assertLess(EXPECTED_COVERAGE["xmin"], EXPECTED_COVERAGE["xmax"])
        self.assertLess(EXPECTED_COVERAGE["ymin"], EXPECTED_COVERAGE["ymax"])


if __name__ == "__main__":
    unittest.main()
