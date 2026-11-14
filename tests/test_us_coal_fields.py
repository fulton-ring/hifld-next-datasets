"""Synthetic-fixture and registration tests for the USGS NCRDS coal-fields service capture.

Real-capture evidence tests require HIFLD_US_COAL_FIELDS_TEST_CAPTURE_ARCHIVE;
the synthetic tests below exercise the full capture flow, every fail-closed
drift path, staging, retained replay and the prebuilt catalog read without
network access or pinned production bytes.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import tarfile
import tempfile
from contextlib import ExitStack
from pathlib import Path
import unittest
from unittest.mock import patch

import httpx
import pyarrow
import pyarrow.parquet as pq

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.us_coal_fields import (
    us_coal_fields, us_coal_fields_complete, us_coal_fields_draft_refresh_schedule,
)
from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import us_coal_fields as source

_CAPTURE_ARCHIVE_ENV = "HIFLD_US_COAL_FIELDS_TEST_CAPTURE_ARCHIVE"


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


_PROVINCES = ["Eastern", "Gulf", "Interior", "Northern Great Plains", "Pacific Coast", "Rocky Mountain"]
_AGES = ["Cretaceous", "Cretaceous-Jurassic", "Mississippian", "Pennsylvanian-Permian", "Tertiary", "Tertiary-Cretaceous", "Triassic"]
_RANKS = ["Anthracite", "Lignite", "Low Volatile Bituminous", "Medium and High Volatile Bituminous", "Subbituminous"]
_DISTINCT_NAMES = 110


def _attribute_values(fid: int) -> dict:
    return {
        "OBJECTID": fid + 1,
        "AREA": round(0.018 + fid * 0.001, 3),
        "PERIMETER": round(0.718 + fid * 0.004, 3),
        "PROVINCE": _PROVINCES[fid % len(_PROVINCES)],
        "NAME": f"Field {fid % _DISTINCT_NAMES}",
        "AGE": _AGES[fid % len(_AGES)],
        "RANK": _RANKS[fid % len(_RANKS)],
        "Shape_Length": round(1.5 + fid * 0.01, 2),
        "Shape_Area": round(2.5 + fid * 0.02, 2),
    }


def _synthetic_rings(fid: int, *, invalid_record: int | None = None, holed: int = 5, multi_shell: int = 7) -> list[list[list[float]]]:
    # Clockwise (negative shoelace area), as served Esri shells.
    base = float(fid) * 10.0
    rings = [[[base, 0.0], [base, 1.0], [base + 1.0, 1.0], [base + 1.0, 0.0], [base, 0.0]]]
    if invalid_record == fid + 1:
        # A shell plus a self-intersecting ("bowtie") hole; served as-is and
        # pinned, never repaired (matches the published Great Falls Field).
        rings.append([[base + 0.2, 0.2], [base + 0.8, 0.8], [base + 0.8, 0.2],
                      [base + 0.2, 0.8], [base + 0.2, 0.2]])
    if fid % holed == 0:
        # Counterclockwise (positive shoelace area) as served Esri holes.
        rings.append([[base + 0.25, 0.25], [base + 0.75, 0.25], [base + 0.75, 0.75],
                      [base + 0.25, 0.75], [base + 0.25, 0.25]])
    if fid % multi_shell == 0:
        rings.append([[base + 4.0, 0.0], [base + 4.0, 1.0], [base + 5.0, 1.0],
                      [base + 5.0, 0.0], [base + 4.0, 0.0]])
    return rings


def _synthetic_feature(fid: int, **kwargs) -> dict:
    return {"attributes": _attribute_values(fid), "geometry": {"rings": _synthetic_rings(fid, **kwargs)}}


def _synthetic_features(records: int, **kwargs) -> list[dict]:
    return [_synthetic_feature(fid, **kwargs) for fid in range(records)]


def _canon(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _layer_fields_doc(schema_drift: bool = False) -> list[dict]:
    fields = []
    for name, kind, length, nullable in source.EXPECTED_FIELDS:
        if schema_drift and name == "PROVINCE":
            name = "REGIONS"
        field = {"name": name, "type": kind, "alias": name, "nullable": nullable}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _crs_doc() -> dict:
    return {"wkid": source.SERVICE_CRS_WKID, "latestWkid": source.SERVICE_CRS_LATEST_WKID}


def _synthetic_bodies(records: int = 602, page_size: int | None = None,
                      invalid_record: int | None = None, *,
                      marker_drift: bool = False, count_drift: bool = False,
                      count_digest_drift: bool = False, ids_drift: bool = False, schema_drift: bool = False,
                      value_drift: bool = False, geometry_drift: bool = False,
                      null_attribute: bool = False, nonfinite_coordinate: bool = False,
                      unclosed_ring: bool = False, exceeded_transfer_limit: bool = False,
                      page_reorder: bool = False, folder_drift: bool = False,
                      layer_volatile_only: bool = False) -> dict:
    """Build the complete synthetic producer-service capture with matching pins."""
    page_size = page_size or source.PAGE_SIZE
    features = _synthetic_features(records, invalid_record=invalid_record)
    clean_features = _synthetic_features(records)
    if value_drift:
        features[3] = json.loads(json.dumps(features[3]))
        features[3]["attributes"]["NAME"] = "Drifted Field"
    if geometry_drift:
        features[4] = json.loads(json.dumps(features[4]))
        # Shift an interior vertex, keeping the served ring closed so the drift
        # surfaces at the pinned geometry digest, not as a structural error.
        features[4]["geometry"]["rings"][0][1][0] += 1.5
    if null_attribute:
        features[5] = json.loads(json.dumps(features[5]))
        features[5]["attributes"]["RANK"] = None
    if nonfinite_coordinate:
        features[6] = json.loads(json.dumps(features[6]))
        features[6]["geometry"]["rings"][0][1][1] = float("nan")
    if unclosed_ring:
        features[7] = json.loads(json.dumps(features[7]))
        features[7]["geometry"]["rings"][0] = features[7]["geometry"]["rings"][0][:-1]
    if page_reorder and len(features) > 3:
        features[1], features[2] = features[2], features[1]

    service_comment = "synthetic-citation-anchor available at synthetic-ofr-link-anchor." if marker_drift \
        else "synthetic-citation-anchor scale 1:5,000,000 available at synthetic-ofr-link-anchor."
    folder = {
        "currentVersion": 12.1, "folders": [],
        "services": [{"name": name, "type": kind} for name, kind in source.EXPECTED_NCRDS_SERVICES],
    }
    if folder_drift:
        folder["services"] = folder["services"][:-1]
    service = {
        "mapName": source.SERVICE_MAP_NAME,
        "documentInfo": {"Title": source.SERVICE_TITLE, "Comments": service_comment},
        "description": service_comment,
    }
    bounds = {
        "xmin": min(point[0] for feature in clean_features for ring in feature["geometry"]["rings"] for point in ring),
        "ymin": min(point[1] for feature in clean_features for ring in feature["geometry"]["rings"] for point in ring),
        "xmax": max(point[0] for feature in clean_features for ring in feature["geometry"]["rings"] for point in ring),
        "ymax": max(point[1] for feature in clean_features for ring in feature["geometry"]["rings"] for point in ring),
    }
    layer = {
        "id": source.LAYER_ID, "name": source.LAYER_NAME, "type": "Feature Layer",
        "geometryType": "esriGeometryPolygon", "displayField": "NAME",
        "maxRecordCount": page_size,
        "advancedQueryCapabilities": {"supportsPagination": True},
        "sourceSpatialReference": _crs_doc(),
        "extent": {**bounds, "spatialReference": _crs_doc()},
        "fields": _layer_fields_doc(schema_drift=schema_drift),
    }
    ids = {"objectIdFieldName": "OBJECTID", "objectIds": list(range(1, records + 1))}
    if ids_drift:
        ids["objectIds"] = list(range(1, records)) + [records + 7]
    count = {"count": records - 1 if count_drift else records}
    if count_digest_drift:
        count["unexpected_extra_field"] = True
    layer_after = layer
    if layer_volatile_only:
        # Only the producer-volatile renderer color ramp changes between the
        # bracket reads; this must not fail the capture.
        layer_after = copy.deepcopy(layer)
        layer_after["drawingInfo"] = {
            "renderer": {"authoringInfo": {"colorRamp": {"name": "drifted-ramp"}},
                         "type": "simple"},
        }
    bodies = {
        "ncrds_directory": json.dumps(folder).encode(),
        "service_before": json.dumps(service).encode(),
        "layer_before": json.dumps(layer).encode(),
        "count_before": json.dumps(count).encode(),
        "ids_before": json.dumps(ids).encode(),
        "layer_after": json.dumps(layer_after).encode(),
        "service_after": json.dumps(service).encode(),
        "count_after": json.dumps(count).encode(),
        "ids_after": json.dumps(ids).encode(),
    }
    page_names = []
    for offset in range(0, records, page_size):
        page = {"spatialReference": _crs_doc(), "features": features[offset:offset + page_size]}
        if exceeded_transfer_limit and offset == 0:
            page["exceededTransferLimit"] = True
        page_names.append(f"page_{len(page_names):03d}")
        bodies[page_names[-1]] = json.dumps(page).encode()
    pins = {
        "count_sha256": _sha(_canon({"count": records})),
        "id_sha256": _sha(_canon(sorted(ids["objectIds"]))),
        "record_sha256": _sha(_canon([feature["attributes"] for feature in
                                      (clean_features if value_drift else features)])),
        "geometry_sha256": _sha(_canon([feature["geometry"]["rings"] for feature in
                                        (clean_features if geometry_drift else features)])),
        "invalid_record": invalid_record,
        "records": records,
        "page_size": page_size,
        "page_count": len(page_names),
        "bounds": [bounds["xmin"], bounds["ymin"], bounds["xmax"], bounds["ymax"]],
        "province_counts": {province: sum(1 for fid in range(records) if _attribute_values(fid)["PROVINCE"] == province)
                            for province in _PROVINCES},
        "age_counts": {age: sum(1 for fid in range(records) if _attribute_values(fid)["AGE"] == age) for age in _AGES},
        "rank_counts": {rank: sum(1 for fid in range(records) if _attribute_values(fid)["RANK"] == rank) for rank in _RANKS},
        "distinct_names": min(records, _DISTINCT_NAMES),
        "marker_drift": marker_drift,
    }
    return {"bodies": bodies, "pins": pins}


def _patched_source(fixtures: dict):
    """Patch the digest pins, identity markers and layout to the synthetic fixtures."""
    pins = fixtures["pins"]
    capture_names = tuple(list(source.CAPTURE_NAMES[:5])
                          + [f"page_{index:03d}" for index in range(pins["page_count"])]
                          + list(source.CAPTURE_NAMES[-4:]))
    return patch.multiple(
        source,
        CAPTURE_NAMES=capture_names,
        PAGE_COUNT=pins["page_count"],
        PAGE_SIZE=pins["page_size"],
        EXPECTED_RECORD_COUNT=pins["records"],
        EXPECTED_COUNT_SHA256=pins["count_sha256"],
        EXPECTED_ID_SHA256=pins["id_sha256"],
        EXPECTED_RECORD_SHA256=pins["record_sha256"],
        EXPECTED_GEOMETRY_SHA256=pins["geometry_sha256"],
        EXPECTED_LAYER_EXTENT={"xmin": pins["bounds"][0], "ymin": pins["bounds"][1],
                               "xmax": pins["bounds"][2], "ymax": pins["bounds"][3]},
        EXPECTED_BOUNDS=list(pins["bounds"]),
        EXPECTED_INVALID_RECORD_NUMBERS=(pins["invalid_record"],) if pins["invalid_record"] is not None else (),
        PROVINCE_COUNTS=pins["province_counts"],
        AGE_COUNTS=pins["age_counts"],
        RANK_COUNTS=pins["rank_counts"],
        DISTINCT_NAME_COUNT=pins["distinct_names"],
        SERVICE_CITATION_MARK="synthetic-citation-anchor" if not pins["marker_drift"] else "drifted-service-citation",
        SERVICE_OFR_LINK_MARK="synthetic-ofr-link-anchor" if not pins["marker_drift"] else "drifted-ofr-link",
    )


class _FixtureTransport(httpx.BaseTransport):
    def __init__(self, bodies: dict[str, bytes], *, fail_page_once: bool = False):
        # URLs are resolved lazily so the transport sees any patched pagination.
        self.bodies = bodies
        self.fail_page_once = fail_page_once
        self.page_calls = 0

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for name, body in self.bodies.items():
            if source._request_url(name) == url:
                if self.fail_page_once and name.startswith("page_"):
                    self.page_calls += 1
                    if self.page_calls == 1:
                        return httpx.Response(503, stream=httpx.ByteStream(b"temporarily unavailable"),
                                              request=request, headers={"content-type": "text/plain"})
                return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                                      headers={"content-type": "application/json"})
        return httpx.Response(404, content=b"not found", request=request, headers={"content-type": "text/plain"})


class UsCoalFieldsRegistrationTests(unittest.TestCase):
    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(us_coal_fields, hifld_ingest_assets)
        self.assertEqual(us_coal_fields.key.to_user_string(), "ingest/us-coal-fields-1/us-coal-fields-1")
        self.assertEqual(us_coal_fields_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = us_coal_fields_complete({"feature_count": source.EXPECTED_RECORD_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        for flag in ("baseline_comparison_accepted", "source_release_accepted", "schema_contract_accepted",
                     "id_contract_accepted", "coverage_accepted", "rights_accepted", "release_accepted"):
            self.assertFalse(check.metadata[flag].value)
        self.assertFalse(us_coal_fields_complete({"feature_count": source.EXPECTED_RECORD_COUNT - 1,
                                                  "source_population_complete": True}).passed)
        self.assertFalse(us_coal_fields_complete({"feature_count": source.EXPECTED_RECORD_COUNT,
                                                  "source_population_complete": False}).passed)


class UsCoalFieldsCaptureTests(unittest.TestCase):
    def _new_storage(self) -> StagingStorageResource:
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-source-")
        self.addCleanup(temporary.cleanup)
        return StagingStorageResource(use_local=True, local_dir=temporary.name)

    def _capture(self, fixtures: dict, *, version: str = "v20261114T000000Z",
                 retained_source_version: str | None = None, fail_page_once: bool = False,
                 storage: StagingStorageResource | None = None):
        storage = storage or self._new_storage()
        with _patched_source(fixtures):
            if retained_source_version is not None:
                result = source.capture_validated(storage, version, retained_source_version=retained_source_version)
            else:
                with httpx.Client(transport=_FixtureTransport(fixtures["bodies"], fail_page_once=fail_page_once)) as client:
                    result = source.capture_validated(storage, version, client=client)
        return storage, result

    def _prefix(self, storage, version="v20261114T000000Z"):
        return storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, version, "")

    def _local_path(self, storage, key):
        return Path(storage.local_dir) / key

    def _refuse(self, fixtures: dict, expected: str, extra_patches: dict | None = None):
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-refusal-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with ExitStack() as patches:
            patches.enter_context(_patched_source(fixtures))
            if extra_patches:
                patches.enter_context(patch.multiple(source, **extra_patches))
            with httpx.Client(transport=_FixtureTransport(fixtures["bodies"])) as client:
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_validated(storage, "v20261114T000000Z", client=client)
        prefix = self._prefix(storage)
        self.assertFalse(storage.object_exists(prefix + f"geoparquet/{source.FILE_SLUG}.parquet"))
        quarantine = json.loads(storage.read_key(prefix + "metadata/quarantine.json"))
        self.assertEqual(quarantine["status"], "quarantined")
        self.assertIs(quarantine["release_accepted"], False)
        return storage

    def test_complete_capture_stages_features_values_ids_and_parquet(self):
        fixtures = _synthetic_bodies()
        pins = fixtures["pins"]
        storage, result = self._capture(fixtures)
        self.assertEqual(result["feature_count"], pins["records"])
        self.assertTrue(result["source_population_complete"])
        prefix = self._prefix(storage)
        table = pq.read_table(self._local_path(storage, prefix + f"geoparquet/{source.FILE_SLUG}.parquet"))
        self.assertEqual(table.num_rows, pins["records"])
        self.assertEqual(table.column("OBJECTID").type, pyarrow.int64())
        self.assertEqual(table.column("AREA").type, pyarrow.float64())
        self.assertEqual(table.column("PERIMETER").type, pyarrow.float64())
        self.assertEqual(table.column("Shape_Length").type, pyarrow.float64())
        self.assertEqual(table.column("Shape_Area").type, pyarrow.float64())
        self.assertEqual(table.column("PROVINCE").type, pyarrow.string())
        self.assertEqual([int(value) for value in table.column("OBJECTID").to_pylist()],
                         list(range(1, pins["records"] + 1)))
        self.assertEqual(table.column("PROVINCE").to_pylist()[:6], _PROVINCES)
        first = table.slice(0, 1).to_pylist()[0]
        self.assertEqual(first["AREA"], 0.018)
        self.assertEqual(first["NAME"], "Field 0")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertIn("3857", json.dumps(geo["columns"]["geometry"]["crs"]))
        native_capture = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(native_capture["record_count"], pins["records"])
        self.assertEqual(native_capture["capture_mode"], "live_http_capture")
        for flag in ("historical_hifld_continuity_accepted", "source_contract_accepted", "schema_contract_accepted",
                     "id_contract_accepted", "coverage_accepted", "rights_accepted", "release_accepted"):
            self.assertIs(native_capture[flag], False)
        receipts = [name for name in storage.list_keys(source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z")
                    if "/raw/" in name and name.endswith(".receipt.json") and ".attempt_" not in name]
        self.assertEqual(len(receipts), len(source.CAPTURE_NAMES))
        page_receipt = json.loads(storage.read_key(prefix + "raw/page_000.receipt.json"))
        self.assertEqual(page_receipt["url"], source._request_url("page_000"))
        self.assertEqual(page_receipt["status"], 200)
        self.assertEqual(page_receipt["bytes"], len(fixtures["bodies"]["page_000"]))

    def test_paged_capture_walks_every_page_in_roster_order(self):
        fixtures = _synthetic_bodies(page_size=100)
        storage, result = self._capture(fixtures)
        self.assertEqual(result["feature_count"], fixtures["pins"]["records"])
        page_receipts = [name for name in storage.list_keys(source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z")
                         if "/raw/page_" in name and name.endswith(".receipt.json") and ".attempt_" not in name]
        self.assertEqual(len(page_receipts), fixtures["pins"]["page_count"])

    def test_staged_native_candidate_reads_through_prebuilt_catalog(self):
        fixtures = _synthetic_bodies(invalid_record=13)
        storage, _result = self._capture(fixtures)
        manifest = json.loads(storage.read_key(self._prefix(storage) + "metadata/source_manifest.json"))
        summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z",
                                           "us-coal-fields-1", source_metadata=manifest)
        self.assertEqual(summary.quality_manifest["feature_count"], fixtures["pins"]["records"])
        self.assertEqual(summary.quality_manifest["invalid_geometry_count"], 1)
        self.assertEqual(summary.quality_manifest["catalog_mode"], "native_geoparquet_full_population")

    def test_source_manifest_declares_pending_contracts_producer_inputs_and_banned_outputs(self):
        fixtures = _synthetic_bodies()
        storage, _result = self._capture(fixtures)
        manifest = json.loads(storage.read_key(self._prefix(storage) + "metadata/source_manifest.json"))
        self.assertIs(manifest["approved"], False)
        self.assertEqual(manifest["agency"], "U.S. Geological Survey (producer; authority acceptance pending)")
        self.assertEqual(manifest["date_issued"], source.RELEASE_DATE)
        self.assertEqual({item["input_id"].split(":")[0] for item in manifest["inputs"]},
                         set(source.CAPTURE_NAMES))
        for item in manifest["inputs"]:
            self.assertEqual(item["role"], "producer_publication")
            self.assertIn("energy.usgs.gov", item["canonical_url"])
            self.assertEqual(item["host"], "energy.usgs.gov")
            self.assertIn("No source, contract, baseline", item["license"])
            self.assertEqual(item["release_date"], source.RELEASE_DATE)
            self.assertTrue(item["release_date_basis"])
        lineage = manifest["lineage"]
        self.assertIn("maintains and publishes", lineage["maintained_publication"])
        self.assertIn("Open-File Report 2012-1205", lineage["origin_publication"])
        self.assertIn("None planned", lineage["origin_publication"])
        self.assertIn("Deprecate", lineage["hifld_continuity"]["basis"])
        self.assertIn("DataLumos archive project 240310", lineage["banned_copies_never_fetched"])
        self.assertIn("esri_landscape2", lineage["banned_copies_never_fetched"])
        for flag in ("source_contract_accepted", "schema_contract_accepted", "id_contract_accepted",
                     "coverage_accepted", "rights_accepted", "baseline_accepted", "release_accepted"):
            self.assertIs(lineage[flag], False)
        self.assertEqual(set(manifest["unsupported_output_formats"]), {"shapefile_zip", "geopackage", "pmtiles"})
        prebuilt = manifest["prebuilt_output_policy"]["formats"]["geoparquet"]
        self.assertEqual(prebuilt["objects"][0]["path"], f"geoparquet/{source.FILE_SLUG}.parquet")
        staged_parquet = storage.read_key(self._prefix(storage) + f"geoparquet/{source.FILE_SLUG}.parquet")
        self.assertEqual(prebuilt["objects"][0]["sha256"], _sha(staged_parquet))
        self.assertEqual(prebuilt["objects"][0]["bytes"], len(staged_parquet))

    def test_retained_capture_replay_reuses_original_bodies_and_emits_provenance(self):
        fixtures = _synthetic_bodies()
        storage, first = self._capture(fixtures)
        with _patched_source(fixtures):
            result = source.capture_validated(storage, "v20261114T000001Z", retained_source_version="v20261114T000000Z")
        self.assertEqual(result["feature_count"], first["feature_count"])
        self.assertEqual(result["records_sha256"], first["records_sha256"])
        self.assertEqual(result["parquet_sha256"], first["parquet_sha256"])
        replay_prefix = self._prefix(storage, "v20261114T000001Z")
        self.assertEqual(storage.read_key(replay_prefix + "raw/page_000"),
                         storage.read_key(self._prefix(storage) + "raw/page_000"))
        native_capture = json.loads(storage.read_key(replay_prefix + "metadata/native_capture.json"))
        self.assertEqual(native_capture["capture_mode"], "retained_response_replay")
        self.assertEqual(native_capture["retained_source_capture_version"], "v20261114T000000Z")
        manifest = json.loads(storage.read_key(replay_prefix + "metadata/source_manifest.json"))
        self.assertEqual(manifest["lineage"]["raw_capture_mode"], "retained_response_replay")

    def test_retained_replay_of_absent_capture_is_refused_and_quarantined(self):
        fixtures = _synthetic_bodies()
        storage, _first = self._capture(fixtures)
        with _patched_source(fixtures):
            with self.assertRaisesRegex(ValueError, "retained_source_response_missing"):
                source.capture_validated(storage, "v20261114T000002Z", retained_source_version="v20261114T000009Z")
        prefix = self._prefix(storage, "v20261114T000002Z")
        quarantine = json.loads(storage.read_key(prefix + "metadata/quarantine.json"))
        self.assertEqual(quarantine["failure_code"], "retained_source_response_missing")

    def test_transient_page_failure_is_retained_before_bounded_retry(self):
        fixtures = _synthetic_bodies()
        storage, result = self._capture(fixtures, fail_page_once=True)
        self.assertEqual(result["feature_count"], fixtures["pins"]["records"])
        keys = storage.list_keys(source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z")
        self.assertTrue(any(key.endswith("raw/page_000.attempt_1.receipt.json") for key in keys))
        failed_receipt = json.loads(storage.read_key(self._prefix(storage) + "raw/page_000.attempt_1.receipt.json"))
        self.assertEqual(failed_receipt["status"], 503)

    def test_service_citation_and_directory_marker_drift_is_refused(self):
        self._refuse(_synthetic_bodies(marker_drift=True), "producer_service_citation_or_description_drift")
        self._refuse(_synthetic_bodies(folder_drift=True), "producer_service_directory_roster_drift")

    def test_count_id_and_schema_drift_is_refused(self):
        self._refuse(_synthetic_bodies(count_drift=True), "source_count_drift")
        self._refuse(_synthetic_bodies(count_digest_drift=True), "source_count_digest_drift")
        self._refuse(_synthetic_bodies(ids_drift=True), "source_id_population_drift")
        self._refuse(_synthetic_bodies(schema_drift=True), "source_schema_contract_drift")

    def test_volatile_renderer_ramp_between_brackets_is_tolerated(self):
        # The producer rotates drawingInfo.renderer.authoringInfo.colorRamp on
        # every layer-document request; the capture must still succeed when
        # only that presentation metadata differs between the bracket reads.
        storage, result = self._capture(_synthetic_bodies(layer_volatile_only=True))
        self.assertEqual(result["feature_count"], source.EXPECTED_RECORD_COUNT)

    def test_page_slice_reorder_and_exceeded_limit_are_refused(self):
        self._refuse(_synthetic_bodies(page_reorder=True), "source_page_id_slice_incomplete_or_reordered")
        self._refuse(_synthetic_bodies(exceeded_transfer_limit=True), "source_page_unexpected_exceeded_transfer_limit")

    def test_record_value_and_geometry_digest_drift_is_refused(self):
        self._refuse(_synthetic_bodies(value_drift=True), "source_record_value_drift")
        self._refuse(_synthetic_bodies(geometry_drift=True), "source_geometry_or_coverage_drift")

    def test_null_nonfinite_and_unclosed_payloads_are_refused(self):
        self._refuse(_synthetic_bodies(null_attribute=True), "null_or_unexpected_source_attribute_type")
        self._refuse(_synthetic_bodies(nonfinite_coordinate=True), "source_polygon_ring_invalid")
        self._refuse(_synthetic_bodies(unclosed_ring=True), "source_polygon_ring_invalid")

    def test_bounds_and_validity_drift_are_refused(self):
        # The served invalid geometry is refused when the pin does not declare it.
        self._refuse(_synthetic_bodies(invalid_record=13), "source_geometry_validity_drift",
                     extra_patches={"EXPECTED_INVALID_RECORD_NUMBERS": ()})
        fixtures = _synthetic_bodies()
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-bounds-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with ExitStack() as patches:
            patches.enter_context(_patched_source(fixtures))
            patches.enter_context(patch.object(source, "EXPECTED_BOUNDS",
                                               [value + 1.0 for value in fixtures["pins"]["bounds"]]))
            with httpx.Client(transport=_FixtureTransport(fixtures["bodies"])) as client:
                with self.assertRaisesRegex(ValueError, "source_geometry_bounds_drift"):
                    source.capture_validated(storage, "v20261114T000000Z", client=client)

    def test_value_population_drift_is_refused(self):
        fixtures = _synthetic_bodies()
        self._refuse(fixtures, "source_attribute_value_population_drift",
                     extra_patches={"PROVINCE_COUNTS": {**fixtures["pins"]["province_counts"], "Eastern": 246}})
        fixtures = _synthetic_bodies()
        self._refuse(fixtures, "source_name_population_drift",
                     extra_patches={"DISTINCT_NAME_COUNT": fixtures["pins"]["distinct_names"] + 1})

    def test_remote_or_published_staging_is_refused_before_any_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            for resource in (PublishedStorageResource(local_dir=tmp),
                             StagingStorageResource(use_local=False, bucket="forbidden")):
                with patch.object(source, "_request") as request:
                    with self.assertRaisesRegex(ValueError, "requires_isolated_local_staging"):
                        source.capture_validated(resource, "v20261114T000000Z")
                    request.assert_not_called()

    def test_total_capture_budget_is_enforced(self):
        fixtures = _synthetic_bodies()
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-budget-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with _patched_source(fixtures), patch.object(source, "TOTAL_CAPTURE_BUDGET", 1):
            with httpx.Client(transport=_FixtureTransport(fixtures["bodies"])) as client:
                with self.assertRaisesRegex(ValueError, "source_total_capture_budget_exceeded|incomplete_source_response"):
                    source.capture_validated(storage, "v20261114T000000Z", client=client)


class _RealArchiveTransport(httpx.BaseTransport):
    """Serve the pinned real capture bodies from a local capture directory."""

    def __init__(self, package: Path):
        self.package = package

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        for name in source.CAPTURE_NAMES:
            if str(request.url) == source._request_url(name):
                body = (self.package / f"{name}.body").read_bytes()
                return httpx.Response(200, stream=httpx.ByteStream(body), request=request,
                                      headers={"content-type": "application/json"})
        return httpx.Response(404, content=b"not found", request=request, headers={"content-type": "text/plain"})


@unittest.skipUnless(os.environ.get(_CAPTURE_ARCHIVE_ENV),
                     "complete USGS NCRDS CoalByRank capture opt-in required; see docs/dataset-sources/us-coal-fields/README.md")
class UsCoalFieldsRealCaptureTests(unittest.TestCase):
    """Replay the pinned real producer capture through the full flow."""

    @classmethod
    def setUpClass(cls):
        cls.package_tmp = tempfile.TemporaryDirectory()
        cls.package = Path(cls.package_tmp.name)
        with tarfile.open(os.environ[_CAPTURE_ARCHIVE_ENV], "r:gz") as archive:
            for member in archive:
                if not member.isfile() or Path(member.name).name != member.name:
                    raise ValueError("unsafe test archive")
                (cls.package / member.name).write_bytes(archive.extractfile(member).read())

    @classmethod
    def tearDownClass(cls):
        cls.package_tmp.cleanup()

    def _local_path(self, storage, key):
        return Path(storage.local_dir) / key

    def _real_capture(self, version: str = "v20261114T000000Z"):
        temporary = tempfile.TemporaryDirectory(prefix="us-coal-fields-real-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with httpx.Client(transport=_RealArchiveTransport(self.package)) as client:
            result = source.capture_validated(storage, version, client=client)
        return storage, result

    def test_real_capture_stages_real_features_values_and_crs(self):
        storage, result = self._real_capture()
        self.assertEqual(result["feature_count"], 602)
        prefix = storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000000Z", "")
        table = pq.read_table(self._local_path(storage, prefix + f"geoparquet/{source.FILE_SLUG}.parquet"))
        self.assertEqual(table.num_rows, 602)
        self.assertEqual(table.column("OBJECTID").type, pyarrow.int64())
        self.assertIn("Great Falls Field", table.column("NAME").to_pylist())
        self.assertIn("Pennsylvanian-Permian", table.column("AGE").to_pylist())
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertIn("3857", json.dumps(geo["columns"]["geometry"]["crs"]))
        manifest = json.loads(storage.read_key(prefix + "metadata/source_manifest.json"))
        self.assertIs(manifest["approved"], False)
        self.assertEqual(manifest["lineage"]["source_release_date"], source.RELEASE_DATE)
        for item in manifest["inputs"]:
            self.assertEqual(item["role"], "producer_publication")
            self.assertEqual(item["host"], "energy.usgs.gov")

    def test_real_capture_replay_reuses_bodies(self):
        storage, _first = self._real_capture()
        result = source.capture_validated(storage, "v20261114T000001Z", retained_source_version="v20261114T000000Z")
        self.assertEqual(result["feature_count"], 602)
        native_capture = json.loads(storage.read_key(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, "v20261114T000001Z", "") + "metadata/native_capture.json"))
        self.assertEqual(native_capture["capture_mode"], "retained_response_replay")


if __name__ == "__main__":
    unittest.main()
