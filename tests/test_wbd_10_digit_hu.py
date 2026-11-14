from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from contextlib import contextmanager
from unittest import mock

import httpx
import pyarrow.parquet as pq

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.wbd_10_digit_hu import (
    wbd_10_digit_hu_complete, wbd_10_digit_hu_draft_refresh_schedule, wbd_10_digit_hu_watershed,
)
from dagster_hifld.catalog import summarize_staged_catalog
from dagster_hifld.native_geoparquet_catalog import discover_prebuilt_native
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import wbd_10_digit_hu as source

_VERSION = "v20261114T000000Z"

_SERVICE_DESCRIPTION = (
    "Abstract: This file contains Hydrologic Unit (HU) polygon boundaries for the United States, "
    "Puerto Rico, and the U.S. Virgin Islands. The data is a seamless National representation of HU "
    "boundaries from 2 to 16 digits compiled from U.S. Geological Survey (USGS) National "
    "Hydrography Dataset (NHD) and U.S. Department of Agriculture (USDA) National Resources "
    "Conservation Service (NRCS) Watershed Boundary Dataset (WBD) sources. Purpose: This data is "
    "intended primarily for geographic display and analysis of regional and national data, and can "
    "also be used for illustration purposes at intermediate or small scales (1:250,000 to "
    "1:2,000,000). See https://apps.nationalmap.gov/help/ for assistance with The National Map "
    "viewer, download client, services, or metadata.Use Constraints: __ None. All data are open and "
    "non-proprietary. However, users should be aware that temporal changes may have occurred since "
    "this dataset was collected and that some parts of this data may no longer represent actual "
    "conditions. Users should not use this data for critical applications without a full awareness "
    "of its limitations. This dataset is not intended to be used for site-specific regulatory "
    "determinations. Acknowledgment of the U.S. Geological Survey would be appreciated for products "
    "derived from these data."
)
_LAYER_DESCRIPTION = (
    "Abstract: This file contains Hydrologic Unit (HU) polygon boundaries for the United States, "
    "Puerto Rico, and the U.S. Virgin Islands. The data is a seamless National representation of HU "
    "boundaries from 2 to 14 digits compiled from U.S. Geological Survey (USGS) National "
    "Hydrography Dataset (NHD) and U.S. Department of Agriculture (USDA) National Resources "
    "Conservation Service (NRCS) Watershed Boundary Dataset (WBD) sources. Purpose: This data is "
    "intended primarily for geographic display and analysis of regional and national data, and can "
    "also be used for illustration purposes at intermediate or small scales (1:250,000 to "
    "1:2,000,000)."
)
_HUTYPE_DOMAIN = {
    "type": "codedValue", "name": "HUType Domain", "description": "",
    "codedValues": [{"name": "Standard", "code": "S"}, {"name": "Closed Basin", "code": "C"},
                    {"name": "Frontal", "code": "F"}, {"name": "Multiple Outlet", "code": "M"},
                    {"name": "Water", "code": "W"}, {"name": "Island", "code": "I"},
                    {"name": "Urban", "code": "U"}, {"name": "Indeterminate", "code": "D"}],
    "mergePolicy": "esriMPTDefaultValue", "splitPolicy": "esriSPTDuplicate",
}

_DIR = "https://hydro.nationalmap.gov/arcgis/rest/services"
_SERVICE_URL = source.SERVICE_URL
_LAYER_URL = source.LAYER_URL


def _fixture_fields() -> list[dict]:
    fields = []
    for name, kind in source.EXPECTED_FIELDS:
        field = {"name": name, "type": kind, "alias": name.upper() if name != "objectid" else "ObjectID",
                 "nullable": kind != "esriFieldTypeOID"}
        if kind == "esriFieldTypeString":
            field["length"] = {"tnmid": 40, "metasourceid": 40, "sourcedatadesc": 100,
                               "sourceoriginator": 130, "sourcefeatureid": 40,
                               "referencegnis_ids": 50, "states": 50, "huc10": 10,
                               "name": 120, "hutype": 1, "humod": 30}[name]
        if name == "hutype":
            field["domain"] = _HUTYPE_DOMAIN
        if name == "globalid":
            field["length"] = 38
        fields.append(field)
    return fields


_FIELDS = _fixture_fields()

_SERVICE = {
    "currentVersion": 11.3, "maxRecordCount": 2000, "supportedQueryFormats": "JSON",
    "serviceDescription": _SERVICE_DESCRIPTION, "copyrightText": source.EXPECTED_COPYRIGHT_TEXT,
    "layers": [{"id": layer_id, "name": name} for layer_id, name in source.EXPECTED_SERVICE_LAYERS],
}
_ALIAS = {
    "currentVersion": 11.3, "maxRecordCount": 2000, "supportedQueryFormats": "JSON, geoJSON, PBF",
    "copyrightText": source.EXPECTED_COPYRIGHT_TEXT,
    "layers": [{"id": layer_id, "name": name} for layer_id, name in source.EXPECTED_SERVICE_LAYERS],
}
_DIRECTORY = {"currentVersion": 11.3, "folders": ["Utilities"], "services": [
    {"name": "3DHP_all", "type": "FeatureServer"}, {"name": "nhd", "type": "MapServer"},
    {"name": "wbd", "type": "FeatureServer"}, {"name": "wbd", "type": "MapServer"},
]}
_LAYER = {
    "id": source.LAYER_ID, "name": "10-digit HU (Watershed)", "type": "Feature Layer",
    "geometryType": "esriGeometryPolygon", "hasZ": False, "hasM": False,
    "maxRecordCount": 2000, "objectIdField": "objectid", "globalIdField": "globalid",
    "supportedQueryFormats": "JSON, geoJSON, PBF", "copyrightText": source.EXPECTED_COPYRIGHT_TEXT,
    "description": _LAYER_DESCRIPTION,
    "advancedQueryCapabilities": {"supportsPagination": True},
    "sourceSpatialReference": {"wkid": 102100, "latestWkid": 3857},
    "extent": {**source.EXPECTED_LAYER_EXTENT, "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
    "fields": _FIELDS,
}

# Fixture shells: a clockwise rectangle per identity (negative shoelace area,
# matching the producer's verified shell semantics), one feature with an extra
# counterclockwise hole inside its single shell and one feature with two
# separate shells, so the grouping contract is exercised without ambiguity.
_WIDTH, _HEIGHT = 0.5, 0.4


def _shell(identity: int) -> list[list[float]]:
    x = -100.0 + (identity % 100)
    y = 30.0 + (identity % 40)
    return [[x, y], [x + _WIDTH, y], [x + _WIDTH, y - _HEIGHT], [x, y - _HEIGHT], [x, y]]


def _hole(identity: int) -> list[list[float]]:
    # Counterclockwise (positive shoelace) so the producer semantics treat it
    # as a hole of the identity's shell.
    x = -100.0 + (identity % 100)
    y = 30.0 + (identity % 40)
    return [[x + 0.1, y - 0.1], [x + 0.1, y - 0.3], [x + 0.3, y - 0.3], [x + 0.3, y - 0.1],
            [x + 0.1, y - 0.1]]


def _second_shell(identity: int) -> list[list[float]]:
    x = -100.0 + (identity % 100) + 2.0
    y = 30.0 + (identity % 40)
    return [[x, y], [x + 0.2, y], [x + 0.2, y - 0.2], [x, y - 0.2], [x, y]]


def _fixture_grouped(identity: int) -> list[list[list[list[float]]]]:
    """Grouped rings per feature: one entry [shell, *holes] per polygon member."""
    if identity % 17 == 0:
        # Two separate exteriors: a two-member MultiPolygon.
        return [[list(_shell(identity))], [list(_second_shell(identity))]]
    if identity % 11 == 0:
        # One exterior with one hole.
        return [[list(_shell(identity)), list(_hole(identity))]]
    return [[list(_shell(identity))]]


def _fixture_attributes(identity: int) -> dict:
    values = {}
    for name, kind in source.EXPECTED_FIELDS:
        if name == "objectid":
            values[name] = identity
        elif name == "loaddate":
            values[name] = 1723726881000 + identity
        elif kind == "esriFieldTypeDouble":
            values[name] = 11.5 * identity
        elif name in ("sourcefeatureid", "humod"):
            values[name] = None
        elif name == "huc10":
            values[name] = f"{identity:010d}"
        elif name == "states":
            values[name] = "SD"
        elif name == "hutype":
            values[name] = "S"
        elif name == "globalid":
            values[name] = "{%08X-0000-0000-0000-000000000000}" % identity
        else:
            values[name] = f"{name}-{identity}"
    return values


def _fixture_population() -> tuple[list[int], list[dict], list[list]]:
    ids = list(range(1, source.EXPECTED_COUNT + 1))
    attributes = [_fixture_attributes(identity) for identity in ids]
    grouped = [_fixture_grouped(identity) for identity in ids]
    return ids, attributes, grouped


def _fixture_pins(ids: list[int], attributes: list[dict], grouped: list[list]) -> dict:
    pins = {"EXPECTED_ID_SHA256": source._sha(source._canonical(sorted(ids))),
            "EXPECTED_SCHEMA_SHA256": source._schema_sha(
                [{key: field[key] for key in ("name", "type", "alias", "length", "nullable", "domain")
                  if key in field} for field in _FIELDS]),
            "EXPECTED_RECORD_SHA256": source._sha(source._canonical(attributes))}
    stream = hashlib.sha256()
    stream.update(b"[")
    for index, identity in enumerate(ids):
        if index:
            stream.update(b",")
        stream.update(source._canonical([identity, grouped[index]]))
    stream.update(b"]")
    pins["EXPECTED_GEOMETRY_SHA256"] = stream.hexdigest()
    xs: list[float] = []
    ys: list[float] = []
    for grouping in grouped:
        for member in grouping:
            for ring in member:
                xs.extend(point[0] for point in ring)
                ys.extend(point[1] for point in ring)
    pins["EXPECTED_OBSERVED_BOUNDS"] = {"xmin": min(xs), "ymin": min(ys),
                                        "xmax": max(xs), "ymax": max(ys)}
    return pins


@contextmanager
def _pinned_pins(pins: dict):
    with mock.patch.multiple(source, **pins, DOC_BASE_DELAY_SECONDS=0.01,
                             PAGE_BASE_DELAY_SECONDS=0.01):
        yield


class ArcGISFixture(httpx.BaseTransport):
    def __init__(self, *, schema_drift=False, changed_page_crs=False, geometry_extra_key=False,
                 unclosed_ring=False, out_of_bounds=False, fail_page_once=None,
                 stubborn_over=None, directory_drift=False, service_change_after=False,
                 shifted_roster=False, count_drift=False, omit_layer_extent=False,
                 layer_description_drift=False, alias_drift=False, page_id_mismatch=False,
                 extra_attribute=False, duplicate_huc10=False, null_geometry=False,
                 hole_unassigned=False, page_unexpected_member=False, transfer_truncated=False,
                 copyright_drift=False):
        self.ids = (list(range(2, source.EXPECTED_COUNT + 2)) if shifted_roster
                    else list(range(1, source.EXPECTED_COUNT + 1)))
        self.schema_drift = schema_drift
        self.changed_page_crs = changed_page_crs
        self.geometry_extra_key = geometry_extra_key
        self.unclosed_ring = unclosed_ring
        self.out_of_bounds = out_of_bounds
        self.fail_page_once = fail_page_once
        # (max_window_size, contained_id): windows larger than max_window_size
        # that contain the id refuse persistently, so bisection below the size
        # threshold escapes the refusal.
        self.stubborn_over = stubborn_over
        self.directory_drift = directory_drift
        self.service_change_after = service_change_after
        self.count_drift = count_drift
        self.omit_layer_extent = omit_layer_extent
        self.layer_description_drift = layer_description_drift
        self.alias_drift = alias_drift
        self.page_id_mismatch = page_id_mismatch
        self.extra_attribute = extra_attribute
        self.duplicate_huc10 = duplicate_huc10
        self.null_geometry = null_geometry
        self.hole_unassigned = hole_unassigned
        self.page_unexpected_member = page_unexpected_member
        self.transfer_truncated = transfer_truncated
        self.copyright_drift = copyright_drift
        self.service_calls = 0
        self.count_calls = 0
        self.id_calls = 0

    def handle_request(self, request):
        query = request.url.params
        path = request.url.path
        if path == "/arcgis/rest/services":
            data = {"services": []} if self.directory_drift else _DIRECTORY
        elif path == "/arcgis/rest/services/wbd/FeatureServer":
            self.service_calls += 1
            data = dict(_SERVICE)
            if self.service_change_after and self.service_calls == 2:
                data = {**_SERVICE, "serviceDescription": "Some other service description."}
            if self.copyright_drift:
                data["copyrightText"] = "Someone else's dataset"
        elif path == "/arcgis/rest/services/wbd/MapServer":
            data = dict(_ALIAS)
            if self.alias_drift:
                data["layers"] = [{"id": 0, "name": "Something else"}]
        elif path == "/arcgis/rest/services/wbd/FeatureServer/5":
            data = dict(_LAYER)
            if self.schema_drift:
                data["fields"] = [dict(field) for field in _FIELDS]
                data["fields"][0]["type"] = "esriFieldTypeDouble"
            if self.omit_layer_extent:
                data.pop("extent")
            if self.layer_description_drift:
                data["description"] = "Unrelated layer description."
        elif path.endswith("/query"):
            if query.get("returnCountOnly") == "true":
                self.count_calls += 1
                count = (source.EXPECTED_COUNT - 1
                         if self.count_drift and self.count_calls == 2 else source.EXPECTED_COUNT)
                data = {"count": count}
            elif query.get("returnIdsOnly") == "true":
                self.id_calls += 1
                data = {"objectIdFieldName": "objectid", "objectIds": self.ids}
            else:
                requested = [int(value) for value in query["objectIds"].split(",")]
                if (self.fail_page_once is not None and requested[0] == self.fail_page_once) \
                        or (self.stubborn_over is not None and len(requested) > self.stubborn_over[0]
                            and requested[0] <= self.stubborn_over[1] <= requested[-1]):
                    if self.fail_page_once is not None and requested[0] == self.fail_page_once:
                        self.fail_page_once = None
                    return httpx.Response(500, stream=httpx.ByteStream(
                        json.dumps({"error": {"code": 500, "message": "Error performing query operation",
                                              "details": []}}).encode()), request=request,
                        headers={"content-type": "application/json"})
                features = []
                for position, identity in enumerate(requested):
                    attributes = _fixture_attributes(identity)
                    if self.extra_attribute and position == 0:
                        attributes["EXTRA"] = "unexpected"
                    if self.duplicate_huc10 and position == 0:
                        attributes["huc10"] = _fixture_attributes(requested[-1])["huc10"]
                    grouped = _fixture_grouped(identity)
                    flat = [list(ring) for member in grouped for ring in member]
                    if self.unclosed_ring:
                        flat = [ring[:-1] for ring in flat]
                    if self.out_of_bounds:
                        flat = [[[200.0, 31.0]] + flat[0][1:-1] + [[200.0, 31.0]]]
                    geometry = {"rings": flat}
                    if self.geometry_extra_key:
                        geometry["z"] = 100.0
                    if self.null_geometry and position == 0:
                        features.append({"attributes": attributes, "geometry": None})
                        continue
                    if self.hole_unassigned and position == 0:
                        # A counterclockwise hole far outside the shell.
                        geometry = {"rings": [flat[0],
                                              [[50.0, 50.0], [50.0, 49.6], [50.4, 49.6],
                                               [50.4, 50.0], [50.0, 50.0]]]}
                        features.append({"attributes": attributes, "geometry": geometry})
                        continue
                    features.append({"attributes": attributes, "geometry": geometry})
                page_ids = [feature["attributes"]["objectid"] for feature in features]
                if self.page_id_mismatch and features:
                    features[0]["attributes"]["objectid"] = features[-1]["attributes"]["objectid"]
                    page_ids = [feature["attributes"]["objectid"] for feature in features]
                data = {"objectIdFieldName": "objectid", "globalIdFieldName": "globalid",
                        "geometryType": "esriGeometryPolygon",
                        "spatialReference": ({"wkid": 4326, "latestWkid": 4326}
                                             if not self.changed_page_crs
                                             else {"wkid": 102100, "latestWkid": 3857}),
                        "features": features}
                if self.page_unexpected_member:
                    data["surprise"] = "unexpected member"
                if self.transfer_truncated:
                    data["exceededTransferLimit"] = True
        else:
            return httpx.Response(404, stream=httpx.ByteStream(b"not found"), request=request,
                                  headers={"content-type": "text/plain"})
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(data).encode()), request=request,
                              headers={"content-type": "application/json"})


class Wbd10DigitHuCaptureTests(unittest.TestCase):
    def _pins(self):
        return _fixture_pins(*_fixture_population())

    def _capture(self, transport):
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-source-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with _pinned_pins(self._pins()):
            with httpx.Client(transport=transport) as client:
                result = source.capture_validated(storage, _VERSION, client=client)
        return storage, result

    def _refuse(self, transport, expected):
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-refusal-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with _pinned_pins(self._pins()):
            with httpx.Client(transport=transport) as client:
                with self.assertRaisesRegex(ValueError, expected):
                    source.capture_validated(storage, _VERSION, client=client)
        self.assertFalse(storage.object_exists(storage.build_target_location(
            source.DATASET_SLUG, source.FILE_SLUG, _VERSION,
            f"geoparquet/{source.FILE_SLUG}.parquet")))
        return storage

    def _prefix(self, storage, version=_VERSION):
        return storage.build_target_location(source.DATASET_SLUG, source.FILE_SLUG, version, "")

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(wbd_10_digit_hu_watershed, hifld_ingest_assets)
        self.assertEqual(wbd_10_digit_hu_draft_refresh_schedule.default_status, DefaultScheduleStatus.STOPPED)
        check = wbd_10_digit_hu_complete({"feature_count": source.EXPECTED_COUNT, "source_population_complete": True})
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = wbd_10_digit_hu_complete({"feature_count": source.EXPECTED_COUNT - 1, "source_population_complete": True})
        self.assertFalse(failed.passed)

    def test_complete_capture_preserves_fields_ids_and_page_raw_wire(self):
        storage, result = self._capture(ArcGISFixture())
        self.assertEqual(result["feature_count"], source.EXPECTED_COUNT)
        self.assertFalse(result["release_accepted"])
        self.assertTrue(result["source_population_complete"])
        prefix = self._prefix(storage)
        manifest = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        self.assertEqual(len(manifest["pages"]), (source.EXPECTED_COUNT + source.PAGE_IDS - 1) // source.PAGE_IDS)
        self.assertEqual(manifest["pages"][0]["feature_count"], source.PAGE_IDS)
        self.assertEqual(manifest["native_crs"], "EPSG:4326")
        self.assertFalse(manifest["release_accepted"])
        self.assertEqual(manifest["page_count"], len(manifest["pages"]))
        for name in ("page_0000001", "page_0000101", "page_0018801"):
            self.assertTrue(storage.object_exists(prefix + f"raw/{name}.json"))
            self.assertTrue(storage.object_exists(prefix + f"raw/{name}.wire"))
            receipt = json.loads(storage.read_key(prefix + f"raw/{name}.receipt.json"))
            self.assertEqual(receipt["sha256"], hashlib.sha256(storage.read_key(prefix + f"raw/{name}.json")).hexdigest())
            self.assertEqual(receipt["url"].split("?")[0], _LAYER_URL + "/query")
        parquet_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        parquet_path.close()
        self.addCleanup(lambda: __import__("os").unlink(parquet_path.name))
        with open(parquet_path.name, "wb") as out:
            out.write(storage.read_key(prefix + f"geoparquet/{source.FILE_SLUG}.parquet"))
        table = pq.read_table(parquet_path.name)
        self.assertEqual(table.num_rows, source.EXPECTED_COUNT)
        self.assertEqual(table.column("objectid").to_pylist()[:3], [1, 2, 3])
        self.assertEqual(table.column("huc10").to_pylist()[0], "0000000001")
        self.assertEqual(table.column("huc10").to_pylist()[10], "0000000011")
        self.assertEqual(table.column("sourcefeatureid").to_pylist()[0], None)
        self.assertEqual(table.column("loaddate").to_pylist()[0], 1723726881001)
        self.assertEqual(table.column("Shape__Area").to_pylist()[0], 11.5)
        self.assertEqual(table.column("tnmid").to_pylist()[0], "tnmid-1")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Polygon", "MultiPolygon"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(geo["primary_column"], "geometry")

    def test_staged_geoparquet_passes_prebuilt_catalog_and_discovery(self):
        storage, _result = self._capture(ArcGISFixture())
        version = _VERSION
        prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{version}"
        self.assertTrue(discover_prebuilt_native(storage, prefix))
        snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        resolved = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG, version)
        summary = summarize_staged_catalog(
            storage, source.DATASET_SLUG, source.FILE_SLUG, version,
            source.DATASET_SLUG, source_metadata=resolved.metadata,
        )
        quality = summary.quality_manifest
        self.assertEqual(quality["feature_count"], source.EXPECTED_COUNT)
        self.assertTrue(quality["quality_check_passed"])
        self.assertEqual(quality["null_geometry_count"], 0)
        self.assertEqual(quality["catalog_mode"], "native_geoparquet_full_population")
        self.assertEqual(quality["native_crs"]["id"]["code"], 4326)
        manifest = resolved.metadata
        self.assertFalse(manifest["lineage"]["release_accepted"])
        self.assertFalse(manifest["lineage"]["hifld_continuity"]["hifld_continuity_accepted"])
        self.assertFalse(manifest["lineage"]["producer_succession_claimed"])
        page_input = next(item for item in manifest["inputs"] if item["input_id"] == "wbd_paged_feature_queries")
        self.assertEqual(page_input["role"], "producer_publication")
        self.assertEqual(page_input["canonical_url"], _LAYER_URL)
        self.assertEqual(page_input["host"], "hydro.nationalmap.gov")

    def test_source_manifest_declares_pending_contracts_and_banned_outputs(self):
        storage, _result = self._capture(ArcGISFixture())
        manifest = json.loads(storage.read_key(self._prefix(storage) + "metadata/source_manifest.json"))
        self.assertGreaterEqual(len(manifest["inputs"]), 11)
        self.assertFalse(manifest["approved"])
        self.assertTrue(all(manifest["lineage"][key] is False for key in (
            "source_contract_accepted", "schema_contract_accepted", "id_contract_accepted",
            "coverage_accepted", "rights_accepted", "baseline_accepted", "release_accepted")))
        self.assertEqual(manifest["lineage"]["crosswalk_row"], "Watershed Boundary Dataset (WBD)")
        self.assertEqual(manifest["lineage"]["crosswalk_open_id"], source.CROSSWALK_OPEN_ID)
        self.assertEqual(manifest["unsupported_output_formats"]["geopackage"],
                         "No approved container-schema or release mapping exists.")
        self.assertTrue(storage.object_exists(
            self._prefix(storage) + f"geoparquet/{source.FILE_SLUG}.parquet"))

    def test_retained_capture_replay_reuses_original_bodies_and_emits_provenance(self):
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with _pinned_pins(self._pins()):
            with httpx.Client(transport=ArcGISFixture()) as client:
                live = source.capture_validated(storage, "v20261114T000000Z", client=client)
            replay = source.capture_validated(storage, "v20261114T000001Z", retained_source_version="v20261114T000000Z")
        self.assertEqual(live["parquet_sha256"], replay["parquet_sha256"])
        self.assertEqual(live["records_sha256"], replay["records_sha256"])
        manifest = json.loads(storage.read_key(self._prefix(storage, "v20261114T000001Z")
                                               + "metadata/source_manifest.json"))
        self.assertEqual(manifest["code_identity_scheme"], "git-commit-blobs-v1")
        self.assertEqual(manifest["lineage"]["retained_source_capture_version"], "v20261114T000000Z")
        self.assertEqual(manifest["lineage"]["raw_capture_mode"], "retained_response_replay")
        self.assertFalse(manifest["lineage"]["release_accepted"])

    def test_transient_page_failure_is_retained_before_bounded_retry(self):
        storage, result = self._capture(ArcGISFixture(fail_page_once=2001))
        self.assertEqual(result["feature_count"], source.EXPECTED_COUNT)
        prefix = self._prefix(storage)
        attempt = json.loads(storage.read_key(prefix + "raw/page_0002001.attempt_1_%dcap.receipt.json" % source.MAX_PAGE_BYTES))
        self.assertEqual(attempt["status"], 500)
        self.assertEqual(attempt["attempt"], 1)
        self.assertTrue(attempt["complete"])
        self.assertTrue(storage.object_exists(prefix + "raw/page_0002001.json"))
        manifest = json.loads(storage.read_key(prefix + "metadata/source_manifest.json"))
        self.assertIn({"artifact": "raw/page_0002001.attempt_1_%dcap.receipt.json" % source.MAX_PAGE_BYTES,
                       "status": 500, "sha256": attempt["sha256"], "wire_sha256": attempt["wire_sha256"]},
                      manifest["lineage"]["failed_retry_responses"])

    def test_stubborn_page_window_bisects_and_stays_complete(self):
        storage, result = self._capture(ArcGISFixture(stubborn_over=(50, 1)))
        self.assertEqual(result["feature_count"], source.EXPECTED_COUNT)
        prefix = self._prefix(storage)
        manifest = json.loads(storage.read_key(prefix + "metadata/native_capture.json"))
        counts = [page["feature_count"] for page in manifest["pages"]]
        self.assertEqual(sum(counts), source.EXPECTED_COUNT)
        self.assertEqual(len(manifest["pages"]),
                         (source.EXPECTED_COUNT + source.PAGE_IDS - 1) // source.PAGE_IDS + 1)
        receipt = json.loads(storage.read_key(
            prefix + f"raw/page_0000001.attempt_1_{source.MAX_PAGE_BYTES}cap.receipt.json"))
        self.assertEqual(receipt["status"], 500)
        first_pages = [page["page"] for page in manifest["pages"][:3]]
        self.assertEqual(first_pages, ["page_0000001", "page_0000051", "page_0000101"])

    def test_mid_capture_id_drift_refuses_a_candidate(self):
        self._refuse(ArcGISFixture(shifted_roster=True), "source_id_roster_drift")

    def test_count_drift_refuses_a_candidate(self):
        self._refuse(ArcGISFixture(count_drift=True), "source_count_drift")

    def test_schema_drift_refuses_before_feature_fetch(self):
        storage = self._refuse(ArcGISFixture(schema_drift=True), "source_schema_drift")
        self.assertTrue(storage.object_exists(self._prefix(storage) + "metadata/quarantine.json"))

    def test_conflicting_page_crs_refuses_before_candidate(self):
        self._refuse(ArcGISFixture(changed_page_crs=True), "source_page_page_0000001_crs_drift")

    def test_unexpected_geometry_key_refuses_instead_of_dropping_it(self):
        self._refuse(ArcGISFixture(geometry_extra_key=True), "source_polygon_geometry_structure_drift")

    def test_unclosed_ring_refuses(self):
        self._refuse(ArcGISFixture(unclosed_ring=True), "source_polygon_ring_invalid")

    def test_out_of_bounds_coordinate_refuses(self):
        self._refuse(ArcGISFixture(out_of_bounds=True), "source_coordinate_range_drift")

    def test_page_id_mismatch_refuses(self):
        self._refuse(ArcGISFixture(page_id_mismatch=True), "source_page_ids_not_ascending")

    def test_unexpected_column_in_page_refuses(self):
        self._refuse(ArcGISFixture(extra_attribute=True), "source_record_attribute_schema_drift")

    def test_unexpected_page_member_refuses(self):
        self._refuse(ArcGISFixture(page_unexpected_member=True), "source_page_unexpected_members")

    def test_null_geometry_drift_refuses(self):
        self._refuse(ArcGISFixture(null_geometry=True), "source_null_geometry_count_drift")

    def test_duplicate_huc10_refuses(self):
        self._refuse(ArcGISFixture(duplicate_huc10=True), "source_huc10_identifier_drift")

    def test_hole_without_containing_shell_refuses(self):
        self._refuse(ArcGISFixture(hole_unassigned=True), "source_polygon_hole_unassigned")

    def test_transfer_truncation_refuses(self):
        self._refuse(ArcGISFixture(transfer_truncated=True),
                     "source_window_persistent_producer_refusal")

    def test_services_directory_drift_is_refused_before_layer_request(self):
        transport = ArcGISFixture(directory_drift=True)
        storage = self._refuse(transport, "usgs_wbd_services_directory_drift")
        self.assertEqual(transport.service_calls, 0)

    def test_service_change_during_capture_is_refused(self):
        self._refuse(ArcGISFixture(service_change_after=True),
                     "official_usgs_wbd_service_changed_during_capture")

    def test_service_copyright_drift_is_refused(self):
        self._refuse(ArcGISFixture(copyright_drift=True), "usgs_wbd_service_identity_drift")

    def test_alias_layer_roster_drift_is_refused(self):
        self._refuse(ArcGISFixture(alias_drift=True), "usgs_wbd_service_layer_roster_drift")

    def test_layer_extent_drift_is_refused(self):
        self._refuse(ArcGISFixture(omit_layer_extent=True), "source_layer_extent_crs_missing")

    def test_layer_description_drift_is_refused(self):
        self._refuse(ArcGISFixture(layer_description_drift=True), "usgs_wbd_layer_description_drift")

    def test_observed_bounds_drift_refuses(self):
        """Served vertices outside the pinned coverage envelope refuse closed."""
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-bounds-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with _pinned_pins(self._pins()):
            with httpx.Client(transport=_BoundsFixture()) as client:
                with self.assertRaisesRegex(ValueError, "source_observed_coverage_drift"):
                    source.capture_validated(storage, _VERSION, client=client)

    def test_pre_chain_retained_capture_without_service_doc_is_refused(self):
        temporary = tempfile.TemporaryDirectory(prefix="wbd-10-digit-hu-pre-chain-replay-")
        self.addCleanup(temporary.cleanup)
        storage = StagingStorageResource(use_local=True, local_dir=temporary.name)
        with _pinned_pins(self._pins()):
            with httpx.Client(transport=ArcGISFixture()) as client:
                source.capture_validated(storage, "v20261114T000000Z", client=client)
        original_prefix = self._prefix(storage)
        for suffix in ("json", "wire", "receipt.json"):
            __import__("os").unlink(__import__("pathlib").Path(temporary.name)
                                    / (original_prefix + f"raw/service_before.{suffix}"))
        with _pinned_pins(self._pins()):
            with self.assertRaisesRegex(ValueError, "retained_source_response_missing:service_before"):
                source.capture_validated(storage, "v20261114T000001Z", retained_source_version="v20261114T000000Z")


class _BoundsFixture(ArcGISFixture):
    """Fixture whose first feature per page carries an extra-far ring."""

    def handle_request(self, request):
        query = request.url.params
        path = request.url.path
        if path.endswith("/query") and query.get("returnIdsOnly") != "true" \
                and query.get("returnCountOnly") != "true":
            requested = [int(value) for value in query["objectIds"].split(",")]
            features = []
            for identity in requested:
                grouped = _fixture_grouped(identity)
                flat = [list(ring) for member in grouped for ring in member]
                if identity == requested[0]:
                    flat = [[[-150.0, 60.0], [-149.0, 60.0], [-149.0, 59.0],
                             [-150.0, 59.0], [-150.0, 60.0]]]
                features.append({"attributes": _fixture_attributes(identity),
                                 "geometry": {"rings": flat}})
            return httpx.Response(200, stream=httpx.ByteStream(json.dumps({
                "objectIdFieldName": "objectid", "globalIdFieldName": "globalid",
                "geometryType": "esriGeometryPolygon",
                "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                "features": features,
            }).encode()), request=request, headers={"content-type": "application/json"})
        return super().handle_request(request)


if __name__ == "__main__":
    unittest.main()
