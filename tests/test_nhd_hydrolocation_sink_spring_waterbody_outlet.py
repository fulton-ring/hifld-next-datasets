"""Focused tests for the USGS 3DHP_all Sink/Spring/Outlet point draft pipeline."""
import gzip
import io
import json
import struct
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import httpx
import pyarrow.parquet as pq

from dagster_hifld.assets.nhd_hydrolocation_sink_spring_waterbody_outlet import (
    nhd_hydrolocation_sink_spring_waterbody_outlet,
    nhd_hydrolocation_sink_spring_waterbody_outlet_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import capture_common
from dagster_hifld.sources import nhd_hydrolocation_sink_spring_waterbody_outlet as src

FIXTURE_COUNT = 7
FIXTURE_OBJECTIDS = tuple(range(1, FIXTURE_COUNT + 1))
LABEL_PAIRS = ((3, "Waterbody Outlet"), (7, "Spring"), (8, "Sink"))


def _fixture_attrs(oid: object) -> dict:
    index = FIXTURE_OBJECTIDS.index(oid)
    featuretype, label = LABEL_PAIRS[index % len(LABEL_PAIRS)]
    return {
        "OBJECTID": oid,
        "id3dhp": f"{index + 1:06X}"[-6:],
        "featuredate": 1709587421000 + index,
        "mainstemid": f"0000{index}X" if index != 1 else None,
        "universalreferenceid": f"ur-{index}" if index != 2 else None,
        "gnisid": 2400 + index if index != 3 else None,
        "gnisidlabel": f"Feature {index}" if index != 3 else None,
        "featuretype": featuretype,
        "featuretypelabel": label,
        "workunitid": "19050401",
    }


def _fixture_geometry(oid: object) -> object:
    index = FIXTURE_OBJECTIDS.index(oid)
    if oid == FIXTURE_OBJECTIDS[0]:
        return {"x": -154.8018182009261, "y": 67.43338158081643, "z": 799.7799999999988}
    if oid == FIXTURE_OBJECTIDS[1]:
        return {"x": -100.5, "y": 45.25, "z": 123}
    if oid == FIXTURE_OBJECTIDS[2]:
        return None
    if oid == FIXTURE_OBJECTIDS[3]:
        return {"x": "NaN", "y": "NaN", "z": "NaN"}
    return {"x": -70.0 - index, "y": 40.0 + index, "z": 0.5 * index}


def _fixture_rows() -> list[dict]:
    return [{"attributes": _fixture_attrs(oid), "geometry": _fixture_geometry(oid)}
            for oid in FIXTURE_OBJECTIDS]


def _fixture_layer_fields() -> list[dict]:
    fields = []
    for name, esri_type, nullable, length in src.NATIVE_FIELDS:
        field = {"name": name, "alias": name, "type": esri_type, "nullable": nullable,
                 "domain": None}
        if length is not None:
            field["length"] = length
        fields.append(field)
    return fields


def _fixture_layer_doc(fields: list[dict]) -> dict:
    return {"id": src.LAYER_ID, "name": src.LAYER_NAME, "type": "Feature Layer",
            "geometryType": src.GEOMETRY_TYPE, "hasZ": True, "hasM": False,
            "maxRecordCount": src.MAX_RECORD_COUNT,
            "advancedQueryCapabilities": {"supportsPagination": True},
            "sourceSpatialReference": {"wkid": src.NATIVE_LAYER_SR["wkid"],
                                       "latestWkid": src.NATIVE_LAYER_SR["latestWkid"]},
            "extent": {**src.EXPECTED_LAYER_EXTENT,
                       "spatialReference": {"latestWkid": src.NATIVE_LAYER_SR["latestWkid"]}},
            "fields": fields}


def _fixture_service_doc() -> dict:
    return {"copyrightText": src.EXPECTED_COPYRIGHT_TEXT,
            "serviceDescription": "The USGS 3D Hydrography Program (3DHP) 3DHP_all service.",
            "spatialReference": {"wkid": 102100, "latestWkid": 3857},
            "layers": [{"id": layer_id, "name": name}
                       for layer_id, name in src.EXPECTED_SERVICE_LAYERS]}


def _fixture_alias_service_doc() -> dict:
    doc = _fixture_service_doc()
    core = len(src.EXPECTED_SERVICE_LAYERS) - len(src.SPATIAL_METADATA_IDS)
    doc["layers"] = doc["layers"][:core] + [
        {"id": 90, "name": "Spatial Metadata - DEM LimitationArea"},
        {"id": 92, "name": "Spatial Metadata - EDH Workunit Area"},
        {"id": 94, "name": "Spatial Metadata - Elevation Workunit Area"},
    ]
    return doc


class JSONStream(httpx.SyncByteStream):
    def __init__(self, payload):
        if isinstance(payload, bytes):
            self.body = payload
        else:
            self.body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()

    def __iter__(self):
        yield self.body

    def close(self):
        return None


def make_responder(rows: list[dict], *, layer_fields=None, count=None, ids=None,
                   service=None, alias=None, layer=None, doc_page=None,
                   refuse_counts=()) -> httpx.MockTransport:
    count = count if count is not None else len(rows)
    objectids = ids if ids is not None else list(FIXTURE_OBJECTIDS)
    layer_fields = layer_fields if layer_fields is not None else _fixture_layer_fields()

    def layer_doc():
        if callable(layer):
            return layer()
        if layer is not None:
            return layer
        return _fixture_layer_doc(layer_fields)

    service_doc = service if service is not None else _fixture_service_doc()
    alias_doc = alias if alias is not None else _fixture_alias_service_doc()

    def respond(request: httpx.Request) -> httpx.Response:
        path = urlparse(str(request.url)).path
        query = parse_qs(urlparse(str(request.url)).query)
        if path.startswith("/3d-hydrography-program/"):
            return httpx.Response(200, stream=JSONStream(doc_page or
                b"<html>3DHP access: https://3dhp.nationalmap.gov/arcgis/rest/services"
                b"/usgs_3dhp_all/FeatureServer</html>"),
                headers={"content-type": "text/html"})
        if path.rstrip("/").endswith("FeatureServer"):
            document = alias_doc if request.url.host == "hydro.nationalmap.gov" else service_doc
            return httpx.Response(200, stream=JSONStream(document),
                                  headers={"content-type": "application/json"})
        if path.rstrip("/").endswith(f"/{src.LAYER_ID}"):
            return httpx.Response(200, stream=JSONStream(layer_doc()),
                                  headers={"content-type": "application/json"})
        if query.get("returnCountOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream({"count": count}),
                                  headers={"content-type": "application/json"})
        if query.get("returnIdsOnly") == ["true"]:
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID", "objectIds": objectids}),
                headers={"content-type": "application/json"})
        if query.get("resultOffset") is not None:
            offset = int(query["resultOffset"][0])
            record_count = int(query["resultRecordCount"][0])
            if record_count in refuse_counts:
                return httpx.Response(200, stream=JSONStream(
                    {"error": {"code": 500, "message": "Error performing query operation",
                               "details": []}}),
                    headers={"content-type": "application/json"})
            features = rows[offset:offset + record_count]
            return httpx.Response(200, stream=JSONStream(
                {"objectIdFieldName": "OBJECTID",
                 "spatialReference": {"wkid": src.SOURCE_CRS_WKID,
                                      "latestWkid": src.SOURCE_CRS_WKID},
                 "hasZ": True, "hasM": False, "features": features,
                 "exceededTransferLimit": False}),
                headers={"content-type": "application/json"})
        raise AssertionError(f"unrouted request {request.url}")

    return httpx.MockTransport(respond)


@contextmanager
def fixture_mode(schema_sha: str, transport: httpx.MockTransport, *, segment_size=2,
                 real_provenance=False):
    contexts = [
        patch.object(src, "EXPECTED_SCHEMA_SHA256", schema_sha),
        patch.object(src, "SEGMENT_START_RECORDS", segment_size),
        patch.object(src, "SEGMENT_PROBE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "DOC_CAPTURE_BASE_DELAY_SECONDS", 0.0),
        patch.object(src, "_default_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
        patch.object(src, "_doc_client",
                     side_effect=lambda: httpx.Client(transport=transport)),
    ]
    if not real_provenance:
        contexts.extend([
            patch.object(src, "_code_identity",
                     return_value={"code_identity_scheme": "test", "code_commit": "synthetic",
                                   "files": [], "working_tree_status": "unknown"}),
            patch.object(src, "validate_construction_code_identity"),
            patch.object(capture_common, "validate_construction_code_identity"),
        ])
    for context in contexts:
        context.start()
    try:
        yield
    finally:
        for context in reversed(contexts):
            context.stop()


def _staging(root: Path) -> StagingStorageResource:
    return StagingStorageResource(use_local=True, local_dir=str(root / "staging"),
                                  prefix="", backend="local")


class NhdHydrolocationSinkSpringWaterbodyOutletTests(unittest.TestCase):
    def test_unusable_geometry_still_checks_every_native_attribute(self):
        for name, esri_type, _nullable, _length in src.NATIVE_FIELDS:
            if name in {"OBJECTID", "featuretype", "featuretypelabel"}:
                continue
            with self.subTest(name=name):
                feature = _fixture_rows()[3]
                feature["attributes"][name] = 99 if esri_type == "esriFieldTypeString" else True
                with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                            "source_native_type_changed:" + name):
                    src._validate_feature(feature, tuple(field[0] for field in src.NATIVE_FIELDS),
                                          feature["attributes"]["OBJECTID"])

    def test_source_is_explicit_and_draft_only(self):
        self.assertEqual(src.DATASET_SLUG, "nhd-hydrolocation-sink-spring-waterbody-outlet")
        self.assertEqual(src.SERVICE_URL,
                         "https://3dhp.nationalmap.gov/arcgis/rest/services/usgs_3dhp_all"
                         "/FeatureServer")
        self.assertEqual(src.LAYER_ID, 20)
        self.assertEqual(src.LAYER_NAME,
                         "HydroLocation - Sink, Spring, Waterbody Outlet")
        self.assertEqual(src.GEOMETRY_TYPE, "esriGeometryPoint")
        self.assertEqual(src.MAX_RECORD_COUNT, 2500)
        self.assertEqual(src.SEGMENT_START_RECORDS, 2000)
        self.assertEqual(src.EXPECTED_SCHEMA_SHA256,
                         "4c4ab8e34f34e4705dd04722bc2e4e0c7cacc210c7bf7e0a34741b86cbebf981")
        self.assertEqual(src.EXPECTED_FEATURETYPE_LABELS,
                         {3: "Waterbody Outlet", 7: "Spring", 8: "Sink"})
        for pin in ("EXPECTED_COUNT", "EXPECTED_IDS_SHA256", "EXPECTED_RECORD_SHA256",
                    "EXPECTED_GEOMETRY_SHA256", "EXPECTED_NULL_GEOMETRY_COUNT",
                    "EXPECTED_NAN_GEOMETRY_COUNT", "EXPECTED_EMPTY_GEOMETRY_COUNT",
                    "EXPECTED_OBSERVED_BOUNDS", "EXPECTED_FEATURETYPE_DISTRIBUTION"):
            self.assertFalse(hasattr(src, pin), pin)
        keys = defs.resolve_asset_graph().get_all_asset_keys()
        self.assertTrue(any(key.path == ["ingest", src.DATASET_SLUG, src.DATASET_SLUG]
                            for key in keys))

    def test_segment_query_contract_is_roster_bounded(self):
        url = src._segment_query(150000, 2000)
        query = parse_qs(urlparse(url).query)
        self.assertEqual(query["where"], ["1=1"])
        self.assertEqual(query["orderByFields"], ["OBJECTID ASC"])
        self.assertEqual(query["resultOffset"], ["150000"])
        self.assertEqual(query["resultRecordCount"], ["2000"])
        self.assertEqual(query["outSR"], [str(src.SOURCE_CRS_WKID)])
        self.assertEqual(query["returnGeometry"], ["true"])
        self.assertEqual(query["returnZ"], ["true"])
        self.assertEqual(query["returnM"], ["false"])
        self.assertLess(len(url), 400)
        self.assertEqual(parse_qs(urlparse(src._ids_url()).query)["returnIdsOnly"], ["true"])
        self.assertEqual(parse_qs(urlparse(src._count_url()).query)["returnCountOnly"], ["true"])

    def test_point_wkb_keeps_ordinates_and_z(self):
        geometry = {"x": -154.8018182009261, "y": 67.43338158081643, "z": 799.7799999999988}
        wkb = src._point_wkb(geometry)
        order, kind = struct.unpack_from("<BI", wkb, 0)
        self.assertEqual((order, kind), (1, 0x80000001))
        # A WKB Point carries its ordinates immediately after the type word
        # (no part count): byte 5 starts the x, y, z doubles.
        self.assertEqual(struct.unpack_from("<3d", wkb, 5),
                         (geometry["x"], geometry["y"], geometry["z"]))
        self.assertEqual(len(wkb), 29)
        int_z = src._point_wkb({"x": -100.5, "y": 45.25, "z": 123})
        self.assertEqual(struct.unpack_from("<3d", int_z, 5), (-100.5, 45.25, 123.0))

    def test_point_geometry_drift_is_refused(self):
        cases = {
            "unexpected_source_geometry_members": {"x": 1.0, "y": 2.0, "z": 3.0, "curves": []},
            "source_point_ordinate_not_numeric:x": {"x": "NaN", "y": 2.0, "z": 3.0},
            "source_point_ordinate_not_numeric:z": {"x": 1.0, "y": 2.0, "z": None},
            "source_point_ordinate_not_finite:x": {"x": float("inf"), "y": 2.0, "z": 3.0},
            "source_point_ordinate_not_finite:y": {"x": 1.0, "y": float("nan"), "z": 3.0},
            "source_coordinate_range_drift": {"x": 200.0, "y": 2.0, "z": 3.0},
        }
        for expected_error, geometry in cases.items():
            with self.subTest(expected_error):
                with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError, expected_error):
                    src._point_wkb(geometry)

    def test_complete_fixture_capture_materializes_and_repeats(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                storage = _staging(root)
                first = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    storage, version="fixture-run", capture_directory=str(capture_root))
                self.assertFalse(first["reused"])
                self.assertEqual(first["feature_count"], FIXTURE_COUNT)
                self.assertFalse(first["release_accepted"])
                self.assertFalse(first["source_release_accepted"])
                second = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    storage, version="fixture-run-2", capture_directory=str(capture_root))
            self.assertEqual(first["version"], second["version"])
            self.assertTrue(second["reused"])
            self.assertTrue((capture_root / "computed_baseline.json").exists())
            parquet_path = capture_root / f"{src.FILE_SLUG}.parquet"
            table = pq.read_table(parquet_path)
            self.assertEqual(table.num_rows, FIXTURE_COUNT)
            self.assertEqual(table.schema.names[-1], "geometry")
            self.assertEqual(table.column("OBJECTID").to_pylist(), list(FIXTURE_OBJECTIDS))
            self.assertEqual(table.column("id3dhp").to_pylist(),
                             [row["attributes"]["id3dhp"] for row in rows])
            nulls = table.column("geometry").to_pylist()
            self.assertIsNone(nulls[2])
            self.assertIsNone(nulls[3])
            self.assertIsNotNone(nulls[0])
            geo_metadata = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["geometry_types"],
                             ["Point Z"])
            self.assertEqual(geo_metadata["columns"]["geometry"]["crs"].get("name", ""),
                             "WGS 84")

            instance_dir = root / "dagster"
            instance_dir.mkdir()
            (instance_dir / "dagster.yaml").write_text("telemetry:\n  enabled: false\n")
            from dagster import DagsterInstance, materialize
            with fixture_mode(schema_sha, transport):
                with DagsterInstance.from_config(str(instance_dir)) as instance:
                    result = materialize(
                        [nhd_hydrolocation_sink_spring_waterbody_outlet, nhd_hydrolocation_sink_spring_waterbody_outlet_complete],
                        resources={"staging_storage": _staging(root)},
                        instance=instance,
                        run_config={"ops": {"ingest__nhd_hydrolocation_sink_spring_waterbody_outlet__"
                                            "nhd_hydrolocation_sink_spring_waterbody_outlet":
                                            {"config": {"capture_directory":
                                                        str(capture_root)}}}},
                    )
                    self.assertTrue(result.success)
                    checks = {check.check_name: check
                              for check in result.get_asset_check_evaluations()}
                    self.assertTrue(checks["nhd_hydrolocation_sink_spring_waterbody_outlet_complete"].passed)
                    candidate = result.output_for_node(
                        "ingest__nhd_hydrolocation_sink_spring_waterbody_outlet__nhd_hydrolocation_sink_spring_waterbody_outlet")
                    self.assertEqual(candidate["feature_count"], FIXTURE_COUNT)
                    self.assertTrue(candidate["reused"])

    def test_retained_geoparquet_seals_into_staging(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="seal-run", capture_directory=str(capture_root))
            prefix = f"{src.DATASET_SLUG}/{src.FILE_SLUG}/{result['version']}"
            sealed = json.loads((root / "staging" / prefix / "metadata"
                                 / "source_audit.json").read_bytes())
            self.assertEqual(sealed["source_feature_count"], FIXTURE_COUNT)
            self.assertEqual(sealed["pagination"]["mode"],
                             "enumerated_roster_bisected_segments")
            self.assertEqual(sealed["pagination"]["segment_start_records"], 2)
            self.assertTrue(sealed["pagination"]["resumable"])
            self.assertFalse(sealed["release_accepted"])
            self.assertEqual(sealed["null_geometry_count"], 1)
            self.assertEqual(sealed["nan_geometry_count"], 1)
            manifest = json.loads((root / "staging" / prefix / "metadata"
                                   / "source_manifest.json").read_bytes())
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["agency"], "U.S. Geological Survey")
            hosts = {entry["host"] for entry in manifest["inputs"]}
            self.assertTrue(all(host in ("3dhp.nationalmap.gov", "hydro.nationalmap.gov",
                                         "www.usgs.gov") for host in hosts))
            parquet_bytes = (root / "staging" / prefix / "geoparquet"
                             / f"{src.DATASET_SLUG}.parquet").read_bytes()
            self.assertEqual(pq.read_table(io.BytesIO(parquet_bytes)).num_rows, FIXTURE_COUNT)

    def test_refused_pages_bisect_down_to_singles(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="bisect-run", capture_directory=str(capture_root))
                self.assertEqual(result["feature_count"], FIXTURE_COUNT)
                self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
                state = json.loads((capture_root / "capture_state.json").read_bytes())
                self.assertEqual(sorted((entry["offset"], entry["count"])
                                        for entry in state["segments"]),
                                 [(offset, 1) for offset in range(FIXTURE_COUNT)])
                resumed = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="bisect-run-2", capture_directory=str(capture_root))
                self.assertTrue(resumed["reused"])

    def test_unservable_single_feature_refuses_explicitly(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows, refuse_counts={1, 2})
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                            "source_single_feature_unservable"):
                    src.fetch_nhd_hydrolocation_sink_spring_waterbody_outlet_pages(root / "capture")
                self.assertTrue(sorted((root / "capture").glob("page_*.error-*.body")))

    def test_drift_and_provenance_failures_are_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        cases = {
            "source_count_drift": dict(count=FIXTURE_COUNT + 1),
            "source_schema_drift": dict(layer_fields=_fixture_layer_fields()
                                        + [{"name": "EXTRA", "type": "esriFieldTypeString",
                                            "alias": "EXTRA", "nullable": True, "length": 4,
                                            "domain": None}]),
            "source_schema_baseline_drift": dict(
                layer_fields=[{**field, "length": (field.get("length") or 0) + 1}
                              if field.get("length") else field
                              for field in _fixture_layer_fields()]),
            "usgs_3dhp_service_identity_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "copyrightText": "Data refreshed October, 2026."}),
            "usgs_3dhp_service_layer_roster_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "layers": _fixture_service_doc()["layers"][:-1]}),
            "usgs_3dhp_service_description_drift:service_before": dict(
                service={**_fixture_service_doc(),
                         "serviceDescription": "legacy NHD service."}),
            "exact_3dhp_hydrolocation_layer_metadata_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()),
                       "geometryType": "esriGeometryPolyline"}),
            "source_layer_z_or_m_contract_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "hasM": True}),
            "source_record_limit_drift": dict(
                layer={**_fixture_layer_doc(_fixture_layer_fields()), "maxRecordCount": 2000}),
            "source_layer_extent_drift": dict(
                layer=lambda: {**_fixture_layer_doc(_fixture_layer_fields()),
                               "extent": {**src.EXPECTED_LAYER_EXTENT,
                                          "xmax": 1.0,
                                          "spatialReference": {
                                              "latestWkid": src.NATIVE_LAYER_SR["latestWkid"]}}}),
            "source_id_population_drift": dict(ids=[*FIXTURE_OBJECTIDS[:-1], FIXTURE_OBJECTIDS[0]]),
            "unexpected_source_geometry_members": dict(rows=[
                {**rows[0], "geometry": {**rows[0]["geometry"], "curves": []}},
                *rows[1:]]),
            "source_coordinate_range_drift": dict(rows=[
                {**rows[0], "geometry": {"x": 200.0, "y": rows[0]["geometry"]["y"],
                                         "z": rows[0]["geometry"]["z"]}},
                *rows[1:]]),
            "source_page_id_slice_incomplete_or_reordered": dict(rows=[
                rows[0], rows[0], *rows[1:]], count=FIXTURE_COUNT),
            "source_record_schema_or_geometry_drift": dict(rows=[
                {**rows[0], "attributes": {"OBJECTID": 1, "id3dhp": "000001"}},
                *rows[1:]]),
            "source_native_type_changed:gnisid": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "gnisid": "2400"}},
                *rows[1:]]),
            "source_id3dhp_value_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "id3dhp": "not base36!"}},
                *rows[1:]]),
            "source_featuretype_outside_hydrolocation_population": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"], "featuretype": 1,
                                           "featuretypelabel": "Catchment Outlet"}},
                *rows[1:]]),
            "source_featuretype_label_drift": dict(rows=[
                {**rows[0], "attributes": {**rows[0]["attributes"],
                                           "featuretypelabel": "Mislabel"}},
                *rows[1:]]),
            "usgs_service_documentation_link_drift": dict(
                doc_page=b"<html>an unrelated page without the service host</html>"),
        }
        for expected_error, kwargs in cases.items():
            with self.subTest(expected_error):
                with tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    capture_root = root / "capture"
                    case_rows = kwargs.pop("rows", rows)
                    transport = make_responder(case_rows, **kwargs)
                    with fixture_mode(schema_sha, transport):
                        storage = _staging(root)
                        with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                                    expected_error):
                            src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                                storage, version="drift-run", capture_directory=str(capture_root))
                    quarantined = sorted(
                        (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                    self.assertTrue(quarantined, f"no quarantine for {expected_error}")
                    failure = json.loads((quarantined[0] / "metadata" / "failure.json")
                                         .read_bytes())
                    self.assertTrue(failure["reason_code"])

    def test_geometry_gates_are_observed_without_population_pins(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(rows)):
                first = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="gate-run", capture_directory=str(capture_root))
                baseline = json.loads((capture_root / "computed_baseline.json").read_bytes())
                self.assertEqual(baseline["source_null_geometry_count"], 1)
                self.assertEqual(baseline["source_nan_geometry_count"], 1)
                self.assertEqual(baseline["source_empty_geometry_count"], 0)
                again = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="gate-run-verify", capture_directory=str(capture_root))
                self.assertTrue(again["reused"])
                self.assertEqual({k: v for k, v in first.items() if k != "reused"},
                                 {k: v for k, v in again.items() if k != "reused"})

    def test_capture_witness_drift_is_refused_and_quarantined(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(rows)):
                storage = _staging(root)
                src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    storage, version="witness-run", capture_directory=str(capture_root))
                _replace_retained_body(capture_root, "ids_before", src._canonical(
                    {"objectIdFieldName": "OBJECTID", "objectIds": [*FIXTURE_OBJECTIDS[:-1], 99]}))
                with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                            "source_id_population_drift:ids_seed_vs_before"):
                    src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                        storage, version="witness-drift", capture_directory=str(capture_root))
                quarantined = sorted((root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
                self.assertTrue(quarantined)
                failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
                self.assertEqual(failure["reason_code"], "source_id_population_drift:ids_seed_vs_before")


def _replace_retained_body(root: Path, name: str, body: bytes, *, retain_wire=False):
    receipt_path = root / f"{name}.receipt.json"
    receipt = json.loads(receipt_path.read_bytes())
    (root / f"{name}.body").write_bytes(body)
    receipt.update(bytes=len(body), sha256=src._sha(body))
    if not retain_wire:
        (root / f"{name}.wire").write_bytes(body)
        receipt.update(wire_bytes=len(body), wire_sha256=src._sha(body), content_encoding="identity")
        if receipt.get("content_length") is not None:
            receipt["content_length"] = len(body)
    receipt_path.write_bytes(src._canonical(receipt) + b"\n")


class NhdHydrolocationSinkSpringWaterbodyOutletWitnessTests(unittest.TestCase):
    def _capture(self, root, capture_root, transport, version="witness"):
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with fixture_mode(schema_sha, transport, real_provenance=True):
            return src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                _staging(root), version=version, capture_directory=str(capture_root))

    def _inventory(self, root):
        return {str(path.relative_to(root)): src._sha(path.read_bytes())
                for path in root.rglob("*") if path.is_file()}

    def _offline(self, requests):
        def reject(request):
            requests.append(str(request.url))
            raise AssertionError("HTTP forbidden during replay")
        return httpx.MockTransport(reject)

    def test_completeness_check_uses_capture_facts_not_an_old_population(self):
        candidate = {"feature_count": 1, "source_reported_count": 1,
                     "source_population_complete": True, "source_id_sha256": "a" * 64,
                     "source_schema_sha256": src.EXPECTED_SCHEMA_SHA256}
        self.assertTrue(nhd_hydrolocation_sink_spring_waterbody_outlet_complete(candidate).passed)
        for changes in ({"source_reported_count": 2}, {"source_reported_count": True},
                        {"feature_count": True}, {"source_population_complete": False},
                        {"source_id_sha256": ""}, {"source_schema_sha256": ""}):
            with self.subTest(changes=changes):
                self.assertFalse(nhd_hydrolocation_sink_spring_waterbody_outlet_complete(
                    {**candidate, **changes}).passed)

    def test_invalid_or_unstable_witnesses_and_incomplete_pages_cannot_seal(self):
        for attack in ("duplicate_ids", "nonpositive_ids", "boolean_ids", "boolean_count",
                       "changed_closing_ids", "changed_closing_count", "missing_row", "reordered_rows"):
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                rows = _fixture_rows()
                ids = list(FIXTURE_OBJECTIDS)
                count = len(ids)
                if attack == "duplicate_ids":
                    ids[-1] = ids[0]
                elif attack == "nonpositive_ids":
                    ids[-1] = 0
                elif attack == "boolean_ids":
                    ids[0] = True
                elif attack == "boolean_count":
                    count = True
                elif attack == "missing_row":
                    rows.pop()
                elif attack == "reordered_rows":
                    rows[0], rows[1] = rows[1], rows[0]
                inner = make_responder(rows, ids=ids, count=count)
                ids_calls = 0
                count_calls = 0

                def respond(request):
                    nonlocal ids_calls, count_calls
                    query = parse_qs(urlparse(str(request.url)).query)
                    if query.get("returnIdsOnly") == ["true"]:
                        ids_calls += 1
                        if attack == "changed_closing_ids" and ids_calls == 3:
                            return httpx.Response(200, stream=JSONStream({"objectIdFieldName": "OBJECTID",
                                "objectIds": [*FIXTURE_OBJECTIDS[:-1], 99]}))
                    if query.get("returnCountOnly") == ["true"]:
                        count_calls += 1
                        if attack == "changed_closing_count" and count_calls == 2:
                            return httpx.Response(200, stream=JSONStream({"count": count + 1}))
                    return inner.handler(request)

                with self.assertRaises(src.NhdHydrolocationSinkSpringWaterbodyOutletError):
                    self._capture(root, root / "capture", httpx.MockTransport(respond))
                self.assertFalse(list((root / "staging").rglob("candidate_ready.json")))

    def test_gapped_added_removed_populations_preserve_native_values_and_cache(self):
        for count in (7, 6, 8):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                rows = _fixture_rows()
                if count == 6:
                    rows.pop()
                elif count == 8:
                    rows.append({"attributes": {**rows[0]["attributes"], "id3dhp": "A123456",
                                                "featuredate": 1709587421999},
                                 "geometry": {"x": 15.123456789, "y": -30.5, "z": -50.125}})
                ids = [11 + index * 19 for index in range(count)]
                ids[-1] = 900001
                for row, oid in zip(rows, ids):
                    row["attributes"]["OBJECTID"] = oid
                capture_root = root / "capture"
                first = self._capture(root, capture_root, make_responder(rows, ids=ids))
                inventory = self._inventory(root / "staging")
                requests = []
                cached = self._capture(root, capture_root, self._offline(requests), "offline")
                self.assertEqual(requests, [])
                self.assertFalse(first["reused"])
                self.assertTrue(cached["reused"])
                self.assertEqual({k: v for k, v in first.items() if k != "reused"},
                                 {k: v for k, v in cached.items() if k != "reused"})
                self.assertEqual(inventory, self._inventory(root / "staging"))
                self.assertEqual(first["feature_count"], count)
                self.assertEqual(first["source_reported_count"], count)
                self.assertTrue(first["source_population_complete"])
                self.assertEqual(first["source_id_sha256"], src._sha(src._canonical(ids)))
                self.assertEqual(first["source_null_geometry_count"], 1)
                self.assertEqual(first["source_nan_geometry_count"], 1)
                self.assertEqual(first["source_empty_geometry_count"], 0)
                expected_distribution = {kind: sum(row["attributes"]["featuretype"] == kind
                                                   for row in rows) for kind, _label in LABEL_PAIRS}
                self.assertEqual(first["source_featuretype_distribution"], expected_distribution)
                for key, value in first.items():
                    if key.endswith(("_accepted", "_approved")):
                        self.assertIs(value, False, key)
                table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
                for name, _type, _nullable, _length in src.NATIVE_FIELDS:
                    self.assertEqual(table.column(name).to_pylist(),
                                     [row["attributes"][name] for row in rows], name)
                for row, wkb in zip(rows, table.column("geometry").to_pylist()):
                    geometry = row["geometry"]
                    if geometry is None or geometry["x"] == "NaN":
                        self.assertIsNone(wkb)
                    else:
                        self.assertEqual(struct.unpack_from("<3d", wkb, 5),
                                         (geometry["x"], geometry["y"], geometry["z"]))
                prefix = root / "staging" / first["candidate_ready"]
                audit = json.loads((prefix.parent / "source_audit.json").read_bytes())
                self.assertEqual(audit["source_feature_count"], count)
                self.assertEqual(audit["source_id_sha256"], first["source_id_sha256"])
                self.assertEqual(audit["observed_bounds"], first["observed_bounds"])
                identity = json.loads((prefix.parent / "processing_identity.json").read_bytes())
                self.assertEqual(identity["code_identity_scheme"], capture_common.CODE_IDENTITY_GIT)
                self.assertTrue(identity["files"])

    def test_genuine_gzip_replay_and_tamper_cannot_change_sealed_candidate(self):
        import shutil

        for encoding in ("identity", "gzip"):
            with self.subTest(encoding=encoding), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                capture_root = root / "capture"
                inner = make_responder(_fixture_rows())

                def respond(request):
                    response = inner.handler(request)
                    if encoding == "identity":
                        return response
                    wire = gzip.compress(response.stream.body, mtime=0)
                    return httpx.Response(200, stream=JSONStream(wire), headers={
                        "content-type": response.headers["content-type"],
                        "content-encoding": "gzip", "content-length": str(len(wire))})

                self._capture(root, capture_root, httpx.MockTransport(respond))
                inventory = self._inventory(root / "staging")
                requests = []
                self.assertTrue(self._capture(root, capture_root, self._offline(requests))["reused"])
                self.assertEqual(requests, [])
                for attack in ("decoded_body", "request_url"):
                    with self.subTest(attack=attack):
                        attacked_root = root / attack
                        shutil.copytree(capture_root, attacked_root)
                        name = src._segment_name(0, 2)
                        if attack == "decoded_body":
                            page = json.loads((attacked_root / f"{name}.body").read_bytes())
                            page["features"][0]["attributes"]["gnisidlabel"] += " forged"
                            _replace_retained_body(attacked_root, name, src._canonical(page), retain_wire=True)
                            expected = "retained_capture_wire_body_mismatch"
                        else:
                            path = attacked_root / f"{name}.receipt.json"
                            receipt = json.loads(path.read_bytes())
                            receipt["url"] = receipt["final_url"] = src._segment_query(1, 2)
                            path.write_bytes(src._canonical(receipt))
                            expected = "retained_capture_request_identity_mismatch"
                        with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError, expected):
                            src._read_capture(attacked_root, name)
                        with self.assertRaises(src.NhdHydrolocationSinkSpringWaterbodyOutletError):
                            self._capture(root, attacked_root, self._offline(requests), "tampered")
                        after = self._inventory(root / "staging")
                        self.assertEqual(inventory, {key: after[key] for key in inventory})
                        self.assertFalse(any(key not in inventory and not key.startswith("_quarantine/")
                                             for key in after))

    def test_recovered_segment_refreshes_all_closing_witnesses_after_interruption(self):
        rows = _fixture_rows()
        ids = [10, 20, 40, 70, 90, 200, 800]
        for row, oid in zip(rows, ids):
            row["attributes"]["OBJECTID"] = oid
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, make_responder(rows, ids=ids), real_provenance=True):
                src.fetch_nhd_hydrolocation_sink_spring_waterbody_outlet_pages(capture_root)
            name = src._segment_name(6, 1)
            previous_digest = src._load_state(capture_root)["segments"][-1]["body_sha256"]
            (capture_root / f"{name}.body").unlink()
            rows[-1]["attributes"]["gnisidlabel"] = "changed during segment recovery"
            inner = make_responder(rows, ids=ids)
            recovered_requests = []

            def interrupted(request):
                query = parse_qs(urlparse(str(request.url)).query)
                recovered_requests.append(query)
                if query.get("returnCountOnly") == ["true"]:
                    raise httpx.ConnectError("closing witness interrupted")
                return inner.handler(request)

            with fixture_mode(schema_sha, httpx.MockTransport(interrupted), real_provenance=True):
                with self.assertRaises(src.NhdHydrolocationSinkSpringWaterbodyOutletError):
                    src.fetch_nhd_hydrolocation_sink_spring_waterbody_outlet_pages(capture_root)
            state = src._load_state(capture_root)
            self.assertIs(state["complete"], False)
            updated_digest = next(entry["body_sha256"] for entry in state["segments"] if entry["name"] == name)
            self.assertNotEqual(updated_digest, previous_digest)
            self.assertEqual(updated_digest, src._sha((capture_root / f"{name}.body").read_bytes()))
            self.assertEqual(recovered_requests[0]["resultOffset"], ["6"])
            closing_requests = []

            def resume(request):
                query = parse_qs(urlparse(str(request.url)).query)
                self.assertNotIn("resultOffset", query)
                closing_requests.append(str(request.url))
                return inner.handler(request)

            self._capture(root, capture_root, httpx.MockTransport(resume), "recovered")
            self.assertEqual(closing_requests, [src.LAYER_URL + "?f=pjson", src.SERVICE_URL + "?f=pjson",
                                                src._count_url(), src._ids_url()])
            self.assertIs(src._load_state(capture_root)["complete"], True)
            for witness in src.AFTER_NAMES:
                self.assertTrue(list(capture_root.glob(witness + ".error-*.receipt.json")))
            requests = []
            self.assertTrue(self._capture(root, capture_root, self._offline(requests))["reused"])
            self.assertEqual(requests, [])

    def test_replay_preflight_rejects_paths_sizes_and_aggregate_before_content_reads(self):
        for attack in ("symlink", "directory", "receipt_size", "body_size", "aggregate"):
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                capture_root = root / "capture"
                capture_root.mkdir()
                if attack == "symlink":
                    (capture_root / "capture_state.json").symlink_to(root / "outside")
                elif attack == "directory":
                    (capture_root / "nested").mkdir()
                else:
                    name = "bad.receipt.json" if attack == "receipt_size" else "bad.body"
                    size = src.MAX_RECEIPT_BYTES + 1 if attack == "receipt_size" else src.MAX_BODY_BYTES + 1
                    if attack == "aggregate":
                        size = 2
                    with (capture_root / name).open("wb") as stream:
                        stream.truncate(size)
                storage = _staging(root)
                limit = 1 if attack == "aggregate" else src.MAX_CAPTURE_BYTES
                with patch.object(src, "MAX_CAPTURE_BYTES", limit), patch.object(
                        Path, "read_bytes", side_effect=AssertionError("preflight read content")), \
                        patch.object(src, "_default_client", side_effect=AssertionError("preflight HTTP")):
                    with self.assertRaises((ValueError, src.NhdHydrolocationSinkSpringWaterbodyOutletError)):
                        src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                            storage, version="preflight", capture_directory=str(capture_root))


def _segment_failures_transport(inner: httpx.MockTransport, *, fail_first: int | None):
    """Fail the first ``fail_first`` (or every) segment-page request with a reset.

    Returns the wrapped transport and the count of segment-page requests seen,
    so a test can assert the shared retry helper's bounded attempt budget.
    """
    seen = {"segment_requests": 0}

    def respond(request: httpx.Request) -> httpx.Response:
        query = parse_qs(urlparse(str(request.url)).query)
        if query.get("resultOffset") is not None:
            seen["segment_requests"] += 1
            if fail_first is None or seen["segment_requests"] <= fail_first:
                raise httpx.ConnectError("connection reset by peer")
        return inner.handler(request)

    return httpx.MockTransport(respond), seen


class NhdHydrolocationSinkSpringWaterbodyOutletRetainedStateFailureTests(unittest.TestCase):
    """Failure paths of the retained-capture primitives this adapter binds.

    The checkpoint/receipt/retry helpers live in
    ``dagster_hifld.sources.arcgis_layer``; every case here drives them through
    this adapter's own error type over retained fixtures: corrupt checkpoint
    JSON, missing and changed segment receipts, incomplete transport captures
    retried by the shared helper, that helper's bounded retry budget, and a
    foreign checkpoint whose coverage claims are never trusted.
    """

    def _first_capture(self, root: Path, capture_root: Path, schema_sha: str,
                       transport: httpx.MockTransport, version: str) -> None:
        with fixture_mode(schema_sha, transport):
            result = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                _staging(root), version=version, capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)

    def _row_ids(self, capture_root: Path) -> list[int]:
        table = pq.read_table(capture_root / f"{src.FILE_SLUG}.parquet")
        return table.column("OBJECTID").to_pylist()

    def test_corrupt_checkpoint_json_refuses_and_retains_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "state-run")
            corrupted = b'{"segments": [ truncated'
            (capture_root / "capture_state.json").write_bytes(corrupted)
            before = sorted(path.name for path in capture_root.iterdir())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                            "capture_state_invalid"):
                    src.fetch_nhd_hydrolocation_sink_spring_waterbody_outlet_pages(capture_root)
            self.assertEqual((capture_root / "capture_state.json").read_bytes(), corrupted)
            self.assertEqual(sorted(path.name for path in capture_root.iterdir()), before)

    def test_missing_segment_receipt_refuses_read_and_refetch_restores_rows(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "receipt-run")
            name = src._segment_name(0, 2)
            (capture_root / f"{name}.receipt.json").unlink()
            with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                        "retained_capture_receipt_invalid:" + name):
                src._read_capture(capture_root, name)
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="receipt-run-2", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            self.assertEqual(self._row_ids(capture_root), list(FIXTURE_OBJECTIDS))
            self.assertTrue(sorted(capture_root.glob(f"{name}.error-*.body")))
            self.assertTrue(sorted(capture_root.glob(f"{name}.error-*.wire")))
            state = json.loads((capture_root / "capture_state.json").read_bytes())
            self.assertIn(name, [entry["name"] for entry in state["segments"]])

    def test_changed_segment_receipt_refuses_read_and_refetch_restores_rows(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "tamper-run")
            name = src._segment_name(2, 2)
            receipt = json.loads((capture_root / f"{name}.receipt.json").read_bytes())
            receipt["sha256"] = "0" * 64
            (capture_root / f"{name}.receipt.json").write_bytes(json.dumps(receipt).encode())
            with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                        "retained_capture_receipt_invalid:" + name):
                src._read_capture(capture_root, name)
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="tamper-run-2", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            self.assertEqual(self._row_ids(capture_root), list(FIXTURE_OBJECTIDS))
            self.assertEqual(len(self._row_ids(capture_root)), len(set(self._row_ids(capture_root))))
            for suffix in ("body", "wire", "receipt.json"):
                self.assertTrue(sorted(capture_root.glob(f"{name}.error-*.{suffix}")))

    def test_incomplete_transport_capture_is_retried_with_retained_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport, seen = _segment_failures_transport(make_responder(rows), fail_first=1)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                result = src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                    _staging(root), version="retry-run", capture_directory=str(capture_root))
            self.assertEqual(result["feature_count"], FIXTURE_COUNT)
            name = src._segment_name(0, 2)
            attempt_receipt = json.loads(
                (capture_root / f"{name}.attempt-01.receipt.json").read_bytes())
            self.assertFalse(attempt_receipt["complete_body"])
            self.assertIsNone(attempt_receipt["status"])
            self.assertEqual(attempt_receipt["error_code"], "request_error")
            for suffix in ("body", "wire", "receipt.json"):
                self.assertTrue((capture_root / f"{name}.attempt-01.{suffix}").exists())
            self.assertFalse(list(capture_root.glob(f"{name}.attempt-02.*")))
            self.assertEqual(self._row_ids(capture_root), list(FIXTURE_OBJECTIDS))

    def test_retry_budget_exhaustion_is_bounded_and_retains_failure_evidence(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport, seen = _segment_failures_transport(make_responder(rows), fail_first=None)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                            "source_capture_failed:ValueError"):
                    src.fetch_nhd_hydrolocation_sink_spring_waterbody_outlet_pages(capture_root)
            self.assertEqual(seen["segment_requests"],
                             src.SEGMENT_PROBE_ATTEMPTS + src.DOC_CAPTURE_ATTEMPTS)
            segment = src._segment_name(0, 2)
            self.assertTrue((capture_root / f"{segment}.attempt-01.receipt.json").exists())
            self.assertTrue((capture_root / f"{segment}.attempt-02.receipt.json").exists())
            self.assertFalse(list(capture_root.glob(f"{segment}.attempt-03.*")))
            single = src._segment_name(0, 1)
            for attempt in range(1, src.DOC_CAPTURE_ATTEMPTS):
                for suffix in ("body", "wire", "receipt.json"):
                    self.assertTrue(
                        (capture_root / f"{single}.attempt-{attempt:02d}.{suffix}").exists(),
                        f"missing attempt evidence {single}.attempt-{attempt:02d}.{suffix}")
            self.assertFalse(
                list(capture_root.glob(f"{single}.attempt-{src.DOC_CAPTURE_ATTEMPTS:02d}.*")))
            final = json.loads((capture_root / f"{single}.receipt.json").read_bytes())
            self.assertFalse(final["complete_body"])
            self.assertEqual(final["error_code"], "request_error")
            self.assertFalse((capture_root / "capture_state.json").exists())
            self.assertFalse((capture_root / "computed_baseline.json").exists())

    def test_foreign_checkpoint_coverage_is_never_trusted(self):
        rows = _fixture_rows()
        schema_sha = src._profile_schema({"fields": _fixture_layer_fields()})[1]
        transport = make_responder(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture_root = root / "capture"
            self._first_capture(root, capture_root, schema_sha, transport, "foreign-run")
            foreign = {"segments": [{"name": "foreign_product_segment", "offset": 0,
                                     "count": FIXTURE_COUNT, "features": FIXTURE_COUNT,
                                     "captured_at": "1999-01-01T00:00:00+00:00",
                                     "body_sha256": "0" * 64}], "complete": True}
            (capture_root / "capture_state.json").write_bytes(json.dumps(foreign).encode())
            with fixture_mode(schema_sha, transport):
                with self.assertRaisesRegex(src.NhdHydrolocationSinkSpringWaterbodyOutletError,
                                            "source_capture_roster_incomplete_or_unexpected"):
                    src.capture_nhd_hydrolocation_sink_spring_waterbody_outlet(
                        _staging(root), version="foreign-run-2",
                        capture_directory=str(capture_root))
            self.assertTrue(sorted(capture_root.glob("page_*.error-*.body")))
            quarantined = sorted(
                (root / "staging" / "_quarantine" / src.DATASET_SLUG).glob("*"))
            self.assertTrue(quarantined)
            failure = json.loads((quarantined[0] / "metadata" / "failure.json").read_bytes())
            self.assertEqual(failure["reason_code"],
                             "source_capture_roster_incomplete_or_unexpected")


if __name__ == "__main__":
    unittest.main()
