"""Focused native fidelity tests for USACE FUDS MRS."""
import copy
import unittest
from contextlib import contextmanager
import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import shapely

from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.native_geoparquet_catalog import geometry_metadata
from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata
from dagster_hifld.sources import fuds_mrs
from dagster_hifld.sources.fuds_mrs import (
    DATASET_SLUG,
    EXPECTED_SCHEMA_SHA256,
    LAYER_ID,
    LAYER_NAME,
    SERVICE_URL,
    _canonical,
    _encode_polygon_z,
    _native_parquet,
    _plain_field,
    _sha,
    capture_fuds_mrs,
)
from dagster_hifld.resources import StagingStorageResource

RING = [
    [0.0, 0.0, 5.0, None], [0.0, 2.0, 6.0, 0.0],
    [2.0, 2.0, 7.0, None], [2.0, 0.0, 8.0, 0.0],
    [0.0, 0.0, 5.0, None],
]
SPATIAL_REFERENCE = {"wkid": 4326, "latestWkid": 4326}
IDS = [11, 12, 13]


def _synthetic_fields():
    return [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID",
         "nullable": False, "domain": None},
        {"name": "FEATURENAME", "type": "esriFieldTypeString", "alias": "Feature Name",
         "length": 80, "nullable": True, "domain": None},
    ]


def _synthetic_features():
    return [
        {"attributes": {"OBJECTID": 11, "FEATURENAME": "MRS Alpha"},
         "geometry": {"hasZ": True, "hasM": True, "rings": [list(RING)]}},
        {"attributes": {"OBJECTID": 12, "FEATURENAME": "MRS Beta"},
         "geometry": {"hasZ": True, "hasM": True, "rings": [list(RING)]}},
        {"attributes": {"OBJECTID": 13, "FEATURENAME": "MRS Gamma"},
         "geometry": {"hasZ": True, "hasM": True, "rings": [list(RING)]}},
    ]


def _layer_document(fields, extent=None):
    return {
        "id": LAYER_ID, "name": LAYER_NAME, "geometryType": "esriGeometryPolygon",
        "hasZ": True, "hasM": True, "fields": fields,
        "extent": dict(extent if extent is not None else fuds_mrs.EXPECTED_EXTENT,
                       spatialReference=dict(SPATIAL_REFERENCE)),
        "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
    }


def _documents(fields, features, layer=None, ids=None):
    ids = list(ids or IDS)
    layer = layer or _layer_document(fields)
    service = {"mapName": "FUDS Data"}
    documents = {
        "service_before": service, "service_after": copy.deepcopy(service),
        "layer_before": layer, "layer_after": copy.deepcopy(layer),
        "count_before": {"count": len(ids)}, "count_after": {"count": len(ids)},
        "ids_before": {"objectIds": list(ids)}, "ids_after": {"objectIds": list(ids)},
    }
    for index, offset in enumerate(range(0, len(ids), fuds_mrs.PAGE_SIZE)):
        documents[f"page_{index:03d}"] = {
            "features": copy.deepcopy(features[offset:offset + fuds_mrs.PAGE_SIZE]),
            "spatialReference": dict(SPATIAL_REFERENCE),
        }
    return documents


@contextmanager
def _mocked_service(documents, fields):
    """Patch the module pins and httpx.Client to a mocked streamed service."""
    schema_sha = _sha(_canonical([_plain_field(f) for f in fields]))
    service_path = "/" + SERVICE_URL.split("://", 1)[1].split("/", 1)[1]
    layer_path = f"{service_path}/{LAYER_ID}"

    def routed(document):
        # capture_get streams the wire; a buffered mock response would be
        # consumed before iter_raw, so serve a real byte stream.
        body = json.dumps(document, ensure_ascii=False, allow_nan=False).encode()
        return httpx.Response(200, headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
        }, stream=httpx.ByteStream(body))

    def handler(request):
        params = dict(request.url.params)
        path = request.url.path
        if path == service_path:
            return routed(documents["service_before"])
        if path == layer_path:
            return routed(documents["layer_before"])
        if params.get("returnCountOnly") == "true":
            return routed(documents["count_before"])
        if params.get("returnIdsOnly") == "true":
            return routed(documents["ids_before"])
        offset = int(params.get("resultOffset", "0"))
        key = f"page_{offset // fuds_mrs.PAGE_SIZE:03d}"
        return routed(documents[key])

    real_client = httpx.Client

    def client_with_mocked_transport(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    with patch.object(fuds_mrs, "EXPECTED_COUNT",
                      len(documents["ids_before"]["objectIds"])), \
            patch.object(fuds_mrs, "EXPECTED_SCHEMA_SHA256", schema_sha), \
            patch.object(httpx, "Client", client_with_mocked_transport):
        yield


def _capture_once(root, documents, fields):
    """Run one full capture against a mocked streamed service; return storage."""
    storage = StagingStorageResource(local_dir=root, use_local=True, backend="local")
    with _mocked_service(documents, fields):
        result = capture_fuds_mrs(storage, version="vtest0001",
                                  capture_directory=f"{root}/captures")
    return storage, result



class FudsMrsNativeFidelityTests(unittest.TestCase):
    def test_candidate_ready_is_sealed_and_cache_reuses_after_catalog(self):
        """Exercise the real capture twice; the sealed roster must include the
        readiness document, and catalog-derived metadata must not block reuse."""
        fields = _synthetic_fields()
        features = _synthetic_features()
        documents = _documents(fields, features)
        with tempfile.TemporaryDirectory() as tmp, _mocked_service(documents, fields):
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            first = capture_fuds_mrs(storage, version="vtest0001",
                                     capture_directory=f"{tmp}/captures")
            self.assertEqual(first["feature_count"], len(IDS))
            self.assertFalse(first["candidate_approved"])
            self.assertNotIn("reused", first)
            version = first["version"]
            prefix = first["prefix"]
            candidate_ready = json.loads(
                storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            # The readiness document cannot contain its own hash, so it lists
            # every other object; the sealed roster must still include it.
            identity_body = storage.read_key(f"{prefix}/metadata/processing_identity.json")
            fingerprint = hashlib.sha256(identity_body.rstrip(b"\n")).hexdigest()
            bundle = json.loads(load_first_construction(
                storage, f"_construction-state/{DATASET_SLUG}/{version}.json",
                version, fingerprint))
            sealed_objects = bundle["payload"]["objects"]
            self.assertIn("metadata/candidate_ready.json", sealed_objects)
            self.assertEqual(set(sealed_objects),
                             set(candidate_ready["objects"]) | {"metadata/candidate_ready.json"})
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["feature_count"], len(IDS))
            self.assertEqual({key: audit["source_declared_extent"][key]
                              for key in fuds_mrs.EXPECTED_EXTENT}, fuds_mrs.EXPECTED_EXTENT)
            self.assertTrue(audit["source_vertices_within_declared_extent"])

            snapshot_source_metadata(storage, DATASET_SLUG, DATASET_SLUG, version)
            resolved = load_resolved_source_manifest(storage, DATASET_SLUG, DATASET_SLUG, version)
            summary = summarize_staged_catalog(
                storage, DATASET_SLUG, DATASET_SLUG, version, DATASET_SLUG,
                source_metadata=resolved.metadata)
            write_catalog_metadata(
                storage, dataset_slug=DATASET_SLUG, file_slug=DATASET_SLUG,
                version=version, quality_dict=summary.quality_manifest,
                dictionary_dict=summary.data_dictionary)
            for derived in ("metadata/quality_manifest.json",
                            "metadata/data_dictionary.json",
                            "metadata/resolved_provenance.json",
                            "metadata/source/source_manifest.json"):
                self.assertIsNotNone(storage.object_snapshot(f"{prefix}/{derived}"))

            second = capture_fuds_mrs(storage, version="vtest0001",
                                       capture_directory=f"{tmp}/captures")
            self.assertEqual(second["version"], version)
            self.assertEqual(second["prefix"], prefix)
            self.assertTrue(second["reused"])
            self.assertFalse(second["candidate_approved"])

    def _assert_quarantined(self, storage, expected_names):
        keys = sorted(storage.list_prefix("_quarantine"))
        self.assertTrue(keys)
        failure_keys = [key for key in keys if key.endswith("metadata/failure.json")]
        self.assertEqual(len(failure_keys), 1)
        base = failure_keys[0][: -len("metadata/failure.json")]
        failure = json.loads(storage.read_key(failure_keys[0]))
        self.assertEqual(failure["reason_code"], "ValueError")
        self.assertEqual(sorted(failure["captured_names"]), sorted(expected_names))
        for name in expected_names:
            for suffix in ("body", "wire", "receipt.json"):
                self.assertIsNotNone(storage.object_snapshot(f"{base}raw/{name}.{suffix}"))

    def test_out_of_domain_geometry_fails_closed_and_quarantines(self):
        """The full capture must refuse impossible EPSG:4326 coordinates before
        sealing and retain the completed acquisition evidence in quarantine."""
        fields = _synthetic_fields()
        features = _synthetic_features()
        features[1]["geometry"]["rings"] = [copy.deepcopy(RING)]
        features[1]["geometry"]["rings"][0][1] = [500.0, 2.0, 6.0, 0.0]
        documents = _documents(fields, features)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_polygon_xy_geographic_domain_violation"):
                _capture_once(tmp, documents, fields)
            self._assert_quarantined(storage, [
                "service_before", "layer_before", "count_before", "ids_before",
                "page_000", "layer_after", "service_after", "count_after", "ids_after"])
            self.assertFalse(storage.list_prefix(DATASET_SLUG))
            self.assertFalse(storage.list_prefix("_construction-state"))

    def test_vertex_outside_declared_extent_fails_closed_and_quarantines(self):
        """In-domain coordinates beyond the declared layer extent are coverage
        drift; the capture must fail closed before sealing and quarantine."""
        fields = _synthetic_fields()
        features = _synthetic_features()
        features[1]["geometry"]["rings"] = [copy.deepcopy(RING)]
        features[1]["geometry"]["rings"][0][1] = [-179.5, 2.0, 6.0, 0.0]
        documents = _documents(fields, features)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_vertex_outside_declared_extent"):
                _capture_once(tmp, documents, fields)
            self._assert_quarantined(storage, [
                "service_before", "layer_before", "count_before", "ids_before",
                "page_000", "layer_after", "service_after", "count_after", "ids_after"])
            self.assertFalse(storage.list_prefix(DATASET_SLUG))

    def test_declared_extent_drift_fails_closed_and_quarantines(self):
        """A silently changed advertised extent must not reach sealing; the
        capture fails on the pinned extent and quarantines partial evidence."""
        fields = _synthetic_fields()
        drifted = _layer_document(fields)
        drifted["extent"]["xmin"] = -190.0
        documents = _documents(fields, _synthetic_features(), layer=drifted)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            with self.assertRaisesRegex(ValueError, "source_layer_extent_drift"):
                _capture_once(tmp, documents, fields)
            self._assert_quarantined(storage, ["service_before", "layer_before"])
            self.assertFalse(storage.list_prefix(DATASET_SLUG))

    def test_reviewer_domain_reproduction_now_fails_closed(self):
        """Exact reviewer reproduction: the previously accepted clockwise ring
        at impossible lon/lat must now be refused by the encoder itself."""
        ring = [[500.0, 100.0, 0.0, None], [500.0, 101.0, 0.0, None],
                [501.0, 101.0, 0.0, None], [501.0, 100.0, 0.0, None],
                [500.0, 100.0, 0.0, None]]
        with self.assertRaisesRegex(ValueError, "source_polygon_xy_geographic_domain_violation"):
            _encode_polygon_z([ring])



    def test_xyz_m_null_and_zero_are_preserved_in_native_source_column(self):
        ring = [
            [0.0, 0.0, 5.0, None], [0.0, 2.0, 6.0, 0.0],
            [2.0, 2.0, 7.0, None], [2.0, 0.0, 8.0, 0.0],
            [0.0, 0.0, 5.0, None],
        ]
        encoded = _encode_polygon_z([ring])
        geometry = shapely.from_wkb(encoded)
        self.assertTrue(geometry.has_z)
        self.assertEqual(shapely.get_coordinates(geometry, include_z=True).tolist(),
                         [[p[0], p[1], p[2]] for p in ring])

        fields = [
            {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
            {"name": "FEATURENAME", "type": "esriFieldTypeString", "nullable": True},
        ]
        features = [
            {"attributes": {"OBJECTID": 1, "FEATURENAME": None},
             "geometry": {"hasZ": True, "hasM": True, "rings": [ring]}},
            {"attributes": {"OBJECTID": 2, "FEATURENAME": "No geometry"}, "geometry": None},
        ]
        table = pq.read_table(pa.BufferReader(_native_parquet(fields, features,
            {1: {"wkid": 4326, "latestWkid": 4326}, 2: {"wkid": 4326, "latestWkid": 4326}})), use_threads=False)
        self.assertEqual(table.schema.names, ["OBJECTID", "FEATURENAME", "geometry", "source_geometry"])
        self.assertEqual(table["OBJECTID"].type, pa.int64())
        self.assertIsNone(table["FEATURENAME"][0].as_py())
        self.assertEqual(table["geometry"].null_count, 1)
        source = table["source_geometry"].to_pylist()
        self.assertEqual(source[0]["rings"][0], [
            {"x": p[0], "y": p[1], "z": p[2], "m": p[3]} for p in ring])
        self.assertEqual(source[0]["spatialReference"], '{"latestWkid":4326,"wkid":4326}')
        self.assertIsNone(source[1])
        self.assertEqual(geometry_metadata(table.schema)[1]["geometry_types"], ["MultiPolygon Z"])

    def test_pinned_schema_digest_is_sha256(self):
        self.assertEqual(len(EXPECTED_SCHEMA_SHA256), 64)
        self.assertTrue(all(char in "0123456789abcdef" for char in EXPECTED_SCHEMA_SHA256))

    def test_nested_island_does_not_steal_containing_hole(self):
        def ring(points):
            return [[x, y, 0, None] for x, y in points]
        # Esri exterior rings are clockwise and holes counterclockwise.
        outer = ring([[0, 0], [0, 10], [10, 10], [10, 0], [0, 0]])
        hole = ring([[2, 2], [8, 2], [8, 8], [2, 8], [2, 2]])
        island = ring([[4, 4], [4, 6], [6, 6], [6, 4], [4, 4]])
        geometry = shapely.from_wkb(_encode_polygon_z([outer, hole, island]))
        self.assertTrue(geometry.is_valid)
        self.assertEqual(len(geometry.geoms), 2)
        self.assertEqual(sorted(len(poly.interiors) for poly in geometry.geoms), [0, 1])


if __name__ == "__main__":
    unittest.main()
