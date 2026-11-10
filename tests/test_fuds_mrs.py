"""Focused native fidelity tests for USACE FUDS MRS."""
import unittest
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


class FudsMrsNativeFidelityTests(unittest.TestCase):
    def test_candidate_ready_is_sealed_and_cache_reuses_after_catalog(self):
        """Exercise the real capture twice; the sealed roster must include the
        readiness document, and catalog-derived metadata must not block reuse."""
        fields = _synthetic_fields()
        features = _synthetic_features()
        schema_sha = _sha(_canonical([_plain_field(f) for f in fields]))
        service = {"mapName": "FUDS Data"}
        layer = {
            "id": LAYER_ID, "name": LAYER_NAME, "geometryType": "esriGeometryPolygon",
            "hasZ": True, "hasM": True, "fields": fields,
            "extent": {"spatialReference": {"wkid": 4326, "latestWkid": 4326}},
            "advancedQueryCapabilities": {"supportsPagination": True, "supportsOrderBy": True},
        }
        ids = [11, 12, 13]
        spatial_reference = {"wkid": 4326, "latestWkid": 4326}

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
                return routed(service)
            if path == layer_path:
                return routed(layer)
            if params.get("returnCountOnly") == "true":
                return routed({"count": len(ids)})
            if params.get("returnIdsOnly") == "true":
                return routed({"objectIds": list(ids)})
            offset = int(params.get("resultOffset", "0"))
            requested = int(params.get("resultRecordCount", str(len(ids))))
            return routed({
                "features": features[offset:offset + requested],
                "spatialReference": spatial_reference,
            })

        real_client = httpx.Client

        def client_with_mocked_transport(*args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            return real_client(*args, **kwargs)

        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(fuds_mrs, "EXPECTED_COUNT", len(ids)), \
                patch.object(fuds_mrs, "EXPECTED_SCHEMA_SHA256", schema_sha), \
                patch.object(httpx, "Client", client_with_mocked_transport):
            storage = StagingStorageResource(local_dir=tmp, use_local=True, backend="local")
            first = capture_fuds_mrs(storage, version="vtest0001",
                                     capture_directory=f"{tmp}/captures")
            self.assertEqual(first["feature_count"], len(ids))
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
