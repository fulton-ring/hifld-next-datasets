"""Source contract tests for the complete official IENC LAND_AREA layer."""

from __future__ import annotations

import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources.land_surface_areas_usace_ienc import (
    DATASET_SLUG,
    EXPECTED_COUNT,
    EXPECTED_FIELD_DECLARATIONS,
    EXPECTED_ID_SHA256,
    EXPECTED_INVALID_GEOMETRY_COUNT,
    EXPECTED_LAYER_EXTENT,
    FILE_SLUG,
    LAYER_URL,
    _esri_polygon,
    _parquet_bytes,
    _validate,
)
from dagster_hifld.assets.publish import _copy_version_files
from dagster_hifld.release_approval import ReleaseApprovalError
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import land_surface_areas_usace_ienc as source


def _ring(x: float, y: float, size: float = 0.001) -> list[list[float]]:
    # ArcGIS exterior rings are clockwise.
    return [
        [x, y], [x, y + size], [x + size, y + size], [x + size, y], [x, y],
    ]


def _crossing_hole_rings() -> list[list[list[float]]]:
    """A CCW hole whose representative point stays inside the shell but whose
    boundary crosses the shell boundary. The hole resolves to a single owner,
    so ownership is unambiguous while the assembled polygon has invalid OGC
    topology. The live source publishes one such ring (OBJECTID 12570).
    """
    x0, y0 = EXPECTED_LAYER_EXTENT["xmin"], EXPECTED_LAYER_EXTENT["ymin"]
    shell = _ring(x0, y0)
    hx, hy, size = x0 + 0.0005, y0 + 0.0005, 0.0015
    hole = [
        [hx, hy], [hx + size, hy], [hx + size, hy + size], [hx, hy + size], [hx, hy],
    ]
    return [shell, hole]


def _features(ids: list[int]) -> list[dict]:
    features = []
    for index, oid in enumerate(ids):
        x = EXPECTED_LAYER_EXTENT["xmin"] + (index % 1000) * 0.005
        features.append({
            "attributes": {
                "OBJECTID": oid,
                "Object_Name": "Test Land Area",
                "Information": "",
                "Source_Date": "20141007",
                "Source_Indication": "US,U3,SURVY,2014_SEAS_LIDAR_Mapping_Survey",
                "Source_Dataset": "U37AG001.000",
                "Feature_ID": f"U3 {oid:010d} 00011",
            },
            "geometry": {"rings": [_ring(x, EXPECTED_LAYER_EXTENT["ymin"])]},
        })
    return features


def _docs(ids: list[int] | None = None) -> dict[str, dict]:
    ids = list(ids or [1, 2, 3])
    item = {
        "id": source.BTS_ITEM_ID,
        "owner": "USDOT_BTS",
        "title": "Inland Electronic Navigational Charts",
        "description": (
            "The Inland Electronic Navigational Charts (IENC) dataset is updated "
            "bi-monthly by the US Army Corps (USACE)/Army Geospatial Center (AGC), "
            "and part of U.S. Department of Transportation (USDOT)/Bureau of "
            "Transportation Statistics (BTS) National Transportation Atlas Database "
            "(NTAD). These vector data, that make up the charts, can be downloaded "
            "as a geodatabase here: https://ienccloud.us/ienc/products/files/U37/"
            "ienc_master_dataset_gdb/USACE_IENC_Master_Service_gdb.zip. In addition, "
            "web mapping services of the feature classes/datasets can be found here: "
            "https://ienccloud.us/arcgis/rest/services/IENC_Feature_Classes."
        ),
    }
    directory = {
        "services": [
            {"name": "IENC_Feature_Classes/LAND_AREA", "type": "MapServer"},
            {"name": "IENC_Feature_Classes/LAND_REGION_AREA", "type": "MapServer"},
        ],
    }
    service = {
        "mapName": "LAND_AREA",
        "copyrightText": "USACE IENC",
        "layers": [{"id": 97, "name": "LAND_AREA", "geometryType": "esriGeometryPolygon"}],
    }
    layer = {
        "id": 97,
        "name": "LAND_AREA",
        "geometryType": "esriGeometryPolygon",
        "spatialReference": {"wkid": 4326, "latestWkid": 4326},
        "extent": {**EXPECTED_LAYER_EXTENT, "spatialReference": {"wkid": 4326, "latestWkid": 4326}},
        "maxRecordCount": 2000,
        "fields": copy.deepcopy(EXPECTED_FIELD_DECLARATIONS),
    }
    crs = {"wkid": 4326, "latestWkid": 4326}
    docs: dict[str, dict] = {
        "bts_item": item,
        "service_directory": directory,
        "service_before": service,
        "service_after": copy.deepcopy(service),
        "layer_before": layer,
        "layer_after": copy.deepcopy(layer),
        "count_before": {"count": len(ids)},
        "count_after": {"count": len(ids)},
        "ids_before": {"objectIds": list(ids)},
        "ids_after": {"objectIds": list(ids)},
        "ids_seed": {"objectIds": list(ids)},
    }
    features = _features(ids)
    docs["page_000"] = {
        "spatialReference": crs,
        "features": copy.deepcopy(features),
    }
    return docs


def _captures(docs: dict[str, dict]) -> dict[str, tuple[bytes, dict]]:
    return {
        name: (json.dumps(doc).encode("utf-8"),
               {"complete_body": True, "status": 200})
        for name, doc in docs.items()
    }


def _pins(docs: dict[str, dict]) -> dict[str, object]:
    features = sorted(docs["page_000"]["features"], key=lambda f: f["attributes"]["OBJECTID"])
    return {
        "EXPECTED_COUNT": len(features),
        "EXPECTED_ID_SHA256": source._sha(source._canonical(docs["ids_before"]["objectIds"])),
        "EXPECTED_SCHEMA_SHA256": source._sha(source._canonical(docs["layer_before"]["fields"])),
        "EXPECTED_RECORD_SHA256": source._sha(source._canonical([f["attributes"] for f in features])),
        "EXPECTED_GEOMETRY_SHA256": source._sha(source._canonical(
            [[f["attributes"]["OBJECTID"], f["geometry"]] for f in features])),
    }


class PopulationPins(unittest.TestCase):
    def test_expected_population_pins_match_the_published_layer(self):
        self.assertEqual(EXPECTED_COUNT, 18903)
        self.assertEqual(EXPECTED_INVALID_GEOMETRY_COUNT, 1)
        self.assertTrue(LAYER_URL.endswith("LAND_AREA/MapServer/97"))
        self.assertEqual(
            EXPECTED_ID_SHA256,
            "933b0ed3c93b8163f716acac4dc27d1e1b94711083991b649cdcb7b9f6a59fda",
        )


class ValidateCompleteCapture(unittest.TestCase):
    def test_complete_capture_passes_and_reports_composition(self):
        docs = _docs()
        with patch.multiple(source, **_pins(docs)):
            fields, features, audit = _validate(_captures(docs))
        self.assertEqual(len(features), 3)
        self.assertEqual(audit["source_feature_count"], 3)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 3)
        self.assertEqual(audit["feature_id_distinct_count"], 3)
        self.assertEqual(audit["feature_id_duplicate_count"], 0)
        self.assertEqual(audit["feature_id_null_count"], 0)
        self.assertEqual(audit["geometry_valid_count"], 3)
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual(audit["geometry_empty_count"], 0)
        self.assertEqual(audit["invalid_geometry_object_ids"], [])
        self.assertIs(audit["geometry_repaired"], False)
        self.assertEqual(audit["source_wkid"], 4326)
        self.assertEqual(fields, EXPECTED_FIELD_DECLARATIONS)

    def test_duplicate_and_null_feature_ids_are_preserved(self):
        docs = _docs()
        features = docs["page_000"]["features"]
        duplicate_value = features[1]["attributes"]["Feature_ID"]
        features[0]["attributes"]["Feature_ID"] = None
        features[2]["attributes"]["Feature_ID"] = duplicate_value
        with patch.multiple(source, **_pins(docs)):
            _fields, validated, audit = _validate(_captures(docs))
        self.assertEqual(audit["feature_id_null_count"], 1)
        self.assertEqual(audit["feature_id_duplicate_count"], 1)
        self.assertEqual(validated[0]["attributes"]["Feature_ID"], None)
        self.assertEqual(validated[2]["attributes"]["Feature_ID"], duplicate_value)

    def test_invalid_source_topology_is_preserved_and_counted(self):
        docs = _docs()
        docs["page_000"]["features"][1]["geometry"]["rings"] = _crossing_hole_rings()
        with patch.multiple(source, **_pins(docs)):
            _fields, validated, audit = _validate(_captures(docs))
        self.assertEqual(audit["geometry_valid_count"], 2)
        self.assertEqual(audit["geometry_invalid_count"], 1)
        self.assertEqual(audit["invalid_geometry_object_ids"], [2])
        self.assertIs(audit["geometry_repaired"], False)
        parquet = _parquet_bytes(EXPECTED_FIELD_DECLARATIONS, validated, 4326)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        wkb = table.column("geometry").to_pylist()[1]
        geometry = shapely.from_wkb(wkb)
        self.assertFalse(geometry.is_valid)
        self.assertEqual(wkb, _esri_polygon(validated[1]["geometry"]))


class ValidateDriftRejections(unittest.TestCase):
    def _mutate(self, mutation) -> tuple[str, ValueError]:
        docs = _docs()
        pins = _pins(docs)
        mutation(docs)
        with patch.multiple(source, **pins):
            with self.assertRaises(ValueError) as raised:
                _validate(_captures(docs))
        return str(raised.exception), raised.exception

    def test_bts_owner_drift(self):
        code, _ = self._mutate(lambda d: d["bts_item"].update(owner="SOMEONE_ELSE"))
        self.assertEqual(code, "bts_ienc_item_identity_drift")

    def test_service_directory_membership_drift(self):
        code, _ = self._mutate(lambda d: d["service_directory"].update(services=[]))
        self.assertEqual(code, "official_ienc_service_directory_drift")

    def test_service_identity_drift(self):
        code, _ = self._mutate(lambda d: d["service_before"].update(mapName="SOMETHING_ELSE"))
        self.assertEqual(code, "official_land_area_service_drift")

    def test_service_change_during_capture(self):
        code, _ = self._mutate(lambda d: d["service_after"].update(currentVersion="9.9"))
        self.assertEqual(code, "source_service_changed_during_capture")

    def test_layer_change_during_capture(self):
        code, _ = self._mutate(lambda d: d["layer_after"].update(id=70))
        self.assertEqual(code, "source_layer_changed_during_capture")

    def test_field_schema_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["fields"].pop(5)
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_field_schema_drift")

    def test_crs_drift(self):
        def mutate(docs):
            docs["layer_before"]["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
            docs["layer_after"]["spatialReference"] = {"wkid": 3857, "latestWkid": 3857}
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_spatial_reference_or_page_limit_drift")

    def test_page_limit_drift(self):
        def mutate(docs):
            docs["layer_before"]["maxRecordCount"] = 100
            docs["layer_after"]["maxRecordCount"] = 100
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_spatial_reference_or_page_limit_drift")

    def test_extent_drift(self):
        def mutate(docs):
            for key in ("layer_before", "layer_after"):
                docs[key]["extent"] = {**EXPECTED_LAYER_EXTENT, "xmax": -70.0,
                                       "spatialReference": {"wkid": 4326, "latestWkid": 4326}}
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_coverage_extent_drift")

    def test_count_drift(self):
        code, _ = self._mutate(lambda d: d["count_after"].update(count=2))
        self.assertEqual(code, "source_count_drift")

    def test_id_roster_drift(self):
        code, _ = self._mutate(lambda d: d["ids_after"].update(objectIds=[1, 2, 4]))
        self.assertEqual(code, "source_id_population_drift")

    def test_seed_roster_drift(self):
        code, _ = self._mutate(lambda d: d["ids_seed"].update(objectIds=[1, 2, 4]))
        self.assertEqual(code, "source_seed_id_population_drift")

    def test_page_roster_incomplete(self):
        docs = _docs()
        docs.pop("page_000")
        with patch.multiple(source, **_pins(docs)):
            with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete"):
                _validate(_captures(docs))

    def test_page_error_rejected(self):
        code, _ = self._mutate(lambda d: d["page_000"].update(error={"code": 500}))
        self.assertEqual(code, "source_page_error_or_truncation")

    def test_page_truncation_rejected(self):
        code, _ = self._mutate(lambda d: d["page_000"].update(exceededTransferLimit=True))
        self.assertEqual(code, "source_page_error_or_truncation")

    def test_feature_population_incomplete(self):
        code, _ = self._mutate(
            lambda d: d["page_000"].update(features=d["page_000"]["features"][1:]))
        self.assertEqual(code, "source_feature_population_incomplete")

    def test_unexpected_attribute_column_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Unmapped"] = "value"
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_record_schema_or_geometry_drift")

    def test_missing_attribute_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"].pop("Information")
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_record_schema_or_geometry_drift")

    def test_oid_type_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["OBJECTID"] = "1"
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_oid_value_type_drift")

    def test_string_value_type_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Feature_ID"] = 12
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_string_value_type_drift:Feature_ID")

    def test_unexpected_geometry_ordinates_rejected(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["geometry"]["curveRings"] = []
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "unexpected_source_geometry_ordinates")

    def test_geometry_outside_declared_coverage(self):
        def mutate(docs):
            feature = docs["page_000"]["features"][0]
            feature["geometry"]["rings"][0] = [
                [-70.0, 30.0], [-69.9, 30.0], [-69.9, 30.1], [-70.0, 30.1], [-70.0, 30.0],
            ]
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_geometry_outside_declared_coverage")

    def test_unclosed_ring_rejected(self):
        def mutate(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            docs["page_000"]["features"][0]["geometry"]["rings"][0] = ring[:-1]
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_polygon_ring_not_closed")

    def test_counter_clockwise_shell_rejected(self):
        def mutate(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            docs["page_000"]["features"][0]["geometry"]["rings"][0] = list(reversed(ring))
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_polygon_shell_missing")

    def test_ambiguous_hole_ownership_rejected(self):
        def mutate(docs):
            x0, y0 = EXPECTED_LAYER_EXTENT["xmin"], EXPECTED_LAYER_EXTENT["ymin"]
            shell_a = _ring(x0, y0)
            shell_b = _ring(x0 + 0.5, y0)
            hole = _crossing_hole_rings()[1]
            docs["page_000"]["features"][0]["geometry"]["rings"] = [shell_a, shell_b, hole]
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_hole_ownership_ambiguous")

    def test_value_baseline_drift(self):
        def mutate(docs):
            docs["page_000"]["features"][0]["attributes"]["Source_Date"] = "19990101"
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_value_baseline_drift")

    def test_geometry_baseline_drift(self):
        def mutate(docs):
            ring = docs["page_000"]["features"][0]["geometry"]["rings"][0]
            moved = [[x + 0.0001, y] for x, y in ring]
            docs["page_000"]["features"][0]["geometry"]["rings"][0] = moved
        code, _ = self._mutate(mutate)
        self.assertEqual(code, "source_geometry_baseline_drift")

    def test_schema_baseline_drift(self):
        docs = _docs()
        pins = _pins(docs)
        docs["layer_before"]["fields"][2]["alias"] = "Renamed"
        docs["layer_after"]["fields"][2]["alias"] = "Renamed"
        pins["EXPECTED_SCHEMA_SHA256"] = ""
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(ValueError, "source_field_schema_drift"):
                _validate(_captures(docs))


class ParquetCandidateContract(unittest.TestCase):
    def test_round_trip_preserves_schema_values_and_geometry(self):
        docs = _docs()
        with patch.multiple(source, **_pins(docs)):
            _fields, features, _audit = _validate(_captures(docs))
        parquet = _parquet_bytes(EXPECTED_FIELD_DECLARATIONS, features, 4326)
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertEqual(table.num_rows, 3)
        self.assertEqual(
            [field.name for field in table.schema],
            ["OBJECTID", "Object_Name", "Information", "Source_Date", "Source_Indication",
             "Source_Dataset", "Feature_ID", "geometry"],
        )
        types = {field.name: str(field.type) for field in table.schema}
        self.assertEqual(types["OBJECTID"], "int64")
        self.assertEqual(types["Feature_ID"], "string")
        self.assertEqual(types["geometry"], "binary")
        column = json.loads(table.schema.metadata[b"geo"].decode("utf-8"))["columns"]["geometry"]
        self.assertEqual(column["encoding"], "WKB")
        self.assertEqual(column["crs"]["id"]["code"], 4326)
        first = features[0]
        self.assertEqual(table.column("OBJECTID").to_pylist()[0], 1)
        self.assertEqual(table.column("Feature_ID").to_pylist()[0], first["attributes"]["Feature_ID"])
        geometry = shapely.from_wkb(table.column("geometry").to_pylist()[0])
        self.assertEqual(geometry.geom_type, "Polygon")
        self.assertTrue(geometry.is_valid)
        self.assertEqual(
            shapely.from_wkb(_esri_polygon(first["geometry"])).wkb,
            table.column("geometry").to_pylist()[0],
        )

    def test_wkb_bytes_are_stable_for_identical_geometry(self):
        geometry = _features([1])[0]["geometry"]
        self.assertEqual(_esri_polygon(geometry), _esri_polygon(geometry))


class BudgetFailureIsNotRetried(unittest.TestCase):
    def test_response_limit_and_budget_failures_raise_immediately(self):
        calls = []

        def failing_capture_get(client, url, root, name, **kwargs):
            calls.append(name)

            def raise_limited():
                raise ValueError("incomplete_source_capture:" + name)

            (root / f"{name}.receipt.json").write_text(
                json.dumps({"error_code": "wire_limit_exceeded"}))
            raise_limited()

        with tempfile.TemporaryDirectory() as directory, patch.object(
                source, "capture_get", failing_capture_get):
            with self.assertRaisesRegex(ValueError, "source_response_limit_exceeded:wire_limit_exceeded"):
                source._fetch_retry(object(), "https://example.test/layer", Path(directory),
                                    "page", [0], schedule=(0.0, 0.0))
        self.assertEqual(calls, ["page.attempt1"])


class FullCaptureLifecycle(unittest.TestCase):
    """Fixture-driven capture through the real HTTP/manifest/staging flow."""

    def _fixture_client(self, responses: list[dict]):
        response_index = [0]
        transport = httpx.MockTransport(lambda request: self._next(request, responses, response_index))
        real_client = httpx.Client

        def fixture_client(**kwargs):
            kwargs.pop("follow_redirects", None)
            kwargs.pop("timeout", None)
            kwargs.pop("headers", None)
            return real_client(transport=transport, **kwargs)

        return fixture_client

    def _next(self, request, responses, index):
        body = responses[index[0]]
        index[0] += 1
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode("utf-8")),
            request=request, headers={"content-type": "application/json"},
        )

    def _responses(self, docs: dict[str, dict], ids: list[int]) -> list[dict]:
        responses = [docs["ids_seed"]]
        for name, _url in source._capture_roster(ids):
            responses.append(docs[name])
        return responses

    def _run(self, storage, docs, ids, version, capture_root):
        with patch.multiple(source, **_pins(docs)), patch.object(
                source.httpx, "Client", self._fixture_client(self._responses(docs, ids))), patch.object(
                source.time, "sleep", return_value=None), patch.object(
                source.shutil, "disk_usage", return_value=SimpleNamespace(free=10_000_000_000)):
            return source.capture_land_surface_areas_usace_ienc(
                storage, version=version, capture_directory=str(capture_root))

    def test_capture_stages_candidate_and_offline_replay_reuses_it(self):
        with tempfile.TemporaryDirectory(prefix="land-surface-areas-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            docs = _docs()
            ids = docs["ids_seed"]["objectIds"]
            first = self._run(storage, docs, ids, "fixture-run", root / "captures")
            self.assertEqual(first["feature_count"], 3)
            self.assertEqual(first["geometry_invalid_count"], 0)
            prefix = f"{first['dataset_slug']}/{first['file_slug']}/{first['version']}"
            keys = set(storage.list_prefix())
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", keys)
            self.assertIn(f"{prefix}/raw/page_000.body", keys)
            self.assertTrue(any(key.startswith("_construction-state/") for key in keys))

            with patch.object(source.httpx, "Client",
                              side_effect=AssertionError("offline replay attempted HTTP")), patch.object(
                    source.shutil, "disk_usage", return_value=SimpleNamespace(free=10_000_000_000)), patch.multiple(
                    source, **_pins(docs)):
                replayed = source.capture_land_surface_areas_usace_ienc(
                    storage, version="offline-replay", capture_directory=first["capture_directory"])
            self.assertTrue(replayed["reused"])
            self.assertEqual(replayed["feature_count"], 3)

            tampered_key = f"{prefix}/raw/page_000.body"
            original = storage.read_key(tampered_key)
            storage.write_key(tampered_key, original + b"x")
            with patch.object(source.httpx, "Client",
                              side_effect=AssertionError("offline replay attempted HTTP")), patch.object(
                    source.shutil, "disk_usage", return_value=SimpleNamespace(free=10_000_000_000)), patch.multiple(
                    source, **_pins(docs)):
                with self.assertRaisesRegex(ValueError, "cached_source_bytes_changed:page_000"):
                    source.capture_land_surface_areas_usace_ienc(
                        storage, version="offline-replay-2", capture_directory=first["capture_directory"])

    def test_failed_validation_quarantines_and_never_publishes(self):
        with tempfile.TemporaryDirectory(prefix="land-surface-areas-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            published = PublishedStorageResource(use_local=True, local_dir=str(root / "published"))
            accepted_key = published.write(
                DATASET_SLUG, FILE_SLUG, "accepted-v1",
                "geoparquet/" + DATASET_SLUG + ".parquet", b"accepted")
            published_before = {key: published.read_key(key) for key in published.list_prefix()}
            self.assertEqual(set(published_before), {accepted_key})

            docs = _docs()
            docs["count_after"] = {"count": 2}
            ids = docs["ids_seed"]["objectIds"]
            with patch.multiple(source, **_pins(docs)), patch.object(
                    source.httpx, "Client", self._fixture_client(self._responses(docs, ids))), patch.object(
                    source.time, "sleep", return_value=None), patch.object(
                    source.shutil, "disk_usage", return_value=SimpleNamespace(free=10_000_000_000)):
                with self.assertRaisesRegex(ValueError, "source_count_drift"):
                    source.capture_land_surface_areas_usace_ienc(
                        storage, version="drift-run", capture_directory=str(root / "captures"))

            keys = list(storage.list_prefix())
            self.assertTrue(any(key.startswith("_quarantine/") for key in keys))
            self.assertFalse(any("candidate_ready" in key for key in keys))
            self.assertFalse(any(key.startswith("_construction-state/") for key in keys))
            self.assertFalse(any(key.startswith(f"{DATASET_SLUG}/") for key in keys))
            manifests = list((root / "captures").glob("*/capture_manifest.json"))
            self.assertEqual(len(manifests), 1)
            manifest = json.loads(manifests[0].read_bytes())
            self.assertIs(manifest["complete"], False)
            self.assertEqual(manifest["error_code"], "ValueError")
            published_after = {key: published.read_key(key) for key in published.list_prefix()}
            self.assertEqual(published_after, published_before)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(DATASET_SLUG, FILE_SLUG, "candidate-v1",
                          "geoparquet/" + DATASET_SLUG + ".parquet", b"candidate")
            accepted_key = published.write(DATASET_SLUG, FILE_SLUG, "accepted-v1",
                                          "geoparquet/" + DATASET_SLUG + ".parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, DATASET_SLUG, FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                    for key in published.list_prefix()}
            self.assertEqual(after, before)


class BlockingCompletenessCheck(unittest.TestCase):
    def _candidate(self) -> dict:
        return {
            "feature_count": EXPECTED_COUNT,
            "source_population_complete": True,
            "source_id_sha256": "0" * 64,
            "source_schema_sha256": "1" * 64,
            "source_record_sha256": "2" * 64,
            "source_geometry_sha256": "3" * 64,
            "source_coverage_scope": "scope",
            "drift_guard": "guard",
            "geometry_valid_count": EXPECTED_COUNT - EXPECTED_INVALID_GEOMETRY_COUNT,
            "geometry_invalid_count": EXPECTED_INVALID_GEOMETRY_COUNT,
            "geometry_empty_count": 0,
            "feature_id_null_count": 0,
            "geometry_repaired": False,
        }

    def test_complete_pinned_composition_passes(self):
        from dagster_hifld.assets.land_surface_areas_usace_ienc import (
            land_surface_areas_usace_ienc_complete,
        )
        result = land_surface_areas_usace_ienc_complete(self._candidate())
        self.assertTrue(result.passed)
        self.assertEqual(result.metadata["geometry_invalid_count"].value, 1)
        self.assertEqual(result.metadata["geometry_valid_count"].value, EXPECTED_COUNT - 1)

    def test_quality_composition_drift_fails_closed(self):
        from dagster_hifld.assets.land_surface_areas_usace_ienc import (
            land_surface_areas_usace_ienc_complete,
        )
        candidate = self._candidate()
        check = land_surface_areas_usace_ienc_complete
        self.assertFalse(check(dict(candidate, geometry_invalid_count=0)).passed)
        self.assertFalse(check(dict(candidate, geometry_invalid_count=2)).passed)
        self.assertFalse(check(dict(candidate, geometry_valid_count=EXPECTED_COUNT)).passed)
        self.assertFalse(check(dict(candidate, geometry_empty_count=1)).passed)
        self.assertFalse(check(dict(candidate, feature_id_null_count=1)).passed)
        self.assertFalse(check(dict(candidate, geometry_repaired=True)).passed)
        incomplete = {key: value for key, value in candidate.items()
                      if key != "geometry_invalid_count"}
        self.assertFalse(check(incomplete).passed)

    def test_population_failures_still_fail_closed(self):
        from dagster_hifld.assets.land_surface_areas_usace_ienc import (
            land_surface_areas_usace_ienc_complete,
        )
        candidate = self._candidate()
        check = land_surface_areas_usace_ienc_complete
        self.assertFalse(check(dict(candidate, feature_count=EXPECTED_COUNT - 1)).passed)
        self.assertFalse(check(dict(candidate, source_population_complete=False)).passed)

    def test_geometry_quality_check_reports_pending_acceptance_without_blocking(self):
        from dagster_hifld.assets.land_surface_areas_usace_ienc import (
            land_surface_areas_usace_ienc_geometry_quality,
        )
        spec = next(iter(land_surface_areas_usace_ienc_geometry_quality.check_specs))
        self.assertFalse(spec.blocking)
        result = land_surface_areas_usace_ienc_geometry_quality(self._candidate())
        self.assertFalse(result.passed)
        self.assertEqual(result.metadata["geometry_invalid_count"].value, 1)
        self.assertEqual(result.metadata["invalid_geometry_object_ids"].value, [])
        self.assertEqual(result.metadata["geometry_repaired"].value, False)


class RegisteredDraftRefresh(unittest.TestCase):
    def test_candidate_is_in_ingest_registry_and_registered_job(self):
        from dagster import AssetKey, DefaultScheduleStatus
        from dagster_hifld.assets.land_surface_areas_usace_ienc import (
            land_surface_areas_usace_ienc_candidate,
            land_surface_areas_usace_ienc_draft_schedule,
        )
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.definitions import defs

        self.assertIn(land_surface_areas_usace_ienc_candidate, hifld_ingest_assets)
        repository = defs.get_repository_def()
        candidate_key = AssetKey(["ingest", DATASET_SLUG, FILE_SLUG])
        self.assertIn(candidate_key, repository.asset_graph.get_all_asset_keys())

        job = repository.get_job("land_surface_areas_usace_ienc_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {candidate_key})
        self.assertEqual(
            repository.get_schedule_def(land_surface_areas_usace_ienc_draft_schedule.name)
            .default_status,
            DefaultScheduleStatus.STOPPED,
        )


if __name__ == "__main__":
    unittest.main()
