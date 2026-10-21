"""Native fidelity, drift, cache, and approval tests for the USGS State Capitols capture."""
import hashlib
import io
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import state_capitols as source


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def sha(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def fixture_features():
    features = []
    for oid, name, state, x, y in ((1, "Alpha Capitol", "AL", 2.0, 3.0), (3, "Gamma Capitol", "WY", 4.0, 5.0)):
        attributes = {key: None for key in source.RESPONSE_KEYS}
        attributes.update({
            "objectid": oid,
            "permanent_identifier": f"{oid:08d}-0000-0000",
            "source_datasetid": "{TEST-DATASET}",
            "source_datadesc": "State Capitols for U.S.",
            "source_originator": "U.S. Geological Survey",
            "loaddate": 1356101516000 + oid,
            "ftype": 830,
            "fcode": 83006,
            "name": name,
            "pointlocationtype": 1,
            "address": "1 Capitol Street",
            "city": "Test City",
            "state": state,
            "zipcode": "00000",
            "gnis_id": str(100000 + oid),
            "globalid": "{GLOBAL-%d}" % oid,
        })
        features.append({"attributes": attributes, "geometry": {"x": x, "y": y}})
    return features


def fixture_page(features):
    return {
        "displayFieldName": "NAME",
        "fieldAliases": dict(source.EXPECTED_FIELD_ALIASES),
        "fields": json.loads(json.dumps(source.EXPECTED_RESPONSE_FIELDS)),
        "geometryType": "esriGeometryPoint",
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "features": features,
    }


def fixture_layer():
    return {
        "id": 6,
        "name": "State Capitols",
        "type": "Feature Layer",
        "geometryType": "esriGeometryPoint",
        "fields": source.EXPECTED_FIELDS,
        "maxRecordCount": 2000,
        "extent": {"xmin": 2.0, "ymin": 3.0, "xmax": 4.0, "ymax": 5.0,
                   "spatialReference": {"wkid": 102100, "latestWkid": 3857}},
    }


def fixture_service():
    return {
        "serviceDescription": "USGS Structures from The National Map test",
        "mapName": source.SERVICE_MAP_NAME,
        "currentVersion": "11.3",
        "layers": [{"id": 6, "name": "State Capitols"},
                   {"id": 41, "name": "State Capitols"}],
    }


def fixture_ids():
    return [3, 1]


class StateCapitolsValidationTests(unittest.TestCase):
    def pristine_nulls_bounds_states(self):
        features = fixture_features()
        nulls = {key: 0 for key in source.RESPONSE_KEYS}
        for feature in features:
            for key, value in feature["attributes"].items():
                if value is None:
                    nulls[key] += 1
        xs = [feature["geometry"]["x"] for feature in features]
        ys = [feature["geometry"]["y"] for feature in features]
        bounds = (min(xs), min(ys), max(xs), max(ys))
        states = sorted({feature["attributes"]["state"] for feature in features})
        return features, nulls, bounds, states

    def pinned(self, **overrides):
        features, nulls, bounds, states = self.pristine_nulls_bounds_states()
        ids = fixture_ids()
        values = {
            "COUNT": 2,
            "EXPECTED_ID_SHA": sha(sorted(ids)),
            "EXPECTED_RECORD_SHA": sha([feature["attributes"] for feature in features]),
            "EXPECTED_GEOMETRY_SHA": sha([[f["attributes"]["objectid"], f["geometry"]]
                                          for f in features]),
            "EXPECTED_NULL_COUNTS": nulls,
            "EXPECTED_BOUNDS": bounds,
            "EXPECTED_EXTENT": {"xmin": bounds[0], "ymin": bounds[1], "xmax": bounds[2], "ymax": bounds[3]},
            "EXPECTED_STATE_ROSTER": states,
        }
        values.update(overrides)
        return patch.multiple(source, **values)

    def fixture_population(self, features, ids=None):
        ids = ids or fixture_ids()
        layer = fixture_layer()
        pages = {"page_000": fixture_page(features)}
        with self.pinned():
            return source._validate_population(source.EXPECTED_RESPONSE_FIELDS, ids, pages)

    def test_declared_names_and_native_points_survive_typed_geoparquet(self):
        features, audit = self.fixture_population(fixture_features())
        parquet = source._parquet_bytes(features)
        table = pq.read_table(io.BytesIO(parquet))
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column("OBJECTID").to_pylist(), [1, 3])
        self.assertEqual(table.schema.field("OBJECTID").type.__str__(), "int64")
        self.assertEqual(table.schema.field("PERMANENT_IDENTIFIER").type.__str__(), "string")
        self.assertEqual(table.schema.field("LOADDATE").type.__str__(), "int64")
        self.assertEqual(table.column("LOADDATE").to_pylist(), [1356101516001, 1356101516003])
        self.assertEqual(table.column("GLOBALID").to_pylist(), ["{GLOBAL-1}", "{GLOBAL-3}"])
        self.assertEqual(table.column("SOURCE_FEATUREID").to_pylist(), [None, None])
        geoms = [from_wkb(value) for value in table.column("geometry").to_pylist()]
        self.assertEqual([(geom.x, geom.y) for geom in geoms], [(2.0, 3.0), (4.0, 5.0)])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 3857})
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertEqual(audit["attribute_null_counts"]["source_featureid"], 2)

    def test_missing_id_fails_closed(self):
        features = fixture_features()
        with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
            self.fixture_population(features[:1])
        features = fixture_features()
        features[1]["attributes"]["objectid"] = 9
        with self.assertRaisesRegex(ValueError, "source_native_id_drift"):
            self.fixture_population(features)

    def test_renamed_or_extra_column_fails_closed(self):
        features = fixture_features()
        features[0]["attributes"]["unexpected"] = "x"
        with self.assertRaisesRegex(ValueError, "source_attribute_roster_drift"):
            self.fixture_population(features)
        page = fixture_page(fixture_features())
        page["fields"][2] = dict(page["fields"][2], name="renamed")
        ids = fixture_ids()
        with self.pinned():
            with self.assertRaisesRegex(ValueError, "source_response_schema_drift"):
                source._validate_population(page["fields"], ids, {"page_000": page})

    def test_value_and_type_loss_fails_closed(self):
        features = fixture_features()
        features[0]["attributes"]["loaddate"] = "2012-12-21"
        with self.assertRaisesRegex(ValueError, "source_date_type_drift"):
            self.fixture_population(features)
        features = fixture_features()
        features[0]["attributes"]["fcode"] = 83006.5
        with self.assertRaisesRegex(ValueError, "source_integer_type_drift"):
            self.fixture_population(features)
        features = fixture_features()
        features[0]["attributes"]["globalid"] = 12
        with self.assertRaisesRegex(ValueError, "source_string_type_drift"):
            self.fixture_population(features)

    def test_geometry_and_coverage_drift_fails_closed(self):
        features = fixture_features()
        features[0]["geometry"] = {"x": 99.0, "y": 99.0}
        with self.assertRaisesRegex(ValueError, "source_geometry_coverage_drift"):
            self.fixture_population(features)
        features = fixture_features()
        features[0]["geometry"] = {"x": float("nan"), "y": 3.0}
        with self.assertRaisesRegex(ValueError, "source_xy_drift"):
            self.fixture_population(features)
        features = fixture_features()
        features[0]["geometry"] = {"x": 2.0, "y": 3.0, "z": 0.0}
        with self.assertRaisesRegex(ValueError, "source_geometry_structure_drift"):
            self.fixture_population(features)

    def test_null_state_and_roster_drift_fails_closed(self):
        features = fixture_features()
        features[0]["attributes"]["gnis_id"] = None
        with self.assertRaisesRegex(ValueError, "source_null_count_drift"):
            self.fixture_population(features)
        features = fixture_features()
        features[0]["attributes"]["state"] = "ZZ"
        with self.assertRaisesRegex(ValueError, "source_state_roster_drift"):
            self.fixture_population(features)

    def test_pinned_source_shas_drift_fails_closed(self):
        features = fixture_features()
        ids = fixture_ids()
        pages = {"page_000": fixture_page(features)}
        with self.pinned(EXPECTED_RECORD_SHA="0" * 64, EXPECTED_GEOMETRY_SHA="0" * 64):
            with self.assertRaisesRegex(ValueError, "source_record_sha_drift"):
                source._validate_population(source.EXPECTED_RESPONSE_FIELDS, ids, pages)
        with self.pinned(EXPECTED_SCHEMA_SHA="0" * 64):
            with self.assertRaisesRegex(ValueError, "source_schema_sha_drift"):
                source._validate_population(source.EXPECTED_RESPONSE_FIELDS, ids, pages)
        layer = fixture_layer()
        self.assertEqual(source._sha(canonical(layer["fields"])), source.EXPECTED_SCHEMA_SHA)

    def test_metadata_identity_drift_fails_closed(self):
        service, layer = fixture_service(), fixture_layer()
        with self.pinned():
            source._validate_metadata(service, layer)
            with self.assertRaisesRegex(ValueError, "official_usgs_service_identity_drift"):
                source._validate_metadata({**service, "mapName": "other"}, layer)
            with self.assertRaisesRegex(ValueError, "official_usgs_service_identity_drift"):
                source._validate_metadata({**service, "layers": [{"id": 7, "name": "State Capitols"}]}, layer)
            with self.assertRaisesRegex(ValueError, "source_layer_schema_or_limit_drift"):
                source._validate_metadata(service, {**layer, "geometryType": "esriGeometryPolyline"})
            with self.assertRaisesRegex(ValueError, "source_layer_schema_or_limit_drift"):
                source._validate_metadata(service, {**layer, "maxRecordCount": 1000})
            drifted = json.loads(json.dumps(layer))
            drifted["fields"][8]["type"] = "esriFieldTypeString"
            with self.assertRaisesRegex(ValueError, "source_layer_schema_or_limit_drift"):
                source._validate_metadata(service, drifted)
            with self.assertRaisesRegex(ValueError, "source_extent_drift"):
                source._validate_metadata(service, {**layer, "extent": {**layer["extent"], "xmin": 0.0}})


class DeliveredStream(httpx.SyncByteStream):
    def __init__(self, blocks, error=None):
        self.blocks = blocks
        self.error = error

    def __iter__(self):
        for block in self.blocks:
            yield block
        if self.error is not None:
            raise self.error


class StateCapitolsCaptureCustodyTests(unittest.TestCase):
    def _attempt(self, root, *, blocks, status=200, error=None):
        stream = DeliveredStream(blocks, error)
        transport = httpx.MockTransport(lambda request: httpx.Response(
            status, headers={"content-type": "application/json"}, stream=stream,
        ))
        root.mkdir(parents=True, exist_ok=True)
        url = source._query({"where": "1=1", "objectIds": "1", "f": "json"})
        with httpx.Client(transport=transport) as client, self.assertRaisesRegex(
            ValueError, "incomplete_source_capture"
        ):
            source._capture(client, url, root, "page_000")
        wire = (root / "page_000.wire").read_bytes()
        body = (root / "page_000.body").read_bytes()
        receipt = json.loads((root / "page_000.receipt.json").read_bytes())
        return wire, body, receipt

    def test_non_200_response_body_and_wire_are_retained_before_refusal(self):
        with TemporaryDirectory() as tmp:
            payload = b'{"error":"temporarily unavailable"}'
            wire, body, receipt = self._attempt(Path(tmp) / "capture", blocks=[payload], status=503)
        self.assertEqual(wire, payload)
        self.assertEqual(body, payload)
        self.assertEqual(receipt["status"], 503)
        self.assertTrue(receipt["complete_body"])
        self.assertEqual(receipt["wire_sha256"], hashlib.sha256(wire).hexdigest())

    def test_partial_transport_bytes_are_retained_before_refusal(self):
        with TemporaryDirectory() as tmp:
            payload = b'{"features":[{"objectid":1}'
            wire, body, receipt = self._attempt(
                Path(tmp) / "capture", blocks=[payload],
                error=httpx.ReadError("simulated interrupted response"),
            )
        self.assertEqual(wire, payload)
        self.assertEqual(body, payload)
        self.assertFalse(receipt["complete_body"])
        self.assertEqual(receipt["error_code"], "transport_error")

    def test_capture_rejects_remote_or_published_storage(self):
        with TemporaryDirectory() as tmp:
            for storage in (PublishedStorageResource(use_local=True, local_dir=tmp),
                            StagingStorageResource(use_local=False, bucket="unused"),
                            StagingStorageResource(use_local=True, local_dir=tmp, prefix="nested")):
                with self.assertRaisesRegex(ValueError, "explicit_isolated_local_staging_required"):
                    source.capture_state_capitols(storage, version="v1")
            with self.assertRaisesRegex(ValueError, "unsafe_capture_version"):
                source.capture_state_capitols(
                    StagingStorageResource(use_local=True, local_dir=tmp), version="../escape")


class StateCapitolsEndToEndTests(unittest.TestCase):
    @staticmethod
    def _pinned_client(client):
        class PinnedClient:
            def __enter__(self):
                return client
            def __exit__(self, *_exc):
                return False
        return PinnedClient()

    def _fixture_repo(self, tmp):
        repo = Path(tmp) / "repo"
        (repo / "src/dagster_hifld").mkdir(parents=True)
        (repo / "src/dagster_hifld/runtime.py").write_text("candidate transform = 1\n")
        (repo / "pyproject.toml").write_text("[project]\nname='fixture'\n")
        (repo / "uv.lock").write_text("version = 1\n")
        return repo

    def _transport(self, docs):
        def blocks(payload):
            class Stream(httpx.SyncByteStream):
                def __iter__(self):
                    yield payload
            return Stream()
        def handler(request: httpx.Request) -> httpx.Response:
            params = request.url.params
            if params.get("returnCountOnly") == "true":
                return httpx.Response(200, headers={"content-type": "application/json"},
                                      stream=blocks(b'{"count":%d}' % len(docs["ids"]["objectIds"])))
            if params.get("returnIdsOnly") == "true":
                return httpx.Response(200, headers={"content-type": "application/json"},
                                      stream=blocks(json.dumps(docs["ids"]).encode()))
            if params.get("objectIds"):
                return httpx.Response(200, headers={"content-type": "application/json"},
                                      stream=blocks(json.dumps(docs["page"]).encode()))
            if request.url.path.endswith("/6"):
                return httpx.Response(200, headers={"content-type": "application/json"},
                                      stream=blocks(json.dumps(docs["layer"]).encode()))
            return httpx.Response(200, headers={"content-type": "application/json"},
                                  stream=blocks(json.dumps(docs["service"]).encode()))
        return httpx.MockTransport(handler)

    def _captured(self, tmp):
        ids = fixture_ids()
        features = fixture_features()
        docs = {"service": fixture_service(), "layer": fixture_layer(),
                "ids": {"objectIds": ids}, "page": fixture_page(features)}
        storage = StagingStorageResource(use_local=True, local_dir=tmp, prefix="")
        transport = self._transport(docs)
        client = httpx.Client(transport=transport)
        self.addCleanup(client.close)
        repo = self._fixture_repo(tmp)
        def run(**kwargs):
            with patch.object(source.httpx, "Client",
                              lambda **_kw: self._pinned_client(client)), \
                 self.pinned_for(ids, features):
                return source.capture_state_capitols(storage, repo_root=repo, **kwargs)
        return run, storage, ids

    def pinned_for(self, ids, features, **overrides):
        nulls = {key: 0 for key in source.RESPONSE_KEYS}
        for feature in features:
            for key, value in feature["attributes"].items():
                if value is None:
                    nulls[key] += 1
        xs = [feature["geometry"]["x"] for feature in features]
        ys = [feature["geometry"]["y"] for feature in features]
        values = {
            "COUNT": 2,
            "EXPECTED_ID_SHA": sha(sorted(ids)),
            "EXPECTED_RECORD_SHA": sha([feature["attributes"] for feature in features]),
            "EXPECTED_GEOMETRY_SHA": sha([[f["attributes"]["objectid"], f["geometry"]] for f in features]),
            "EXPECTED_NULL_COUNTS": nulls,
            "EXPECTED_BOUNDS": (min(xs), min(ys), max(xs), max(ys)),
            "EXPECTED_EXTENT": {"xmin": min(xs), "ymin": min(ys), "xmax": max(xs), "ymax": max(ys)},
            "EXPECTED_STATE_ROSTER": sorted({feature["attributes"]["state"] for feature in features}),
        }
        values.update(overrides)
        return patch.multiple(source, **values)

    def test_full_capture_cache_replay_and_tamper_fail_closed(self):
        with TemporaryDirectory() as tmp:
            run, storage, _ids = self._captured(tmp)
            first = run(version="v1")
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], 2)
            prefix = f"{first['dataset_slug']}/{first['file_slug']}/{first['version']}"
            keys = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/geoparquet/state-capitols.parquet", keys)
            self.assertIn(f"{prefix}/metadata/candidate_ready.json", keys)
            ready = json.loads(storage.read_key(f"{prefix}/metadata/candidate_ready.json"))
            self.assertFalse(ready["approved"])
            self.assertFalse(ready["release_accepted"])
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertFalse(manifest["approved"])
            self.assertEqual(manifest["inputs"][0]["role"], "producer_publication")
            parquet = storage.read_key(f"{prefix}/geoparquet/state-capitols.parquet")

            second = run(version="v2")
            self.assertTrue(second["reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/state-capitols.parquet"), parquet)

            tampered = Path(tmp) / prefix / "raw" / "ids_before.body"
            tampered.write_bytes(b'{"objectIds":[3]}')
            with self.assertRaisesRegex(ValueError, "cached_candidate_object_integrity_failed:raw/ids_before.body"):
                run(version="v3")
            self.assertEqual(storage.read_key(f"{prefix}/geoparquet/state-capitols.parquet"), parquet)

            # The sealed first-construction state survives and binds the reuse.
            state_key = f"_construction-state/state-capitols/{first['version']}.json"
            self.assertIsNotNone(storage.object_snapshot(state_key))

    def test_quarantine_retains_incomplete_capture_evidence(self):
        with TemporaryDirectory() as tmp:
            ids = fixture_ids()
            features = fixture_features()
            bad_page = fixture_page(features)
            bad_page["features"] = bad_page["features"][:1]
            docs = {"service": fixture_service(), "layer": fixture_layer(),
                    "ids": {"objectIds": ids}, "page": bad_page}
            storage = StagingStorageResource(use_local=True, local_dir=tmp, prefix="")
            transport = self._transport(docs)
            client = httpx.Client(transport=transport)
            self.addCleanup(client.close)
            repo = self._fixture_repo(tmp)
            with patch.object(source.httpx, "Client",
                              lambda **_kw: self._pinned_client(client)), \
                 self.pinned_for(ids, features):
                with self.assertRaisesRegex(ValueError, "source_page_population_incomplete"):
                    source.capture_state_capitols(storage, version="v1", repo_root=repo)
            quarantine = list(storage.list_prefix("_quarantine/state-capitols"))
            self.assertTrue(any(name.endswith("/raw/page_000.body") for name in quarantine))
            failures = [name for name in quarantine if name.endswith("/metadata/failure.json")]
            self.assertTrue(failures)
            reason = json.loads(storage.read_key(failures[0]))
            self.assertEqual(reason["reason_code"], "ValueError")
            self.assertEqual(set(storage.list_prefix("state-capitols")), set())


if __name__ == "__main__":
    unittest.main()