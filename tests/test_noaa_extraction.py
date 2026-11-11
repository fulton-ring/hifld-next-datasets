"""Draft NOAA extraction behavior; synthetic snapshots grant no real acceptance."""
import base64
import json
import tempfile
import unittest
from pathlib import Path
from http.client import IncompleteRead
from urllib.error import HTTPError
import io
import hashlib
import sqlite3
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from shapely import from_wkb

from dagster_hifld.noaa_extract import (
    fetch_noaa_layer, noaa_capture_urls,
    noaa_reference_fingerprints, stage_noaa_candidate, validate_noaa_capture,
)
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.source_manifest import load_resolved_source_manifest


def fixture():
    types = {"OBJECTID": "esriFieldTypeOID", "BOUND_ID": "esriFieldTypeString", "REGION": "esriFieldTypeString", "FEAT_TYPE": "esriFieldTypeString", "UNILATERAL": "esriFieldTypeDouble", "PUB_DATE": "esriFieldTypeDate", "SHAPE": "esriFieldTypeGeometry"}
    layer = {"geometryType": "esriGeometryPolyline", "maxRecordCount": 1000, "fields": [{"name": k, "type": v} for k, v in types.items()]}
    features = {"geometryType": "esriGeometryPolyline", "spatialReference": {"wkid": 4326}, "features": [{"attributes": {"OBJECTID": key, "BOUND_ID": "00" + str(key), "REGION": "test", "FEAT_TYPE": "test", "UNILATERAL": 0.0, "PUB_DATE": None}, "geometry": {"paths": [[[1.0, 2.0], [3.0, 4.0]]]}} for key in [2, 1]]}
    geo = {"type": "FeatureCollection", "features": [{"type": "Feature", "id": f["attributes"]["OBJECTID"], "properties": f["attributes"], "geometry": {"type": "LineString", "coordinates": f["geometry"]["paths"][0]}} for f in features["features"]]}
    ids = {"objectIdFieldName": "OBJECTID", "objectIds": [1, 2]}
    objects = {"layer": layer, "features": features, "geojson": geo, "ids_before": ids, "ids_after": ids, "count_before": {"count": 2}, "count_after": {"count": 2}}
    captures = {k: json.dumps(v).encode() for k, v in objects.items()}
    kwargs = {"expected_fields": types, "expected_fingerprints": noaa_reference_fingerprints(layer, features), "expected_count": 2}
    return captures, kwargs


TEST_LAYER = "https://maritimeboundaries.noaa.gov/arcgis/rest/services/MaritimeBoundaries/US_Maritime_Limits_Boundaries/MapServer/3"


def receipt_fixture(captures):
    return [{"capture": name, "url": noaa_capture_urls(TEST_LAYER)[name], "final_url": noaa_capture_urls(TEST_LAYER)[name], "http_status": 200,
             "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "fetched_at": "2026-10-19T04:19:08Z"}
            for name, raw in captures.items()]


class NoaaExtractionTests(unittest.TestCase):
    def test_staged_provenance_uses_installed_code_identity_when_run_tag_is_absent(self):
        captures, kwargs = fixture()
        for run_tag, expected_scheme in ((None, "hifld-installed-package-sha256-v1"),
                                         ("a" * 40, "run-tag:hifld/source_code_commit")):
            with self.subTest(run_tag=run_tag), tempfile.TemporaryDirectory() as directory:
                result = stage_noaa_candidate(
                    staging=StagingStorageResource(local_dir=directory, use_local=True),
                    dataset_slug="synthetic", file_slug="synthetic", captures=captures,
                    receipts=receipt_fixture(captures), expected_layer_url=TEST_LAYER,
                    code_commit=run_tag, **kwargs,
                )
                manifest_path = Path(directory, "synthetic", "synthetic", result["version"],
                                     "metadata", "source_manifest.json")
                manifest = json.loads(manifest_path.read_text())
                if run_tag is None:
                    self.assertEqual(manifest["code_commit"],
                                     "hifld-installed-package-sha256-v1:" + result["processing_fingerprint"])
                else:
                    self.assertEqual(manifest["code_commit"], run_tag)
                self.assertEqual(manifest["code_identity_scheme"], expected_scheme)
                validate_provenance(manifest)
                resolved = load_resolved_source_manifest(
                    StagingStorageResource(local_dir=directory, use_local=True),
                    "synthetic", "synthetic", result["version"],
                )
                self.assertEqual(resolved.metadata["code_commit"], manifest["code_commit"])
                self.assertEqual(resolved.metadata["code_identity_scheme"], expected_scheme)
                self.assertFalse(manifest["baseline_accepted"])
                self.assertFalse(manifest["contract_approved"])
                self.assertFalse(result["promotion_approved"])

    def test_declared_integer_boundaries_keep_int32_in_candidate_and_geopackage(self):
        captures, kwargs = fixture()
        layer = json.loads(captures['layer'])
        layer['fields'].append({'name': 'RULEID', 'type': 'esriFieldTypeInteger'})
        full = json.loads(captures['features'])
        geo = json.loads(captures['geojson'])
        for feature, independent, value in zip(full['features'], geo['features'], [-(2**31), 2**31-1]):
            feature['attributes']['RULEID'] = value
            independent['properties']['RULEID'] = value
        captures.update(layer=json.dumps(layer).encode(), features=json.dumps(full).encode(), geojson=json.dumps(geo).encode())
        kwargs.update(expected_fields=dict(kwargs['expected_fields'], RULEID='esriFieldTypeInteger'), expected_fingerprints=noaa_reference_fingerprints(layer, full))
        table, report = validate_noaa_capture(captures, **kwargs)
        self.assertEqual(table.schema.field('RULEID').type, pa.int32())
        self.assertEqual(sorted(table['RULEID'].to_pylist()), [-(2**31), 2**31-1])
        self.assertIn('OID', report['proposed_storage_encodings'])
        with tempfile.TemporaryDirectory() as directory:
            staging = StagingStorageResource(local_dir=directory, use_local=True)
            output = stage_noaa_candidate(staging=staging, dataset_slug='synthetic32', file_slug='synthetic32', captures=captures, receipts=receipt_fixture(captures), expected_layer_url=TEST_LAYER, **kwargs)
            self.assertEqual(pq.read_table(Path(directory)/output['candidate_key']).schema.field('RULEID').type, pa.int32())
            with sqlite3.connect(Path(directory)/output['catalog_source_key']) as connection:
                declarations = {row[1]: row[2] for row in connection.execute('PRAGMA table_info("synthetic32")')}
            self.assertEqual(declarations['RULEID'], 'MEDIUMINT')
        for value in [-(2**31)-1, 2**31, True, 1.0]:
            changed = json.loads(captures['features'])
            independently_changed = json.loads(captures['geojson'])
            changed['features'][0]['attributes']['RULEID'] = value
            independently_changed['features'][0]['properties']['RULEID'] = value
            bad = dict(captures, features=json.dumps(changed).encode(), geojson=json.dumps(independently_changed).encode())
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_noaa_capture(bad, **kwargs)

    def test_native_types_ids_vertices_and_staging_are_lossless_idempotent(self):
        captures, kwargs = fixture()
        table, report = validate_noaa_capture(captures, **kwargs)
        self.assertEqual(table.column_names, list(kwargs["expected_fields"]))
        self.assertEqual(table.schema.field("UNILATERAL").type, pa.float64())
        self.assertEqual(table.schema.field("PUB_DATE").type, pa.int64())
        self.assertEqual(table["BOUND_ID"].to_pylist(), ["001", "002"])
        self.assertEqual(table["OBJECTID"].to_pylist(), [1, 2])
        self.assertEqual(list(from_wkb(table["SHAPE"][0].as_py()).coords), [(1, 2), (3, 4)])
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            args = {"staging": storage, "dataset_slug": "synthetic", "file_slug": "synthetic", "captures": captures, "receipts": receipt_fixture(captures), **kwargs}
            first = stage_noaa_candidate(expected_layer_url=TEST_LAYER, **args)
            second = stage_noaa_candidate(expected_layer_url=TEST_LAYER, **args)
            self.assertEqual(first, second)
            self.assertFalse(first["promotion_approved"])
            self.assertTrue(pq.read_table(Path(directory) / first["candidate_key"]).equals(table))
            raw = Path(directory) / "synthetic" / "synthetic" / first["version"] / "raw"
            for name, body in captures.items():
                self.assertEqual((raw / (name + ".json")).read_bytes(), body)

    def test_every_receipt_origin_and_observed_redirect_is_bound_before_candidate(self):
        captures, kwargs = fixture()
        for name in captures:
            for mode in ('mirror_request', 'mirror_redirect', 'wrong_layer', 'missing_redirect'):
                receipts = receipt_fixture(captures)
                item = next(r for r in receipts if r['capture'] == name)
                if mode == 'mirror_request': item['url'] = item['url'].replace('maritimeboundaries.noaa.gov', 'example.invalid')
                if mode == 'mirror_redirect': item['final_url'] = item['final_url'].replace('maritimeboundaries.noaa.gov', 'example.invalid')
                if mode == 'wrong_layer': item['url'] = item['url'].replace('MapServer/3', 'MapServer/2')
                if mode == 'missing_redirect': item.pop('final_url')
                with self.subTest(capture=name, mode=mode), tempfile.TemporaryDirectory() as directory:
                    with self.assertRaisesRegex(ValueError, 'quarantined'):
                        stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug='synthetic', file_slug='synthetic', captures=captures, receipts=receipts, **kwargs)
                    self.assertEqual(next(Path(directory).rglob('features.json')).read_bytes(), captures['features'])
                    self.assertEqual(json.loads(next(Path(directory).rglob('raw/receipts/*.json')).read_text()), receipts)
                    self.assertEqual(len(list(Path(directory).rglob('quarantine.json'))), 1)
                    self.assertFalse(list(Path(directory).rglob('*.parquet')))
                    self.assertFalse(list(Path(directory).rglob('source_manifest.json')))
        for bad in ('http://maritimeboundaries.noaa.gov', 'https://user@maritimeboundaries.noaa.gov', 'https://maritimeboundaries.noaa.gov:443', 'https://maritimeboundaries.noaa.gov.evil'):
            receipts = receipt_fixture(captures)
            receipts[0]['final_url'] = receipts[0]['url'].replace('https://maritimeboundaries.noaa.gov', bad)
            with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ValueError, 'quarantined'):
                stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug='synthetic', file_slug='synthetic', captures=captures, receipts=receipts, **kwargs)

    def test_rejects_partial_error_type_id_schema_geometry_and_coverage_changes(self):
        cases = [
            ("features", lambda d: d.update(error={"code": 500})),
            ("geojson", lambda d: d["features"][1].update(id=True)),
            ("geojson", lambda d: d["features"][1]["properties"].update(OBJECTID=True)),
            ("geojson", lambda d: d["features"][1]["properties"].update(UNILATERAL=False)),
            ("geojson", lambda d: d["features"][1]["geometry"].update(type="Polygon")),
            ("features", lambda d: d.update(exceededTransferLimit=True)),
            ("count_after", lambda d: d.update(count=3)),
            ("ids_after", lambda d: d.update(objectIds=[1, 1])),
            ("features", lambda d: d["features"][0]["attributes"].update(BOUND_ID="001")),
            ("features", lambda d: d["features"][0]["attributes"].update(OBJECTID="2")),
            ("layer", lambda d: d["fields"].append({"name": "surprise", "type": "esriFieldTypeString"})),
            ("features", lambda d: d["features"][0]["geometry"]["paths"][0][0].__setitem__(0, 7.0)),
            ("features", lambda d: d["features"][0]["attributes"].update(REGION="changed")),
            ("features", lambda d: d["features"][0]["attributes"].update(PUB_DATE=1.5)),
        ]
        for name, mutate in cases:
            with self.subTest(name=name, mutate=mutate):
                captures, kwargs = fixture()
                obj = json.loads(captures[name]); mutate(obj); captures[name] = json.dumps(obj).encode()
                with tempfile.TemporaryDirectory() as directory:
                    with self.assertRaisesRegex(ValueError, "quarantined"):
                        stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=receipt_fixture(captures), **kwargs)
                    self.assertEqual(len(list(Path(directory).rglob("quarantine.json"))), 1)
                    self.assertFalse(list(Path(directory).rglob("*.parquet")))
                    self.assertFalse(list(Path(directory).rglob("*.gpkg")))

    def test_id_witness_cardinality_and_duplicates_are_quarantined_with_raw(self):
        for witness in ("ids_before", "ids_after"):
            for representation in ("objectIds", "features"):
                for values in ([1, 2, 2], [1, 2, None], [1], [1, 1], [1, 2, True]):
                    with self.subTest(witness=witness, representation=representation, values=values):
                        captures, kwargs = fixture()
                        body = {"objectIds": values} if representation == "objectIds" else {"features": [{"attributes": {"OBJECTID": value}} for value in values]}
                        captures[witness] = json.dumps(body).encode()
                        with tempfile.TemporaryDirectory() as directory:
                            with self.assertRaisesRegex(ValueError, "quarantined.*ID reconciliation"):
                                stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=receipt_fixture(captures), **kwargs)
                            self.assertEqual(next(Path(directory).rglob(witness + ".json")).read_bytes(), captures[witness])
                            self.assertEqual(len(list(Path(directory).rglob("quarantine.json"))), 1)
                            self.assertFalse(list(Path(directory).rglob("*.parquet")))

    def test_valid_shaped_final_http503_is_quarantined_with_raw_receipts(self):
        captures, kwargs = fixture()
        statuses = receipt_fixture(captures)
        for receipt in statuses:
            receipt["http_status"] = 503
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "quarantined.*terminal HTTP"):
                stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=statuses, **kwargs)
            self.assertEqual(next(Path(directory).rglob("features.json")).read_bytes(), captures["features"])
            self.assertEqual(json.loads(next(Path(directory).rglob("raw/receipts/*.json")).read_text()), statuses)
            self.assertFalse(list(Path(directory).rglob("*.parquet")))

    def test_exhausted_http_error_with_plausible_body_stays_failed(self):
        captures, kwargs = fixture()
        ordered = ["layer", "count_before", "ids_before", "features", "geojson", "ids_after", "count_after"]
        errors = [HTTPError("https://example.test/layer", 503, "unavailable", None, io.BytesIO(captures[name])) for name in ordered for _ in range(2)]
        with patch("dagster_hifld.noaa_extract.urlopen", side_effect=errors), patch("dagster_hifld.noaa_extract.time.sleep"):
            fetched, receipts = fetch_noaa_layer(TEST_LAYER, attempts=2)
        self.assertEqual(fetched, captures)
        self.assertTrue(all(r["http_status"] == 503 for r in receipts))
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "quarantined.*terminal HTTP"):
                stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug="synthetic", file_slug="synthetic", captures=fetched, receipts=receipts, **kwargs)
            self.assertEqual(next(Path(directory).rglob("features.json")).read_bytes(), captures["features"])
            self.assertFalse(list(Path(directory).rglob("*.parquet")))

    def test_body_read_reset_and_partial_exhaustion_retain_prior_raw_for_quarantine(self):
        class Response:
            status = 200
            url = TEST_LAYER
            def __init__(self, value): self.value = value
            def read(self, limit):
                if isinstance(self.value, Exception): raise self.value
                return self.value
            def __enter__(self): return self
            def __exit__(self, *args): return False
        for error in (ConnectionResetError("reset"), IncompleteRead(b'{"cou', 5)):
            with self.subTest(error=type(error).__name__), patch("dagster_hifld.noaa_extract.urlopen", side_effect=[Response(b'{"layer":"preserved"}'), *[Response(error) for _ in range(3)]]), patch("dagster_hifld.noaa_extract.time.sleep"):
                captures, receipts = fetch_noaa_layer(TEST_LAYER, attempts=3)
                self.assertEqual(captures["layer"], b'{"layer":"preserved"}')
                self.assertEqual(len(receipts[-1]["failed_attempts"]), 3)
                if isinstance(error, IncompleteRead): self.assertEqual(captures["count_before"], error.partial)
                _, kwargs = fixture()
                with tempfile.TemporaryDirectory() as directory:
                    with self.assertRaisesRegex(ValueError, "quarantined"):
                        stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(local_dir=directory, use_local=True), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=receipts, **kwargs)
                    self.assertEqual(next(Path(directory).rglob("layer.json")).read_bytes(), captures["layer"])
                    self.assertEqual(len(list(Path(directory).rglob("quarantine.json"))), 1)
                    self.assertFalse(list(Path(directory).rglob("*.parquet")))

    def test_duplicate_json_members_quarantine_instead_of_overriding(self):
        captures, kwargs = fixture()
        captures["count_after"] = b'{"count":2,"count":2}'
        with self.assertRaisesRegex(ValueError, "duplicate JSON member"):
            validate_noaa_capture(captures, **kwargs)

    def test_changed_processing_policy_gets_new_namespace_and_stale_provenance_rejects(self):
        captures, kwargs = fixture()
        with tempfile.TemporaryDirectory() as directory:
            storage = StagingStorageResource(local_dir=directory, use_local=True)
            args = {"staging": storage, "dataset_slug": "synthetic", "file_slug": "synthetic", "captures": captures, "receipts": receipt_fixture(captures), **kwargs}
            with patch("dagster_hifld.noaa_extract.verifier_implementation_sha256", return_value="a" * 64):
                first = stage_noaa_candidate(expected_layer_url=TEST_LAYER, **args)
            with patch("dagster_hifld.noaa_extract.verifier_implementation_sha256", return_value="b" * 64):
                changed = stage_noaa_candidate(expected_layer_url=TEST_LAYER, **args)
                self.assertNotEqual(first["version"], changed["version"])
                path = Path(directory) / "synthetic/synthetic" / changed["version"] / "metadata/source_manifest.json"
                value = json.loads(path.read_text()); value["unsupported_output_formats"] = {}; path.write_text(json.dumps(value))
                with self.assertRaisesRegex(ValueError, "provenance semantics changed"):
                    stage_noaa_candidate(expected_layer_url=TEST_LAYER, **args)
            self.assertTrue((Path(directory) / first["candidate_key"]).exists())

    def test_bounded_application_error_retries_retain_failed_raw_body(self):
        class Response:
            status = 200
            url = TEST_LAYER
            def __init__(self, raw): self.raw = raw
            def read(self, limit): return self.raw[:limit]
            def __enter__(self): return self
            def __exit__(self, *args): return False
        error = b'{"error":{"code":500,"message":"upstream unavailable"}}'
        with patch("dagster_hifld.noaa_extract.urlopen", side_effect=[Response(error), *[Response(b'{}') for _ in range(7)]]), patch("dagster_hifld.noaa_extract.time.sleep"):
            captures, receipts = fetch_noaa_layer(TEST_LAYER, attempts=2)
        self.assertEqual(len(captures), 7)
        self.assertEqual(len(receipts[0]["failed_attempts"]), 1)
        self.assertEqual(base64.b64decode(receipts[0]["failed_attempts"][0]["body_base64"]), error)

    def test_published_resource_rejected_without_writing(self):
        captures, kwargs = fixture()
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "isolated local storage"):
                stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=PublishedStorageResource(use_local=True, local_dir=directory), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=receipt_fixture(captures), **kwargs)
            self.assertEqual(list(Path(directory).rglob("*")), [])

    def test_remote_storage_rejected_for_unapproved_draft(self):
        captures, kwargs = fixture()
        with self.assertRaisesRegex(ValueError, "isolated local storage"):
            stage_noaa_candidate(expected_layer_url=TEST_LAYER, staging=StagingStorageResource(use_local=False, bucket="not-real"), dataset_slug="synthetic", file_slug="synthetic", captures=captures, receipts=receipt_fixture(captures), **kwargs)


if __name__ == "__main__":
    unittest.main()
