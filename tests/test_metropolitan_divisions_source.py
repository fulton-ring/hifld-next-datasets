"""Focused source contract and failure guards for the unapproved draft."""
import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
import subprocess

import httpx
import pyarrow as pa
import shapely

from dagster_hifld.assets.metropolitan_divisions import metdiv_source_integrity_schedule
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import seal_ready_candidate, verify_cached_candidate
from dagster_hifld.sources import metropolitan_divisions as source


class MetropolitanDivisionsSourceTests(unittest.TestCase):
    def setUp(self):
        self.geometry = shapely.to_wkb(
            shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])])
        )
        self.rows = 37
        props = {
            "OBJECTID": list(range(1, self.rows + 1)),
            "CSAFP": [None if i < 2 else "120" for i in range(self.rows)],
            "CBSAFP": ["12060"] * self.rows,
            "METDIVFP": [f"{12054 + i}" for i in range(self.rows)],
            "GEOID": [str(1206012054 + i) for i in range(self.rows)],
            "GEOIDFQ": [f"314M700US{1206012054 + i}" for i in range(self.rows)],
            "NAME": [f"CBSA {i}" for i in range(self.rows)],
            "NAMELSAD": [f"Division {i}" for i in range(self.rows)],
            "LSAD": ["M2"] * self.rows,
            "MTFCC": ["G3110"] * self.rows,
            "ALAND": [float(i + 1) for i in range(self.rows)],
            "AWATER": [float(i) for i in range(self.rows)],
            "INTPTLAT": ["+33.6457373"] * self.rows,
            "INTPTLON": ["-84.2948951"] * self.rows,
            "SHAPE": [self.geometry] * self.rows,
        }
        schema = pa.schema([pa.field(name, typ) for name, typ in zip(source.FIELDS, source.TYPES)])
        self.table = pa.Table.from_pydict(props, schema=schema)
        self.metadata = {"geometry_type": "MultiPolygon", "crs": "EPSG:4269", "fid_column": "OBJECTID"}

    def receipt(self, body=b"x"):
        return {"url": source.URL, "final_url": source.URL, "status": 200, "bytes": len(body),
                "sha256": source.digest(body), "complete_body": True, "observed_at": "2026-10-19T00:00:00Z"}

    def synthetic_baseline(self):
        """Synthetic pinned GCS baseline: one collection plus five full assets."""
        assets = {}
        bodies = {}
        for key, name, fmt in (
            ("geojson-synthetic", "metropolitan-divisions-2-geojson.geojson", "geojson"),
            ("geopackage-synthetic", "metropolitan-divisions-2.gpkg", "geopackage"),
            ("geoparquet-synthetic", "metropolitan-divisions-2.parquet", "geoparquet"),
            ("pmtiles-synthetic", "metropolitan-divisions-2.pmtiles", "pmtiles"),
            ("shapefile-synthetic", "metropolitan-divisions-2.zip", "shapefile_zip"),
        ):
            body = f"synthetic-{key}".encode()
            url = f"https://baseline.test/{name}"
            assets[key] = {"name": name, "format": fmt, "url": url, "bytes": len(body),
                "generation": f"1791506395{len(body):04d}", "md5": hashlib.md5(body).hexdigest(),
                "collection_checksum": "d50110" + hashlib.md5(body).hexdigest(), "sha256": source.digest(body)}
            bodies[key] = body
        collection = {"hifld:feature_count": 37, "hifld:agency": "U.S. Census Bureau", "hifld:geometry_type": "MultiPolygon",
            "assets": {key: {"href": spec["url"], "file:size": spec["bytes"], "file:checksum": spec["collection_checksum"]}
                       for key, spec in assets.items()}}
        bodies["collection"] = json.dumps(collection).encode()
        specs = {"name": "collection.json", "url": "https://baseline.test/collection.json", "bytes": len(bodies["collection"]),
            "generation": "1791506395736945", "md5": hashlib.md5(bodies["collection"]).hexdigest(),
            "sha256": source.digest(bodies["collection"])}
        served = {}
        generations = {}
        etags = {}
        for key, spec in {**assets, "collection": specs}.items():
            url = spec["url"] + "?generation=" + spec["generation"]
            served[url] = bodies[key]
            generations[url] = spec["generation"]
            etags[url] = f'"{spec["md5"]}"'

        def handler(request):
            url = str(request.url)
            return httpx.Response(200, content=served[url], headers={"content-length": str(len(served[url])),
                "x-goog-generation": generations[url], "etag": etags[url]})

        real_client = httpx.Client

        def client_factory(**kwargs):
            return real_client(transport=httpx.MockTransport(handler), follow_redirects=kwargs.get("follow_redirects", False),
                timeout=kwargs.get("timeout", 60))

        return assets, specs, bodies, client_factory

    def counting_clock(self):
        """Deterministic strictly increasing wall clock for receipt observed_at."""
        ticks = iter(range(1, 10_000))
        return type("Clock", (), {"now": staticmethod(
            lambda tz=None: datetime(2026, 11, 13, 0, 0, 0, next(ticks), tz or timezone.utc))})

    def test_registered_source_and_refresh_stopped(self):
        from dagster_hifld.assets.metropolitan_divisions import census_metropolitan_divisions
        self.assertEqual(census_metropolitan_divisions.key.path, ["ingest", source.SLUG, source.SLUG])
        self.assertIn(census_metropolitan_divisions, hifld_ingest_assets)
        self.assertIn(census_metropolitan_divisions.key, {key for asset in defs.assets for key in asset.keys})
        self.assertEqual(metdiv_source_integrity_schedule.default_status.value, "STOPPED")
        self.assertIn(metdiv_source_integrity_schedule.name, {schedule.name for schedule in defs.schedules})

    def test_local_staging_only_and_strict_json(self):
        for storage in (PublishedStorageResource(local_dir="/tmp/never-metdiv"), StagingStorageResource(bucket="never")):
            with patch.object(source, "acquire") as acquire, self.assertRaises(source.MetdivQuarantineError):
                source.stage_metdiv_candidate(storage)
            acquire.assert_not_called()
        for body in (b'{"x":1,"x":2}', b'{"error":{}}', b"[]", b"null"):
            with self.assertRaises(ValueError):
                source.strict_json(body)

    def test_full_native_schema_ids_values_and_geometry_validate(self):
        source.validate_table(self.table, self.metadata)
        variants = [
            self.table.slice(0, 36), self.table.drop(["OBJECTID"]),
            self.table.append_column("unexpected", pa.array([1] * self.rows)),
            self.table.set_column(10, "ALAND", pa.array([1] * self.rows)),
            self.table.set_column(0, "OBJECTID", pa.array([1] * self.rows)),
            self.table.set_column(14, "SHAPE", pa.array([None] * self.rows, type=pa.binary())),
        ]
        for candidate in variants:
            with self.subTest(schema=candidate.schema), self.assertRaises(source.MetdivQuarantineError):
                source.validate_table(candidate, self.metadata)
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(self.table, {**self.metadata, "crs": "EPSG:4326"})
        changed = self.table.set_column(1, "GEOID", pa.array(["bad"] * self.rows))
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(changed, self.metadata)

    def test_producer_nullable_field_population_is_pinned(self):
        source.validate_table(self.table, self.metadata)
        variants = (
            self.table.set_column(1, "CSAFP", pa.array([None] * self.rows, type=pa.string())),
            self.table.set_column(1, "CSAFP", pa.array(["120"] * self.rows)),
            self.table.set_column(2, "CBSAFP", pa.array([None] * self.rows, type=pa.string())),
        )
        for candidate in variants:
            with self.subTest(schema=candidate.column(1).null_count), self.assertRaises(source.MetdivQuarantineError):
                source.validate_table(candidate, self.metadata)

    def test_missing_renamed_ids_and_geometry_coverage_drift_fail(self):
        for name in ("OBJECTID", "GEOID"):
            with self.subTest(field=name), self.assertRaises(source.MetdivQuarantineError):
                source.validate_table(self.table.drop([name]), self.metadata)
        duplicate_ids = self.table.set_column(0, "OBJECTID", pa.array([1] * self.rows, type=pa.int64()))
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(duplicate_ids, self.metadata)
        invalid = shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])]))
        bad_geometry = self.table.set_column(14, "SHAPE", pa.array([invalid] * self.rows, type=pa.binary()))
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(bad_geometry, self.metadata)

    def test_untrusted_or_incomplete_receipt_never_creates_candidate(self):
        changes = ({"observed_at": "not-a-date"}, {"url": "https://example.invalid"},
                   {"final_url": "https://example.invalid"}, {"status": 403}, {"bytes": 99},
                   {"sha256": "0" * 64}, {"complete_body": False})
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            protected = Path(tmp) / "prior-fixture"
            protected.write_bytes(b"keep")
            for change in changes:
                receipt = self.receipt()
                receipt.update(change)
                with patch.object(source, "acquire", return_value=(b"x", receipt)), \
                     patch.object(source, "processing_identity", return_value={"test": True}), \
                     self.assertRaises(ValueError):
                    source.stage_metdiv_candidate(storage)
                self.assertFalse((Path(tmp) / source.SLUG).exists())
                self.assertEqual(protected.read_bytes(), b"keep")

    def test_live_baseline_http_branch_populates_receipt_sha256_and_pins_capture(self):
        assets, specs, bodies, client_factory = self.synthetic_baseline()
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            with patch.object(source.httpx, "Client", client_factory), \
                 patch.object(source, "BASELINE_COLLECTION", specs), patch.object(source, "BASELINE_ASSETS", assets):
                blobs, receipts = source.acquire_published_baseline(storage)
            self.assertEqual(set(blobs), {"collection", *assets})
            self.assertEqual(blobs["collection"], bodies["collection"])
            for key, spec in {**assets, "collection": specs}.items():
                with self.subTest(key=key):
                    summary = receipts[key]
                    self.assertEqual(summary["sha256"], spec["sha256"])
                    self.assertEqual(summary["md5"], spec["md5"])
                    self.assertEqual(summary["bytes"], spec["bytes"])
                    self.assertEqual(summary["generation"], spec["generation"])
                    captured_key = (f"_raw-captures/{source.SLUG}/baseline/{spec['generation']}/"
                                    + ("collection.json" if key == "collection" else spec["name"]))
                    self.assertEqual(summary["capture_key"], captured_key)
                    self.assertEqual(storage.read_key(captured_key), bodies[key])
                    raw = source.strict_json(storage.read_key(summary["receipt_key"]))
                    self.assertEqual(summary["receipt_key"],
                                     captured_key + ".receipt-" + summary["receipt_sha256"] + ".json")
                    self.assertEqual(source.digest(storage.read_key(summary["receipt_key"])), summary["receipt_sha256"])
                    self.assertEqual(raw["sha256"], spec["sha256"])
                    self.assertEqual(raw["bytes"], spec["bytes"])
                    self.assertEqual(raw["generation"], spec["generation"])
                    self.assertEqual(raw["etag"], f'"{spec["md5"]}"')
                    self.assertEqual(raw["status"], 200)
                    self.assertTrue(raw["complete_body"])
            smaller = {**specs, "bytes": len(bodies["collection"]) - 1}
            with patch.object(source.httpx, "Client", client_factory), \
                 patch.object(source, "BASELINE_COLLECTION", smaller), patch.object(source, "BASELINE_ASSETS", assets), \
                 self.assertRaisesRegex(ValueError, "published_baseline_oversized_response:collection"):
                source.acquire_published_baseline(storage)

    def test_repeated_live_baseline_acquisition_on_same_storage_is_repeatable(self):
        assets, specs, bodies, client_factory = self.synthetic_baseline()
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            with patch.object(source.httpx, "Client", client_factory), \
                 patch.object(source, "BASELINE_COLLECTION", specs), patch.object(source, "BASELINE_ASSETS", assets), \
                 patch.object(source, "datetime", self.counting_clock()):
                first_blobs, first_receipts = source.acquire_published_baseline(storage)
                second_blobs, second_receipts = source.acquire_published_baseline(storage)
            self.assertEqual(first_blobs, second_blobs)
            for key, spec in {**assets, "collection": specs}.items():
                with self.subTest(key=key):
                    name = "collection.json" if key == "collection" else spec["name"]
                    body_key = f"_raw-captures/{source.SLUG}/baseline/{spec['generation']}/{name}"
                    self.assertEqual(first_receipts[key]["capture_key"], second_receipts[key]["capture_key"])
                    self.assertNotEqual(first_receipts[key]["receipt_key"], second_receipts[key]["receipt_key"])
                    for receipts in (first_receipts, second_receipts):
                        stored = storage.read_key(receipts[key]["receipt_key"])
                        self.assertEqual(receipts[key]["receipt_key"], body_key + ".receipt-" + source.digest(stored) + ".json")
                        parsed = source.strict_json(stored)
                        self.assertEqual(parsed["sha256"], spec["sha256"])
                        self.assertEqual(parsed["bytes"], spec["bytes"])
                        self.assertEqual(parsed["generation"], spec["generation"])
                        self.assertEqual(parsed["status"], 200)
                        self.assertTrue(parsed["complete_body"])
            with patch.object(source.httpx, "Client", client_factory), \
                 patch.object(source, "BASELINE_COLLECTION", specs), patch.object(source, "BASELINE_ASSETS", assets), \
                 patch.object(source, "datetime", self.counting_clock()):
                storage.write_key(first_receipts["collection"]["capture_key"], b"tampered-baseline-body")
                with self.assertRaisesRegex(source.MetdivQuarantineError, "immutable_capture_changed"):
                    source.acquire_published_baseline(storage)

    def test_default_ingest_path_stages_candidate_with_default_baseline_wiring(self):
        comparison = {"baseline_collection": {}, "baseline_artifacts": {}, "baseline_receipts": {}, "formats": {},
            "comparison_status": "breaking_change_review_required", "source_schema_contract_approved": False,
            "source_id_contract_approved": False, "baseline_approved": False, "release_approved": False}
        url = "https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_nationgeo.gdb.zip"
        receipt = {"url": url, "final_url": url, "status": 200, "bytes": 1,
            "sha256": source.digest(b"x"), "complete_body": True, "observed_at": "2026-11-13T00:00:00Z"}
        definition = {"definition_xml": "<DEFeatureClassInfo/>", "fields": {"OBJECTID": {"FieldType": "esriFieldTypeOID"}},
            "driver_oid_representation": "test", "draft_output_oid_representation": "test"}
        calls = []

        def baseline(storage_arg, baseline_dir=None):
            calls.append(baseline_dir)
            return {"collection": b"{}"}, {"collection": {"url": "https://baseline.test/collection.json",
                "generation": "0", "bytes": 2, "md5": "0" * 32, "sha256": source.digest(b"{}"),
                "capture_key": "_raw-captures/x", "receipt_sha256": "0" * 64}}

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            with patch.object(source, "acquire", return_value=(b"x", receipt)) as acquire, \
                 patch.object(source, "URL", url), patch.object(source, "BYTES", 1), \
                 patch.object(source, "SHA256", source.digest(b"x")), \
                 patch.object(source, "inspect_archive", return_value=({**self.metadata, "native_layer_definition": definition},
                                                                        self.table, {})), \
                 patch.object(source, "processing_identity", return_value={"test": True}), \
                 patch.object(source, "compare_published_baseline", return_value=comparison), \
                 patch.object(source, "acquire_published_baseline", side_effect=baseline):
                result = source.stage_metdiv_candidate(storage)
            acquire.assert_called_once_with(storage, None, None)
            self.assertEqual(calls, [None])
            self.assertFalse(result["reused"])
            self.assertEqual(result["feature_count"], 37)
            root = Path(tmp) / source.SLUG / source.SLUG / result["version"]
            self.assertTrue((root / "geopackage" / f"{source.SLUG}.gpkg").is_file())
            self.assertTrue((root / "candidate" / "typed-source.parquet").is_file())
            self.assertTrue((root / "metadata" / "baseline_comparison.json").is_file())
            ready = source.strict_json((root / "metadata" / "metdiv_candidate_ready.json").read_bytes())
            self.assertFalse(ready["approved"])
            self.assertEqual(ready["source_sha256"], source.digest(b"x"))
            self.assertTrue(ready["version"].startswith("v0.0.0-draft-census2026-"))

    def test_second_default_staging_run_reuses_candidate_and_repeats_live_baseline(self):
        assets, specs, bodies, client_factory = self.synthetic_baseline()
        url = "https://www2.census.gov/geo/tiger/TGRGDB26/tlgdb_2026_us_nationgeo.gdb.zip"
        receipt = {"url": url, "final_url": url, "status": 200, "bytes": 1,
            "sha256": source.digest(b"x"), "complete_body": True, "observed_at": "2026-11-13T00:00:00Z"}
        definition = {"definition_xml": "<DEFeatureClassInfo/>", "fields": {"OBJECTID": {"FieldType": "esriFieldTypeOID"}},
            "driver_oid_representation": "test", "draft_output_oid_representation": "test"}
        comparison = {"baseline_collection": {}, "baseline_artifacts": {}, "baseline_receipts": {}, "formats": {},
            "comparison_status": "breaking_change_review_required", "source_schema_contract_approved": False,
            "source_id_contract_approved": False, "baseline_approved": False, "release_approved": False}
        identity = {"test": True, "code_commit": "b" * 40, "files": {}}
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            with patch.object(source, "acquire", return_value=(b"x", receipt)) as acquire, \
                 patch.object(source, "URL", url), patch.object(source, "BYTES", 1), \
                 patch.object(source, "SHA256", source.digest(b"x")), \
                 patch.object(source, "inspect_archive", return_value=({**self.metadata, "native_layer_definition": definition},
                                                                        self.table, {})), \
                 patch.object(source, "processing_identity", return_value=identity), \
                 patch.object(source, "compare_published_baseline", side_effect=lambda table, blobs: dict(comparison)), \
                 patch.object(source.httpx, "Client", client_factory), \
                 patch.object(source, "BASELINE_COLLECTION", specs), patch.object(source, "BASELINE_ASSETS", assets), \
                 patch.object(source, "datetime", self.counting_clock()), \
                 patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                first = source.stage_metdiv_candidate(storage)
                second = source.stage_metdiv_candidate(storage)
            self.assertEqual(acquire.call_count, 2)
            self.assertFalse(first["reused"])
            self.assertTrue(second["reused"])
            self.assertEqual(first["version"], second["version"])
            self.assertEqual(first["feature_count"], second["feature_count"])
            baseline_keys = set(storage.list_prefix(f"_raw-captures/{source.SLUG}/baseline/"))
            for key, spec in {**assets, "collection": specs}.items():
                name = "collection.json" if key == "collection" else spec["name"]
                body_key = f"_raw-captures/{source.SLUG}/baseline/{spec['generation']}/{name}"
                receipt_keys = sorted(item for item in baseline_keys
                                      if item.startswith(body_key + ".receipt-") and item.endswith(".json"))
                with self.subTest(key=key):
                    self.assertEqual(storage.read_key(body_key), bodies[key])
                    self.assertEqual(len(receipt_keys), 2)
                    for receipt_key in receipt_keys:
                        stored = storage.read_key(receipt_key)
                        self.assertEqual(receipt_key, body_key + ".receipt-" + source.digest(stored) + ".json")
                        parsed = source.strict_json(stored)
                        self.assertEqual(parsed["sha256"], spec["sha256"])
                        self.assertEqual(parsed["bytes"], spec["bytes"])
                        self.assertEqual(parsed["generation"], spec["generation"])
                        self.assertEqual(parsed["status"], 200)
                        self.assertTrue(parsed["complete_body"])

    def test_cached_outputs_are_rederived_from_original_native_rows(self):
        expected = source.expected_parquet_table(self.table)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "geopackage").mkdir()
            (root / "candidate").mkdir()
            gpkg = root / "geopackage" / f"{source.SLUG}.gpkg"
            parquet = root / "candidate" / "typed-source.parquet"
            source.write_native_gpkg(self.table, gpkg)
            source.pq.write_table(expected, parquet)
            source.reconcile_cached_native_outputs(root, self.table)
            altered = self.table.set_column(10, "ALAND", pa.array([999.0] * self.rows))
            source.pq.write_table(source.expected_parquet_table(altered), parquet)
            with self.assertRaises(source.MetdivQuarantineError):
                source.reconcile_cached_native_outputs(root, self.table)

    def test_shared_cache_verifier_accepts_catalog_metadata_but_rejects_unknown_objects(self):
        from dagster_hifld.catalog import summarize_staged_catalog
        from dagster_hifld.source_manifest import load_resolved_source_manifest

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            version = "v152-test"
            prefix = f"{source.SLUG}/{source.SLUG}/{version}"
            fingerprint = "a" * 64
            state_key = f"_construction-state/{source.SLUG}/{version}.json"
            identity = {"code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "files": {}}
            manifest_body = b"{}\n"
            scratch = Path(tmp) / "scratch.gpkg"
            source.write_native_gpkg(self.table, scratch)
            gpkg_body = scratch.read_bytes()
            scratch.unlink()
            gpkg_name = f"geopackage/{source.SLUG}.gpkg"
            storage.write_key(f"{prefix}/{gpkg_name}", gpkg_body)
            storage.write_key(f"{prefix}/metadata/source_manifest.json", manifest_body)
            for relative in ("metadata/source/source_manifest.json",
                             f"metadata/source/version_{source.digest(manifest_body)}_source_manifest.json"):
                storage.write_key(f"{prefix}/{relative}", manifest_body)
            objects = {
                gpkg_name: {"bytes": len(gpkg_body), "sha256": source.digest(gpkg_body)},
                "metadata/source_manifest.json": {"bytes": len(manifest_body), "sha256": source.digest(manifest_body)},
            }
            payload = {"processing_identity": identity, "source_record_sha256": "b" * 64,
                       "source_schema_sha256": "c" * 64, "capture_receipts": {}, "objects": objects}
            seal_ready_candidate(storage, state_key, version, fingerprint, payload)
            resolved_manifest = load_resolved_source_manifest(storage, source.SLUG, source.SLUG, version)
            summary = summarize_staged_catalog(storage, source.SLUG, source.SLUG, version, source.SLUG,
                source_metadata=resolved_manifest.metadata)
            storage.write_key(f"{prefix}/metadata/quality_manifest.json", json.dumps(summary.quality_manifest).encode())
            storage.write_key(f"{prefix}/metadata/data_dictionary.json", json.dumps(summary.data_dictionary).encode())
            with patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"):
                result = verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})
            self.assertTrue(result["reused"])
            storage.write_key(f"{prefix}/unexpected/source.json", b"{}\n")
            with patch("dagster_hifld.sources.epa_frs_common.validate_construction_code_identity"), self.assertRaisesRegex(ValueError, "cached_candidate_storage_roster_changed"):
                verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})

    def test_native_definition_rejects_oid_width_or_field_drift(self):
        fields = "".join(
            f"<GPFieldInfoEx><Name>{name}</Name><FieldType>{kind}</FieldType><Length>{length}</Length>"
            f"<IsNullable>{nullable}</IsNullable>{required}</GPFieldInfoEx>"
            for name, kind, length, nullable, required in [
                ("CSAFP", "esriFieldTypeString", 3, "true", ""),
                ("CBSAFP", "esriFieldTypeString", 5, "true", ""),
                ("METDIVFP", "esriFieldTypeString", 5, "true", ""),
                ("GEOID", "esriFieldTypeString", 10, "true", ""),
                ("GEOIDFQ", "esriFieldTypeString", 19, "true", ""),
                ("NAME", "esriFieldTypeString", 100, "true", ""),
                ("NAMELSAD", "esriFieldTypeString", 100, "true", ""),
                ("LSAD", "esriFieldTypeString", 2, "true", ""),
                ("MTFCC", "esriFieldTypeString", 5, "true", ""),
                ("ALAND", "esriFieldTypeDouble", 8, "true", ""),
                ("AWATER", "esriFieldTypeDouble", 8, "true", ""),
                ("INTPTLAT", "esriFieldTypeString", 11, "true", ""),
                ("INTPTLON", "esriFieldTypeString", 12, "true", ""),
                ("OBJECTID", "esriFieldTypeOID", 4, "false", "<Required>true</Required><Editable>false</Editable>"),
                ("SHAPE", "esriFieldTypeGeometry", 0, "true", ""),
            ]
        )
        xml = f'<DEFeatureClassInfo><OIDFieldName>OBJECTID</OIDFieldName><HasZ>false</HasZ><HasM>false</HasM><GPFieldInfoExs>{fields}</GPFieldInfoExs></DEFeatureClassInfo>'
        with patch.object(source.pyogrio, "read_arrow", return_value=({}, pa.table({"FIELD_1": [xml]}))):
            declaration = source.read_native_definition(Path("/synthetic-unit-only.zip"))
        self.assertEqual(declaration["fields"]["OBJECTID"]["Length"], "4")
        self.assertEqual(source.TYPES[0], pa.int64())
        changed = xml.replace("<Length>4</Length>", "<Length>8</Length>")
        with patch.object(source.pyogrio, "read_arrow", return_value=({}, pa.table({"FIELD_1": [changed]}))), self.assertRaises(ValueError):
            source.read_native_definition(Path("/synthetic-unit-only.zip"))


if __name__ == "__main__":
    unittest.main()
