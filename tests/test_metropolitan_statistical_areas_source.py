"""Draft guard tests for the current-vintage MSA adapter.

The synthetic TGRGDB27 national geodatabase shared with the Metdiv tests
carries the Core_Based_Statistical_Area layer; the MEMI=1 selection is the
staged draft. No dataset acceptance is implied.
"""
import json
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
import subprocess

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import shapely

from dagster_hifld.assets.metropolitan_statistical_areas import census_metropolitan_statistical_areas, msa_source_integrity_schedule
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.capture_common import seal_ready_candidate, verify_cached_candidate
from dagster_hifld.sources import metropolitan_statistical_areas as source
from test_census_substate_construction import snapshot
from test_metropolitan_divisions_source import VINTAGE, URL, ANNUAL, LISTING, build_metro_native_archive, _release


def _layer_table():
    geometry = shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])]))
    specs = ((1, None, "1", "M1"), (2, "120", "1", "M1"), (3, "121", "2", "M2"), (4, None, "2", "M2"))
    oids = [s[0] for s in specs]
    props = {
        "OBJECTID": oids,
        "CSAFP": [s[1] for s in specs],
        "CBSAFP": [f"{oid:05d}" for oid in oids],
        "GEOID": [f"{oid:05d}" for oid in oids],
        "GEOIDFQ": [f"310M700US{oid:05d}" for oid in oids],
        "NAME": [f"CBSA {oid}" for oid in oids],
        "NAMELSAD": [f"Area {oid}" for oid in oids],
        "LSAD": [s[3] for s in specs],
        "MEMI": [s[2] for s in specs],
        "MTFCC": ["G3110"] * 4,
        "ALAND": [100.0 + oid for oid in oids],
        "AWATER": [0.0] * 4,
        "INTPTLAT": ["+33.6457373"] * 4,
        "INTPTLON": ["-84.2948951"] * 4,
        "SHAPE": [geometry] * 4,
    }
    schema = pa.schema([pa.field(name, typ) for name, typ in zip(source.FIELDS, source.TYPES)])
    return pa.Table.from_pydict(props, schema=schema)


def _selection_table():
    layer = _layer_table()
    return layer.filter(pc.equal(layer["MEMI"], "1"))


class MetropolitanStatisticalAreasSourceTests(unittest.TestCase):
    def setUp(self):
        self.layer_table = _layer_table()
        self.selection_table = _selection_table()
        self.metadata = {"geometry_type": "MultiPolygon", "crs": "EPSG:4269", "fid_column": "OBJECTID"}
        # Fixture-scale counts; the lifecycle tests patch the same ranges.
        self._patches = [
            patch.object(source, "LAYER_COUNT_RANGE", (3, 5)),
            patch.object(source, "METROPOLITAN_RANGE", (1, 3)),
            patch.object(source, "MICROPOLITAN_RANGE", (1, 3)),
        ]
        for p in self._patches:
            p.start()
            self.addCleanup(p.stop)

    def receipt(self, body=b"x"):
        release = _release()
        return {"url": release.source_url, "final_url": release.source_url, "status": 200,
                "bytes": len(body), "sha256": source.digest(body), "complete_body": True,
                "observed_at": "2026-11-14T00:00:00Z"}

    def test_registered_source_and_refresh_stopped(self):
        self.assertEqual(census_metropolitan_statistical_areas.key.path, ["ingest", source.SLUG, source.SLUG])
        self.assertIn(census_metropolitan_statistical_areas, hifld_ingest_assets)
        self.assertIn(census_metropolitan_statistical_areas.key, {key for asset in defs.assets for key in asset.keys})
        self.assertEqual(msa_source_integrity_schedule.default_status.value, "STOPPED")
        self.assertIn(msa_source_integrity_schedule.name, {schedule.name for schedule in defs.schedules})

    def test_local_staging_only_and_strict_json(self):
        for storage in (PublishedStorageResource(local_dir="/tmp/never-msa"), StagingStorageResource(bucket="never")):
            with patch.object(source, "acquire") as acquire, self.assertRaises(source.MsaQuarantineError):
                source.stage_msa_candidate(storage)
            acquire.assert_not_called()
        for body in (b'{"x":1,"x":2}', b'{"error":{}}', b"[]", b"null"):
            with self.assertRaises(ValueError):
                source.strict_json(body)

    def test_complete_layer_schema_distribution_and_ids_validate(self):
        source.validate_layer(self.layer_table, self.metadata)
        variants = [
            self.layer_table.drop(["MEMI"]),
            self.layer_table.append_column("unexpected", pa.array([1] * 4)),
            self.layer_table.set_column(10, "ALAND", pa.array([1] * 4)),
            self.layer_table.set_column(0, "OBJECTID", pa.array([1] * 4)),
        ]
        for candidate in variants:
            with self.subTest(schema=candidate.schema), self.assertRaises(source.MsaQuarantineError):
                source.validate_layer(candidate, self.metadata)
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_layer(self.layer_table, {**self.metadata, "crs": "EPSG:4326"})
        out_of_range = self.layer_table.filter(pa.compute.equal(self.layer_table["MEMI"], "1"))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_layer(out_of_range, self.metadata)
        duplicate_geoid = self.layer_table.set_column(3, "GEOID",
            pa.array(["00002", "00002", "00003", "00004"], type=pa.string()))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_layer(duplicate_geoid, self.metadata)
        changed_lsad = self.layer_table.set_column(7, "LSAD",
            pa.array(["M0", "M1", "M2", "M2"], type=pa.string()))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_layer(changed_lsad, self.metadata)
        unknown_memi = self.layer_table.set_column(8, "MEMI",
            pa.array(["1", "1", "3", "2"], type=pa.string()))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_layer(unknown_memi, self.metadata)

    def test_selection_filters_metropolitan_rows(self):
        selection = source.select_metropolitan_rows(self.layer_table)
        source.validate_selection(selection)
        self.assertEqual(sorted(selection["OBJECTID"].to_pylist()), [1, 2])
        self.assertEqual(set(selection["MEMI"].to_pylist()), {"1"})
        self.assertEqual(selection.num_rows, 2)

    def test_selection_schema_ids_values_and_geometry_validate(self):
        source.validate_selection(self.selection_table)
        variants = [
            self.selection_table.slice(0, 0),
            self.selection_table.drop(["OBJECTID"]),
            self.selection_table.append_column("unexpected", pa.array([1] * 2)),
            self.selection_table.set_column(10, "ALAND", pa.array([1] * 2)),
            self.selection_table.set_column(0, "OBJECTID", pa.array([1] * 2)),
            self.selection_table.set_column(14, "SHAPE", pa.array([None] * 2, type=pa.binary())),
        ]
        for candidate in variants:
            with self.subTest(schema=candidate.schema), self.assertRaises(source.MsaQuarantineError):
                source.validate_selection(candidate)
        changed = self.selection_table.set_column(3, "GEOID", pa.array(["bad"] * 2))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_selection(changed)

    def test_nullable_field_population_is_range_checked(self):
        populated = self.selection_table.set_column(1, "CSAFP", pa.array(["120"] * 2, type=pa.string()))
        source.validate_selection(populated)
        for candidate in (
            self.selection_table.set_column(1, "CSAFP", pa.array([None] * 2, type=pa.string())),
            self.selection_table.set_column(2, "CBSAFP", pa.array([None] * 2, type=pa.string())),
        ):
            with self.subTest(column=candidate.column_names[1]), self.assertRaises(source.MsaQuarantineError):
                source.validate_selection(candidate)

    def test_area_type_and_identifier_construction_drift_fail(self):
        for name, column, value in (("MEMI", 8, "2"), ("LSAD", 7, "M2"), ("MTFCC", 9, "G3010")):
            values = list(self.selection_table.column(column).to_pylist())
            values[0] = value
            with self.subTest(field=name), self.assertRaises(source.MsaQuarantineError):
                source.validate_selection(self.selection_table.set_column(column, name, pa.array(values, type=pa.string())))
        mismatched = self.selection_table.set_column(2, "CBSAFP", pa.array(["00000"] * 2, type=pa.string()))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_selection(mismatched)
        wrong_fq = self.selection_table.set_column(4, "GEOIDFQ", pa.array(["314M700US11640"] * 2, type=pa.string()))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_selection(wrong_fq)

    def test_missing_renamed_ids_and_geometry_coverage_drift_fail(self):
        for name in ("OBJECTID", "GEOID"):
            with self.subTest(field=name), self.assertRaises(source.MsaQuarantineError):
                source.validate_selection(self.selection_table.drop([name]))
        invalid = shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])]))
        bad_geometry = self.selection_table.set_column(14, "SHAPE", pa.array([invalid] * 2, type=pa.binary()))
        with self.assertRaises(source.MsaQuarantineError):
            source.validate_selection(bad_geometry)

    def test_provenance_preflight_all_invalid_receipts_no_candidate(self):
        mutations = ({"observed_at": "not-a-date"}, {"url": "https://example.com/a"},
                     {"final_url": "https://example.com/b"}, {"status": True}, {"status": 403},
                     {"bytes": True}, {"sha256": "0" * 64}, {"complete_body": False})
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            protected = Path(tmp) / "prior-fixture"
            protected.write_bytes(b"keep")
            for mutation in mutations:
                receipt = self.receipt()
                receipt.update(mutation)
                with patch.object(source, "acquire", return_value=(b"x", receipt, _release())), \
                     patch.object(source, "processing_identity",
                                  return_value={"code_commit": "fixture", "code_identity_scheme": "fixture",
                                                "working_tree_status": "clean", "files": {}}), \
                     self.assertRaises(ValueError):
                    source.stage_msa_candidate(storage)
                self.assertFalse((Path(tmp) / source.SLUG).exists())
                self.assertEqual(protected.read_bytes(), b"keep")
            self.assertEqual(len(list((Path(tmp) / "_quarantine").rglob("reason.json"))), len(mutations))

    def test_manifest_uses_resolved_vintage_and_discovery_evidence(self):
        release = _release(VINTAGE)
        body = b"current official archive"
        receipt = self.receipt(body)
        identity = {"code_commit": "fixture", "code_identity_scheme": "fixture", "working_tree_status": "clean"}
        manifest = source.manifest_for(body, receipt, identity, release)
        self.assertEqual(manifest["source_url"], release.source_url)
        self.assertEqual(manifest["source_release"], release.as_metadata())
        self.assertIn(str(VINTAGE), manifest["title"])
        self.assertIn(str(VINTAGE), manifest["inputs"][0]["attribution"])
        old = {**receipt, "url": "https://www2.census.gov/geo/tiger/TIGER2024/tl_2024_us_nationgeo.zip"}
        with self.assertRaises(source.MsaQuarantineError):
            source.manifest_for(body, old, identity, release)

    def test_acquire_replay_accepts_current_release_and_rejects_old_url(self):
        release = _release()
        body = b"archive bytes"
        receipt = self.receipt(body)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "staging"))
            archive = Path(tmp) / "archive.zip"
            archive.write_bytes(body)
            receipt_path = Path(tmp) / "receipt.json"
            receipt_path.write_text(json.dumps(receipt))
            captured, captured_receipt, resolved = source.acquire(storage, archive, receipt_path)
            self.assertEqual(captured, body)
            self.assertEqual(captured_receipt, receipt)
            self.assertEqual(resolved.vintage, VINTAGE)
            old_receipt = {**receipt, "url": "https://www2.census.gov/geo/tiger/TIGER2024/tl_2024_us_nationgeo.zip"}
            receipt_path.write_text(json.dumps(old_receipt))
            with self.assertRaisesRegex(source.MsaQuarantineError, "source_url_not_current_tgrgdb"):
                source.acquire(storage, archive, receipt_path)

    def test_release_from_receipt_rejects_wrong_product(self):
        with self.assertRaisesRegex(ValueError, "source_url_not_current_tgrgdb"):
            census_latest.geodatabase_release_from_receipt(
                {"url": "https://www2.census.gov/geo/tiger/TGRGDB27/tlgdb_2027_us_addr.gdb.zip"},
                "nationgeo")

    def test_native_definition_rejects_oid_width_or_field_drift(self):
        fields = "".join(
            f"<GPFieldInfoEx><Name>{name}</Name><FieldType>{kind}</FieldType><Length>{length}</Length>"
            f"<IsNullable>{nullable}</IsNullable>{required}</GPFieldInfoEx>"
            for name, kind, length, nullable, required in [
                ("CSAFP", "esriFieldTypeString", 3, "true", ""),
                ("CBSAFP", "esriFieldTypeString", 5, "true", ""),
                ("GEOID", "esriFieldTypeString", 5, "true", ""),
                ("GEOIDFQ", "esriFieldTypeString", 14, "true", ""),
                ("NAME", "esriFieldTypeString", 100, "true", ""),
                ("NAMELSAD", "esriFieldTypeString", 100, "true", ""),
                ("LSAD", "esriFieldTypeString", 2, "true", ""),
                ("MEMI", "esriFieldTypeString", 1, "true", ""),
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
            declaration = source.read_native_definition(Path("/synthetic-unit-only.zip"), _release())
        self.assertEqual(declaration["fields"]["OBJECTID"]["Length"], "4")
        self.assertEqual(declaration["fields"]["MEMI"]["Length"], "1")
        self.assertEqual(source.TYPES[0], pa.int64())
        changed = xml.replace("<Length>4</Length>", "<Length>8</Length>")
        with patch.object(source.pyogrio, "read_arrow", return_value=({}, pa.table({"FIELD_1": [changed]}))), \
             self.assertRaises(ValueError):
            source.read_native_definition(Path("/synthetic-unit-only.zip"), _release())
        changed_memi = xml.replace("<Name>MEMI</Name><FieldType>esriFieldTypeString</FieldType><Length>1</Length>",
                                   "<Name>MEMI</Name><FieldType>esriFieldTypeString</FieldType><Length>2</Length>")
        with patch.object(source.pyogrio, "read_arrow", return_value=({}, pa.table({"FIELD_1": [changed_memi]}))), \
             self.assertRaises(ValueError):
            source.read_native_definition(Path("/synthetic-unit-only.zip"), _release())

    def test_identity_adapter_uses_shared_scheme_and_records_gdal(self):
        identity = {"code_identity_scheme": "sha256-packaged-dagster-hifld-v1", "working_tree_status": "unknown",
                    "code_commit": "packaged-sha256:" + "0" * 64, "files": {}, "installed_distributions": [],
                    "python": "3.12", "pyarrow": "23", "proj": "9", "geos": "3"}
        with patch.object(source, "package_processing_identity", return_value=identity):
            actual = source.processing_identity()
        self.assertEqual(actual["code_identity_scheme"], "sha256-packaged-dagster-hifld-v1")
        self.assertEqual(actual["gdal"], source.pyogrio.__gdal_version_string__)

    def test_processing_identity_is_supported_by_the_real_construction_validator(self):
        from dagster_hifld.construction_provenance import validate_construction_code_identity
        identity = source.processing_identity()
        self.assertIn(identity.get("code_identity_scheme"),
                      ("git-commit-blobs-v1", "sha256-packaged-dagster-hifld-v1"))
        validate_construction_code_identity(Path(__file__).parents[1], identity["code_commit"],
                                            identity["files"], identity=identity)

    def test_cached_outputs_are_rederived_from_original_native_rows(self):
        expected = source.expected_parquet_table(self.selection_table)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "geopackage").mkdir()
            (root / "candidate").mkdir()
            gpkg = root / "geopackage" / f"{source.SLUG}.gpkg"
            parquet = root / "candidate" / "typed-source.parquet"
            source.write_native_gpkg(self.selection_table, gpkg)
            source.pq.write_table(expected, parquet)
            source.reconcile_cached_native_outputs(root, self.selection_table)
            altered = self.selection_table.set_column(10, "ALAND", pa.array([999.0] * 2))
            source.pq.write_table(source.expected_parquet_table(altered), parquet)
            with self.assertRaises(source.MsaQuarantineError):
                source.reconcile_cached_native_outputs(root, self.selection_table)

    def test_shared_cache_verifier_accepts_catalog_metadata_but_rejects_unknown_objects(self):
        from dagster_hifld.catalog import summarize_staged_catalog
        from dagster_hifld.source_manifest import load_resolved_source_manifest

        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(local_dir=tmp)
            version = "v152-test"
            prefix = f"{source.SLUG}/{source.SLUG}/{version}"
            fingerprint = "a" * 64
            state_key = f"_construction-state/{source.SLUG}/{version}.json"
            identity = {"code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                        "files": {}}
            manifest_body = b"{}\n"
            scratch = Path(tmp) / "scratch.gpkg"
            source.write_native_gpkg(self.selection_table, scratch)
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
            with patch("dagster_hifld.sources.capture_common.validate_construction_code_identity"):
                result = verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})
            self.assertTrue(result["reused"])
            storage.write_key(f"{prefix}/unexpected/source.json", b"{}\n")
            with patch("dagster_hifld.sources.capture_common.validate_construction_code_identity"), \
                 self.assertRaisesRegex(ValueError, "cached_candidate_storage_roster_changed"):
                verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})


class MetropolitanStatisticalAreasNativeLifecycleTests(unittest.TestCase):
    @contextmanager
    def captured(self):
        with tempfile.TemporaryDirectory(prefix="msa-fixture-") as fixture_dir:
            body = build_metro_native_archive(fixture_dir)
            directory = census_latest.ROOT + f"TGRGDB{VINTAGE % 100:02d}/"
            listings = {census_latest.ROOT: ANNUAL, directory: LISTING}
            client_type = httpx.Client

            def respond(request):
                return httpx.Response(200, stream=httpx.ByteStream(
                    body if str(request.url) == URL else listings[str(request.url)]))

            with tempfile.TemporaryDirectory() as tmp, \
                 patch.object(source, "LAYER_COUNT_RANGE", (3, 5)), \
                 patch.object(source, "METROPOLITAN_RANGE", (1, 3)), \
                 patch.object(source, "MICROPOLITAN_RANGE", (1, 3)), \
                 patch.object(source.httpx, "Client", side_effect=lambda **kwargs: client_type(
                     transport=httpx.MockTransport(respond), **kwargs)):
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                yield storage, body, listings

    def test_real_capture_receipt_replays_without_network_or_evidence_loss(self):
        with self.captured() as (storage, body, listings):
            first = source.stage_msa_candidate(storage)
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], 2)
            target = Path(storage.local_dir) / source.SLUG / source.SLUG / first["version"]
            before = snapshot(target)
            receipt_path = target / "raw/retrieval.json"
            receipt = source.strict_json(receipt_path.read_bytes())
            archive = Path(storage.local_dir) / "_raw-captures" / source.SLUG / source.digest(body) / "original.gdb.zip"
            with patch.object(source.httpx, "Client", side_effect=AssertionError("replay fetched HTTP")):
                replay = source.stage_msa_candidate(storage, archive_path=archive, receipt_path=receipt_path)
                self.assertTrue(replay["reused"])
                cold_storage = StagingStorageResource(local_dir=str(Path(storage.local_dir).parent / "cold"))
                cold = source.stage_msa_candidate(cold_storage, archive_path=archive, receipt_path=receipt_path)
            self.assertFalse(cold["reused"])
            self.assertEqual(first["version"], cold["version"])
            self.assertEqual(snapshot(target), before)
            self.assertEqual(snapshot(Path(cold_storage.local_dir) / source.SLUG / source.SLUG / cold["version"]), before)
            directory = census_latest.ROOT + f"TGRGDB{VINTAGE % 100:02d}/"
            release = census_latest.geodatabase_release_from_receipt(receipt, "nationgeo")
            self.assertEqual(release.vintage, VINTAGE)
            self.assertEqual(release.listing_sha256, source.digest(listings[directory]))
            self.assertEqual(release.annual_listing_sha256, source.digest(listings[census_latest.ROOT]))
            self.assertEqual(receipt["source_release"], release.as_receipt_evidence())
            manifest = source.strict_json((target / "metadata/source_manifest.json").read_bytes())
            self.assertEqual(manifest["source_release"], release.as_metadata())
            selection = source.strict_json((target / "metadata/msa_source_audit.json").read_bytes())["layer_selection"]
            self.assertEqual(selection, {"field": "MEMI", "equals": "1", "layer_rows": 4,
                                         "selection_rows": 2, "micropolitan_rows": 2})
            self.assertIn(str(VINTAGE), manifest["title"])
            self.assertFalse((Path(storage.local_dir) / "_quarantine").exists())

    def test_unrelated_listing_changes_keep_original_candidate_evidence(self):
        with self.captured() as (storage, body, listings):
            first = source.stage_msa_candidate(storage)
            target = Path(storage.local_dir) / source.SLUG / source.SLUG / first["version"]
            before = snapshot(target)
            directory = census_latest.ROOT + f"TGRGDB{VINTAGE % 100:02d}/"
            original_hash = source.digest(listings[directory])
            listings[directory] += b"\n<!-- unrelated directory update -->"
            listings[census_latest.ROOT] += b"\n<!-- unrelated annual update -->"
            second = source.stage_msa_candidate(storage)
            self.assertEqual(second["version"], first["version"])
            self.assertTrue(second["reused"])
            self.assertEqual(snapshot(target), before)
            depot = Path(storage.local_dir) / "_raw-captures" / source.SLUG / source.digest(body)
            receipts = [source.strict_json(path.read_bytes()) for path in depot.glob("*.receipt-*.json")]
            self.assertEqual({item["source_release"]["listing_sha256"] for item in receipts},
                             {original_hash, source.digest(listings[directory])})
            self.assertFalse((Path(storage.local_dir) / "_quarantine").exists())

    def test_runtime_change_creates_distinct_version_without_mutating_history(self):
        with self.captured() as (storage, body, listings):
            first = source.stage_msa_candidate(storage)
            before = snapshot(Path(storage.local_dir))
            identity = source.processing_identity()
            with patch.object(source.pyogrio, "__gdal_version_string__", "different-build"):
                changed = source.processing_identity()
                self.assertEqual(identity["code_commit"], changed["code_commit"])
                self.assertNotEqual(identity, changed)
                second = source.stage_msa_candidate(storage)
                self.assertNotEqual(first["version"], second["version"])
                self.assertFalse(second["reused"])
                self.assertTrue(source.stage_msa_candidate(storage)["reused"])
            after = snapshot(Path(storage.local_dir))
            self.assertEqual({name: after[name] for name in before}, before)
            self.assertTrue(source.stage_msa_candidate(storage)["reused"])
            self.assertFalse((Path(storage.local_dir) / "_quarantine").exists())

    def test_repinned_cached_discovery_claim_is_rejected(self):
        with self.captured() as (storage, body, listings):
            first = source.stage_msa_candidate(storage)
            target = Path(storage.local_dir) / source.SLUG / source.SLUG / first["version"]
            path = target / "metadata/source_manifest.json"
            manifest = source.strict_json(path.read_bytes())
            manifest["source_release"]["source_listing_sha256"] = "f" * 64
            path.write_bytes(source.encode(manifest))
            ready_path = target / "metadata/msa_candidate_ready.json"
            ready = source.strict_json(ready_path.read_bytes())
            ready["objects"]["metadata/source_manifest.json"] = source.sha256_file(path)
            ready_path.write_bytes(source.encode(ready))
            with self.assertRaisesRegex(ValueError, "cached_provenance_or_processing_identity_changed"):
                source.stage_msa_candidate(storage)


if __name__ == "__main__":
    unittest.main()
