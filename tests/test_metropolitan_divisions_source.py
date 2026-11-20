"""Draft guard tests for the current-vintage Metdiv adapter.

The synthetic TGRGDB27 national geodatabase is built in-test with the same
native writer the producer uses (OpenFileGDB via GDAL) so the discovery,
capture, replay and cache paths run on a fixture vintage distinct from any
pinned archive. No dataset acceptance is implied.
"""
import io
import json
import re
import tempfile
import unittest
import zipfile
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
import subprocess

import httpx
import pyarrow as pa
import pyogrio
import shapely

from dagster_hifld.assets.metropolitan_divisions import census_metropolitan_divisions, metdiv_source_integrity_schedule
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.definitions import defs
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources.capture_common import seal_ready_candidate, verify_cached_candidate
from dagster_hifld.sources import metropolitan_divisions as source
from test_census_substate_construction import snapshot

VINTAGE = 2027
GDB_NAME = f'tlgdb_{VINTAGE}_us_nationgeo.gdb'
URL = census_latest.ROOT + f'TGRGDB{VINTAGE % 100:02d}/' + GDB_NAME + '.zip'
ANNUAL = f'<a href="TGRGDB{VINTAGE % 100:02d}/">{VINTAGE % 100:02d}</a>'.encode()
LISTING = f'<a href="{GDB_NAME}.zip">archive</a>'.encode()
# The GDB writer omits the producer OID Editable declaration and pads string
# widths to the driver default; the same-length patch restores the producer
# declarations the adapter must retain.
ET.register_namespace('typens', 'http://www.esri.com/schemas/ArcGIS/10.3')
ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
ET.register_namespace('xs', 'http://www.w3.org/2001/XMLSchema')
ET.register_namespace('', '')


def _metdiv_rows():
    rows = []
    for i, csafp in ((1, None), (2, '120')):
        rows.append([csafp, '12060', f'1205{i + 3}', f'120601205{i + 3}', f'314M700US120601205{i + 3}',
                     f'Metro div {i}', f'Metro div {i}', 'M2', 'G3110', 100.0 + i, 0.0,
                     '+33.6457373', '-84.2948951'])
    return rows


def _msa_rows():
    rows = []
    for oid, csafp, memi, lsad in ((1, None, '1', 'M1'), (2, '120', '1', 'M1'), (3, '121', '2', 'M2'), (4, None, '2', 'M2')):
        rows.append([csafp, f'{oid:05d}', f'{oid:05d}', f'310M700US{oid:05d}', f'CBSA {oid}', f'CBSA {oid}',
                     lsad, memi, 'G3110', 100.0 + oid, 0.0, '+33.6457373', '-84.2948951'])
    return rows


def _write_layer(gdb, name, fields, types, rows):
    geometry = shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 0), (1, 1), (0, 0)])]))
    schema = pa.schema([pa.field(n, t) for n, t in zip(fields, types)] + [pa.field('SHAPE', pa.binary())])
    data = {n: [r[i] for r in rows] for i, n in enumerate(fields)}
    data['SHAPE'] = [geometry] * len(rows)
    pyogrio.write_arrow(pa.Table.from_pydict(data, schema=schema), gdb, layer=name, driver='OpenFileGDB',
                        geometry_name='SHAPE', geometry_type='MultiPolygon', crs='EPSG:4269',
                        layer_options={'GEOMETRY_NAME': 'SHAPE'})


def _patch_definition(table_file, layer, widths):
    body = table_file.read_bytes()
    start = None
    for match in re.finditer(re.escape(b'<?xml'), body):
        if (b'DEFeatureClassInfo' in body[match.start():match.start() + 400]
                and layer.encode().upper() in body[max(0, match.start() - 200):match.start()]):
            start = match.start()
            break
    if start is None:
        return
    end = body.find(b'</typens:DEFeatureClassInfo>', start) + len(b'</typens:DEFeatureClassInfo>')
    root = ET.fromstring(body[start:end].decode('utf-8'))
    fields = {f.findtext('Name'): f for f in root.findall('./GPFieldInfoExs/GPFieldInfoEx')}
    oid = fields['OBJECTID']
    if oid.find('Editable') is None:
        element = ET.SubElement(oid, 'Editable')
        element.text = 'false'
    for name, width in widths.items():
        fields[name].find('Length').text = width
    new_xml = ET.tostring(root, encoding='unicode')
    old_len = end - start
    if len(new_xml) <= old_len:
        pad = ' ' * (old_len - len(new_xml))
        new_xml = new_xml.replace('<GPFieldInfoExs', f'<GPFieldInfoExs{pad}', 1)
    assert len(new_xml) == old_len, (len(new_xml), old_len)
    table_file.write_bytes(body[:start] + new_xml.encode('utf-8') + body[end:])


MSA_WIDTHS = {'CSAFP': '3', 'CBSAFP': '5', 'GEOID': '5', 'GEOIDFQ': '14', 'NAME': '100',
              'NAMELSAD': '100', 'LSAD': '2', 'MEMI': '1', 'MTFCC': '5', 'INTPTLAT': '11', 'INTPTLON': '12'}


def build_metro_native_archive(fixture_dir):
    """One synthetic TGRGDB27 nationgeo GDB carrying both metropolitan layers."""
    gdb = Path(fixture_dir) / GDB_NAME
    _write_layer(gdb, 'Metropolitan_Division',
                 ('CSAFP', 'CBSAFP', 'METDIVFP', 'GEOID', 'GEOIDFQ', 'NAME', 'NAMELSAD', 'LSAD', 'MTFCC',
                  'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON'),
                 (pa.string(),) * 9 + (pa.float64(), pa.float64()) + (pa.string(), pa.string()),
                 _metdiv_rows())
    _write_layer(gdb, 'Core_Based_Statistical_Area',
                 ('CSAFP', 'CBSAFP', 'GEOID', 'GEOIDFQ', 'NAME', 'NAMELSAD', 'LSAD', 'MEMI', 'MTFCC',
                  'ALAND', 'AWATER', 'INTPTLAT', 'INTPTLON'),
                 (pa.string(),) * 9 + (pa.float64(), pa.float64()) + (pa.string(), pa.string()),
                 _msa_rows())
    for path in sorted(gdb.iterdir()):
        if not path.name.endswith('.gdbtable'):
            continue
        _patch_definition(path, 'Metropolitan_Division', {})
        _patch_definition(path, 'Core_Based_Statistical_Area', MSA_WIDTHS)
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, 'w') as archive:
        for path in sorted(gdb.iterdir()):
            archive.writestr(GDB_NAME + '/' + path.name, path.read_bytes())
    return sink.getvalue()


def _table(rows=25):
    geometry = shapely.to_wkb(shapely.MultiPolygon([shapely.Polygon([(0, 0), (1, 0), (0, 1), (0, 0)])]))
    props = {
        "OBJECTID": list(range(1, rows + 1)),
        "CSAFP": [None if i < 2 else "120" for i in range(rows)],
        "CBSAFP": ["12060"] * rows,
        "METDIVFP": [f"{12054 + i}" for i in range(rows)],
        "GEOID": [str(1206012054 + i) for i in range(rows)],
        "GEOIDFQ": [f"314M700US{1206012054 + i}" for i in range(rows)],
        "NAME": [f"CBSA {i}" for i in range(rows)],
        "NAMELSAD": [f"Division {i}" for i in range(rows)],
        "LSAD": ["M2"] * rows,
        "MTFCC": ["G3110"] * rows,
        "ALAND": [float(i + 1) for i in range(rows)],
        "AWATER": [float(i) for i in range(rows)],
        "INTPTLAT": ["+33.6457373"] * rows,
        "INTPTLON": ["-84.2948951"] * rows,
        "SHAPE": [geometry] * rows,
    }
    schema = pa.schema([pa.field(name, typ) for name, typ in zip(source.FIELDS, source.TYPES)])
    return pa.Table.from_pydict(props, schema=schema)


def _release(vintage=VINTAGE):
    directory = census_latest.ROOT + f'TGRGDB{vintage % 100:02d}/'
    source_file = f'tlgdb_{vintage}_us_nationgeo.gdb.zip'
    return census_latest.GeodatabaseRelease(
        vintage=vintage, product='nationgeo', source_file=source_file,
        source_url=directory + source_file, directory_url=directory,
        listing_sha256='', listing_bytes=0,
        latest_checked_at='2026-11-15T00:00:00+00:00', annual_listing_sha256='')


class MetropolitanDivisionsSourceTests(unittest.TestCase):
    def setUp(self):
        self.rows = 25
        self.table = _table(self.rows)
        self.metadata = {"geometry_type": "MultiPolygon", "crs": "EPSG:4269", "fid_column": "OBJECTID"}

    def receipt(self, body=b"x"):
        release = _release()
        return {"url": release.source_url, "final_url": release.source_url, "status": 200,
                "bytes": len(body), "sha256": source.digest(body), "complete_body": True,
                "observed_at": "2026-10-19T00:00:00Z"}

    def test_registered_source_and_refresh_stopped(self):
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
            self.table.slice(0, source.FEATURE_COUNT_RANGE[0] - 1),
            self.table.drop(["OBJECTID"]),
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
        changed = self.table.set_column(4, "GEOID", pa.array(["bad"] * self.rows))
        with self.assertRaises(source.MetdivQuarantineError):
            source.validate_table(changed, self.metadata)

    def test_nullable_field_population_is_range_checked(self):
        populated = self.table.set_column(1, "CSAFP", pa.array(["120"] * self.rows, type=pa.string()))
        source.validate_table(populated, self.metadata)
        for candidate in (
            self.table.set_column(1, "CSAFP", pa.array([None] * self.rows, type=pa.string())),
            self.table.set_column(2, "CBSAFP", pa.array([None] * self.rows, type=pa.string())),
        ):
            with self.subTest(column=candidate.column_names[1]), self.assertRaises(source.MetdivQuarantineError):
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
                    source.stage_metdiv_candidate(storage)
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
        with self.assertRaises(source.MetdivQuarantineError):
            source.manifest_for(body, old, identity, release)
        with self.assertRaises(source.MetdivQuarantineError):
            source.manifest_for(body, {**receipt, "final_url": release.source_url + "?changed=1"}, identity, release)

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
            with self.assertRaisesRegex(source.MetdivQuarantineError, "source_url_not_current_tgrgdb"):
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
            declaration = source.read_native_definition(Path("/synthetic-unit-only.zip"), _release())
        self.assertEqual(declaration["fields"]["OBJECTID"]["Length"], "4")
        self.assertEqual(source.TYPES[0], pa.int64())
        changed = xml.replace("<Length>4</Length>", "<Length>8</Length>")
        with patch.object(source.pyogrio, "read_arrow", return_value=({}, pa.table({"FIELD_1": [changed]}))), \
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
            identity = {"code_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                        "files": {}}
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
            with patch("dagster_hifld.sources.capture_common.validate_construction_code_identity"):
                result = verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})
            self.assertTrue(result["reused"])
            storage.write_key(f"{prefix}/unexpected/source.json", b"{}\n")
            with patch("dagster_hifld.sources.capture_common.validate_construction_code_identity"), \
                 self.assertRaisesRegex(ValueError, "cached_candidate_storage_roster_changed"):
                verify_cached_candidate(storage, Path(__file__).parents[1], state_key, prefix, version,
                    fingerprint, identity, "b" * 64, "c" * 64, objects, {})


class MetropolitanDivisionsNativeLifecycleTests(unittest.TestCase):
    @contextmanager
    def captured(self):
        with tempfile.TemporaryDirectory(prefix="metdiv-fixture-") as fixture_dir:
            body = build_metro_native_archive(fixture_dir)
            directory = census_latest.ROOT + f"TGRGDB{VINTAGE % 100:02d}/"
            listings = {census_latest.ROOT: ANNUAL, directory: LISTING}
            client_type = httpx.Client

            def respond(request):
                return httpx.Response(200, stream=httpx.ByteStream(
                    body if str(request.url) == URL else listings[str(request.url)]))

            with tempfile.TemporaryDirectory() as tmp, patch.object(source, "FEATURE_COUNT_RANGE", (2, 3)), \
                 patch.object(source.httpx, "Client", side_effect=lambda **kwargs: client_type(
                     transport=httpx.MockTransport(respond), **kwargs)):
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                yield storage, body, listings

    def test_real_capture_receipt_replays_without_network_or_evidence_loss(self):
        with self.captured() as (storage, body, listings):
            first = source.stage_metdiv_candidate(storage)
            self.assertFalse(first["reused"])
            self.assertEqual(first["feature_count"], 2)
            target = Path(storage.local_dir) / source.SLUG / source.SLUG / first["version"]
            before = snapshot(target)
            receipt_path = target / "raw/retrieval.json"
            receipt = source.strict_json(receipt_path.read_bytes())
            archive = Path(storage.local_dir) / "_raw-captures" / source.SLUG / source.digest(body) / "original.gdb.zip"
            with patch.object(source.httpx, "Client", side_effect=AssertionError("replay fetched HTTP")):
                replay = source.stage_metdiv_candidate(storage, archive_path=archive, receipt_path=receipt_path)
                self.assertTrue(replay["reused"])
                cold_storage = StagingStorageResource(local_dir=str(Path(storage.local_dir).parent / "cold"))
                cold = source.stage_metdiv_candidate(cold_storage, archive_path=archive, receipt_path=receipt_path)
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
            self.assertIn(str(VINTAGE), manifest["title"])
            self.assertFalse((Path(storage.local_dir) / "_quarantine").exists())

    def test_unrelated_listing_changes_keep_original_candidate_evidence(self):
        with self.captured() as (storage, body, listings):
            first = source.stage_metdiv_candidate(storage)
            target = Path(storage.local_dir) / source.SLUG / source.SLUG / first["version"]
            before = snapshot(target)
            directory = census_latest.ROOT + f"TGRGDB{VINTAGE % 100:02d}/"
            original_hash = source.digest(listings[directory])
            listings[directory] += b"\n<!-- unrelated directory update -->"
            listings[census_latest.ROOT] += b"\n<!-- unrelated annual update -->"
            second = source.stage_metdiv_candidate(storage)
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
            first = source.stage_metdiv_candidate(storage)
            before = snapshot(Path(storage.local_dir))
            identity = source.processing_identity()
            with patch.object(source.pyogrio, "__gdal_version_string__", "different-build"):
                changed = source.processing_identity()
                self.assertEqual(identity["code_commit"], changed["code_commit"])
                self.assertNotEqual(identity, changed)
                second = source.stage_metdiv_candidate(storage)
                self.assertNotEqual(first["version"], second["version"])
                self.assertFalse(second["reused"])
                self.assertTrue(source.stage_metdiv_candidate(storage)["reused"])
            after = snapshot(Path(storage.local_dir))
            self.assertEqual({name: after[name] for name in before}, before)
            self.assertTrue(source.stage_metdiv_candidate(storage)["reused"])
            self.assertFalse((Path(storage.local_dir) / "_quarantine").exists())

    def test_repinned_cached_discovery_claim_is_rejected(self):
        with self.captured() as (storage, body, listings):
            first = source.stage_metdiv_candidate(storage)
            target = Path(storage.local_dir) / source.SLUG / source.SLUG / first["version"]
            path = target / "metadata/source_manifest.json"
            manifest = source.strict_json(path.read_bytes())
            manifest["source_release"]["source_listing_sha256"] = "f" * 64
            path.write_bytes(source.encode(manifest))
            ready_path = target / "metadata/metdiv_candidate_ready.json"
            ready = source.strict_json(ready_path.read_bytes())
            ready["objects"]["metadata/source_manifest.json"] = source.sha256_file(path)
            ready_path.write_bytes(source.encode(ready))
            with self.assertRaisesRegex(ValueError, "cached_provenance_or_processing_identity_changed"):
                source.stage_metdiv_candidate(storage)


if __name__ == "__main__":
    unittest.main()
