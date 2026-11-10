"""Focused contract checks for the EIA natural gas processing plants source adapter."""
from __future__ import annotations

import hashlib
import io
import json
import os
import struct
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
from dagster import DefaultScheduleStatus

from dagster_hifld.sources import eia_natural_gas_processing_plants as source

FIXTURE_LAYER = "Fixture_ProcessingPlants_US_2017"
FIXTURE_PUBLICATION = "2017-01-01"
PRJ_TEXT = ('GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],'
            'PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]')


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


FIXTURE_FIELDS = [
    ("Plant_Name", "C", 32, 0), ("Owner", "C", 32, 0), ("Operator", "C", 32, 0),
    ("City", "C", 24, 0), ("State", "C", 2, 0), ("County", "C", 24, 0), ("ZipCode", "C", 10, 0),
    ("Period", "N", 10, 0), ("Cap_MMcfd", "F", 19, 11), ("Plant_Flow", "F", 19, 11),
    ("BTU_Conten", "F", 19, 11), ("Dry_Stor", "N", 10, 0), ("NGL_Stor", "N", 10, 0),
    ("Longitude", "F", 19, 11), ("Latitude", "F", 19, 11), ("Source", "C", 32, 0),
]

FIXTURE_ROWS = [
    {"Plant_Name": "Fixture Plant Alpha", "Owner": "Fixture Owner", "Operator": "Fixture Operator",
     "City": "Fixture City", "State": "TX", "County": "Fixture County", "ZipCode": "00000",
     "Period": 2017, "Cap_MMcfd": 1.5, "Plant_Flow": 1.25, "BTU_Conten": 1050.0,
     "Dry_Stor": 0, "NGL_Stor": 1, "Longitude": -95.5, "Latitude": 31.0, "Source": "FIXTURE SOURCE"},
    {"Plant_Name": "Fixture Plant Beta", "Owner": "", "Operator": "", "City": "",
     "State": "WY", "County": "Fixture County Two", "ZipCode": "82718",
     "Period": 2017, "Cap_MMcfd": 90.0, "Plant_Flow": 81.0, "BTU_Conten": 1274.0,
     "Dry_Stor": 0, "NGL_Stor": 1, "Longitude": -105.5, "Latitude": 41.0, "Source": "FIXTURE SOURCE"},
]


def _field_bytes(value, field):
    name, kind, size, _ = field
    if kind == "C":
        raw = b"" if value in (None, "") else str(value).encode("ascii")
        return raw.ljust(size, b" ")
    if kind == "N":
        raw = str(int(value)).encode("ascii")
    else:
        raw = ("%.11e" % float(value)).encode("ascii")
    return raw.rjust(size, b" ")


def _dbf_bytes(fields, rows):
    descriptors = b""
    for name, kind, size, scale in fields:
        descriptors += name.encode("ascii").ljust(11, b"\0") + kind.encode("ascii") + b"\0" * 4
        descriptors += struct.pack("<BB", size, scale) + b"\0" * 14
    rlen = 1 + sum(field[2] for field in fields)
    hlen = 32 + 32 * len(fields) + 1
    header = bytes([3, 120, 11, 10]) + struct.pack("<IHH", len(rows), hlen, rlen) + b"\0" * 20
    body = b""
    for row in rows:
        body += b"\x20" + b"".join(_field_bytes(row[field[0]], field) for field in fields)
    return header + descriptors + b"\x0d" + body + b"\x1a"


def _point_geometry_bytes(points):
    shp = bytearray(100 + 28 * len(points))
    shx = bytearray(100 + 8 * len(points))
    for buffer in (shp, shx):
        struct.pack_into(">i", buffer, 0, 9994)
    struct.pack_into(">i", shp, 24, (100 + 28 * len(points)) // 2)
    struct.pack_into(">i", shx, 24, (100 + 8 * len(points)) // 2)
    struct.pack_into("<ii", shp, 28, 1000, 1)
    struct.pack_into("<ii", shx, 28, 1000, 1)
    xs = [p[0] for p in points]; ys = [p[1] for p in points]
    struct.pack_into("<4d", shp, 36, min(xs), min(ys), max(xs), max(ys))
    struct.pack_into("<4d", shx, 36, min(xs), min(ys), max(xs), max(ys))
    for ordinal, (x, y) in enumerate(points):
        offset = 100 + ordinal * 28
        struct.pack_into(">ii", shp, offset, ordinal + 1, 10)
        struct.pack_into("<idd", shp, offset + 8, 1, x, y)
        struct.pack_into(">ii", shx, 100 + ordinal * 8, offset // 2, 10)
    return bytes(shp), bytes(shx)


def _fixture_zip(layer=FIXTURE_LAYER, fields=FIXTURE_FIELDS, rows=FIXTURE_ROWS, points=None):
    if points is None:
        points = [(row["Longitude"], row["Latitude"]) for row in rows]
    shp, shx = _point_geometry_bytes(points)
    members = {
        f"{layer}.cpg": b"UTF-8",
        f"{layer}.dbf": _dbf_bytes(fields, rows),
        f"{layer}.prj": PRJ_TEXT.encode("ascii"),
        f"{layer}.shp": shp,
        f"{layer}.shx": shx,
        f"{layer}.shp.xml": (b'<?xml version="1.0"?><metadata><dataIdInfo><idCitation><resTitle>Fixture</resTitle>'
                            b'<date><pubDate>' + (FIXTURE_PUBLICATION + "T00:00:00").encode("ascii") +
                            b'</pubDate></date></idCitation></dataIdInfo></metadata>'),
    }
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body in members.items():
            z.writestr(name, body)
    return sink.getvalue(), set(members)


def _fixture_pins(zip_body, members, layer=FIXTURE_LAYER, fields=FIXTURE_FIELDS, row_count=2):
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "shape"
        root.mkdir()
        with zipfile.ZipFile(io.BytesIO(zip_body)) as z:
            for name in members:
                (root / name).write_bytes(z.read(name))
        fids = source._fid_roster(root / f"{layer}.shp", root / f"{layer}.shx", row_count)
        frame = pyogrio.read_dataframe(root / f"{layer}.shp")
    ids, _attrs, idsha, recsha, geomsha, bounds = source._content_digests(
        frame, [f[0] for f in fields], fids)
    return dict(EXPECTED_LAYER=layer, EXPECTED_COUNT=len(ids), EXPECTED_FIELDS=list(fields),
                MEMBERS=set(members), ID_SHA=idsha, RECORD_SHA=recsha, GEOMETRY_SHA=geomsha,
                BOUNDS=bounds, EXPECTED_ZIP_BYTES=len(zip_body),
                EXPECTED_ZIP_SHA256=hashlib.sha256(zip_body).hexdigest(),
                SOURCE_GIS_PUBLICATION=FIXTURE_PUBLICATION)


def _fake_capture_get(bodies):
    def fake(_client, url, capture_root, name, *, max_bytes):
        body = bodies[name]
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-10T00:00:00+00:00",
                   "status": 200, "complete_body": True, "wire_bytes": len(body), "wire_sha256": digest,
                   "bytes": len(body), "sha256": digest, "content_encoding": None,
                   "content_length": str(len(body)), "error_code": None}
        (capture_root / f"{name}.body").write_bytes(body)
        (capture_root / f"{name}.wire").write_bytes(body)
        (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
        return body, receipt
    return fake


class EmptyClient:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class EiaNaturalGasProcessingPlantsTests(unittest.TestCase):
    def test_pinned_source_contract(self):
        self.assertEqual(source.SOURCE_URL, "https://www.eia.gov/maps/map_data/NaturalGas_ProcessingPlants_US_EIA.zip")
        self.assertEqual(source.SOURCE_RELEASE, "2017")
        self.assertEqual(source.SOURCE_PACKAGE_MODIFIED, "2020-01-10")
        self.assertEqual(source.SOURCE_GIS_PUBLICATION, "2020-01-13")
        self.assertEqual(source.IDENTIFIER_COLUMN, "source_record_number")
        self.assertEqual(source.EXPECTED_LAYER, "NaturalGas_ProcessingPlants_US_2017_v2")
        self.assertEqual(source.EXPECTED_ZIP_BYTES, 78776)
        self.assertEqual(source.EXPECTED_COUNT, 478)
        self.assertEqual(source.EXPECTED_FIELDS[0], ("Plant_Name", "C", 254, 0))
        self.assertEqual([f[0] for f in source.EXPECTED_FIELDS],
                         ["Plant_Name", "Owner", "Operator", "City", "State", "County", "ZipCode", "Period",
                          "Cap_MMcfd", "Plant_Flow", "BTU_Conten", "Dry_Stor", "NGL_Stor", "Longitude",
                          "Latitude", "Source"])
        self.assertEqual(len(source.EXPECTED_ZIP_SHA256), 64)
        # Pinned FID identity roster: source shapefile record numbers 1..478.
        self.assertEqual(source.ID_SHA, hashlib.sha256(_canonical(list(range(1, 479)))).hexdigest())
        self.assertEqual(len(source.MEMBERS), 8)
        self.assertIn(f"{source.EXPECTED_LAYER}.shp.xml", source.MEMBERS)

    def test_geometry_encoding_preserves_native_xy(self):
        self.assertEqual(source._point_wkb(1.5, -2.25).hex(), "0101000000000000000000f83f00000000000002c0")

    def test_modified_eia_package_is_rejected(self):
        zip_body, members = _fixture_zip()
        pins = _fixture_pins(zip_body, members)
        corrupt = bytearray(zip_body)
        corrupt[-1] ^= 0xFF
        with patch.multiple(source, **pins), tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "pinned_eia_snapshot_drift"):
                source._decode(bytes(corrupt), Path(tmp))

    def test_dbf_schema_drift_is_rejected(self):
        zip_body, members = _fixture_zip()
        drifted = [("XPlant_Name", "C", 32, 0), *FIXTURE_FIELDS[1:]]
        drifted_rows = [{("XPlant_Name" if k == "Plant_Name" else k): v for k, v in row.items()}
                       for row in FIXTURE_ROWS]
        drift_zip, _drift_members = _fixture_zip(fields=drifted, rows=drifted_rows)
        pins = _fixture_pins(zip_body, members)
        # The drifted package checksum is expected, so decoding reaches the
        # DBF schema comparison instead of the archive checksum guard.
        pins.update(EXPECTED_ZIP_BYTES=len(drift_zip),
                    EXPECTED_ZIP_SHA256=hashlib.sha256(drift_zip).hexdigest())
        with patch.multiple(source, **pins), tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_dbf_schema_drift"):
                source._decode(drift_zip, Path(tmp))

    def test_fid_roster_and_period_drift_are_rejected(self):
        zip_body, members = _fixture_zip()
        pins = _fixture_pins(zip_body, members)
        # Non-contiguous FID roster: shapefile record numbers start at 2.
        shp, shx = _point_geometry_bytes([(r["Longitude"], r["Latitude"]) for r in FIXTURE_ROWS])
        patched_shp = bytearray(shp)
        struct.pack_into(">i", patched_shp, 100, 2)
        struct.pack_into(">i", patched_shp, 128, 3)
        with zipfile.ZipFile(io.BytesIO(zip_body)) as fixture:
            drifted_members = {name: fixture.read(name) for name in members}
        drifted_members[f"{FIXTURE_LAYER}.shp"] = bytes(patched_shp)
        drifted_members[f"{FIXTURE_LAYER}.shx"] = shx
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w", zipfile.ZIP_DEFLATED) as z:
            for name, body in drifted_members.items():
                z.writestr(name, body)
        fid_zip = sink.getvalue()
        with patch.multiple(source, **{**pins, "EXPECTED_ZIP_BYTES": len(fid_zip),
                                       "EXPECTED_ZIP_SHA256": hashlib.sha256(fid_zip).hexdigest()}), \
                tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_fid_roster_drift"):
                source._decode(fid_zip, Path(tmp))
        # Period drift: one row outside the pinned 2017 survey period.
        rows = [dict(FIXTURE_ROWS[0]), dict(FIXTURE_ROWS[1])]
        rows[1]["Period"] = 2014
        period_zip, _period_members = _fixture_zip(rows=rows)
        with patch.multiple(source, **{**pins, "EXPECTED_ZIP_BYTES": len(period_zip),
                                       "EXPECTED_ZIP_SHA256": hashlib.sha256(period_zip).hexdigest()}), \
                tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_period_drift"):
                source._decode(period_zip, Path(tmp))

    def test_native_fields_and_values_preserved_in_parquet(self):
        zip_body, members = _fixture_zip()
        pins = _fixture_pins(zip_body, members)
        with patch.multiple(source, **pins):
            with tempfile.TemporaryDirectory() as tmp:
                frame, fids, audit = source._decode(zip_body, Path(tmp))
                body = source._parquet(frame, fids)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.column_names,
                         [source.IDENTIFIER_COLUMN] + [f[0] for f in FIXTURE_FIELDS] + ["geometry"])
        # The validated native record numbers survive as an explicit identifier
        # column rather than an implicit row position.
        self.assertEqual(table.column(source.IDENTIFIER_COLUMN).to_pylist(), [1, 2])
        self.assertEqual(table.column("Plant_Name").to_pylist(),
                         ["Fixture Plant Alpha", "Fixture Plant Beta"])
        self.assertEqual(table.column("State").to_pylist(), ["TX", "WY"])
        self.assertEqual(table.column("ZipCode").to_pylist(), ["00000", "82718"])
        self.assertEqual(table.column("Period").to_pylist(), [2017, 2017])
        self.assertEqual(table.column("Cap_MMcfd").to_pylist(), [1.5, 90.0])
        self.assertEqual(table.column("NGL_Stor").to_pylist(), [1, 1])
        self.assertEqual(table.column("Owner").to_pylist()[1], None)
        self.assertEqual(audit["source_feature_count"], 2)
        self.assertEqual(audit["source_geometry_null_count"], 0)
        self.assertEqual(audit["source_id_min"], 1)
        self.assertEqual(audit["source_id_max"], 2)
        # Producer-declared GIS publication date is recorded separately from the
        # survey period, survey data release and package modification.
        self.assertEqual(audit["source_gis_publication_date"], FIXTURE_PUBLICATION)
        self.assertEqual(audit["source_package_modified"], "2020-01-10")
        self.assertEqual(audit["source_period"], "2017")
        # Completeness is scoped to the pinned GIS package, not the full survey.
        self.assertIn("All 478 records of the pinned official EIA GIS package",
                      audit["source_coverage_scope"])
        self.assertIn("Not the complete EIA-757 survey population", audit["source_coverage_scope"])
        self.assertIn("510 active natural gas processing plants", audit["source_coverage_scope"])
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        self.assertEqual(source._point_wkb(-95.5, 31.0), table["geometry"][0].as_py())

    def test_publication_date_drift_is_rejected(self):
        zip_body, members = _fixture_zip()
        pins = _fixture_pins(zip_body, members)
        with zipfile.ZipFile(io.BytesIO(zip_body)) as fixture:
            drifted_members = {name: fixture.read(name) for name in members}
        drifted_members[f"{FIXTURE_LAYER}.shp.xml"] = (
            drifted_members[f"{FIXTURE_LAYER}.shp.xml"]
            .replace((FIXTURE_PUBLICATION + "T00:00:00").encode("ascii"), b"2020-01-13T00:00:00"))
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w", zipfile.ZIP_DEFLATED) as z:
            for name, body in drifted_members.items():
                z.writestr(name, body)
        drift_zip = sink.getvalue()
        with patch.multiple(source, **{**pins, "EXPECTED_ZIP_BYTES": len(drift_zip),
                                       "EXPECTED_ZIP_SHA256": hashlib.sha256(drift_zip).hexdigest()}), \
                tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "source_publication_date_drift"):
                source._decode(drift_zip, Path(tmp))

    def test_incomplete_capture_is_quarantined_with_retained_receipt(self):
        def fake_capture_get(_client, url, capture_root, name, *, max_bytes):
            body = b"partial fixture bytes"
            digest = hashlib.sha256(body).hexdigest()
            (capture_root / f"{name}.body").write_bytes(body)
            (capture_root / f"{name}.wire").write_bytes(body)
            receipt = {"method": "GET", "url": url, "final_url": url, "observed_at": "2026-11-10T00:00:00+00:00",
                       "status": 200, "complete_body": False, "wire_bytes": len(body), "wire_sha256": digest,
                       "bytes": len(body), "sha256": digest, "content_encoding": None,
                       "content_length": None, "error_code": "incomplete_source_capture"}
            (capture_root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
            raise OSError("connection reset")

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage = source.StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.object(source, "capture_get", side_effect=fake_capture_get), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                with self.assertRaises(OSError):
                    source.capture_natural_gas_processing_plants(storage, version="vfixture",
                                                                capture_directory=str(root / "captures"))
            quarantine = sorted(str(p) for p in (root / "staging" / "_quarantine").rglob("eia_zip.body"))
            self.assertTrue(quarantine, "quarantine must retain the partial body")
            self.assertTrue(list((root / "staging" / "_quarantine").rglob("failure.json")))
            manifest = json.loads((root / "captures" / "capture_manifest.json").read_bytes())
            self.assertFalse(manifest["complete"])
            self.assertEqual(manifest["error_code"], "OSError")

    def test_tiny_capture_seal_catalog_then_postcatalog_cache(self):
        from dagster_hifld.catalog import summarize_staged_catalog, write_catalog_metadata
        from dagster_hifld.resources import StagingStorageResource
        from dagster_hifld.source_manifest import load_resolved_source_manifest, snapshot_source_metadata

        zip_body, members = _fixture_zip()
        pins = _fixture_pins(zip_body, members)
        bodies = {"eia_zip": zip_body}

        with tempfile.TemporaryDirectory(prefix="ngpp-e2e-") as temporary:
            root = Path(temporary)
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"), prefix="")
            with patch.multiple(source, **pins), \
                 patch.object(source, "capture_get", side_effect=_fake_capture_get(bodies)), \
                 patch.object(source.httpx, "Client", side_effect=lambda *args, **kwargs: EmptyClient()):
                candidate = source.capture_natural_gas_processing_plants(storage, version="vfixture",
                                                                          capture_directory=str(root / "captures"))
                self.assertEqual(candidate["feature_count"], 2)
                self.assertFalse(candidate["reused"])
                prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{candidate['version']}"
                state_key = f"_construction-state/{source.DATASET_SLUG}/{candidate['version']}.json"
                self.assertTrue(storage.object_snapshot(state_key))
                self.assertEqual(storage.read_key(f"{prefix}/raw/eia_zip.body"), zip_body)
                sealed = storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")
                sealed_table = pq.read_table(pa.BufferReader(sealed))
                self.assertEqual(sealed_table.column(source.IDENTIFIER_COLUMN).to_pylist(), [1, 2])
                snapshot_source_metadata(storage, source.DATASET_SLUG, source.FILE_SLUG, candidate["version"])
                manifest = load_resolved_source_manifest(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                          candidate["version"]).metadata
                self.assertEqual(manifest["date_issued"], FIXTURE_PUBLICATION)
                self.assertEqual(manifest["inputs"][0]["release_date"], FIXTURE_PUBLICATION)
                self.assertNotIn("release_date_unknown_reason", manifest["inputs"][0])
                self.assertEqual(manifest["inputs"][0]["survey_period"],
                                 "2017 Form EIA-757 (data as of 2017-12-31)")
                self.assertEqual(manifest["inputs"][0]["survey_data_release"], "2019-01-31")
                self.assertEqual(manifest["inputs"][0]["package_modified"], "2020-01-10")
                summary = summarize_staged_catalog(storage, source.DATASET_SLUG, source.FILE_SLUG,
                                                    candidate["version"], source.DATASET_SLUG,
                                                    source_metadata=manifest)
                self.assertEqual(summary.quality_manifest["feature_count"], 2)
                self.assertEqual(summary.quality_manifest["null_geometry_count"], 0)
                self.assertTrue(summary.quality_manifest["quality_check_passed"])
                write_catalog_metadata(storage, dataset_slug=source.DATASET_SLUG, file_slug=source.FILE_SLUG,
                                        version=candidate["version"], quality_dict=summary.quality_manifest,
                                        dictionary_dict=summary.data_dictionary)
                reused = source.capture_natural_gas_processing_plants(storage, version="vfixture",
                                                                       capture_directory=str(root / "captures"))
                self.assertTrue(reused["reused"])
                self.assertEqual(reused["version"], candidate["version"])
                # Replay verifies and reuses the sealed candidate bytes exactly,
                # including the explicit identifier column.
                self.assertEqual(storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet"), sealed)

    def test_missing_approval_preserves_populated_accepted_inventory(self):
        from dagster_hifld.assets.publish import _copy_version_files
        from dagster_hifld.release_approval import ReleaseApprovalError
        from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

        with tempfile.TemporaryDirectory() as staging_dir, tempfile.TemporaryDirectory() as published_dir:
            staging = StagingStorageResource(local_dir=staging_dir, use_local=True)
            published = PublishedStorageResource(local_dir=published_dir, use_local=True)
            staging.write(source.DATASET_SLUG, source.FILE_SLUG, "candidate-v1",
                          "geoparquet/natural-gas-processing-plants-1.parquet", b"candidate")
            accepted_key = published.write(source.DATASET_SLUG, source.FILE_SLUG, "accepted-v1",
                                           "geoparquet/natural-gas-processing-plants-1.parquet", b"accepted")
            before = {key: (published.object_snapshot(key), published.read_key(key))
                      for key in published.list_prefix()}
            self.assertEqual(set(before), {accepted_key})
            with patch.dict(os.environ, {"HIFLD_RELEASE_APPROVALS": ""}):
                with self.assertRaisesRegex(ReleaseApprovalError,
                                            "missing_independent_release_approval_registry"):
                    _copy_version_files(staging, published, source.DATASET_SLUG,
                                        source.FILE_SLUG, "candidate-v1")
            after = {key: (published.object_snapshot(key), published.read_key(key))
                     for key in published.list_prefix()}
            self.assertEqual(after, before)

    def test_refresh_schedule_is_stopped_and_pipeline_registered(self):
        from dagster_hifld.assets.eia_natural_gas_processing_plants import (
            eia_natural_gas_processing_plants_draft_schedule,
        )
        from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
        from dagster_hifld.definitions import defs
        self.assertEqual(eia_natural_gas_processing_plants_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        slug = source.DATASET_SLUG
        self.assertEqual(sum(any(tuple(k.path) == ("ingest", slug, slug) for k in asset.keys)
                              for asset in hifld_ingest_assets), 1)
        graph = defs.resolve_asset_graph()
        keys = {tuple(k.path) for k in graph.get_all_asset_keys()}
        self.assertIn(("ingest", slug, slug), keys)
        self.assertTrue(any(k.name == "eia_natural_gas_processing_plants_complete"
                            for k in graph.asset_check_keys))
        job = defs.resolve_job_def("eia_natural_gas_processing_plants_draft_refresh")
        job_checks = list(job.asset_layer.asset_graph.asset_check_keys)
        self.assertEqual(len(job_checks), 1)
        self.assertEqual(job_checks[0].name, "eia_natural_gas_processing_plants_complete")
        schedule = defs.get_schedule_def("eia_natural_gas_processing_plants_draft_refresh_schedule")
        self.assertEqual(schedule.name, "eia_natural_gas_processing_plants_draft_refresh_schedule")


if __name__ == "__main__":
    unittest.main()
