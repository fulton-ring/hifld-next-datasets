"""Small default mechanics for the Census 2020 VTD capture; full captures are opt-in."""
import gzip
import json
import os
import tempfile
import unittest
import zipfile
from contextlib import ExitStack, contextmanager
from pathlib import Path
from unittest.mock import patch

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import shapely
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import census_voting_districts as vtd


class CaptureMechanicsTests(unittest.TestCase):
    """Bounded acquisition keeps honest receipts and refuses incomplete responses."""

    @contextmanager
    def mocked_capture(self, *, status=200, encoded=False, length_delta=0, stream=None):
        original = httpx.Client
        payload = b"official archive bytes served exactly once"

        def handler(request):
            headers = {}
            body = payload
            if encoded:
                body = gzip.compress(payload)
                headers["Content-Encoding"] = "gzip"
            if stream is not None:
                return httpx.Response(status, headers=headers, stream=stream)
            headers["Content-Length"] = str(len(body) + length_delta)
            return httpx.Response(status, headers=headers, stream=httpx.ByteStream(body))

        transport = httpx.MockTransport(handler)
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=tmp)
            with patch.object(vtd.httpx, "Client",
                              lambda **kwargs: original(transport=transport, **kwargs)):
                yield storage

    def test_identity_encoding_receipt_pins_decoded_bytes(self):
        with self.mocked_capture() as storage:
            captures = vtd.acquire(storage)
            body, receipt = captures["census2020pl_vtd_directory"]
            self.assertEqual(body, b"official archive bytes served exactly once")
            self.assertEqual(receipt["status"], 200)
            self.assertEqual(receipt["bytes"], len(body))
            self.assertEqual(receipt["sha256"], vtd.digest(body))
            self.assertIs(receipt["complete_body"], True)

    def test_content_length_mismatch_is_refused(self):
        with self.mocked_capture(length_delta=1) as storage:
            with self.assertRaisesRegex(ValueError, "non200_or_incomplete_http_response"):
                vtd.acquire(storage)

    def test_non_200_is_refused(self):
        with self.mocked_capture(status=404) as storage:
            with self.assertRaisesRegex(ValueError, "non200_or_incomplete_http_response"):
                vtd.acquire(storage)

    def test_interrupted_stream_retains_three_attempt_receipts(self):
        class Broken(httpx.SyncByteStream):
            def __iter__(self):
                yield b"partial official bytes"
                raise httpx.ReadError("test interrupted stream")

        with self.mocked_capture(stream=Broken()) as storage:
            with self.assertRaisesRegex(ValueError, "official_census_capture_failed"):
                vtd.acquire(storage)
            receipts = list(Path(storage.local_dir).rglob(
                "census2020pl_vtd_directory.receipt.json"))
            self.assertEqual(len(receipts), 1)
            retained = json.loads(receipts[0].read_text("utf-8"))
            self.assertIs(retained["complete_body"], False)
            self.assertEqual(retained["attempt"], 3)

    def test_published_bucket_and_prefixed_storage_are_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "real").mkdir()
            alias = root / "alias"
            alias.symlink_to(root / "real")
            cases = (PublishedStorageResource(local_dir=str(root / "real")),
                     StagingStorageResource(bucket="never"),
                     StagingStorageResource(local_dir=str(root / "real"), prefix="unexpected"),
                     StagingStorageResource(local_dir=str(alias)))
            for storage in cases:
                with self.assertRaises(ValueError, msg=type(storage).__name__):
                    vtd._safe_local_staging(storage)

    def test_changed_bytes_fail_pin_validation_and_are_retained(self):
        payload = b"changed census archive bytes"
        receipt = {"url": vtd.DIRECTORY, "final_url": vtd.DIRECTORY, "status": 200,
                   "bytes": len(payload), "sha256": vtd.digest(payload),
                   "complete_body": True, "observed_at": "2026-11-14T00:00:00Z"}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "inputs"
            source.mkdir()
            (source / "census2020pl_vtd_directory.body").write_bytes(payload)
            (source / "census2020pl_vtd_directory.receipt.json").write_text(json.dumps(receipt))
            storage = StagingStorageResource(use_local=True, local_dir=str(root / "staging"))
            with self.assertRaisesRegex(ValueError, "capture_dir_file_missing"):
                vtd.acquire(storage, capture_dir=source)
            retained = list((root / "staging" / "_quarantine").rglob("*.body"))
            self.assertEqual(len(retained), 2)
            self.assertIn(b"", [path.read_bytes() for path in retained])


class DbfDecodeTests(unittest.TestCase):
    """The independent DBF decode checks physical declarations and value precision."""

    @staticmethod
    def properties():
        return {"STATEFP20": "56", "COUNTYFP20": "001", "VTDST20": "000115",
                "GEOID20": "56001000115", "VTDI20": "P", "NAME20": "TEST PRECINCT",
                "NAMELSAD20": "TEST PRECINCT Voting District", "LSAD20": "V2",
                "MTFCC20": "G5240", "FUNCSTAT20": "S", "ALAND20": 99999999999999,
                "AWATER20": 0, "INTPTLAT20": "+41.5202859", "INTPTLON20": "-105.5377676"}

    @classmethod
    def dbf(cls, values=None, rows=1):
        values = values or cls.properties()
        header = 33 + 32 * len(vtd.DBF_FIELDS)
        width = 1 + sum(field[2] for field in vtd.DBF_FIELDS)
        body = bytearray(header + width * rows + 1)
        body[0] = 3
        body[4:8] = rows.to_bytes(4, "little")
        body[8:10] = header.to_bytes(2, "little")
        body[10:12] = width.to_bytes(2, "little")
        for row_index in range(rows):
            cursor = header + row_index * width
            body[cursor] = 32
            cursor += 1
            for index, (name, kind, size, _scale) in enumerate(vtd.DBF_FIELDS):
                offset = 32 + index * 32
                body[offset:offset + len(name)] = name.encode()
                body[offset + 11] = ord(kind)
                body[offset + 16] = size
                body[offset + 17] = _scale
                value = values[name]
                text = "" if value is None else str(value)
                raw = (text.encode().rjust(size, b" ") if kind == "N"
                       else text.encode().ljust(size, b" "))
                assert len(raw) == size
                body[cursor:cursor + size] = raw
                cursor += size
        body[header - 1] = 13
        body[-1] = 26
        return bytes(body)

    def test_strings_integers_and_null_decode_exactly(self):
        values = self.properties()
        values["NAME20"] = " LEADING SPACE PRECINCT"
        values["VTDI20"] = None
        decoded = vtd.decode_dbf_rows(self.dbf(values))
        self.assertEqual(decoded, [values])
        self.assertIsNone(decoded[0]["VTDI20"])
        self.assertIs(type(decoded[0]["ALAND20"]), int)
        self.assertEqual(decoded[0]["ALAND20"], 99999999999999)

    def test_truncated_deleted_renamed_and_fractional_rows_are_refused(self):
        original = self.dbf()
        header = int.from_bytes(original[8:10], "little")
        changes = {"truncated": original[:-2]}
        deleted = bytearray(original)
        deleted[header] = 42
        changes["deleted_row_flag"] = bytes(deleted)
        renamed = bytearray(original)
        renamed[32] = ord("X")
        changes["renamed_field"] = bytes(renamed)
        fractional = bytearray(original)
        offset = header + 1 + sum(field[2] for field in vtd.DBF_FIELDS[:10])
        fractional[offset:offset + 14] = b"           1.5"
        changes["fractional_number"] = bytes(fractional)
        for label, body in changes.items():
            with self.assertRaises(ValueError, msg=label):
                vtd.decode_dbf_rows(body)


def synthetic_table(rows=2):
    polygon = shapely.Polygon([(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)])
    multipolygon = shapely.MultiPolygon([polygon, shapely.Polygon([(2, 2), (2, 3), (3, 3), (3, 2), (2, 2)])])
    names = ("tl_2020_01_vtd20", "tl_2020_01_vtd20")
    records = []
    for index, (geom, name) in enumerate(zip((polygon, multipolygon), names)):
        vtdst = "000115" if index == 0 else "000113"
        records.append({
            "OGC_FID": index, "STATEFP20": "01", "COUNTYFP20": "001", "VTDST20": vtdst,
            "GEOID20": "01" + "001" + vtdst, "VTDI20": "P",
            "NAME20": f"TEST PRECINCT {index}", "NAMELSAD20": f"TEST PRECINCT {index} Voting District",
            "LSAD20": "V2", "MTFCC20": "G5240", "FUNCSTAT20": "S",
            "ALAND20": 123456789 + index, "AWATER20": index,
            "INTPTLAT20": "+41.5202859", "INTPTLON20": "-105.5377676",
            "wkb_geometry": shapely.to_wkb(geom), "source_archive": name,
        })
    if rows == 1:
        records = records[:1]
        records[0]["wkb_geometry"] = shapely.to_wkb(polygon)
    return pa.Table.from_arrays(
        [pa.array([record[name] for record in records], type=kind)
         for name, kind in zip(vtd.FIELDS, vtd.TYPES)],
        names=vtd.FIELDS)


class SyntheticValidationTests(unittest.TestCase):
    """Pinned combined-table invariants refuse every drift class without any network."""

    @contextmanager
    def pinned(self, table):
        ids = table.column("GEOID20").to_pylist()
        type_counts, coordinates, bounds = synthetic_geometry_facts(table)
        import collections as _collections

        def counter(column):
            return dict(sorted((str(key), value) for key, value in
                               _collections.Counter(table.column(column).to_pylist()).items()))
        pins = {
            "PINS": {"tl_2020_01_vtd20": {"bytes": 16, "sha256": "0" * 64}},
            "STATE_COUNTS": {"01": table.num_rows},
            "NULL_COUNTS": {name: 0 for name in vtd.FIELDS},
            "VTDI_COUNTS": counter("VTDI20"),
            "LSAD_COUNTS": counter("LSAD20"),
            "FUNCSTAT_COUNTS": counter("FUNCSTAT20"),
            "MTFCC_COUNTS": counter("MTFCC20"),
            "GEOM_TYPE_COUNTS": type_counts,
            "COUNT": table.num_rows,
            "COORDINATES": coordinates,
            "BOUNDS": bounds,
            "GEOIDS_SHA256": vtd.digest(vtd.encode(sorted(ids))),
            "SOURCE_RECORDS_SHA256": vtd.record_hash_stream([table]),
        }
        with contextlib_patched(vtd, pins):
            yield table

    def test_valid_synthetic_table_passes(self):
        table = synthetic_table()
        with self.pinned(table):
            vtd.validate_table(table)

    def _refuses(self, mutate, label, repin_record_hash=False):
        table = synthetic_table()
        with self.pinned(table):
            changed = mutate(table)
            with ExitStack() as stack:
                if repin_record_hash:
                    stack.enter_context(patch.object(
                        vtd, "SOURCE_RECORDS_SHA256", vtd.record_hash_stream([changed])))
                with self.assertRaisesRegex(ValueError, label, msg=label):
                    vtd.validate_table(changed)

    def test_attribute_value_drift_is_refused(self):
        def mutate(table):
            column = table.column("VTDI20").to_pylist()
            column[0] = "A"
            return table.set_column(
                table.column_names.index("VTDI20"), "VTDI20", pa.array(column))
        self._refuses(mutate, "source_attribute_value_roster_drift")

    def test_missing_or_duplicate_geoid_is_refused(self):
        def mutate(table):
            ids = table.column("GEOID20").to_pylist()
            ids[1] = ids[0]
            return table.set_column(
                table.column_names.index("GEOID20"), "GEOID20", pa.array(ids))
        self._refuses(mutate, "missing_duplicate_changed_source_GEOID20")

    def test_geoid_roster_drift_is_refused(self):
        def mutate(table):
            ids = table.column("GEOID20").to_pylist()
            ids[1] = ids[0]
            return table.set_column(
                table.column_names.index("GEOID20"), "GEOID20", pa.array(ids))
        self._refuses(mutate, "missing_duplicate_changed_source_GEOID20")

    def test_null_value_drift_is_refused(self):
        def mutate(table):
            names = table.column("NAME20").to_pylist()
            names[0] = None
            return table.set_column(
                table.column_names.index("NAME20"), "NAME20", pa.array(names))
        self._refuses(mutate, "source_nullability_drift")

    def test_record_value_drift_is_refused(self):
        def mutate(table):
            names = table.column("NAME20").to_pylist()
            names[0] = names[0] + " RENAMED"
            return table.set_column(
                table.column_names.index("NAME20"), "NAME20", pa.array(names))
        self._refuses(mutate, "complete_original_attribute_values_and_native_WKB_drift")

    def test_invalid_geometry_is_refused(self):
        def mutate(table):
            wkbs = table.column("wkb_geometry").to_pylist()
            geoms = shapely.from_wkb(wkbs)
            geoms[0] = shapely.Polygon([(0, 0), (1, 1), (0, 1), (1, 0), (0, 0)])
            return table.set_column(
                table.column_names.index("wkb_geometry"), "wkb_geometry",
                pa.array(shapely.to_wkb(geoms)))
        self._refuses(mutate, "missing_invalid_empty_or_changed_native_geometry",
                      repin_record_hash=True)

    def test_z_geometry_is_refused(self):
        def mutate(table):
            wkbs = table.column("wkb_geometry").to_pylist()
            geoms = shapely.from_wkb(wkbs)
            geoms[0] = shapely.Polygon([(0, 0, 1), (0, 1, 1), (1, 1, 1), (1, 0, 1), (0, 0, 1)])
            return table.set_column(
                table.column_names.index("wkb_geometry"), "wkb_geometry",
                pa.array(shapely.to_wkb(geoms)))
        self._refuses(mutate, "missing_invalid_empty_or_changed_native_geometry",
                      repin_record_hash=True)

    def test_coordinate_population_drift_is_refused(self):
        def mutate(table):
            wkbs = table.column("wkb_geometry").to_pylist()
            geoms = shapely.from_wkb(wkbs)
            geoms[1] = shapely.MultiPolygon([shapely.Polygon([(2, 2), (2, 3), (3, 3), (3, 2), (2, 2)])])
            return table.set_column(
                table.column_names.index("wkb_geometry"), "wkb_geometry",
                pa.array(shapely.to_wkb(geoms)))
        self._refuses(mutate, "native_coordinate_population_drift",
                      repin_record_hash=True)

    def test_coordinate_and_bounds_drift_is_refused(self):
        def mutate(table):
            wkbs = table.column("wkb_geometry").to_pylist()
            geoms = shapely.from_wkb(wkbs)
            geoms[1] = shapely.MultiPolygon([
                shapely.Polygon([(2, 2), (2, 3), (3, 3), (3, 2), (2, 2)]),
                shapely.Polygon([(10, 10), (10, 11), (11, 11), (11, 10), (10, 10)]),
            ])
            return table.set_column(
                table.column_names.index("wkb_geometry"), "wkb_geometry",
                pa.array(shapely.to_wkb(geoms)))
        self._refuses(mutate, "native_coverage_extent_drift",
                      repin_record_hash=True)

    def test_geometry_type_drift_is_refused(self):
        def mutate(table):
            wkbs = table.column("wkb_geometry").to_pylist()
            geoms = shapely.from_wkb(wkbs)
            geoms[1] = shapely.Polygon([(2, 2), (2, 3), (3, 3), (3, 2), (2, 2)])
            return table.set_column(
                table.column_names.index("wkb_geometry"), "wkb_geometry",
                pa.array(shapely.to_wkb(geoms)))
        self._refuses(mutate, "native_geometry_type_drift",
                      repin_record_hash=True)


def contextlib_patched(module, values):
    manager = ExitStack()
    for name, value in values.items():
        manager.enter_context(patch.object(module, name, value))
    return manager


def synthetic_geometry_facts(table):
    wkbs = table.column("wkb_geometry").to_pylist()
    geoms = shapely.from_wkb(wkbs)
    type_counts = {}
    for kind in map(int, shapely.get_type_id(geoms)):
        label = "Polygon" if kind == 3 else "MultiPolygon"
        type_counts[label] = type_counts.get(label, 0) + 1
    return type_counts, int(shapely.get_num_coordinates(geoms).sum()), \
        [float(value) for value in shapely.total_bounds(geoms)]


def synthetic_shapefile_zip(directory: Path, name: str = "tl_2020_01_vtd20") -> Path:
    """Build one real single-state synthetic archive through the GDAL driver."""
    import shutil

    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True, exist_ok=True)
    table = synthetic_table(rows=2)
    attribute = table.drop(["source_archive", "OGC_FID"])
    shape_path = directory / name
    pyogrio.write_arrow(attribute, str(shape_path) + ".shp", driver="ESRI Shapefile",
                        geometry_name="wkb_geometry", geometry_type="Unknown",
                        encoding="UTF-8", crs="EPSG:4269")
    members = sorted(p.name for p in directory.iterdir())
    # GDAL writes the attribute sidecars; add the roster members the official
    # archives carry (.cpg and the two ISO sidecars) before zipping.
    with zipfile.ZipFile(shape_path.with_suffix(".zip"), "w", zipfile.ZIP_DEFLATED) as archive:
        for member in members:
            if member.startswith(name + "."):
                archive.write(directory / member, member)
        if name + ".cpg" not in members:
            archive.writestr(name + ".cpg", "UTF-8")
        archive.writestr(name + ".shp.ea.iso.xml", "<gmd:MD_Metadata/>")
        archive.writestr(name + ".shp.iso.xml", "<gmd:MD_Metadata/>")
    return shape_path.with_suffix(".zip")


class StagingCandidateTests(unittest.TestCase):
    """One synthetic producer archive exercises the full staging and reuse path."""

    def stage(self, root: Path):
        source = root / "inputs"
        source.mkdir(exist_ok=True)
        name = "tl_2020_01_vtd20"
        if (source / (name + ".body")).is_file():
            body = (source / (name + ".body")).read_bytes()
        else:
            zip_path = synthetic_shapefile_zip(root / "build")
            body = zip_path.read_bytes()
            (source / (name + ".body")).write_bytes(body)
        table = synthetic_table(rows=2)
        type_counts, coordinates, bounds = synthetic_geometry_facts(table)
        pins = {
            "PINS": {name: {"bytes": len(body), "sha256": vtd.digest(body)}},
            "STATE_COUNTS": {"01": 2},
            "NULL_COUNTS": {col: 0 for col in vtd.FIELDS},
            "VTDI_COUNTS": {"P": 2},
            "LSAD_COUNTS": {"V2": 2},
            "FUNCSTAT_COUNTS": {"S": 2},
            "MTFCC_COUNTS": {"G5240": 2},
            "GEOM_TYPE_COUNTS": type_counts,
            "COUNT": 2,
            "COORDINATES": coordinates,
            "BOUNDS": bounds,
            "GEOIDS_SHA256": vtd.digest(vtd.encode(sorted(table.column("GEOID20").to_pylist()))),
            "SOURCE_RECORDS_SHA256": vtd.record_hash_stream([table]),
        }
        # The GDAL-written DBF declares its own field widths; pin the decode
        # contract to what the synthetic archive physically carries.
        with zipfile.ZipFile(source / (name + ".body")) as archive:
            raw_dbf = archive.read(name + ".dbf")
        header = int.from_bytes(raw_dbf[8:10], "little")
        observed = []
        for index in range(32, header - 1, 32):
            field = raw_dbf[index:index + 32]
            observed.append((field[:11].split(b"\x00")[0].decode("ascii"),
                             chr(field[11]), field[16], field[17]))
        if not (source / "census2020pl_vtd_directory.body").exists():
            listing = f'<html><a href="{name}.zip">{name}.zip</a></html>'
            (source / "census2020pl_vtd_directory.body").write_bytes(listing.encode())
            listing_receipt = {"url": vtd.DIRECTORY, "final_url": vtd.DIRECTORY,
                               "status": 200, "bytes": len(listing),
                               "sha256": vtd.digest(listing.encode()),
                               "complete_body": True,
                               "observed_at": "2026-11-14T00:00:00Z"}
            (source / "census2020pl_vtd_directory.receipt.json").write_text(json.dumps(listing_receipt))
            zip_receipt = {"url": vtd.DIRECTORY + name + ".zip",
                           "final_url": vtd.DIRECTORY + name + ".zip", "status": 200,
                           "bytes": len(body), "sha256": vtd.digest(body),
                           "complete_body": True, "observed_at": "2026-11-14T00:00:00Z"}
            (source / (name + ".receipt.json")).write_text(json.dumps(zip_receipt))
        storage = StagingStorageResource(use_local=True,
                                         local_dir=str(root / "staging"))
        with (contextlib_patched(vtd, pins), patch.object(vtd, "DBF_FIELDS", observed),
              patch.object(vtd, "DRIVER_GEOMETRY_TYPE", "Polygon")):
            return storage, vtd.stage_voting_districts(storage, capture_dir=source)

    def test_stage_produces_pinned_candidate_then_reuses(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage, result = self.stage(root)
            self.assertFalse(result["reused"])
            self.assertEqual(result["feature_count"], 2)
            self.assertEqual(result["source_archive_count"], 1)
            self.assertEqual(result["native_value_parity"], True)
            prefix = (Path(storage.local_dir) / result["dataset_slug"]
                      / result["file_slug"] / result["version"])
            roster = {path.relative_to(prefix).as_posix()
                      for path in prefix.rglob("*") if path.is_file()}
            self.assertEqual(roster, set(vtd.CANDIDATE_FILES))
            parquet = pq.ParquetFile(prefix / "geoparquet" / vtd.OUTPUT_NAME)
            self.assertEqual(parquet.metadata.num_rows, 2)
            geo = json.loads((parquet.schema_arrow.metadata or {}).get(b"geo", b"{}"))
            self.assertEqual(geo["primary_column"], "wkb_geometry")
            self.assertEqual(geo["columns"]["wkb_geometry"]["geometry_types"],
                             ["MultiPolygon", "Polygon"])
            raw = list((Path(storage.local_dir) / "_raw-captures" / vtd.DATASET_SLUG).rglob("*.body"))
            self.assertEqual(len(raw), 1)
            _, repeated = self.stage(root)
            self.assertTrue(repeated["reused"])
            self.assertEqual(repeated["native_parquet_sha256"],
                             result["native_parquet_sha256"])

    def test_tampered_candidate_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            storage, result = self.stage(root)
            parquet = (Path(storage.local_dir) / result["dataset_slug"] / result["file_slug"]
                       / result["version"] / "geoparquet" / vtd.OUTPUT_NAME)
            body = parquet.read_bytes()
            parquet.write_bytes(body[:-1] + bytes([body[-1] ^ 0xFF]))
            with self.assertRaisesRegex(ValueError, "existing_candidate_bytes_drift"):
                self.stage(root)


class LiveCaptureTests(unittest.TestCase):
    """Full pinned-capture staging against the real producer files; explicit opt-in."""

    def test_full_pinned_capture_stages_complete_population(self):
        capture_dir = os.environ.get("VTD_TEST_CAPTURE_DIR")
        if not capture_dir:
            self.skipTest("VTD_TEST_CAPTURE_DIR not set; live pinned capture skipped")
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            result = vtd.stage_voting_districts(storage, capture_dir=Path(capture_dir))
            self.assertTrue(result["source_population_complete"])
            self.assertEqual(result["feature_count"], vtd.COUNT)
            self.assertEqual(result["source_archive_count"], len(vtd.PINS))
            self.assertEqual(result["source_id_sha256"], vtd.GEOIDS_SHA256)


if __name__ == "__main__":
    unittest.main()
