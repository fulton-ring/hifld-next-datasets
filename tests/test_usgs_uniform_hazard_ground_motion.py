"""Focused contract checks for the USGS uniform-hazard ground motion source adapter."""
from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import fiona
import pyarrow.parquet as pq
import pyproj
import shapely

from dagster_hifld.sources import usgs_uniform_hazard_ground_motion as source


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


ITEM_BODY = {
    "id": source.ITEM_ID,
    "title": source.EXPECTED_ITEM_TITLE,
    "files": [{
        "name": source.ARCHIVE_NAME,
        "title": source.EXPECTED_ARCHIVE_FILE_TITLE,
        "contentType": "application/zip",
        "size": source.ARCHIVE_BYTES,
        "checksum": {"value": source.ARCHIVE_MD5, "type": "MD5"},
        "url": (f"https://www.sciencebase.gov/catalog/file/get/{source.ITEM_ID}"
                "?f=__disk__aa%2Fbb%2Fcc%2Fdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
    }],
}

SCHEMA = {"properties": dict(source.EXPECTED_FIELD_KINDS), "geometry": "Polygon"}
CRS = "EPSG:4326"

# Synthetic contour-band features mirroring the producer population shape:
# band polygons, one no-data row, one MultiPolygon, one self-intersecting band.
FEATURES = [
    {"properties": {"low_cont": "0.060000", "high_cont": "0.062500",
                    "range_cont": "0.06 - 0.0625", "valley": "Y",
                    "Shape_Leng": 0.922571039709, "Shape_Area": 0.00310886661697},
     "geometry": {"type": "Polygon", "coordinates": [[(-121.5, 47.0), (-121.4, 47.0),
                                                     (-121.4, 47.1), (-121.5, 47.1),
                                                     (-121.5, 47.0)]]}},
    {"properties": {"low_cont": "-1000000", "high_cont": "-1000000",
                    "range_cont": "<NoData>", "valley": "N",
                    "Shape_Leng": 2.96322523191, "Shape_Area": 0.298735276184},
     "geometry": {"type": "Polygon", "coordinates": [[(-100.0, 40.0), (-99.9, 40.0),
                                                     (-99.9, 40.1), (-100.0, 40.1),
                                                     (-100.0, 40.0)]]}},
    {"properties": {"low_cont": "0.180000", "high_cont": "0.182500",
                    "range_cont": "0.18 - 0.1825", "valley": "N",
                    "Shape_Leng": 1.26635253536, "Shape_Area": 0.0109930597886},
     "geometry": {"type": "MultiPolygon", "coordinates": [
         [[(-105.0, 41.0), (-104.9, 41.0), (-104.9, 41.1), (-105.0, 41.1), (-105.0, 41.0)]],
         [[(-104.8, 41.0), (-104.7, 41.0), (-104.7, 41.1), (-104.8, 41.1), (-104.8, 41.0)]]]}},
    {"properties": {"low_cont": "0.357500", "high_cont": "0.362500",
                    "range_cont": "0.3575 - 0.3625", "valley": "N",
                    "Shape_Leng": 0.462883160573, "Shape_Area": 0.00364318709342},
     "geometry": {"type": "Polygon", "coordinates": [[(-160.0, 52.0), (-158.9, 52.9),
                                                     (-158.9, 52.0), (-160.0, 52.9),
                                                     (-160.0, 52.0)]]}},
]


def _write_shapefile(directory: Path, features, schema=SCHEMA, crs=CRS,
                     stem=source.TARGET_STEM) -> dict:
    path = directory / f"{stem}.shp"
    with fiona.open(path, "w", driver="ESRI Shapefile", schema=schema, crs=crs) as sink:
        order = list(sink.schema["properties"].keys())
        normalized = []
        for feature in features:
            values = dict(feature["properties"])
            normalized.append({**feature, "properties": {name: values[name] for name in order}})
        sink.writerecords(normalized)
    members = {}
    for suffix in (".shp", ".shx", ".dbf", ".prj"):
        members[stem + suffix] = path.with_suffix(suffix).read_bytes()
    return members


def _fixture_pins(members: dict, overrides: dict | None = None) -> dict:
    """Compute consistent expected pins from the round-tripped synthetic rows."""
    with tempfile.TemporaryDirectory(prefix="unif-hazard-pins-") as tmp:
        for name in (source.TARGET_STEM + suffix for suffix in source.TARGET_SUFFIXES):
            (Path(tmp) / name).write_bytes(members[name])
        with fiona.open(Path(tmp) / f"{source.TARGET_STEM}.shp") as src:
            schema = dict(src.schema)
            rows = list(src)
    records, geometry_records, stats = source._classify_rows(rows)
    pins = {
        "EXPECTED_COUNT": len(records),
        "EXPECTED_SCHEMA_SHA256": source._sha(source._canonical(schema)),
        "EXPECTED_RECORD_SHA256": source._list_sha(records),
        "EXPECTED_GEOMETRY_SHA256": source._list_sha(geometry_records),
        "EXPECTED_OBSERVED_BOUNDS": stats["observed_bounds"],
        "EXPECTED_VERTEX_TOTAL": stats["vertex_total"],
        "EXPECTED_MULTIPOLYGON_COUNT": stats["multipolygon_count"],
        "EXPECTED_INVALID_GEOMETRY_ROWS": frozenset(stats["invalid_rows"]),
        "EXPECTED_NODATA_ROW_COUNT": stats["nodata_rows"],
        "EXPECTED_VALLEY_VALUES": frozenset(stats["valley_values"]),
        "EXPECTED_DISTINCT_CONTOUR_COMBOS": len(stats["contour_combos"]),
        "EXPECTED_NOMINAL_RANGE_EXCEEDED_COUNT": stats["nominal_range_exceeded"],
    }
    return {**pins, **(overrides or {})}


def _decoded(members: dict, pins: dict):
    with patch.multiple(source, **pins):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            return source._decode_shapefile(members, Path(tmp))


def _mutated_features(index: int, **overrides) -> list[dict]:
    features = [json.loads(json.dumps(feature)) for feature in FEATURES]
    features[index] = {**features[index], **overrides}
    return features


class UniformHazardGroundMotionSourceTests(unittest.TestCase):
    def test_streamed_list_sha_matches_canonical_digest(self):
        items = [[1, {"a": "b", "list": [1, 2, 3]}], None, "x", 4.5]
        self.assertEqual(source._list_sha(items), _sha(_canonical(items)))

    def test_item_document_resolves_producer_archive_url(self):
        self.assertEqual(source._validate_item_document(ITEM_BODY), ITEM_BODY["files"][0]["url"])

    def test_item_document_identity_drift_is_rejected(self):
        for mutation in ({"title": "Some repackaged hazard grid"},
                         {"id": "0" * 32}):
            document = {**ITEM_BODY, **mutation}
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                        "source_item_identity_drift"):
                source._validate_item_document(document)

    def test_item_document_file_roster_drift_is_rejected(self):
        document = json.loads(json.dumps(ITEM_BODY))
        document["files"] = []
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_item_file_roster_drift"):
            source._validate_item_document(document)
        document["files"] = [dict(ITEM_BODY["files"][0]),
                             dict(ITEM_BODY["files"][0], url="https://www.sciencebase.gov/x")]
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_item_file_roster_drift"):
            source._validate_item_document(document)

    def test_item_document_file_metadata_drift_is_rejected(self):
        base = ITEM_BODY["files"][0]
        for mutation, message in (
            ({"title": "repackaged"}, "source_item_file_identity_drift"),
            ({"contentType": "text/plain"}, "source_item_file_identity_drift"),
            ({"size": base["size"] + 1}, "source_item_file_size_drift"),
            ({"checksum": {"value": base["checksum"]["value"], "type": "SHA-1"}},
             "source_item_file_checksum_drift"),
            ({"checksum": {"value": "0" * 32, "type": "MD5"}},
             "source_item_file_checksum_drift"),
            ({"url": "https://mirror.example.invalid/catalog/file/get/xyz"},
             "source_item_file_host_drift"),
            ({"url": "http://www.sciencebase.gov/catalog/file/get/" + source.ITEM_ID},
             "source_item_file_host_drift"),
        ):
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError, message):
                source._validate_item_document({**ITEM_BODY, "files": [{**base, **mutation}]})

    def _roster_pins(self, roster: dict) -> dict:
        return {"ARCHIVE_SHA256": _sha(self._zipped(roster)), "ARCHIVE_BYTES": len(self._zipped(roster)),
                "MAX_EXPANDED_BYTES": max(source.MAX_EXPANDED_BYTES, len(roster) * 8_000_000),
                "EXPECTED_ARCHIVE_MEMBERS": tuple(
                    (name, len(data), _sha(data)) for name, data in sorted(roster.items()))}

    @staticmethod
    def _zipped(roster: dict) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for name, data in roster.items():
                archive.writestr(name, data)
        return buffer.getvalue()

    def _synthetic_archive(self, members: dict, *, extra=None, drop=None, mutate=None) -> tuple[bytes, dict]:
        roster = {"SA5P0_2Pct.lyrx": b"lyrx-placeholder",
                  "US_2023_5sec_5PctIn50Yrs_Vs30_760.pdf": b"pdf-placeholder"}
        roster.update(members)
        if extra:
            roster.update(extra)
        for name in (drop or ()):
            roster.pop(name, None)
        for name, data in (mutate or {}).items():
            roster[name] = data
        body = self._zipped(roster)
        pins = {"ARCHIVE_SHA256": _sha(body), "ARCHIVE_BYTES": len(body),
                "MAX_EXPANDED_BYTES": max(source.MAX_EXPANDED_BYTES, len(body) * 8),
                "EXPECTED_ARCHIVE_MEMBERS": tuple(
                    (name, len(data), _sha(data)) for name, data in sorted(roster.items()))}
        return body, pins

    def test_archive_validation_accepts_synthetic_producer_archive(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        body, pins = self._synthetic_archive(members)
        with patch.multiple(source, **pins):
            decoded = source._validate_archive(body)
        self.assertEqual(decoded["US_SA5P0_5Pct50Yrs_BC_poly.shp"], members["US_SA5P0_5Pct50Yrs_BC_poly.shp"])
        self.assertEqual(len(decoded), len(members) + 2)

    def test_archive_bytes_sha_or_size_drift_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        body, pins = self._synthetic_archive(members)
        for drifted in (body[:-1], body + b"x"):
            with patch.multiple(source, **pins):
                with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                            "changed_truncated_or_unknown_producer_archive"):
                    source._validate_archive(drifted)
        flipped = bytearray(body)
        flipped[-1] ^= 0xFF
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                        "changed_truncated_or_unknown_producer_archive"):
                source._validate_archive(bytes(flipped))

    def test_archive_roster_and_member_drift_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        original_roster = {"SA5P0_2Pct.lyrx": b"lyrx-placeholder",
                           "US_2023_5sec_5PctIn50Yrs_Vs30_760.pdf": b"pdf-placeholder"}
        original_roster.update(members)
        original_members_pin = tuple(
            (name, len(data), _sha(data)) for name, data in sorted(original_roster.items()))

        def variant_pins(roster: dict) -> dict:
            body = self._zipped(roster)
            return {"ARCHIVE_SHA256": _sha(body), "ARCHIVE_BYTES": len(body),
                    "EXPECTED_ARCHIVE_MEMBERS": original_members_pin}

        dropped = {name: data for name, data in original_roster.items() if name != "SA5P0_2Pct.lyrx"}
        with patch.multiple(source, **variant_pins(dropped)):
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                        "unexpected_or_missing_archive_member"):
                source._validate_archive(self._zipped(dropped))
        extra = dict(original_roster, **{"extra.txt": b"x"})
        with patch.multiple(source, **variant_pins(extra)):
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                        "unexpected_or_missing_archive_member"):
                source._validate_archive(self._zipped(extra))
        mutated_name = "US_SA5P0_5Pct50Yrs_BC_poly.dbf"
        mutated_roster = dict(original_roster)
        mutated_roster[mutated_name] = members[mutated_name][:-4] + b"zzzz"
        mutated_body = self._zipped(mutated_roster)
        pins = {"ARCHIVE_SHA256": _sha(mutated_body), "ARCHIVE_BYTES": len(mutated_body),
                "EXPECTED_ARCHIVE_MEMBERS": original_members_pin}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                        "archive_member_digest_drift:" + mutated_name):
                source._validate_archive(mutated_body)

    def test_archive_path_traversal_member_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        body, pins = self._synthetic_archive(members, extra={"../evil.shp": b"x"})
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                        "unsafe_encrypted_or_symlink_archive_member"):
                source._validate_archive(body)

    def test_archive_crc_corruption_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        roster = {"SA5P0_2Pct.lyrx": b"lyrx-placeholder",
                  "US_2023_5sec_5PctIn50Yrs_Vs30_760.pdf": b"pdf-placeholder"}
        roster.update(members)
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_STORED) as archive:
            for name, data in roster.items():
                archive.writestr(name, data)
        body = bytearray(buffer.getvalue())
        position = body.find(b"lyrx-placeholder")
        body[position] ^= 0xFF
        pins = {"ARCHIVE_SHA256": _sha(bytes(body)), "ARCHIVE_BYTES": len(body),
                "MAX_EXPANDED_BYTES": max(source.MAX_EXPANDED_BYTES, len(body) * 8),
                "EXPECTED_ARCHIVE_MEMBERS": tuple(
                    (name, len(data), _sha(data)) for name, data in sorted(roster.items()))}
        with patch.multiple(source, **pins):
            with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                        "archive_container_invalid"):
                source._validate_archive(bytes(body))

    def test_target_shapefile_members_missing_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        reduced = {name: data for name, data in members.items() if not name.endswith(".prj")}
        with patch.multiple(source, EXPECTED_COUNT=len(FEATURES)):
            with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as scratch:
                with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                            "target_shapefile_members_missing"):
                    source._decode_shapefile(reduced, Path(scratch))

    def test_feature_count_drift_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        pins = _fixture_pins(members, overrides={"EXPECTED_COUNT": len(FEATURES) + 1})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "target_dbf_population_drift"):
            _decoded(members, pins)

    def test_synthetic_population_decodes_with_full_fidelity(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        records, geometry_records, audit = _decoded(members, _fixture_pins(members))
        self.assertEqual(len(records), len(FEATURES))
        self.assertEqual(audit["source_crs"], "EPSG:4326")
        self.assertEqual(audit["source_multipolygon_count"], 1)
        self.assertEqual(audit["source_nodata_row_count"], 1)
        self.assertEqual(audit["source_invalid_geometry_rows"], [3])
        self.assertIn(3, audit["source_invalid_geometry_reasons"])
        self.assertIn("Self-intersection", audit["source_invalid_geometry_reasons"][3])
        first = records[0]
        self.assertEqual(first["low_cont"], "0.060000")
        self.assertEqual(first["high_cont"], "0.062500")
        self.assertEqual(first["range_cont"], "0.06 - 0.0625")
        self.assertEqual(first["valley"], "Y")
        self.assertEqual(first["Shape_Leng"], records[0]["Shape_Leng"])
        self.assertEqual(first["Shape_Area"], records[0]["Shape_Area"])
        self.assertEqual(first["geometry"]["type"], "Polygon")
        self.assertEqual(first["geometry"]["coordinates"][0][0], [-121.5, 47.0])
        nodata = records[1]
        self.assertEqual(nodata["low_cont"], "-1000000")
        self.assertEqual(nodata["range_cont"], "<NoData>")
        multi = records[2]
        self.assertEqual(multi["geometry"]["type"], "MultiPolygon")
        self.assertEqual(len(multi["geometry"]["coordinates"]), 2)
        self.assertEqual(len(geometry_records), len(FEATURES))
        self.assertEqual(geometry_records[3][0], 3)

    def test_schema_roster_drift_is_rejected(self):
        minimal = {"EXPECTED_COUNT": len(FEATURES)}
        extra_schema = {**SCHEMA, "properties": {**SCHEMA["properties"], "EXTRA": "str:10"}}
        extra_features = [{**feature, "properties": {**feature["properties"], "EXTRA": "x"}}
                          for feature in FEATURES]
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), extra_features, schema=extra_schema)
        with patch.multiple(source, **minimal):
            with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as scratch:
                with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                            "source_schema_contract_drift"):
                    source._decode_shapefile(members, Path(scratch))
        renamed_schema = {**SCHEMA, "properties": {
            ("low_cont2" if name == "low_cont" else name): kind
            for name, kind in SCHEMA["properties"].items()}}
        renamed_features = []
        for feature in FEATURES:
            renamed_features.append({**feature, "properties": {
                ("low_cont2" if name == "low_cont" else name): value
                for name, value in feature["properties"].items()}})
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), renamed_features, schema=renamed_schema)
        with patch.multiple(source, **minimal):
            with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as scratch:
                with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                            "source_schema_contract_drift"):
                    source._decode_shapefile(members, Path(scratch))

    def test_crs_drift_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES, crs="EPSG:4269")
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError, "source_crs_drift"):
            _decoded(members, _fixture_pins(members))

    def test_null_geometry_population_drift_is_rejected(self):
        null_features = _mutated_features(1, geometry=None)
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), null_features)
        pins = _fixture_pins(members, overrides={"EXPECTED_NULL_GEOMETRY_COUNT": 0})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_null_geometry_population_drift"):
            _decoded(members, pins)

    def test_coordinate_range_drift_is_rejected(self):
        far_features = _mutated_features(0, geometry={"type": "Polygon", "coordinates": [
            [(-200.5, 47.0), (-200.4, 47.0), (-200.4, 47.1), (-200.5, 47.1), (-200.5, 47.0)]]})
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), far_features)
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_coordinate_range_drift"):
            _decoded(members, _fixture_pins(members))

    def test_nominal_range_excess_population_is_pinned(self):
        antimeridian = _mutated_features(0, geometry={"type": "Polygon", "coordinates": [
            [(180.0000000000001, 47.0), (179.5, 47.0), (179.5, 47.1), (180.0000000000001, 47.1),
             (180.0000000000001, 47.0)]]})
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), antimeridian)
        records, _geometry_records, audit = _decoded(members, _fixture_pins(members))
        self.assertEqual(audit["source_nominal_range_exceeded_vertex_count"], 3)
        self.assertGreater(audit["source_observed_bounds"]["xmax"], 180.0)
        beyond = _mutated_features(0, geometry={"type": "Polygon", "coordinates": [
            [(180.5, 47.0), (179.5, 47.0), (179.5, 47.1), (180.5, 47.1), (180.5, 47.0)]]})
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), beyond)
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_coordinate_range_drift"):
            _decoded(members, _fixture_pins(members))

    def test_vertex_and_bounds_pin_drift_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        pins = _fixture_pins(members, overrides={"EXPECTED_VERTEX_TOTAL": 999})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_vertex_total_drift"):
            _decoded(members, pins)
        bounds = dict(_fixture_pins(members)["EXPECTED_OBSERVED_BOUNDS"])
        bounds["xmin"] = bounds["xmin"] - 1.0
        pins = _fixture_pins(members, overrides={"EXPECTED_OBSERVED_BOUNDS": bounds})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_observed_coverage_drift"):
            _decoded(members, pins)

    def test_ring_structure_drift_is_rejected(self):
        sparse_features = _mutated_features(0, geometry={"type": "Polygon", "coordinates": [
            [(-121.5, 47.0), (-121.4, 47.0)]]})
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), sparse_features)
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_ring_structure_drift"):
            _decoded(members, _fixture_pins(members))

    def test_record_and_geometry_pin_drift_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        pins = _fixture_pins(members, overrides={"EXPECTED_RECORD_SHA256": "0" * 64})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_record_value_drift"):
            _decoded(members, pins)
        pins = _fixture_pins(members, overrides={"EXPECTED_GEOMETRY_SHA256": "0" * 64})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_geometry_or_coverage_drift"):
            _decoded(members, pins)

    def test_valley_nodata_and_invalid_population_drift_is_rejected(self):
        drifted = _mutated_features(0)
        drifted[0]["properties"]["valley"] = "Z"
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), drifted)
        pins = _fixture_pins(members, overrides={"EXPECTED_VALLEY_VALUES": frozenset({"Y", "N"})})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_valley_value_drift"):
            _decoded(members, pins)
        cleared = _mutated_features(1, properties={**FEATURES[1]["properties"],
                                                   "low_cont": "0.1", "high_cont": "0.2",
                                                   "range_cont": "0.1 - 0.2"})
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), cleared)
        pins = _fixture_pins(members, overrides={"EXPECTED_NODATA_ROW_COUNT": 2})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_nodata_population_drift"):
            _decoded(members, pins)
        repaired = _mutated_features(3, geometry={"type": "Polygon", "coordinates": [
            [(-160.0, 52.0), (-158.9, 52.5), (-158.0, 52.2), (-158.9, 51.8), (-160.0, 52.0)]]})
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), repaired)
        pins = _fixture_pins(members, overrides={"EXPECTED_INVALID_GEOMETRY_ROWS": frozenset({3})})
        with self.assertRaisesRegex(source.UniformHazardGroundMotionError,
                                    "source_invalid_geometry_population_drift"):
            _decoded(members, pins)

    def test_parquet_round_trips_all_native_values_and_wkb(self):
        with tempfile.TemporaryDirectory(prefix="unif-hazard-test-") as tmp:
            members = _write_shapefile(Path(tmp), FEATURES)
        records, _geometry_records, _audit = _decoded(members, _fixture_pins(members))
        body = source._parquet_bytes(records)
        with tempfile.NamedTemporaryFile(suffix=".parquet") as stream:
            stream.write(body)
            stream.flush()
            table = pq.read_table(stream.name)
        self.assertEqual(table.num_rows, len(FEATURES))
        self.assertEqual(table.column("low_cont").to_pylist()[0], "0.060000")
        self.assertEqual(table.column("range_cont").to_pylist()[1], "<NoData>")
        self.assertEqual(table.column("valley").to_pylist(), ["Y", "N", "N", "N"])
        self.assertEqual(table.column("Shape_Leng").to_pylist()[0], records[0]["Shape_Leng"])
        self.assertEqual(table.column("Shape_Area").to_pylist()[2], records[2]["Shape_Area"])
        geoms = [shapely.from_wkb(wkb) if wkb is not None else None
                 for wkb in table.column("geometry").to_pylist()]
        self.assertIsInstance(geoms[0], shapely.Polygon)
        self.assertEqual(shapely.get_coordinates(geoms[0]).tolist()[0], [-121.5, 47.0])
        self.assertIsInstance(geoms[2], shapely.MultiPolygon)
        self.assertEqual(len(geoms[2].geoms), 2)
        self.assertEqual(geoms[3].geom_type, "Polygon")
        geo = json.loads(table.schema.metadata[b"geo"])
        self.assertEqual(geo["version"], "1.1.0")
        self.assertEqual(geo["primary_column"], "geometry")
        self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Polygon", "MultiPolygon"])
        self.assertEqual(geo["columns"]["geometry"]["crs"]["id"]["code"], 4326)
        crs = pyproj.CRS.from_json_dict(geo["columns"]["geometry"]["crs"])
        self.assertTrue(crs.equals(pyproj.CRS.from_epsg(4326)))

    def test_pinned_producer_constants_are_consistent(self):
        self.assertEqual(len(source.EXPECTED_ARCHIVE_MEMBERS), 29)
        member_names = [name for name, _, _ in source.EXPECTED_ARCHIVE_MEMBERS]
        for suffix in source.TARGET_SUFFIXES:
            self.assertIn(source.TARGET_STEM + suffix, member_names)
        self.assertEqual(len(source.EXPECTED_FIELD_NAMES), 6)
        self.assertEqual(len(source.EXPECTED_INVALID_GEOMETRY_ROWS), 7)
        self.assertEqual(source.EXPECTED_COUNT, 2757)
        self.assertGreater(source.EXPECTED_NOMINAL_RANGE_EXCEEDED_COUNT, 0)
        self.assertLess(source.NOMINAL_RANGE_TOLERANCE, 1e-6)
        self.assertTrue(source.EXPECTED_RECORD_SHA256.islower() or
                        source.EXPECTED_RECORD_SHA256.islower())
        self.assertRegex(source.EXPECTED_RECORD_SHA256, r"[0-9a-f]{64}")
        self.assertRegex(source.EXPECTED_GEOMETRY_SHA256, r"[0-9a-f]{64}")
        self.assertRegex(source.EXPECTED_SCHEMA_SHA256, r"[0-9a-f]{64}")


if __name__ == "__main__":
    unittest.main()
