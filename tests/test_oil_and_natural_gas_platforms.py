"""Focused BSEE/BOEM platform composite source checks."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

import pyarrow.parquet as pq

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import oil_and_natural_gas_platforms as source


def _layout_html(layout):
    rows = ["<table><tr><th>Start Position</th><th>Item Length</th><th>Column Alias</th><th>Definition</th></tr>"]
    for start, length, name, alias, machine in layout:
        rows.append(f"<tr><td>{start}</td><td>{length}</td><td>{alias or name.replace('_', ' ').title()}</td>"
                    f"<td>{machine or name} - definition</td></tr>")
    rows.append("</table>")
    return "".join(rows).encode()


def _fixed_row(layout, values):
    end = max(start - 1 + length for start, length, *_ in layout)
    line = [" "] * end
    for start, length, name, *_ in layout:
        value = str(values.get(name, ""))[:length].ljust(length)
        line[start - 1:start - 1 + length] = value
    return "".join(line)


def _csv_body(layout, rows):
    sink = io.StringIO()
    writer = csv.writer(sink, lineterminator="\n")
    for row in rows:
        writer.writerow([str(row.get(name, "")) for _, _, name, *_ in layout])
    return sink.getvalue().encode("latin1")


def _zip(body, member):
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        archive.writestr(member, body)
    return sink.getvalue()


def _landing(region):
    if region == "gulf":
        links = " ".join(source.GULF_URLS.values())
        return ("Gulf of Mexico Platform Downloads " + links +
                " Platform Masters 11/2/2026 10:02:22 AM Platform Structures 11/2/2026 10:12:11 AM"
                " Platform Locations 11/2/2026 10:06:56 AM").encode()
    links = " ".join(source.PACIFIC_URLS.values())
    return ("Pacific Platform Downloads " + links +
            " Platform Masters 11/1/2026 6:32:10 AM Platform Structures 11/1/2026 6:32:11 AM"
            " Platform Locations 11/1/2026 6:32:09 AM").encode()


GULF_MASTERS = [
    {"COMPLEX_ID_NUM": "1", "MMS_COMPANY_NUM": "00231", "MANNED_24_HR_FLAG": "Y",
     "BED_COUNT": "10", "LAST_REVISION_DATE": "01-JAN-2020"},
    {"COMPLEX_ID_NUM": "2", "MMS_COMPANY_NUM": "00232", "MANNED_24_HR_FLAG": "N",
     "BED_COUNT": "", "LAST_REVISION_DATE": "02-JAN-2021"},
    {"COMPLEX_ID_NUM": "3", "MMS_COMPANY_NUM": "00233", "MANNED_24_HR_FLAG": "N",
     "BED_COUNT": "4", "LAST_REVISION_DATE": "03-JAN-2022"},
]
GULF_STRUCTURES = [
    {"COMPLEX_ID_NUM": "1", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "ALPHA",
     "INSTALL_DATE": "01-JAN-2000", "REMOVAL_DATE": "", "SLOT_COUNT": "1",
     "LAST_REVISION_DATE": "01-JAN-2020"},
    {"COMPLEX_ID_NUM": "2", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "BETA",
     "INSTALL_DATE": "02-JAN-2001", "REMOVAL_DATE": "03-JAN-2020", "SLOT_COUNT": "2",
     "LAST_REVISION_DATE": "02-JAN-2021"},
]
GULF_LOCATIONS = [
    {"COMPLEX_ID_NUM": "1", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "ALPHA",
     "LONGITUDE": "-90.0", "LATITUDE": "28.0"},
    {"COMPLEX_ID_NUM": "2", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "BETA",
     "LONGITUDE": "-89.0", "LATITUDE": "27.0"},
    {"COMPLEX_ID_NUM": "2", "STRUCTURE_NUMBER": "2", "STRUCTURE_NAME": "GAMMA",
     "LONGITUDE": "-88.0", "LATITUDE": "26.0"},
    {"COMPLEX_ID_NUM": "3", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "DELTA",
     "LONGITUDE": "", "LATITUDE": ""},
]
PACIFIC_MASTERS = [
    {"COMPLEX_ID_NUM": "51000", "MMS_COMPANY_NUM": "02531", "BED_COUNT": "4",
     "LAST_REVISION_DATE": "20110629"},
    {"COMPLEX_ID_NUM": "51001", "MMS_COMPANY_NUM": "02531", "BED_COUNT": "2",
     "LAST_REVISION_DATE": "20250224"},
]
PACIFIC_STRUCTURES = [
    {"COMPLEX_ID_NUM": "51000", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "HOGAN",
     "INSTALL_DATE": "19670901", "REMOVAL_DATE": "", "SLOT_COUNT": "66"},
    {"COMPLEX_ID_NUM": "51001", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "HOUCHIN",
     "INSTALL_DATE": "19680801", "REMOVAL_DATE": "", "SLOT_COUNT": "60"},
]
PACIFIC_LOCATIONS = [
    {"COMPLEX_ID_NUM": "51000", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "HOGAN",
     "LONGITUDE": "-119.61343007", "LATITUDE": "34.33188363"},
    {"COMPLEX_ID_NUM": "51001", "STRUCTURE_NUMBER": "1", "STRUCTURE_NAME": "HOUCHIN",
     "LONGITUDE": "0", "LATITUDE": "0"},
]


def _captures():
    return {
        "gulf_landing": (_landing("gulf"), {"status": 200}),
        "pacific_landing": (_landing("pacific"), {"status": 200}),
        "gulf_masters_layout": (_layout_html(source.GULF_MASTERS), {"status": 200}),
        "gulf_structures_layout": (_layout_html(source.GULF_STRUCTURES), {"status": 200}),
        "gulf_locations_layout": (_layout_html(source.GULF_LOCATIONS), {"status": 200}),
        "pacific_masters_layout": (_layout_html(source.PACIFIC_MASTERS), {"status": 200}),
        "pacific_structures_layout": (_layout_html(source.PACIFIC_STRUCTURES), {"status": 200}),
        "pacific_locations_layout": (_layout_html(source.PACIFIC_LOCATIONS), {"status": 200}),
        "gulf_masters": (_zip("\n".join(_fixed_row(source.GULF_MASTERS, row) for row in GULF_MASTERS),
                              source.TABLE_MEMBERS["gulf_masters"]), {"status": 200}),
        "gulf_structures": (_zip("\n".join(_fixed_row(source.GULF_STRUCTURES, row) for row in GULF_STRUCTURES),
                                 source.TABLE_MEMBERS["gulf_structures"]), {"status": 200}),
        "gulf_locations": (_zip("\n".join(_fixed_row(source.GULF_LOCATIONS, row) for row in GULF_LOCATIONS),
                                source.TABLE_MEMBERS["gulf_locations"]), {"status": 200}),
        "pacific_masters": (_zip(_csv_body(source.PACIFIC_MASTERS, PACIFIC_MASTERS),
                                 source.TABLE_MEMBERS["pacific_masters"]), {"status": 200}),
        "pacific_structures": (_zip(_csv_body(source.PACIFIC_STRUCTURES, PACIFIC_STRUCTURES),
                                    source.TABLE_MEMBERS["pacific_structures"]), {"status": 200}),
        "pacific_locations": (_zip(_csv_body(source.PACIFIC_LOCATIONS, PACIFIC_LOCATIONS),
                                   source.TABLE_MEMBERS["pacific_locations"]), {"status": 200}),
    }


class OilAndNaturalGasPlatformsTests(unittest.TestCase):
    def test_composite_joins_preserve_native_values_and_defects(self):
        rows, audit = source._validate_capture(_captures())
        by_key = {(row["REGION"], row["COMPLEX_ID"], row["STRUCTURE_NUMBER"]): row for row in rows}
        self.assertEqual(len(rows), 6)
        self.assertEqual(audit["source_count_witness"], 6)
        self.assertEqual(audit["source_feature_count"], 6)
        self.assertTrue(audit["source_population_complete"])
        self.assertTrue(audit["source_id_roster_complete"])
        gulf = audit["source_counts"]["gulf"]
        self.assertEqual((gulf["masters"], gulf["structures"], gulf["locations"]), (3, 2, 4))
        self.assertEqual((gulf["structure_only_pairs"], gulf["location_only_pairs"]), (0, 2))
        self.assertEqual(gulf["masters_without_structures"], 1)
        self.assertEqual(gulf["null_coordinates"], 1)
        pacific = audit["source_counts"]["pacific"]
        self.assertEqual((pacific["masters"], pacific["structures"], pacific["locations"]), (2, 2, 2))
        self.assertEqual(pacific["zero_coordinates"], 1)
        self.assertEqual(audit["null_geometry_count"], 1)
        self.assertEqual(audit["zero_coordinate_count"], 1)
        self.assertEqual(audit["proposed_crs_epsg"], 4267)

        alpha = by_key[("Gulf of America", "1", "1")]
        self.assertEqual(alpha["STRUCTURE_NAME"], "ALPHA")
        self.assertEqual(alpha["MASTER_MMS_COMPANY_NUM"], "00231")
        self.assertEqual(alpha["MASTER_BED_COUNT"], "10")
        self.assertEqual(alpha["STRUCTURE_SLOT_COUNT"], "1")
        self.assertEqual(alpha["LOCATION_LONGITUDE"], "-90.0")
        self.assertIsNotNone(alpha["geometry"])

        gamma = by_key[("Gulf of America", "2", "2")]
        self.assertEqual(gamma["STRUCTURE_NAME"], "GAMMA")
        self.assertIsNone(gamma["STRUCTURE_SLOT_COUNT"])
        self.assertIsNotNone(gamma["geometry"])

        self.assertIsNone(by_key[("Gulf of America", "3", "1")]["geometry"])
        zero = by_key[("Pacific", "51001", "1")]
        self.assertEqual((zero["LOCATION_LONGITUDE"], zero["LOCATION_LATITUDE"]), ("0", "0"))
        self.assertIsNotNone(zero["geometry"])

    def test_parquet_body_roundtrips_all_fields(self):
        rows, _ = source._validate_capture(_captures())
        body = source._parquet_body(rows)
        table = pq.read_table(io.BytesIO(body))
        self.assertEqual(table.column_names, [*source.OUTPUT_FIELDS, "geometry"])
        self.assertEqual(table.num_rows, 6)
        self.assertEqual(table.column("MASTER_MMS_COMPANY_NUM").to_pylist().count("00231"), 1)

    def test_landing_and_layout_drift_rejected(self):
        captures = _captures()
        broken = dict(captures)
        body, receipt = broken["gulf_landing"]
        broken["gulf_landing"] = (body.replace(b"platmastfixed.zip", b"other.zip"), receipt)
        with self.assertRaisesRegex(ValueError, "source_landing_links_or_scope_drift"):
            source._validate_capture(broken)

        broken = dict(captures)
        body, receipt = broken["pacific_locations_layout"]
        broken["pacific_locations_layout"] = (
            body.replace(b"<td>108</td><td>15</td>", b"<td>108</td><td>14</td>"), receipt)
        with self.assertRaisesRegex(ValueError, "source_layout_position_drift"):
            source._validate_capture(broken)

    def test_duplicate_and_orphan_native_keys_rejected(self):
        captures = _captures()
        broken = dict(captures)
        duplicated = [*GULF_MASTERS, GULF_MASTERS[0]]
        broken["gulf_masters"] = (_zip("\n".join(_fixed_row(source.GULF_MASTERS, row) for row in duplicated),
                                       source.TABLE_MEMBERS["gulf_masters"]), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_duplicate_master_complex_id"):
            source._validate_capture(broken)

        broken = dict(captures)
        orphan = [*GULF_LOCATIONS, {"COMPLEX_ID_NUM": "9", "STRUCTURE_NUMBER": "1",
                                    "STRUCTURE_NAME": "ORPHAN", "LONGITUDE": "-80", "LATITUDE": "25"}]
        broken["gulf_locations"] = (_zip("\n".join(_fixed_row(source.GULF_LOCATIONS, row) for row in orphan),
                                         source.TABLE_MEMBERS["gulf_locations"]), {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_location_master_orphan"):
            source._validate_capture(broken)

    def test_table_shape_drift_rejected(self):
        captures = _captures()
        broken = dict(captures)
        broken["pacific_structures"] = (_zip(b'"LA"," 6660"\n', source.TABLE_MEMBERS["pacific_structures"]),
                                        {"status": 200})
        with self.assertRaisesRegex(ValueError, "source_delimited_field_count_drift"):
            source._validate_capture(broken)

        broken = dict(captures)
        body, receipt = broken["gulf_structures"]
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            member = archive.read(source.TABLE_MEMBERS["gulf_structures"])
        broken["gulf_structures"] = (_zip(member + b"X", source.TABLE_MEMBERS["gulf_structures"]), receipt)
        with self.assertRaisesRegex(ValueError, "source_fixed_width_row_overlong"):
            source._validate_capture(broken)

    def test_invalid_replay_is_quarantined_and_raises(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            replay = root / "capture"
            replay.mkdir()
            staging = root / "staging"
            staging.mkdir()
            for name, (body, _) in _captures().items():
                (replay / f"{name}.body").write_bytes(body)
                (replay / f"{name}.wire").write_bytes(body)
                digest = hashlib.sha256(body).hexdigest()
                (replay / f"{name}.receipt.json").write_text(json.dumps({
                    "method": "GET", "status": 200, "complete_body": True,
                    "bytes": len(body), "sha256": digest, "wire_bytes": len(body),
                    "wire_sha256": digest, "url": source.CAPTURE_URLS[name],
                    "final_url": source.CAPTURE_URLS[name],
                    "observed_at": "2026-11-18T00:00:00Z"}))
            bad = b"not a zip"
            (replay / "gulf_masters.body").write_bytes(bad)
            (replay / "gulf_masters.wire").write_bytes(bad)
            digest = hashlib.sha256(bad).hexdigest()
            receipt = json.loads((replay / "gulf_masters.receipt.json").read_text())
            receipt.update({"bytes": len(bad), "sha256": digest, "wire_bytes": len(bad), "wire_sha256": digest})
            (replay / "gulf_masters.receipt.json").write_text(json.dumps(receipt))
            storage = StagingStorageResource(use_local=True, local_dir=str(staging))
            with self.assertRaisesRegex(ValueError, "source_archive_not_a_zip"):
                source.capture_oil_and_natural_gas_platforms(
                    storage, version="draft", capture_directory=str(replay))
            self.assertTrue(list((staging / "_quarantine" / source.DATASET_SLUG).iterdir()))

    def test_roster_and_page_time_contracts(self):
        captures = _captures()
        with self.assertRaisesRegex(ValueError, "source_capture_roster_incomplete_or_unexpected"):
            source._validate_capture({k: v for k, v in captures.items() if k != "pacific_landing"})
        broken = dict(captures)
        body, receipt = broken["gulf_landing"]
        broken["gulf_landing"] = (body.replace(b"11/2/2026 10:02:22 AM", b"unknown"), receipt)
        with self.assertRaisesRegex(ValueError, "source_page_update_time_missing"):
            source._validate_capture(broken)


if __name__ == "__main__":
    unittest.main()
