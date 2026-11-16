"""Focused EIA Biodiesel Plants source-contract and replay checks."""
from __future__ import annotations

import contextlib
import hashlib
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import fiona
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import Point

from dagster_hifld.assets.eia_biodiesel_plants import (
    eia_biodiesel_plants_complete, eia_biodiesel_plants_draft_schedule,
)
from dagster_hifld.sources.eia_biodiesel_plants import (
    CAPACITY_REPORT_URL, DATASET_SLUG, EXPECTED_COUNT, EXPECTED_FIELDS, EXPECTED_LAYER,
    EXPECTED_ZIP_BYTES, EXPECTED_ZIP_SHA256, FILE_SLUG, ID_SHA, MEMBERS, SOURCE_RELEASE, SOURCE_URL,
    _decode, _load_capture, _point_wkb, _sha, _canonical,
)

_XML = (
    b"<metadata><Esri><CreaDate>20220203</CreaDate></Esri>"
    b"<dataIdInfo><idCitation><resTitle>Biodiesel_Plants_US_20210101</resTitle></idCitation>"
    b"<idAbs>Biodiesel plants in the United States as of January 1, 2021. Attribute data come from EIA, "
    b"U.S. Biodiesel Plant Production Capacity. Source: Form EIA-819. Locations determined through publicly "
    b"available information such as company websites and media reports.</idAbs>"
    b"<idCredit>U.S. Energy Information Administration</idCredit></dataIdInfo>"
    b"<Esri><itemProps><itemName>" + EXPECTED_LAYER.encode() + b"</itemName></itemProps></Esri></metadata>"
)


class EiaBiodieselPlantsSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "biodiesel-plants-1")
        self.assertEqual(SOURCE_URL, "https://www.eia.gov/maps/map_data/Biodiesel_Plants_US_EIA.zip")
        self.assertEqual(CAPACITY_REPORT_URL, "https://www.eia.gov/biofuels/capacity/")
        self.assertEqual(EXPECTED_COUNT, 75)
        self.assertEqual(SOURCE_RELEASE, "2021-01")
        self.assertEqual(EXPECTED_LAYER, "Biodiesel_Plants_US_20210101")
        self.assertEqual(EXPECTED_ZIP_BYTES, 12169)
        self.assertEqual(EXPECTED_ZIP_SHA256, "754e171fabbeaacd1cbebe9af65c8497fd85cff7ca08de3bd87bd0d7d07b5aa5")
        self.assertEqual(MEMBERS, {f"Biodiesel_Plants_US_20210101.{s}" for s in
                                   ("cpg", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx")})
        self.assertEqual(EXPECTED_FIELDS, [
            ("OBJECTID", "N", 10, 0), ("Company", "C", 254, 0), ("Site_Name", "C", 254, 0),
            ("Cap_Mmgal", "C", 254, 0), ("State", "C", 254, 0), ("Source", "C", 254, 0),
            ("Period", "C", 254, 0), ("Longitude", "F", 19, 11), ("Latitude", "F", 19, 11),
            ("PADD", "N", 10, 0),
        ])
        self.assertEqual(ID_SHA, "ca71ac0f54ac2844186941563c77ac9806fd819293fe64ed28da67535878fab7")

    def test_geometry_encoding_preserves_native_xy(self):
        self.assertEqual(_point_wkb(1.5, -2.25).hex(), "0101000000000000000000f83f00000000000002c0")

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(eia_biodiesel_plants_draft_schedule.default_status.value, "STOPPED")

    @staticmethod
    def _rows():
        return [
            {"geometry": {"type": "Point", "coordinates": (-87.6259, 33.0098)},
             "properties": {"OBJECTID": 1, "Company": "Hero Bx Alabama LLC", "Site_Name": "Moundville",
                            "Cap_Mmgal": "20", "State": "Alabama", "PADD": 3,
                            "Source": "Form EIA-819, Monthly Report of Biofuels, Fuels from Non-Biogenic Wastes, "
                                      "Fuel Oxygenates, Isooctane, and Isooctene",
                            "Period": "As of Jan. 1, 2021", "Longitude": -87.6259, "Latitude": 33.0098}},
            {"geometry": {"type": "Point", "coordinates": (-149.8616, 61.1638)},
             "properties": {"OBJECTID": 2, "Company": "Alaska Green Waste Solutions LLC", "Site_Name": "Anchorage",
                            "Cap_Mmgal": "less than 0.5", "State": "Alaska", "PADD": 5,
                            "Source": "Form EIA-819, Monthly Report of Biofuels, Fuels from Non-Biogenic Wastes, "
                                      "Fuel Oxygenates, Isooctane, and Isooctene",
                            "Period": "As of Jan. 1, 2021", "Longitude": -149.8616, "Latitude": 61.1638}},
        ]

    @contextlib.contextmanager
    def _fixture(self, rows=None):
        rows = self._rows() if rows is None else rows
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / f"{EXPECTED_LAYER}.shp"
            schema = {"geometry": "Point", "properties": {
                "OBJECTID": "int:10", "Company": "str:254", "Site_Name": "str:254", "Cap_Mmgal": "str:254",
                "State": "str:254", "Source": "str:254", "Period": "str:254",
                "Longitude": "float:19.11", "Latitude": "float:19.11", "PADD": "int:10"}}
            with fiona.open(path, "w", driver="ESRI Shapefile", crs="EPSG:4326", schema=schema,
                            encoding="UTF-8") as sink:
                for row in rows:
                    sink.write(row)
            (root / f"{EXPECTED_LAYER}.shp.xml").write_bytes(_XML)
            for missing in ("sbn", "sbx"):
                (root / f"{EXPECTED_LAYER}.{missing}").write_bytes(b"")
            archive = root / "biodiesel.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                for item in sorted(root.glob(f"{EXPECTED_LAYER}.*")):
                    if item != archive:
                        zf.write(item, item.name)
            body = archive.read_bytes()
            # The synthetic fixture carries different content than the pinned
            # historical archive, so its own observed DBF schema and content
            # baseline become the pin set for offline validation.
            import dagster_hifld.sources.eia_biodiesel_plants as src
            dbf = (root / f"{EXPECTED_LAYER}.dbf").read_bytes()
            header = int.from_bytes(dbf[8:10], "little")
            actual_schema = []
            for offset in range(32, header - 1, 32):
                descriptor = dbf[offset:offset + 32]
                actual_schema.append((descriptor[:11].split(b"\0", 1)[0].decode("ascii"),
                                      chr(descriptor[11]), descriptor[16], descriptor[17]))
            g = gpd.read_file(path, engine="pyogrio")
            names = [x[0] for x in actual_schema]
            attrs = [[v.item() if hasattr(v, "item") else v for v in row]
                     for row in g[names].itertuples(index=False, name=None)]
            ids = sorted(int(r[0]) for r in attrs)
            geom = [[int(r[0]), float(x), float(y)]
                    for r, x, y in zip(attrs, g.geometry.x, g.geometry.y)]
            bounds = list(map(float, g.total_bounds))
            with patch.multiple(src, EXPECTED_COUNT=len(rows), EXPECTED_ZIP_BYTES=len(body),
                                EXPECTED_ZIP_SHA256=_sha(body), EXPECTED_FIELDS=actual_schema,
                                ID_SHA=_sha(_canonical(ids)), RECORD_SHA=_sha(_canonical(attrs)),
                                GEOMETRY_SHA=_sha(_canonical(geom)), BOUNDS=bounds):
                yield src, root, body

    def test_tiny_native_capture_validates(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                frame, audit = src._decode(body, Path(tmp))
            self.assertEqual(len(frame), 2)
            self.assertEqual(audit["source_feature_count"], 2)
            self.assertEqual(audit["source_id_unique"], True)
            self.assertEqual(audit["source_id_min"], 1)
            self.assertEqual(audit["source_id_max"], 2)
            self.assertEqual(audit["source_crs"], "EPSG:4326")
            self.assertEqual(audit["source_geometry_null_count"], 0)
            self.assertEqual(audit["source_release"], "2021-01")
            self.assertEqual(audit["source_capacity_native_type"], "string")
            self.assertEqual(audit["source_xy_attribute_max_delta"], 0.0)

    def test_native_capacity_string_preserved_in_candidate(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                frame, _audit = src._decode(body, Path(tmp))
                parquet = src._parquet(frame)
            table = pq.read_table(pa.BufferReader(pa.py_buffer(parquet)))
            self.assertEqual(table["Cap_Mmgal"].type, pa.string())
            self.assertEqual(table["Cap_Mmgal"].to_pylist(), ["20", "less than 0.5"])
            self.assertEqual(table["PADD"].type, pa.int64())
            self.assertEqual(table["PADD"].to_pylist(), [3, 5])

    def test_pinned_archive_byte_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, \
                    patch.object(src, "EXPECTED_ZIP_SHA256", "0" * 64):
                with self.assertRaisesRegex(ValueError, "pinned_eia_snapshot_drift"):
                    src._decode(body, Path(tmp))

    def test_zip_member_roster_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                trimmed = Path(tmp) / "trimmed.zip"
                with zipfile.ZipFile(trimmed, "w") as zf:
                    for member in zipfile.ZipFile(root / "biodiesel.zip").namelist():
                        if not member.endswith(".sbn"):
                            zf.writestr(member, zipfile.ZipFile(root / "biodiesel.zip").read(member))
                with patch.object(src, "EXPECTED_ZIP_SHA256", _sha(trimmed.read_bytes())), \
                        patch.object(src, "EXPECTED_ZIP_BYTES", len(trimmed.read_bytes())):
                    with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
                        src._decode(trimmed.read_bytes(), Path(tmp))

    def test_embedded_metadata_contract_drift_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self._fixture() as (src, froot, body):
                with zipfile.ZipFile(froot / "biodiesel.zip") as zf:
                    zf.extractall(root)
                (root / f"{EXPECTED_LAYER}.shp.xml").write_bytes(_XML.replace(b"Form EIA-819", b"Form X"))
                rebuilt = root / "rebuilt.zip"
                with zipfile.ZipFile(rebuilt, "w") as zf:
                    for item in sorted(root.glob(f"{EXPECTED_LAYER}.*")):
                        if item != rebuilt:
                            zf.write(item, item.name)
                work = root / "work"
                work.mkdir()
                with patch.object(src, "EXPECTED_ZIP_SHA256", _sha(rebuilt.read_bytes())), \
                        patch.object(src, "EXPECTED_ZIP_BYTES", len(rebuilt.read_bytes())):
                    with self.assertRaisesRegex(ValueError, "source_embedded_metadata_contract_drift"):
                        src._decode(rebuilt.read_bytes(), work)

    def test_data_period_drift_rejected(self):
        rows = self._rows()
        retitled = [rows[0], dict(rows[1], properties=dict(rows[1]["properties"], Period="As of Jan. 1, 2024"))]
        with self._fixture(rows=retitled) as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_data_period_drift"):
                    src._decode(body, Path(tmp))

    def test_duplicate_source_identifier_rejected(self):
        rows = self._rows()
        duplicated = [rows[0], dict(rows[1], properties=dict(rows[1]["properties"], OBJECTID=1))]
        with self._fixture(rows=duplicated) as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaisesRegex(ValueError, "source_id_drift"):
                    src._decode(body, Path(tmp))

    def test_xy_attribute_divergence_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp:
                shifted = gpd.read_file(root / f"{EXPECTED_LAYER}.shp", engine="pyogrio")
                shifted.geometry = [Point(x + 1e-5, y) for x, y in zip(shifted.geometry.x, shifted.geometry.y)]
                with patch.object(src.gpd, "read_file", lambda *a, **k: shifted):
                    with self.assertRaisesRegex(ValueError, "source_xy_attribute_drift"):
                        src._decode(body, Path(tmp))

    def test_content_snapshot_drift_rejected(self):
        with self._fixture() as (src, root, body):
            with tempfile.TemporaryDirectory() as tmp, patch.object(src, "RECORD_SHA", "1" * 64):
                with self.assertRaisesRegex(ValueError, "source_content_snapshot_drift"):
                    src._decode(body, Path(tmp))

    def test_replay_receipt_requires_exact_get_origin(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            body = b"retained source bytes"
            digest = hashlib.sha256(body).hexdigest()
            for name in ("capacity_page", "eia_zip"):
                for suffix in ("body", "wire"):
                    (root / f"{name}.{suffix}").write_bytes(body)
                receipt = {"method": "GET", "status": 200, "complete_body": True,
                           "bytes": len(body), "sha256": digest, "wire_bytes": len(body),
                           "wire_sha256": digest, "url": SOURCE_URL if name == "eia_zip" else CAPACITY_REPORT_URL,
                           "final_url": SOURCE_URL if name == "eia_zip" else CAPACITY_REPORT_URL}
                (root / f"{name}.receipt.json").write_text(json.dumps(receipt))
                self.assertEqual(_load_capture(name, root)[0], body)
                for changes in ({"url": "https://example.org/mirror.zip", "final_url": "https://example.org/mirror.zip"},
                                {"method": "POST"}, {"final_url": "https://www.eia.gov/maps/redirect.zip"},
                                {"complete_body": False}):
                    with self.subTest(name=name, changes=changes):
                        (root / f"{name}.receipt.json").write_text(json.dumps({**receipt, **changes}))
                        with self.assertRaisesRegex(ValueError, "capture_receipt_invalid"):
                            _load_capture(name, root)
                    (root / f"{name}.receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "unexpected_source_capture_name"):
                _load_capture("unknown", root)


class EiaBiodieselPlantsRegistrationTest(unittest.TestCase):
    def test_ingest_asset_job_check_and_schedule_registered_in_defs(self):
        from dagster import AssetKey, DefaultScheduleStatus

        from dagster_hifld.definitions import defs

        key = AssetKey(["ingest", DATASET_SLUG, FILE_SLUG])
        graph = defs.resolve_asset_graph()
        self.assertIn(key, set(graph.get_all_asset_keys()))
        repo = defs.get_repository_def()
        job = repo.get_job("eia_biodiesel_plants_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {key})
        check_keys = {str(check_key) for check_key in graph.asset_check_keys}
        self.assertTrue(any("eia_biodiesel_plants_complete" in check_key for check_key in check_keys))
        self.assertTrue(next(iter(eia_biodiesel_plants_complete.check_specs)).blocking)
        schedule = repo.get_schedule_def("eia_biodiesel_plants_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.cron_schedule, "0 8 1 * *")


if __name__ == "__main__":
    unittest.main()
