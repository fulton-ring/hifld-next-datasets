"""Focused EIA Ethanol Plants source-contract and replay checks."""
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
from shapely.geometry import Point

from dagster_hifld.assets.eia_ethanol_plants import eia_ethanol_plants_draft_schedule
from dagster_hifld.sources.eia_ethanol_plants import (
    CAPACITY_REPORT_URL, DATASET_SLUG, EXPECTED_COUNT, EXPECTED_FIELDS, EXPECTED_LAYER,
    EXPECTED_ZIP_BYTES, EXPECTED_ZIP_SHA256, ID_SHA, MEMBERS, SOURCE_RELEASE, SOURCE_URL,
    _decode, _load_capture, _point_wkb, _sha, _canonical,
)

_PAGE = (b"U.S. Fuel Ethanol Plant Production Capacity EIA-819, Monthly Report of Biofuels")
_XML = (
    b"<metadata><Esri><CreaDate>20220203</CreaDate></Esri>"
    b"<dataIdInfo><idCitation><resTitle>Ethanol Plants in the United States</resTitle></idCitation>"
    b"<idAbs>Operating fuel ethanol production plants in the United States as of January 1, 2021. "
    b"Attribute data come from EIA, U.S. Fuel Ethanol Plant Production Capacity. "
    b"Source: Form EIA-819. Locations determined through publicly available information.</idAbs>"
    b"<idCredit>U.S. Energy Information Administration</idCredit></dataIdInfo>"
    b"<Esri><itemProps><itemName>" + EXPECTED_LAYER.encode() + b"</itemName></itemProps></Esri></metadata>"
)


class EiaEthanolPlantsSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_complete_native_contract(self):
        self.assertEqual(DATASET_SLUG, "ethanol-plants")
        self.assertEqual(SOURCE_URL, "https://www.eia.gov/maps/map_data/Ethanol_Plants_US_EIA.zip")
        self.assertEqual(CAPACITY_REPORT_URL, "https://www.eia.gov/petroleum/ethanolcapacity/")
        self.assertEqual(EXPECTED_COUNT, 197)
        self.assertEqual(SOURCE_RELEASE, "2021-01")
        self.assertEqual(EXPECTED_LAYER, "Ethanol_Plants_US_20210101")
        self.assertEqual(EXPECTED_ZIP_BYTES, 37644)
        self.assertEqual(EXPECTED_ZIP_SHA256, "a342f4af270f48d8512ad18e393eee65394f831cc045044d85790cfae77f829c")
        self.assertEqual(MEMBERS, {f"Ethanol_Plants_US_20210101.{s}" for s in
                                   ("cpg", "dbf", "prj", "sbn", "sbx", "shp", "shp.xml", "shx")})
        self.assertEqual(EXPECTED_FIELDS, [
            ("OBJECTID", "N", 10, 0), ("Company", "C", 254, 0), ("Site_Name", "C", 254, 0),
            ("Capacity", "N", 10, 0), ("State", "C", 254, 0), ("PADD", "N", 10, 0),
            ("Data_Perio", "C", 254, 0), ("Source", "C", 254, 0),
            ("Longitude", "F", 19, 11), ("Latitude", "F", 19, 11),
        ])
        self.assertEqual(ID_SHA, "b3de5218d8feaa9eec1e5f9cd6f585de1e9192a44ce80bb6e7b54bebedb07c4a")

    def test_geometry_encoding_preserves_native_xy(self):
        self.assertEqual(_point_wkb(1.5, -2.25).hex(), "0101000000000000000000f83f00000000000002c0")

    def test_refresh_schedule_is_stopped(self):
        self.assertEqual(eia_ethanol_plants_draft_schedule.default_status.value, "STOPPED")

    @staticmethod
    def _rows():
        return [
            {"geometry": {"type": "Point", "coordinates": (-112.0033, 33.0228)},
             "properties": {"OBJECTID": 1, "Company": "Pinal Energy LLC", "Site_Name": "Maricopa", "Capacity": 55,
                            "State": "Arizona", "PADD": 5, "Data_Perio": "As of January 1, 2021",
                            "Source": "Form EIA-819", "Longitude": -112.0033, "Latitude": 33.0228}},
            {"geometry": {"type": "Point", "coordinates": (-96.2231, 43.649)},
             "properties": {"OBJECTID": 2, "Company": "Agri-Energy LLC", "Site_Name": "Luverne", "Capacity": 24,
                            "State": "Minnesota", "PADD": 2, "Data_Perio": "As of January 1, 2021",
                            "Source": "Form EIA-819", "Longitude": -96.2231, "Latitude": 43.649}},
        ]

    @contextlib.contextmanager
    def _fixture(self, rows=None):
        rows = self._rows() if rows is None else rows
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / f"{EXPECTED_LAYER}.shp"
            schema = {"geometry": "Point", "properties": {
                "OBJECTID": "int:10", "Company": "str:254", "Site_Name": "str:254", "Capacity": "int:10",
                "State": "str:254", "PADD": "int:10", "Data_Perio": "str:254", "Source": "str:254",
                "Longitude": "float:19.11", "Latitude": "float:19.11"}}
            with fiona.open(path, "w", driver="ESRI Shapefile", crs="EPSG:4326", schema=schema,
                            encoding="UTF-8") as sink:
                for row in rows:
                    sink.write(row)
            (root / f"{EXPECTED_LAYER}.shp.xml").write_bytes(_XML)
            for missing in ("sbn", "sbx"):
                (root / f"{EXPECTED_LAYER}.{missing}").write_bytes(b"")
            archive = root / "ethanol.zip"
            with zipfile.ZipFile(archive, "w") as zf:
                for item in sorted(root.glob(f"{EXPECTED_LAYER}.*")):
                    if item != archive:
                        zf.write(item, item.name)
            body = archive.read_bytes()
            # The synthetic fixture carries different content than the pinned
            # historical archive, so its own observed DBF schema and content
            # baseline become the pin set for offline validation.
            import dagster_hifld.sources.eia_ethanol_plants as src
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
            self.assertEqual(audit["source_xy_attribute_max_delta"], 0.0)

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
                    for member in zipfile.ZipFile(root / "ethanol.zip").namelist():
                        if not member.endswith(".sbn"):
                            zf.writestr(member, zipfile.ZipFile(root / "ethanol.zip").read(member))
                with patch.object(src, "EXPECTED_ZIP_SHA256", _sha(trimmed.read_bytes())), \
                        patch.object(src, "EXPECTED_ZIP_BYTES", len(trimmed.read_bytes())):
                    with self.assertRaisesRegex(ValueError, "source_zip_member_roster_drift"):
                        src._decode(trimmed.read_bytes(), Path(tmp))

    def test_embedded_metadata_contract_drift_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self._fixture() as (src, froot, body):
                with zipfile.ZipFile(froot / "ethanol.zip") as zf:
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


if __name__ == "__main__":
    unittest.main()
