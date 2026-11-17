"""Synthetic-fixture tests for the agricultural-minerals USGS mineplant draft pipeline.

The fixtures reconstruct the producer package layout at a small scale; they
exercise the strict decode, capture, candidate and quarantine paths without
granting any source, schema, baseline, or release acceptance. The real pinned
captures are exercised opt-in through the capture archive artifact.
"""
from __future__ import annotations

import io
import json
import struct
import tarfile
import tempfile
import unittest
import zipfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources import agricultural_minerals as m
from dagster_hifld.sources.agricultural_minerals import (
    AGRICULTURAL_COMMODITIES, AGRICULTURAL_ROWS, ATLAS_LEGACY_FGDC_URL, DATASET_SLUG,
    EXPECTED_COUNT, EXPECTED_FIELDS, FIELD_NAMES, FILE_SLUG, ID_SHA, MINEPLANT_FGDC_URL,
    MINEPLANT_ZIP_BYTES, MINEPLANT_ZIP_SHA256, RECORD_SHA, SOURCE_URL, _canonical, _sha,
)

NAD27_WKT = ('GEOGCS["GCS_North_American_1927",DATUM["D_North_American_1927",SPHEROID["Clarke_1866", '
             '6378206.4, 294.9786982138982, AUTHORITY["EPSG","7008"]],TOWGS84[-9.0, 151.0, 185.0, 0.0, 0.0, '
             '0.0, 0.0],AUTHORITY["EPSG","6267"]],PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]],'
             'UNIT["degree", 0.017453292519943295],AXIS["Lon", EAST],AXIS["Lat", NORTH],'
             'AUTHORITY["EPSG","4267"]]')

MINEPLANT_FIELDS = EXPECTED_FIELDS
# (commodity, site, company, state, county, latitude_text, longitude_text, plant_min)
FIXTURE_ROWS = [
    ("Magnesium Compounds", "Silver Creek Plant", "Desert Minerals LLC", "Nevada", "Lyon",
     "39.20135", "-119.70215", "P"),
    ("Magnesium Compounds", "Great Salt Lake Pond", "Utah Brine Corp", "Utah", "Tooele",
     "40.71230", "-112.53940", "M/P"),
    ("Peat", "North Bog Operation", "Boreal Peat Co", "Minnesota", "Lake",
     "47.91005", "-91.64455", "M"),
    ("Peat", "Cranberry Marsh Works", "Marsh LLC", "Wisconsin", "Juneau",
     "43.90515", "-90.12235", "M"),
    ("Phosphate", "Bone Valley Mine", "Phos FL Inc", "Florida", "Polk",
     "27.91285", "-81.77065", "M"),
    ("Potash", "Carlsbad Shaft", "NM Potash LLC", "New Mexico", "Eddy",
     "32.31245", "-104.23475", "M/P"),
    ("Sulfur", "Gulf Frasch Plant", "Gulf Sulfur Inc", "Texas", "Brazoria",
     "28.93735", "-95.36585", "P"),
    ("Vermiculite", "Libby Annex", "Montana Vermiculite", "Montana", "Lincoln",
     "48.24495", "-115.54625", "M"),
    ("Aluminum", "Mobile Smelter", "Gulf Aluminum", "Alabama", "Mobile",
     "30.61255", "-88.05665", "P"),
    ("Gold", "Carlin Pit", "NV Gold Inc", "Nevada", "Eureka",
     "40.91215", "-116.21945", "M"),
]
FIXTURE_COUNT = len(FIXTURE_ROWS)
FIXTURE_SUBSET_ROWS = 8
FIXTURE_STATES = 8
FIXTURE_COMMODITY_COUNTS = {"Magnesium Compounds": 2, "Peat": 2, "Phosphate": 1, "Potash": 1,
                            "Sulfur": 1, "Vermiculite": 1}
FIXTURE_MINEPLANT_FGDC = (
    b"<?xml version='1.0' encoding='UTF-8'?><metadata>"
    b"<idinfo><citation><citeinfo><title>Active Mines and Mineral Processing Plants in the "
    b"United States in 2003</title><pubdate>2005</pubdate></citeinfo></citation>"
    b"<spdinfo><accconst>None</accconst><useconst>None. Acknowledgment of the National Minerals "
    b"Information Center appreciated.</useconst></spdinfo></idinfo>"
    b"<spref><horizcoordd><horizdn>North American Datum of 1927</horizdn>"
    b"<coordrep><absres>0.0001</absres><ordres>0.0001</ordres></coordrep>"
    b"<geogunit>Decimal degrees</geogunit></horizcoordd></spref></metadata>"
)
FIXTURE_ATLAS_FGDC = (
    b"<?xml version='1.0' encoding='UTF-8'?><metadata>"
    b"<idinfo><citation><citeinfo><title>Agricultural Minerals Operations - Direct Download</title>"
    b"<pubdate>200512</pubdate></citeinfo></citation></idinfo>"
    b"<spref><horizcoordd><horizdn>North American Datum of 1983</horizdn>"
    b"<geogunit>Decimal degrees</geogunit></horizcoordd></spref></metadata>"
)


def _field(name, kind, width, scale):
    descriptor = name.encode("ascii")[:10].ljust(11, b"\0") + kind.encode("ascii") + b"\0" * 4
    return descriptor + bytes([width, scale]) + b"\0" * 14


def _dbf_number(value, width, scale):
    text = value if isinstance(value, str) else (
        f"{value:.{scale}f}" if scale else str(int(value)))
    return text.rjust(width).encode("ascii")[:width]


def dbf_bytes(rows):
    width = 1 + sum(field[2] for field in MINEPLANT_FIELDS)
    header = 32 + 32 * len(MINEPLANT_FIELDS) + 1
    body = b"\x03" + b"\0" * 3 + struct.pack("<I", len(rows)) + struct.pack("<H", header)
    body += struct.pack("<H", width) + b"\0" * 20
    for field in MINEPLANT_FIELDS:
        body += _field(*field)
    body += b"\x0d"
    for position, row in enumerate(rows, 1):
        record = b" "
        values = {"id": position, "COMMODITY": row[0], "SITE_NAME": row[1],
                  "COMPANY_NA": "" if row[1] == "North Bog Operation" else row[2],
                  "STATE_LOCA": row[3], "COUNTY": row[4], "LATITUDE": row[5],
                  "LONGITUDE": row[6], "PLANT_MIN": row[7]}
        for name, kind, size, scale in MINEPLANT_FIELDS:
            value = values[name]
            record += (_dbf_number(value, size, scale) if kind == "N"
                       else value.encode("ascii").ljust(size)[:size])
        body += record
    return body


def _shapefile_header(point_count, points):
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    header = struct.pack(">iiiiii", 9994, 0, 0, 0, 0, 0)
    header += struct.pack(">i", (100 + 28 * point_count) // 2)
    header += struct.pack("<ii", 1000, 1)
    header += struct.pack("<4d", min(xs), min(ys), max(xs), max(ys))
    header += struct.pack("<4d", 0.0, 0.0, 0.0, 0.0)
    return header


def shp_bytes(points):
    body = _shapefile_header(len(points), points)
    for index, (x, y) in enumerate(points, 1):
        body += struct.pack(">ii", index, 10) + struct.pack("<idd", 1, x, y)
    return body


def shx_bytes(points):
    header = struct.pack(">iiiiii", 9994, 0, 0, 0, 0, 0)
    header += struct.pack(">i", (100 + 8 * len(points)) // 2)
    header += struct.pack("<ii", 1000, 1)
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    header += struct.pack("<4d", min(xs), min(ys), max(xs), max(ys))
    header += struct.pack("<4d", 0.0, 0.0, 0.0, 0.0)
    offset = 50
    for _ in points:
        header += struct.pack(">ii", offset, 10)
        offset += 14
    return header


def txt_bytes(rows):
    lines = ["\t".join(m.TXT_HEADER)]
    for position, row in enumerate(rows, 1):
        cells = [str(position), row[0], row[1],
                 "NULL" if row[1] == "North Bog Operation" else row[2],
                 row[3], row[4], row[5], row[6], row[7]]
        lines.append("\t".join(cells))
    return ("\n".join(lines) + "\n").encode("ascii")


def package_bytes(rows=None, prj=NAD27_WKT):
    rows = rows or FIXTURE_ROWS
    points = [(float(row[6]), float(row[5])) for row in rows]
    members = {
        "mineplant.dbf": dbf_bytes(rows),
        "mineplant.shp": shp_bytes(points),
        "mineplant.shx": shx_bytes(points),
        "mineplant.txt": txt_bytes(rows),
        "mineplant.prj": prj.encode("ascii"),
    }
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, data in sorted(members.items()):
            archive.writestr(name, data)
    return stream.getvalue()


def _receipt(body, wire, url):
    return {"method": "GET", "url": url, "final_url": url, "status": 200,
            "complete_body": True, "bytes": len(body), "sha256": _sha(body),
            "wire_bytes": len(wire), "wire_sha256": _sha(wire),
            "observed_at": "2026-11-15T00:00:00+00:00"}


def _sealed_capture_dir(root: Path) -> Path:
    root.mkdir(parents=True)
    package = package_bytes()
    bodies = {"mineplant_zip": package, "mineplant_fgdc": FIXTURE_MINEPLANT_FGDC,
              "atlas_legacy_fgdc": FIXTURE_ATLAS_FGDC}
    wires = {name: f"wire:{name}".encode() for name in bodies}
    receipts = {name: _receipt(bodies[name], wires[name], m._EXPECTED_URLS[name]) for name in bodies}
    for name in bodies:
        (root / f"{name}.body").write_bytes(bodies[name])
        (root / f"{name}.wire").write_bytes(wires[name])
        (root / f"{name}.receipt.json").write_bytes(_canonical(receipts[name]) + b"\n")
    (root / "capture_manifest.json").write_bytes(_canonical({
        "complete": True, "captured_at": "2026-11-15T00:00:01+00:00",
        "receipts": receipts, "artifact_roster": m.capture_roster_digests(
            root, exclude={"capture_manifest.json"})}) + b"\n")
    return root


class _PatchedContext:
    """Patch every content pin to the synthetic fixture's observed values."""

    def __init__(self, package=None, extra_patches=None):
        self.package = package_bytes() if package is None else package
        self.extra_patches = dict(extra_patches or {})

    def __enter__(self):
        bodies = {"mineplant_zip": self.package, "mineplant_fgdc": FIXTURE_MINEPLANT_FGDC,
                  "atlas_legacy_fgdc": FIXTURE_ATLAS_FGDC}
        pins = {name: {"bytes": len(body), "sha256": _sha(body)} for name, body in bodies.items()}
        base = {
            "MINEPLANT_ZIP_BYTES": pins["mineplant_zip"]["bytes"],
            "MINEPLANT_ZIP_SHA256": pins["mineplant_zip"]["sha256"],
            "MINEPLANT_FGDC_BYTES": pins["mineplant_fgdc"]["bytes"],
            "MINEPLANT_FGDC_SHA256": pins["mineplant_fgdc"]["sha256"],
            "ATLAS_LEGACY_FGDC_BYTES": pins["atlas_legacy_fgdc"]["bytes"],
            "ATLAS_LEGACY_FGDC_SHA256": pins["atlas_legacy_fgdc"]["sha256"],
            "PINS": pins,
            "EXPECTED_COUNT": FIXTURE_COUNT,
            "AGRICULTURAL_COMMODITY_COUNTS": FIXTURE_COMMODITY_COUNTS,
            "AGRICULTURAL_ROWS": FIXTURE_SUBSET_ROWS,
            "AGRICULTURAL_STATES": FIXTURE_STATES,
            "processing_identity": lambda repo_root: {
                "code_commit": "0" * 40, "code_identity_scheme": "test-fixed-identity", "files": {}},
            "validate_construction_code_identity": lambda *args, **kwargs: None,
        }
        import contextlib
        with contextlib.ExitStack() as stack:
            # Decode first to derive the fixture's own content pins.
            stack.enter_context(patch.multiple(m, **{**base, **self.extra_patches}))
            audit = m._decode(self.package)[4]
            base.update({"ID_SHA": audit["source_id_sha256"],
                         "RECORD_SHA": audit["source_record_sha256"],
                         "GEOMETRY_SHA": audit["source_geometry_sha256"],
                         "BOUNDS": audit["source_bounds"]})
        self.exit_stack = contextlib.ExitStack()
        self.exit_stack.enter_context(patch.multiple(m, **{**base, **self.extra_patches}))
        from dagster_hifld.sources import capture_common
        self.exit_stack.enter_context(patch.object(capture_common, "validate_construction_code_identity",
                                                   lambda *args, **kwargs: None))
        return self

    def __exit__(self, *exc_info):
        self.exit_stack.close()
        return False


class AgriculturalMineralsSourceContractTest(unittest.TestCase):
    def test_pinned_source_and_assigned_scope(self):
        self.assertEqual(DATASET_SLUG, "agricultural-minerals-operations")
        self.assertEqual(SOURCE_URL, "https://mrdata.usgs.gov/mineplant/")
        self.assertEqual(MINEPLANT_FGDC_URL, "https://mrdata.usgs.gov/metadata/mineplant.xml")
        self.assertEqual(ATLAS_LEGACY_FGDC_URL,
                         "https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop1x020.xml")
        self.assertEqual(MINEPLANT_ZIP_BYTES, 513191)
        self.assertEqual(MINEPLANT_ZIP_SHA256,
                         "6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e")
        self.assertEqual(EXPECTED_COUNT, 6785)
        self.assertEqual(AGRICULTURAL_ROWS, 236)
        self.assertEqual(m.AGRICULTURAL_STATES, 37)
        self.assertEqual(m.AGRICULTURAL_COMMODITY_COUNTS,
                         {"Magnesium Compounds": 11, "Peat": 52, "Phosphate": 31, "Potash": 7,
                          "Sulfur": 113, "Vermiculite": 22})
        self.assertEqual(AGRICULTURAL_COMMODITIES, tuple(sorted(
            m.AGRICULTURAL_COMMODITY_COUNTS, key=m.AGRICULTURAL_COMMODITIES.index)))
        self.assertEqual(ID_SHA, "cd88665adeef2a48dc7a6f514b3f7f52fa79cec5560cdc9e3c4d8487b7afb644")
        self.assertEqual(RECORD_SHA,
                         "09bbd54dfd55ffe54fc87de2d8ec2d7cebfecfc508e705e8d709fdc5f6460c34")
        self.assertEqual(m.GEOMETRY_SHA,
                         "8569acb27f34c408eeb214a4be2282eeef638f0181ed3079e6e277afdcf56ac5")
        self.assertEqual(m.BOUNDS, [-122.69, 26.243, -67.659, 48.858])


class AgriculturalMineralsStageTests(unittest.TestCase):
    def stage(self, storage, capture_directory=None):
        return m.capture_agricultural_minerals(storage, capture_directory=capture_directory)

    def test_explicit_local_storage_required_before_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            for resource in (PublishedStorageResource(local_dir=tmp),
                             StagingStorageResource(bucket="forbidden", use_local=False),
                             StagingStorageResource(local_dir=tmp, prefix="no")):
                with self.assertRaisesRegex(ValueError, "staging_resource"):
                    self.stage(resource)
            with self.assertRaisesRegex(ValueError, "unsafe_local_staging_root"):
                self.stage(StagingStorageResource(local_dir=tmp + "/../unsafe"))

    def test_full_stage_replay_values_types_ids_and_geometry(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = _sealed_capture_dir(Path(tmp) / "captures")
            with _PatchedContext():
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                result = self.stage(storage, str(capture_dir))
                self.assertFalse(result["reused"])
                self.assertEqual(result["feature_count"], FIXTURE_SUBSET_ROWS)
                self.assertTrue(result["source_population_complete"])
                self.assertFalse(result["release_accepted"])
                root = Path(tmp) / "stage" / DATASET_SLUG / FILE_SLUG / result["version"]
                table = pq.read_table(root / f"geoparquet/{FILE_SLUG}.parquet")
                self.assertEqual(table.num_rows, FIXTURE_SUBSET_ROWS)
                self.assertEqual(table["id"].type, __import__("pyarrow").decimal128(8, 0))
                self.assertEqual(table["LATITUDE"].type, __import__("pyarrow").decimal128(10, 5))
                self.assertEqual(table["LONGITUDE"].type, __import__("pyarrow").decimal128(10, 5))
                self.assertEqual(table["COMMODITY"].to_pylist(),
                                 [row[0] for row in FIXTURE_ROWS[:8]])
                self.assertEqual([str(value) for value in table["LATITUDE"].to_pylist()],
                                 [row[5] for row in FIXTURE_ROWS[:8]])
                self.assertEqual(table["COMPANY_NA"].to_pylist()[2], "")
                import shapely
                geometry = shapely.from_wkb(table["geometry"].to_pylist())
                self.assertEqual([(point.x, point.y) for point in geometry],
                                 [(float(row[6]), float(row[5])) for row in FIXTURE_ROWS[:8]])
                geo = json.loads(table.schema.metadata[b"geo"])
                self.assertEqual(geo["columns"]["geometry"]["geometry_types"], ["Point"])
                self.assertIn("1927", json.dumps(geo["columns"]["geometry"]["crs"]))
                self.assertEqual(geo["columns"]["geometry"]["bbox"], m.BOUNDS)
                audit = json.loads((root / "metadata/agricultural_minerals_source_audit.json").read_bytes())
                self.assertEqual(audit["agricultural_commodities"], FIXTURE_COMMODITY_COUNTS)
                self.assertFalse(audit["source_release_accepted"])
                self.assertIn("North American Datum of 1927", audit["mineplant_fgdc"]["horizontal_datum"])
                self.assertIn("North American Datum of 1983", audit["atlas_legacy_fgdc"]["horizontal_datum"])
                manifest = json.loads((root / "metadata/source_manifest.json").read_bytes())
                self.assertFalse(manifest["approved"])
                self.assertEqual([item["role"] for item in manifest["inputs"]],
                                 ["producer_publication"] * 3)
                self.assertTrue(manifest["prebuilt_output_policy"]["formats"]["geoparquet"]["objects"])
                self.assertIn("pmtiles", manifest["unsupported_output_formats"])
                ready = json.loads((root / "metadata/candidate_ready.json").read_bytes())
                self.assertFalse(ready["approved"])
                self.assertTrue(ready["source_population_complete"])
                raw_names = {path.name for path in (root / "raw").iterdir()}
                self.assertEqual(raw_names, {f"{name}.{part}" for name in m._EXPECTED_URLS
                                             for part in ("body", "wire", "receipt.json")})

    def test_second_stage_reuses_and_tamper_is_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = _sealed_capture_dir(Path(tmp) / "captures")
            with _PatchedContext():
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                first = self.stage(storage, str(capture_dir))
                second = self.stage(storage, str(capture_dir))
                self.assertTrue(second["reused"])
                self.assertEqual(first["version"], second["version"])
                prefix = DATASET_SLUG + "/" + FILE_SLUG + "/" + first["version"]
                target = Path(tmp) / "stage" / prefix / f"geoparquet/{FILE_SLUG}.parquet"
                original = target.read_bytes()
                target.write_bytes(original + b"x")
                with self.assertRaisesRegex(ValueError, "cached_candidate"):
                    self.stage(storage, str(capture_dir))
                target.write_bytes(original)
                self.assertTrue(self.stage(storage, str(capture_dir))["reused"])

    def test_reuse_after_receipt_timestamp_change_keeps_sealed_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = _sealed_capture_dir(Path(tmp) / "captures")
            with _PatchedContext():
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                first = self.stage(storage, str(capture_dir))
                prefix = DATASET_SLUG + "/" + FILE_SLUG + "/" + first["version"]
                sealed_receipt = json.loads(
                    (Path(tmp) / "stage" / prefix / "raw/mineplant_zip.receipt.json").read_bytes())
                receipt_path = capture_dir / "mineplant_zip.receipt.json"
                receipt = json.loads(receipt_path.read_bytes())
                receipt["observed_at"] = "2026-11-15T12:00:00+00:00"
                receipt_path.write_bytes(_canonical(receipt) + b"\n")
                manifest_path = capture_dir / "capture_manifest.json"
                manifest = json.loads(manifest_path.read_bytes())
                manifest["receipts"]["mineplant_zip"] = receipt
                manifest["artifact_roster"] = m.capture_roster_digests(capture_dir,
                                                                       exclude={"capture_manifest.json"})
                manifest_path.write_bytes(_canonical(manifest) + b"\n")
                second = self.stage(storage, str(capture_dir))
                self.assertTrue(second["reused"])
                self.assertEqual(second["version"], first["version"])
                # The candidate evidence stays the sealed capture, not the new receipt.
                stored = json.loads((Path(tmp) / "stage" / prefix / "raw/mineplant_zip.receipt.json").read_bytes())
                self.assertEqual(stored, sealed_receipt)

    def test_unsealed_capture_directory_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = Path(tmp) / "captures"
            capture_dir.mkdir()
            (capture_dir / "mineplant_zip.body").write_bytes(b"stray")
            with _PatchedContext():
                with self.assertRaisesRegex(ValueError, "unsealed_capture_attempt"):
                    self.stage(StagingStorageResource(local_dir=str(Path(tmp) / "stage")), str(capture_dir))

    def test_capture_roster_and_receipt_drift_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = _sealed_capture_dir(Path(tmp) / "captures")
            (capture_dir / "extra.txt").write_bytes(b"rogue")
            with _PatchedContext():
                with self.assertRaisesRegex(ValueError, "capture_roster"):
                    self.stage(StagingStorageResource(local_dir=str(Path(tmp) / "stage")), str(capture_dir))
            (capture_dir / "extra.txt").unlink()
            body_path = capture_dir / "mineplant_zip.body"
            body_path.write_bytes(body_path.read_bytes()[:-1])
            manifest_path = capture_dir / "capture_manifest.json"
            manifest = json.loads(manifest_path.read_bytes())
            manifest["artifact_roster"] = m.capture_roster_digests(capture_dir,
                                                                   exclude={"capture_manifest.json"})
            manifest_path.write_bytes(_canonical(manifest) + b"\n")
            with _PatchedContext():
                with self.assertRaisesRegex(ValueError, "capture_receipt_invalid"):
                    self.stage(StagingStorageResource(local_dir=str(Path(tmp) / "stage")), str(capture_dir))

    def test_live_pin_drift_fails_closed(self):
        package = package_bytes()
        with _PatchedContext(package=package):
            with tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
                drifted = package[:-1]

                def fake_capture_get(client, url, root, name, max_bytes):
                    body = drifted if name == "mineplant_zip" else {
                        "mineplant_fgdc": FIXTURE_MINEPLANT_FGDC,
                        "atlas_legacy_fgdc": FIXTURE_ATLAS_FGDC}[name]
                    wire = f"wire:{name}".encode()
                    (root / f"{name}.body").write_bytes(body)
                    (root / f"{name}.wire").write_bytes(wire)
                    receipt = _receipt(body, wire, m._EXPECTED_URLS[name])
                    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                    return body, receipt

                with patch.object(m, "capture_get", side_effect=fake_capture_get):
                    with self.assertRaisesRegex(ValueError, "pinned_live_capture_drift"):
                        self.stage(storage)

    def test_live_capture_seals_and_stages(self):
        package = package_bytes()
        with _PatchedContext(package=package):
            with tempfile.TemporaryDirectory() as tmp:
                storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))

                def fake_capture_get(client, url, root, name, max_bytes):
                    body = {"mineplant_zip": package, "mineplant_fgdc": FIXTURE_MINEPLANT_FGDC,
                            "atlas_legacy_fgdc": FIXTURE_ATLAS_FGDC}[name]
                    wire = f"wire:{name}".encode()
                    (root / f"{name}.body").write_bytes(body)
                    (root / f"{name}.wire").write_bytes(wire)
                    receipt = _receipt(body, wire, url)
                    (root / f"{name}.receipt.json").write_bytes(_canonical(receipt) + b"\n")
                    return body, receipt

                with patch.object(m, "capture_get", side_effect=fake_capture_get):
                    result = self.stage(storage)
                self.assertFalse(result["reused"])
                live_roots = list((Path("/tmp/hifld-native-reconstruction/7/captures")).iterdir())
                manifests = [json.loads((path / "capture_manifest.json").read_bytes())
                             for path in live_roots if (path / "capture_manifest.json").is_file()]
                self.assertTrue(manifests)
                self.assertTrue(any(manifest["complete"] is True for manifest in manifests))

    def test_decode_failure_paths(self):
        def zipped(members):
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
                for name, data in sorted(members.items()):
                    archive.writestr(name, data)
            return stream.getvalue()

        fixture_points = [(float(row[6]), float(row[5])) for row in FIXTURE_ROWS]
        good = {"mineplant.dbf": dbf_bytes(FIXTURE_ROWS), "mineplant.shp": shp_bytes(fixture_points),
                "mineplant.shx": shx_bytes(fixture_points), "mineplant.txt": txt_bytes(FIXTURE_ROWS),
                "mineplant.prj": NAD27_WKT.encode()}
        renamed_dbf = bytearray(good["mineplant.dbf"])
        renamed_dbf[32:40] = b"IDENT\0\0\0"
        cases = {
            "renamed_column": zipped({**good, "mineplant.dbf": bytes(renamed_dbf)}),
            "rogue_member": zipped({**good, "mineplant.sbn": b"rogue"}),
            "projected_prj": zipped({**good, "mineplant.prj": b'PROJCS["projected"]'}),
            "txt_drift": zipped({**good, "mineplant.txt": txt_bytes(FIXTURE_ROWS).replace(
                b"Silver Creek Plant", b"Silver Creek Mine", 1)}),
            "truncated_shp": zipped({**good, "mineplant.shp": shp_bytes(fixture_points)[:-28]}),
            "extra_records": zipped({**good, "mineplant.shp": shp_bytes(fixture_points) + b"junk"}),
        }
        drift_rows = [list(row) for row in FIXTURE_ROWS]
        drift_rows[0][5] = "39.20136"
        drift_rows = [tuple(row) for row in drift_rows]
        cases["coordinate_drift"] = zipped({
            "mineplant.dbf": dbf_bytes(drift_rows), "mineplant.shp": shp_bytes(fixture_points),
            "mineplant.shx": shx_bytes(fixture_points),
            "mineplant.txt": txt_bytes(drift_rows), "mineplant.prj": NAD27_WKT.encode()})
        with tempfile.TemporaryDirectory() as tmp:
            with _PatchedContext():
                for label, body in cases.items():
                    with self.subTest(label), self.assertRaises(ValueError):
                        m._decode(body)

    def test_dbf_value_and_encoding_drift_refused(self):
        with _PatchedContext():
            rows = [tuple(row) for row in FIXTURE_ROWS]
            dbf = bytearray(dbf_bytes(rows))
            dbf[321 + 1 + 8 + 30 + 50] = 0xE9
            with self.assertRaisesRegex(ValueError, "nonASCII"):
                m._decode_dbf(bytes(dbf))
            dbf = bytearray(dbf_bytes(rows))
            lat_offset = 321 + 1 + 8 + 30 + 50 + 50 + 40 + 40
            dbf[lat_offset:lat_offset + 10] = b"39.201356 "
            with self.assertRaisesRegex(ValueError, "scale"):
                m._decode_dbf(bytes(dbf))
            dbf[lat_offset:lat_offset + 10] = b"  39.20135"
            decoded = m._decode_dbf(bytes(dbf))
            self.assertEqual(decoded[0]["LATITUDE"], Decimal("39.20135"))

    def test_content_snapshot_drift_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            capture_dir = _sealed_capture_dir(Path(tmp) / "captures")
            with _PatchedContext(extra_patches={"RECORD_SHA": "0" * 64}):
                with self.assertRaisesRegex(ValueError, "source_content_snapshot_drift"):
                    self.stage(StagingStorageResource(local_dir=str(Path(tmp) / "stage")), str(capture_dir))

    def test_real_capture_archive_opt_in(self):
        import os
        archive_env = os.environ.get("HIFLD_AGRICULTURAL_MINERALS_TEST_CAPTURE_ARCHIVE")
        if not archive_env:
            self.skipTest("real pinned USGS capture archive opt-in required")
        with tempfile.TemporaryDirectory() as tmp:
            package = Path(tmp) / "captures"
            package.mkdir()
            with tarfile.open(archive_env, "r:gz") as archive:
                for member in archive:
                    if not member.isfile() or Path(member.name).name != member.name:
                        raise ValueError("unsafe test archive")
                    (package / Path(member.name).name).write_bytes(archive.extractfile(member).read())
            storage = StagingStorageResource(local_dir=str(Path(tmp) / "stage"))
            result = m.capture_agricultural_minerals(storage, capture_directory=str(package))
            self.assertEqual(result["feature_count"], AGRICULTURAL_ROWS)
            self.assertEqual(result["source_id_sha256"], ID_SHA)


class AgriculturalMineralsRegistrationTest(unittest.TestCase):
    def test_ingest_asset_job_check_and_schedule_registered_in_defs(self):
        from dagster import AssetKey, DefaultScheduleStatus

        from dagster_hifld.definitions import defs

        key = AssetKey(["ingest", DATASET_SLUG, FILE_SLUG])
        graph = defs.resolve_asset_graph()
        self.assertIn(key, set(graph.get_all_asset_keys()))
        repo = defs.get_repository_def()
        job = repo.get_job("agricultural_minerals_draft_refresh")
        self.assertEqual(set(job.asset_layer.executable_asset_keys), {key})
        check_keys = {str(check_key) for check_key in graph.asset_check_keys}
        self.assertTrue(any("agricultural_minerals_population_complete" in check_key
                            for check_key in check_keys))
        from dagster_hifld.assets.agricultural_minerals import agricultural_minerals_population_complete
        self.assertTrue(next(iter(agricultural_minerals_population_complete.check_specs)).blocking)
        schedule = repo.get_schedule_def("agricultural_minerals_draft_refresh_schedule")
        self.assertEqual(schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertEqual(schedule.cron_schedule, "0 9 1 * *")


if __name__ == "__main__":
    unittest.main()
