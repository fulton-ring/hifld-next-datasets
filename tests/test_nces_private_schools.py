import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import fiona
import httpx
from fiona.crs import CRS

from dagster_hifld.assets.nces_private_schools import nces_private_schools_draft_schedule
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.source_manifest import CATALOG_METADATA_FIELDS
from dagster_hifld.sources import nces_private_schools as source
from dagster_hifld.sources.nces_private_schools import (
    EXPECTED_FIELDS,
    _decode,
    _point_wkb,
)

STEM = "EDGE_GEOCODE_PRIVATESCH_2324"
SCHOOLYEAR = "2023-2024"
RELEASE = {
    "schoolyear": SCHOOLYEAR,
    "stem": STEM,
    "source_url": f"https://nces.ed.gov/programs/edge/data/{STEM}.zip",
}
LANDING_HTML = (
    '<main><a href="/programs/edge/data/EDGE_GEOCODE_PRIVATESCH_1920.zip">old</a>'
    f'<a href="/programs/edge/data/{STEM}.zip">new</a>'
    '<!-- <a href="~/data/EDGE_GEOCODE_PRIVATESCH_2425.zip">future</a> -->'
    "</main>"
).encode()


def _archive(stem: str = STEM, schoolyear: str = SCHOOLYEAR) -> bytes:
    properties = {
        name: (f"str:{width}" if kind == "C" else f"float:{width}.{decimals}")
        for name, kind, width, decimals in EXPECTED_FIELDS
    }
    short = {
        "STFIP": "AK", "CNTY": "001", "LOCALE": "12", "CBSATYPE": "1",
        "CSA": "001", "CD": "01", "SLDL": "01", "SLDU": "01", "ZIP": "99999",
    }
    rows = []
    for ppin, state, x, y in (("A0000001", "AK", -149.9, 61.2), ("A0000002", "HI", -157.8, 21.3)):
        row = {}
        for name, kind, _width, _decimals in EXPECTED_FIELDS:
            if name == "PPIN":
                row[name] = ppin
            elif name == "STATE":
                row[name] = state
            elif name == "LAT":
                row[name] = y
            elif name == "LON":
                row[name] = x
            elif name == "SCHOOLYEAR":
                row[name] = schoolyear
            elif name in short:
                row[name] = short[name]
            else:
                row[name] = "X"
        rows.append(row)
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        with fiona.open(root / f"{stem}.shp", "w", driver="ESRI Shapefile",
                        schema={"geometry": "Point", "properties": properties},
                        crs=CRS.from_epsg(4326)) as target:
            for row in rows:
                target.write({"geometry": {"type": "Point", "coordinates": (row["LON"], row["LAT"])},
                              "properties": row})
        dbf = bytearray((root / f"{stem}.dbf").read_bytes())
        header = int.from_bytes(dbf[8:10], "little")
        offset = 32
        while offset < header and dbf[offset] != 0x0D:
            name = dbf[offset:offset + 11].split(b"\0", 1)[0].decode()
            if name in {"LAT", "LON"}:
                dbf[offset + 11] = ord("F")
            offset += 32
        (root / f"{stem}.dbf").write_bytes(dbf)
        members = source._members(stem)
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as archive:
            for name in sorted(members):
                path = root / name
                archive.writestr(name, path.read_bytes() if path.exists() else b"")
        return output.getvalue()


def _response(body: bytes, status: int = 200) -> httpx.Response:
    return httpx.Response(status, stream=httpx.ByteStream(body),
                          headers={"Content-Length": str(len(body))})


def _client(source_response: httpx.Response) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("SchoolLocations"):
            return _response(LANDING_HTML)
        return source_response
    return httpx.Client(transport=httpx.MockTransport(handler))


class NcesPrivateSchoolsSourceContractTest(unittest.TestCase):
    def test_no_module_level_release_or_count_pins(self):
        for name in ("EXPECTED_COUNT", "EXPECTED_ZIP_BYTES", "EXPECTED_ZIP_SHA256",
                     "SOURCE_URL", "SOURCE_RELEASE", "EXPECTED_SOURCE_ID_SHA256",
                     "EXPECTED_SERVICE_COUNT", "SERVICE_URL"):
            self.assertFalse(hasattr(source, name), name)

    def test_decode_uses_publisher_counts_and_records_school_year(self):
        with tempfile.TemporaryDirectory() as directory:
            frame, audit = _decode(_archive(), Path(directory) / "shape", RELEASE)
        self.assertEqual(len(frame), 2)
        self.assertEqual(audit["source_counts"], {"dbf": 2, "shx": 2, "shp": 2, "decoded": 2})
        self.assertEqual(audit["schoolyear"], SCHOOLYEAR)
        self.assertEqual(audit["publication_selection"], RELEASE)
        self.assertEqual(audit["source_id_field"], "PPIN")

    def test_live_capture_discovers_newest_active_private_release(self):
        body = _archive()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "capture"
            storage = StagingStorageResource(local_dir=str(Path(directory) / "staging"), use_local=True)
            with patch.object(source.httpx, "Client", return_value=_client(_response(body))):
                artifacts, receipts, release = source._capture(root, storage, "v-test")
            self.assertEqual(release["schoolyear"], SCHOOLYEAR)
            self.assertEqual(release["source_url"], RELEASE["source_url"])
            self.assertEqual(artifacts["source.body"], body)
            self.assertIn("landing.body", artifacts)
            self.assertEqual(receipts["source"]["status"], 200)

    def test_geometry_and_refresh_policy_preserve_pending_state(self):
        self.assertEqual(_point_wkb(-71.25, 42.5).hex(), "01010000000000000000d051c00000000000404540")
        self.assertEqual(nces_private_schools_draft_schedule.default_status.value, "STOPPED")

    def test_versioned_processing_identity_survives_provenance_projection(self):
        scheme = "sha256-packaged-dagster-hifld-v1"
        manifest = {
            "schema_version": 1,
            "processor": "HIFLD Next",
            "code_commit": "packaged-sha256:" + "0" * 64,
            "code_identity_scheme": scheme,
            "inputs": [{
                "input_id": "nces-private-schools",
                "producer": "National Center for Education Statistics",
                "host": "nces.ed.gov",
                "role": "producer_publication",
                "canonical_url": RELEASE["source_url"],
                "retrieval_url": RELEASE["source_url"],
                "retrieved_at": "2026-10-20T03:44:19Z",
                "sha256": "a" * 64,
                "bytes": 1,
                "license": "review pending",
                "attribution": "review pending",
                "release_date": None,
                "release_date_unknown_reason": "Source collection period does not establish a producer release date.",
            }],
            "transformations": [{"operation": "NCES native fields and points to GeoParquet"}],
            "prebuilt_output_policy": {"schema_version": 1, "formats": {"geoparquet": {
                "reason": "native fields retained",
                "objects": [{"path": "geoparquet/private-schools.parquet", "bytes": 1, "sha256": "b" * 64}],
            }}},
        }
        self.assertIn("code_identity_scheme", CATALOG_METADATA_FIELDS)
        self.assertEqual(provenance_projection(manifest)["code_identity_scheme"], scheme)


if __name__ == "__main__":
    unittest.main()
