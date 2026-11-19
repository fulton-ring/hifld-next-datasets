"""Native school FileGDB discovery and immutable construction regressions."""
from __future__ import annotations

import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx
import pyogrio

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import census_latest
from dagster_hifld.sources import school_district_administrative_areas as source
from test_census_substate_construction import snapshot
from test_school_district_administrative_areas_source import _fixture_table


ROOT = census_latest.ROOT
DIRECTORY = ROOT + "TGRGDB26/"
URL = DIRECTORY + "tlgdb_2026_us_school.gdb.zip"
ANNUAL = b'<a href="TGRGDB26/">2026 geodatabases</a>'
LISTING = b'<a href="tlgdb_2026_us_school.gdb.zip">school</a>'


def native_archive(root):
    gdb = root / "school.gdb"
    pyogrio.write_arrow(_fixture_table(), gdb, layer=source.LAYER_NAME,
                        driver="OpenFileGDB", geometry_name="SHAPE",
                        geometry_type="MultiPolygon", crs="EPSG:4269")
    sink = io.BytesIO()
    with zipfile.ZipFile(sink, "w") as archive:
        for path in sorted(gdb.iterdir()):
            archive.writestr(gdb.name + "/" + path.name, path.read_bytes())
    return sink.getvalue()


class AdministrativeSchoolConstructionTests(unittest.TestCase):
    def test_discovery_capture_and_original_preservation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = native_archive(root)
            storage = StagingStorageResource(local_dir=str(root / "stage"))
            listings = {ROOT: ANNUAL, DIRECTORY: LISTING}
            requests = []

            def respond(request):
                requests.append(str(request.url))
                return httpx.Response(200, stream=httpx.ByteStream({**listings, URL: archive}[str(request.url)]),
                                      headers={"Content-Type": "application/zip"})

            with httpx.Client(transport=httpx.MockTransport(respond)) as client:
                first = source.capture(storage, "ignored-run-id", client)
                self.assertEqual(requests, [ROOT, DIRECTORY, URL])
                target = root / "stage" / source.DATASET_SLUG / source.FILE_SLUG / first["version"]
                before = snapshot(target)
                witnesses = snapshot(root / "stage" / "_construction-state")
                receipt_bytes = before["raw/source_archive.receipt.json"]
                receipt = json.loads(receipt_bytes)
                release = census_latest.geodatabase_release_from_receipt(
                    {**receipt, "url": receipt["requested_url"]}, "school")
                self.assertEqual(receipt["source_release"], release.as_receipt_evidence())
                self.assertEqual(release.listing_sha256, source._sha256(LISTING))
                self.assertEqual(release.annual_listing_sha256, source._sha256(ANNUAL))
                self.assertEqual(release.listing_bytes, len(LISTING))
                self.assertTrue(witnesses)
                listings[ROOT] += b"<!-- unrelated annual update -->"
                listings[DIRECTORY] += b"<!-- unrelated directory update -->"
                refreshed = source.capture(storage, "another-run-id", client)
                self.assertEqual(refreshed["version"], first["version"])
                self.assertTrue(refreshed["reused"])
                self.assertEqual(snapshot(target), before)
                self.assertEqual(snapshot(root / "stage" / "_construction-state"), witnesses)
                with patch.object(source._shared.httpx, "Client", side_effect=AssertionError("replay used HTTP")):
                    replay = source.replay_retained_capture(storage, first["version"], archive, receipt_bytes)
                self.assertTrue(replay["reused"])
                self.assertEqual(snapshot(target), before)
                self.assertEqual(snapshot(root / "stage" / "_construction-state"), witnesses)
                self.assertFalse((root / "stage" / "_quarantine").exists())
