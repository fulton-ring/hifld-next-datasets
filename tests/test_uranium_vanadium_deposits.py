import hashlib
import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import pyarrow.parquet as pq
from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.uranium_vanadium_deposits import (
    uranium_vanadium_deposits, uranium_vanadium_deposits_draft_schedule,
    uranium_vanadium_deposits_source_complete,
)
from dagster_hifld.definitions import defs
from dagster_hifld.sources import uranium_vanadium_deposits as source
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _canonical

# The live archive holds 304632 digest records with a 16340-record U/V subset;
# tests exercise the identical logic against a small synthetic archive.
FEATURES = [
    {"DEP_ID": "10000010", "SITE_NAME": "Uranium Knoll", "DEV_STAT": "Prospect",
     "URL": "https://mrdata.usgs.gov/mrds/show-mrds.php?dep_id=10000010", "CODE_LIST": "U PB",
     "x": -110.5, "y": 38.5},
    {"DEP_ID": "10000020", "SITE_NAME": "Vanadium Flats", "DEV_STAT": "Producer",
     "URL": "https://mrdata.usgs.gov/mrds/show-mrds.php?dep_id=10000020", "CODE_LIST": "V U",
     "x": -109.25, "y": 39.25},
    {"DEP_ID": "10000030", "SITE_NAME": "Copper Ridge", "DEV_STAT": "Occurrence",
     "URL": "https://mrdata.usgs.gov/mrds/show-mrds.php?dep_id=10000030", "CODE_LIST": "CU AU",
     "x": -111.75, "y": 37.75},
    {"DEP_ID": "10000040", "SITE_NAME": "Vanadium Pasture", "DEV_STAT": "Past Producer",
     "URL": "https://mrdata.usgs.gov/mrds/show-mrds.php?dep_id=10000040", "CODE_LIST": None,
     "x": -108.5, "y": 40.5},
]
SUBSET_IDS = ["10000010", "10000020"]


def _archive_members():
    return {f"mrds-trim{suffix}" for suffix in (".prj", ".shp", ".shx", ".dbf")} | {"mrds.met"}


def _pins():
    subset = [f for f in FEATURES if f["DEP_ID"] in SUBSET_IDS]
    def records():
        return [{k: v for k, v in f.items() if k not in ("x", "y")} for f in subset]
    def geometry():
        return [[f["DEP_ID"], f["x"], f["y"]] for f in subset]
    return {
        "EXPECTED_SHA256": None,  # filled by _synthetic_archive
        "EXPECTED_BYTES": None,
        "EXPECTED_ARCHIVE_MEMBERS": _archive_members(),
        "EXPECTED_FEATURE_COUNT": len(FEATURES),
        "EXPECTED_ID_MIN": min(f["DEP_ID"] for f in FEATURES),
        "EXPECTED_ID_MAX": max(f["DEP_ID"] for f in FEATURES),
        "EXPECTED_FULL_ROSTER_SHA256": source._list_sha(sorted(f["DEP_ID"] for f in FEATURES)),
        "EXPECTED_FULL_BOUNDS": [min(f["x"] for f in FEATURES), min(f["y"] for f in FEATURES),
                                 max(f["x"] for f in FEATURES), max(f["y"] for f in FEATURES)],
        "EXPECTED_CODE_LIST_NULL_COUNT": 1,
        "EXPECTED_DEV_STAT_COUNTS": {"Prospect": 1, "Producer": 1, "Occurrence": 1, "Past Producer": 1},
        "EXPECTED_SUBSET_COUNT": len(SUBSET_IDS),
        "EXPECTED_SUBSET_ROSTER_SHA256": source._list_sha(SUBSET_IDS),
        "EXPECTED_SUBSET_RECORD_SHA256": source._list_sha(records()),
        "EXPECTED_SUBSET_GEOMETRY_SHA256": source._list_sha(geometry()),
        "EXPECTED_SUBSET_BOUNDS": [min(f["x"] for f in subset), min(f["y"] for f in subset),
                                   max(f["x"] for f in subset), max(f["y"] for f in subset)],
        "EXPECTED_SUBSET_TOKEN_COUNTS": {"U": 2, "V": 1, "both": 1},
        "EXPECTED_SUBSET_DEV_STAT_COUNTS": {"Prospect": 1, "Producer": 1},
    }


def _synthetic_archive(directory: Path) -> tuple[Path, int, str]:
    frame = gpd.GeoDataFrame(
        {name: [f[name] for f in FEATURES] for name in source.EXPECTED_FIELDS},
        geometry=gpd.points_from_xy([f["x"] for f in FEATURES], [f["y"] for f in FEATURES]),
        crs="EPSG:4326",
    )
    with tempfile.TemporaryDirectory() as tmp:
        shapefile = Path(tmp) / "mrds-trim.shp"
        frame.to_file(shapefile, engine="pyogrio")
        body_path = directory / "mrds-trim.zip"
        with zipfile.ZipFile(body_path, "w") as archive:
            for member in ("mrds-trim.prj", "mrds-trim.shp", "mrds-trim.shx", "mrds-trim.dbf"):
                source_path = Path(tmp) / member
                if source_path.exists():
                    archive.write(source_path, member)
            archive.writestr("mrds.met", "FGDC metadata placeholder")
    body = body_path.read_bytes()
    return body_path, len(body), hashlib.sha256(body).hexdigest()


class UraniumVanadiumDepositsTests(unittest.TestCase):
    def setUp(self):
        with tempfile.TemporaryDirectory() as tmp:
            body_path, self._bytes, self._sha = _synthetic_archive(Path(tmp))
            self._body = body_path.read_bytes()
        pins = _pins()
        pins["EXPECTED_SHA256"] = self._sha
        pins["EXPECTED_BYTES"] = self._bytes
        self._patches = [patch.object(source, name, value) for name, value in pins.items()]
        for item in self._patches:
            item.start()
            self.addCleanup(item.stop)

    def _capture(self, body=None):
        body = self._body if body is None else body
        receipt = {
            "method": "GET", "url": source.SOURCE_URL, "final_url": source.SOURCE_URL,
            "status": 200, "complete_body": True, "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(), "content_encoding": "identity",
            "wire_bytes": len(body), "wire_sha256": hashlib.sha256(body).hexdigest(),
            "content_length": len(body), "observed_at": "2026-11-14T00:00:00+00:00",
        }
        return body, receipt

    def test_registered_stopped_asset_and_unapproved_blocking_check(self):
        self.assertIn(uranium_vanadium_deposits, hifld_ingest_assets)
        self.assertEqual(uranium_vanadium_deposits_draft_schedule.default_status, DefaultScheduleStatus.STOPPED)
        self.assertIn(
            "uranium_vanadium_deposits_draft_refresh",
            {job.name for job in defs.resolve_all_job_defs()},
        )
        check = uranium_vanadium_deposits_source_complete({
            "feature_count": 16340, "source_population_complete": True,
            "source_archive_bytes": 11236725,
            "source_archive_sha256": "f24f995faaa6f5c66415971b1bb0fa1906273adb37f82c1098266aece47f6a84",
        })
        self.assertTrue(check.passed)
        self.assertFalse(check.metadata["release_accepted"].value)
        failed = uranium_vanadium_deposits_source_complete({
            "feature_count": 16339, "source_population_complete": True,
            "source_archive_bytes": 11236725,
            "source_archive_sha256": "f24f995faaa6f5c66415971b1bb0fa1906273adb37f82c1098266aece47f6a84",
        })
        self.assertFalse(failed.passed)

    def test_candidate_version_and_code_identity_need_no_git(self):
        self.assertRegex(source.candidate_version(), r"^v[0-9a-f]{32}$")

    def test_complete_digest_and_uv_subset_are_validated(self):
        body, receipt = self._capture()
        audit = source._inspect_archive(body)
        self.assertEqual(audit["feature_count"], source.EXPECTED_FEATURE_COUNT)
        self.assertEqual(audit["subset_count"], source.EXPECTED_SUBSET_COUNT)
        self.assertEqual(audit["source_id_sha256"], source.EXPECTED_FULL_ROSTER_SHA256)
        self.assertEqual(audit["subset_roster_sha256"], source.EXPECTED_SUBSET_ROSTER_SHA256)
        self.assertEqual(audit["subset_record_sha256"], source.EXPECTED_SUBSET_RECORD_SHA256)
        self.assertEqual(audit["subset_geometry_sha256"], source.EXPECTED_SUBSET_GEOMETRY_SHA256)
        self.assertEqual(audit["subset_token_counts"], source.EXPECTED_SUBSET_TOKEN_COUNTS)
        self.assertEqual(audit["code_list_null_count"], 1)
        self.assertIn("exact commodity-code tokens", audit["subset_predicate"])

    def test_capture_rejects_changed_archive_bytes_with_quarantine(self):
        body, receipt = self._capture(b"not the pinned MRDS archive")
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            capture_root = Path(tmp) / "capture"
            capture_root.mkdir()
            (capture_root / "source.body").write_bytes(body)
            (capture_root / "source.wire").write_bytes(body)
            (capture_root / "source.receipt.json").write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "official_mrds_archive_identity_or_release_drift"):
                source._capture(capture_root, storage, "vtest")
            keys = storage.list_prefix("_quarantine/uranium-and-vanadium-deposits")
            self.assertEqual(len(keys), 4)
            failure_key = next(key for key in keys if key.endswith("/metadata/failure.json"))
            self.assertEqual(json.loads(storage.read_key(failure_key))["reason_code"],
                             "official_mrds_archive_identity_or_release_drift")

    def test_archive_member_roster_drift_is_rejected_before_extract(self):
        sink = io.BytesIO()
        with zipfile.ZipFile(sink, "w") as archive:
            archive.writestr("unexpected.txt", "not the MRDS archive")
        with self.assertRaisesRegex(ValueError, "mrds_archive_member_roster_drift"):
            source._inspect_archive(sink.getvalue())

    def test_population_schema_crs_and_value_drift_fail_closed(self):
        cases = [
            ("count", lambda g: g.iloc[:-1], "mrds_source_population_drift"),
            ("duplicate ids", lambda g: g.assign(DEP_ID=["10000010"] * len(g)),
             "mrds_source_id_drift"),
            ("id range", lambda g: g.assign(DEP_ID=["1"] + list(g.DEP_ID[1:])),
             "mrds_source_id_range_drift"),
            ("null name", lambda g: g.assign(SITE_NAME=[None] + list(g.SITE_NAME[1:])),
             "mrds_source_null_attribute_drift:SITE_NAME"),
            ("code list nulls", lambda g: g.assign(CODE_LIST=[None] * len(g)),
             "mrds_source_null_population_drift:CODE_LIST"),
            ("dev stat values", lambda g: g.assign(DEV_STAT=["Plant"] * len(g)),
             "mrds_source_dev_stat_population_drift"),
            ("geometry type", lambda g: g.set_geometry(g.geometry.buffer(0.5)),
             "mrds_source_geometry_type_drift"),
        ]
        for label, mutate, expected in cases:
            with self.subTest(label=label):
                frame = gpd.GeoDataFrame(
                    {name: [f[name] for f in FEATURES] for name in source.EXPECTED_FIELDS},
                    geometry=gpd.points_from_xy([f["x"] for f in FEATURES], [f["y"] for f in FEATURES]),
                    crs="EPSG:4326",
                )
                frame = mutate(frame)
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "mrds-trim.shp"
                    frame.to_file(path, engine="pyogrio")
                    body_path = Path(tmp) / "mrds-trim.zip"
                    with zipfile.ZipFile(body_path, "w") as archive:
                        for member in ("mrds-trim.prj", "mrds-trim.shp", "mrds-trim.shx", "mrds-trim.dbf"):
                            member_path = Path(tmp) / member
                            if member_path.exists():
                                archive.write(member_path, member)
                        archive.writestr("mrds.met", "FGDC metadata placeholder")
                    body = body_path.read_bytes()
                    with self.assertRaisesRegex(ValueError, expected):
                        source._inspect_archive(body)

    def test_subset_contract_drift_fails_closed(self):
        def rebuild(changed_features):
            frame = gpd.GeoDataFrame(
                {name: [f[name] for f in changed_features] for name in source.EXPECTED_FIELDS},
                geometry=gpd.points_from_xy([f["x"] for f in changed_features], [f["y"] for f in changed_features]),
                crs="EPSG:4326",
            )
            frame = frame.sort_values("DEP_ID", key=lambda series: series.astype(int))
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "mrds-trim.shp"
                frame.to_file(path, engine="pyogrio")
                body_path = Path(tmp) / "mrds-trim.zip"
                with zipfile.ZipFile(body_path, "w") as archive:
                    for member in ("mrds-trim.prj", "mrds-trim.shp", "mrds-trim.shx", "mrds-trim.dbf"):
                        member_path = Path(tmp) / member
                        if member_path.exists():
                            archive.write(member_path, member)
                    archive.writestr("mrds.met", "FGDC metadata placeholder")
                return body_path.read_bytes()

        changed = json.loads(json.dumps(FEATURES))
        changed[0]["SITE_NAME"] = "Renamed Knoll"
        with self.subTest(label="subset record values"):
            with self.assertRaisesRegex(ValueError, "mrds_uv_subset_record_drift"):
                source._inspect_archive(rebuild(changed))

        changed = json.loads(json.dumps(FEATURES))
        changed[0]["CODE_LIST"] = "U PB V"
        with self.subTest(label="subset token counts"):
            with self.assertRaisesRegex(ValueError, "mrds_uv_token_count_drift"):
                source._inspect_archive(rebuild(changed))

        changed = json.loads(json.dumps(FEATURES))
        changed[0]["x"] = -110.0
        changed[0]["y"] = 38.0
        with self.subTest(label="subset coverage"):
            with self.assertRaisesRegex(ValueError, "mrds_uv_subset_geometry_drift"):
                source._inspect_archive(rebuild(changed))

    def test_capture_stages_archive_manifest_audit_and_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            storage = StagingStorageResource(use_local=True, local_dir=str(Path(tmp) / "staging"))
            body, receipt = self._capture()
            with patch.object(source, "capture_get", return_value=(body, receipt)):
                first = source.capture_uranium_vanadium_deposits(storage, version="regression")
                self.assertFalse(first["source_cache_reused"])
                second = source.capture_uranium_vanadium_deposits(storage, version="regression")
            self.assertTrue(second["source_cache_reused"])
            self.assertEqual(second["version"], first["version"])
            self.assertEqual(first["feature_count"], source.EXPECTED_SUBSET_COUNT)
            self.assertTrue(first["source_population_complete"])
            self.assertFalse(first["release_accepted"])
            prefix = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{first['version']}"
            staged = set(storage.list_prefix(prefix))
            self.assertIn(f"{prefix}/shapefile/{source.SOURCE_FILE}", staged)
            self.assertIn(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet", staged)
            self.assertIn(f"{prefix}/metadata/source_manifest.json", staged)
            self.assertIn(f"{prefix}/metadata/source_audit.json", staged)
            manifest = json.loads(storage.read_key(f"{prefix}/metadata/source_manifest.json"))
            self.assertEqual(manifest["inputs"][0]["sha256"], source.EXPECTED_SHA256)
            audit = json.loads(storage.read_key(f"{prefix}/metadata/source_audit.json"))
            self.assertEqual(audit["subset_count"], source.EXPECTED_SUBSET_COUNT)
            table = pq.read_table(io.BytesIO(storage.read_key(f"{prefix}/geoparquet/{source.FILE_SLUG}.parquet")))
            self.assertEqual(table.num_rows, source.EXPECTED_SUBSET_COUNT)
            self.assertEqual(table.column("DEP_ID").to_pylist(), SUBSET_IDS)
            self.assertEqual(table.schema.field("geometry").type.__str__(), "binary")
            geo = json.loads(table.schema.metadata[b"geo"])
            self.assertEqual(geo["columns"]["geometry"]["crs"]["id"], {"authority": "EPSG", "code": 4326})


if __name__ == "__main__":
    unittest.main()
