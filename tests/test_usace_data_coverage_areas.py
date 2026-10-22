import json
import os
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq
import shapely

from dagster import DefaultScheduleStatus
from dagster_hifld.assets.ingest_registry import hifld_ingest_assets
from dagster_hifld.assets.usace_data_coverage_areas import (
    usace_data_coverage_areas_draft_schedule,
    usace_data_coverage_areas_usace_ienc,
)
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import usace_data_coverage_areas as source


class UsaceDataCoverageAreasTests(unittest.TestCase):
    def setUp(self):
        self.capture = Path(os.environ["HIFLD_DATA_COVERAGE_AREAS_CAPTURE_DIR"])
        manifest = json.loads((self.capture / "capture-manifest.json").read_bytes())
        self.captures = {
            name: ((self.capture / f"{name}.body").read_bytes(), receipt)
            for name, receipt in manifest["receipts"].items()
        }

    def test_native_population_and_geoparquet_preserve_all_records(self):
        fields, features, audit = source._validate(self.captures)
        self.assertEqual(len(features), 317)
        self.assertEqual(audit["category_counts"], {"Coverage": 109, "No Coverage": 208})
        self.assertEqual(audit["geometry_invalid_count"], 0)
        self.assertEqual([f["attributes"]["OBJECTID"] for f in features], list(range(1, 318)))

        parquet = source._parquet(fields, features, audit["geometry_types"])
        table = pq.read_table(__import__("io").BytesIO(parquet))
        self.assertEqual(table.num_rows, 317)
        self.assertEqual(table.column_names, [field["name"] for field in source.ATTRIBUTE_FIELDS] + ["geometry"])
        raw_by_id = {f["attributes"]["OBJECTID"]: f for f in features}
        for row in table.to_pylist():
            source_row = raw_by_id[row["OBJECTID"]]
            for field in source.ATTRIBUTE_FIELDS[1:]:
                self.assertEqual(row[field["name"]], source_row["attributes"][field["name"]])
            self.assertEqual(
                shapely.from_wkb(row["geometry"]).wkb,
                shapely.from_wkb(source._esri_polygon(source_row["geometry"])).wkb,
            )

    def test_source_drift_is_rejected_and_asset_is_registered_stopped(self):
        changed = dict(self.captures)
        count_body, receipt = changed["count_after"]
        changed["count_after"] = (b'{"count":316}', {**receipt, "bytes": 12})
        with self.assertRaisesRegex(ValueError, "source_count_or_full_id_roster_drift"):
            source._validate(changed)
        self.assertIn(usace_data_coverage_areas_usace_ienc, hifld_ingest_assets)
        self.assertEqual(usace_data_coverage_areas_draft_schedule.default_status,
                         DefaultScheduleStatus.STOPPED)

    def test_registered_asset_materializes_retained_candidate_locally(self):
        with tempfile.TemporaryDirectory() as directory:
            result = source.capture_usace_data_coverage_areas(
                StagingStorageResource(use_local=True, local_dir=directory),
                capture_directory=str(self.capture),
            )
            self.assertEqual(result["feature_count"], 317)
            self.assertTrue(result["source_population_complete"])
            parquet_key = f"{source.DATASET_SLUG}/{source.FILE_SLUG}/{result['version']}/geoparquet/{source.FILE_SLUG}.parquet"
            self.assertTrue((Path(directory) / parquet_key).is_file())
            self.assertFalse(result["release_accepted"])


if __name__ == "__main__":
    unittest.main()
