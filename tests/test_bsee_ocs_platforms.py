"""Focused BSEE source-contract and evidence-identity checks."""
from __future__ import annotations

import tempfile
import unittest
import zipfile
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import fiona
import pandas as pd
from shapely.geometry import Point

from dagster_hifld.sources.bsee_ocs_platforms import _candidate_version, _validate_capture


class BseeOcsPlatformsTests(unittest.TestCase):
    @contextmanager
    def fixture(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            path=root/"platform.shp"
            schema={"geometry":"Point","properties":{"STRUCTURE_":"int:5","COMPLEX_ID":"int:10","STRUCTURE1":"str:15","INSTALL_DA":"date","REMOVAL_DA":"date"}}
            with fiona.open(path,"w",driver="ESRI Shapefile",crs="EPSG:4267",schema=schema,encoding="UTF-8") as sink:
                sink.write({"geometry":{"type":"Point","coordinates":(-90.0,28.0)},
                            "properties":{"STRUCTURE_":1,"COMPLEX_ID":12345,"STRUCTURE1":None,"INSTALL_DA":"2001-02-03","REMOVAL_DA":None}})
            archive=root/"platform.zip"
            expected={"platform.cpg","platform.dbf","platform.prj","platform.sbn","platform.sbx","platform.shp","platform.shp.xml","platform.shx"}
            with zipfile.ZipFile(archive,"w") as zf:
                for item in path.parent.glob("platform.*"):
                    if item != archive: zf.write(item,item.name)
                for name in sorted(expected-set(zf.namelist())): zf.writestr(name,b"")
            mapping=b"/Mapping/Files/platform.zip Platforms Identifies the location of platforms in GOA. All platforms existing in the database are included."
            metadata=("Publication_Date: 20260302 Only platforms in the Gulf of Mexico, Federal Outer Continental Shelf (OCS) waters are included. "
                      "Maintenance_and_Update_Frequency: Monthly Point_and_Vector_Object_Count: 7317 Horizontal_Datum_Name: D North American 1927").encode()
            captures={"mapping_page":(mapping,{"status":200}),"platform_metadata":(metadata,{"status":200}),"platform":(archive.read_bytes(),{"status":200})}
            yield captures

    def test_tiny_native_schema_geometry_and_nulls_validate(self):
        with self.fixture() as captures, patch("dagster_hifld.sources.bsee_ocs_platforms.EXPECTED_COUNT",1):
            frame,audit=_validate_capture(captures)
            self.assertEqual(len(frame),1)
            self.assertEqual(audit["source_crs_epsg"],4267)
            self.assertEqual(audit["identifier_pair_unique_count"],1)
            self.assertTrue(pd.isna(frame.iloc[0]["STRUCTURE1"]))

    def test_incomplete_or_scope_drift_capture_rejected(self):
        with self.fixture() as captures, patch("dagster_hifld.sources.bsee_ocs_platforms.EXPECTED_COUNT",1):
            broken=dict(captures)
            body,receipt=broken["platform_metadata"]
            broken["platform_metadata"]=(body.replace(b"Only platforms in the Gulf",b"Only platforms outside the Gulf"),receipt)
            with self.assertRaisesRegex(ValueError,"official_platform_metadata_contract_drift"):
                _validate_capture(broken)
            with self.assertRaisesRegex(ValueError,"source_capture_roster_incomplete_or_unexpected"):
                _validate_capture({k:v for k,v in captures.items() if k!="platform"})

    def test_candidate_identity_binds_wire_and_body_bytes_not_receipt_times(self):
        records="same normalized records"; schema="same schema"; identity={"code":"same"}
        first={"platform":{"body_sha256":"body-a","wire_sha256":"wire-a"}}
        same={"platform":{"body_sha256":"body-a","wire_sha256":"wire-a"}}
        changed_wire={"platform":{"body_sha256":"body-a","wire_sha256":"wire-b"}}
        changed_body={"platform":{"body_sha256":"body-b","wire_sha256":"wire-b"}}
        self.assertEqual(_candidate_version(records,schema,first,identity),_candidate_version(records,schema,same,identity))
        self.assertNotEqual(_candidate_version(records,schema,first,identity),_candidate_version(records,schema,changed_wire,identity))
        self.assertNotEqual(_candidate_version(records,schema,first,identity),_candidate_version(records,schema,changed_body,identity))


if __name__ == "__main__":
    unittest.main()
