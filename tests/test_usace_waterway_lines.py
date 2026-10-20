import json
import shutil
import tempfile
import unittest
from pathlib import Path

from dagster import DefaultScheduleStatus

from dagster_hifld.assets.usace_waterway_lines import (
    usace_navigable_waterway_network_lines_complete,
    usace_waterway_lines_draft_schedule,
)
from dagster_hifld.sources import usace_waterway_lines as source
from dagster_hifld.sources.arcgis_filegdb import stage_filegdb_archive, verify_extracted_filegdb_cache
from dagster_hifld.resources import StagingStorageResource


class UsaceWaterwayLinesContractTests(unittest.TestCase):
    def test_filegdb_archive_descriptor_uses_canonical_layout(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); archive=root/'source.zip'; archive.write_bytes(b'zip bytes')
            gdb=root/'gdb'; gdb.mkdir(); (gdb/'a00000001.gdbtable').write_bytes(b'part')
            storage=StagingStorageResource(local_dir=str(root/'staging'))
            objects=stage_filegdb_archive(storage,'dataset/file/version',archive,gdb,'source.gdb')
            self.assertEqual(objects[0]['path'],'file_geodatabase/source_filegdb.zip')
            archive_sha=objects[0]['sha256']
            descriptor=next(x for x in objects if x['sha256']==archive_sha and x['bytes']==len(b'zip bytes'))
            self.assertEqual(descriptor['path'],'file_geodatabase/source_filegdb.zip')

    def test_candidate_cache_key_is_stable_and_processing_identity_is_versioned(self):
        base=source._candidate_version('a'*64,['OBJECTID'],[1],{'code':'one'})
        self.assertEqual(base,source._candidate_version('a'*64,['OBJECTID'],[1],{'code':'one'}))
        self.assertNotEqual(base,source._candidate_version('a'*64,['OBJECTID'],[1],{'code':'two'}))
        self.assertEqual(source._construction_key(base),f'_construction-state/{source.DATASET_SLUG}/{source.FILE_SLUG}/{base}.json')

    def test_catalog_extracted_filegdb_cache_requires_exact_native_mirror(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);archive=root/'source.zip';archive.write_bytes(b'zip')
            gdb=root/'gdb';gdb.mkdir();(gdb/'table.gdbtable').write_bytes(b'native table')
            storage=StagingStorageResource(local_dir=str(root/'staging'))
            objects=stage_filegdb_archive(storage,'dataset/file/version',archive,gdb,'source.gdb')
            canonical=Path(storage.local_dir)/'dataset/file/version/file_geodatabase/source.gdb'
            extracted=Path(storage.local_dir)/'dataset/file/version/file_geodatabase/.extracted/source_filegdb/source.gdb'
            shutil.copytree(canonical,extracted)
            mirror=verify_extracted_filegdb_cache(storage,'dataset/file/version',objects)
            self.assertEqual(len(mirror),1)
            (extracted/'unexpected.bin').write_bytes(b'extra')
            with self.assertRaisesRegex(ValueError,'extracted_cache_roster_changed'):
                verify_extracted_filegdb_cache(storage,'dataset/file/version',objects)

    def _metadata(self):
        layer_fields=[]
        for name in source.EXPECTED_FIELDS:
            kind = ("esriFieldTypeBlob" if name == "SE_ANNO_CAD_DATA" else
                    "esriFieldTypeOID" if name == "OBJECTID" else
                    "esriFieldTypeDouble" if name in {"LENGTH","LINKNUM","AMILE","BMILE","LENGTH1","MIN_MEAS","MAX_MEAS","Shape__Length"} else
                    "esriFieldTypeSmallInteger" if name == "DIR" else
                    "esriFieldTypeInteger" if name in {"ID","ANODE","BNODE","WTWY_TYPE","NUM_PAIRS"} else "esriFieldTypeString")
            item={"name":name,"type":kind}
            if name == "LINKNAME": item["length"]=35
            layer_fields.append(item)
        return (
            {"id":source.BTS_ITEM_ID,"owner":"USDOT_BTS","url":source.LAYER_URL},
            {"id":source.USACE_SERVICE_ITEM_ID,"owner":"usace_iwr_cac","url":source.SERVICE_URL},
            {"serviceItemId":source.USACE_SERVICE_ITEM_ID,"capabilities":"Query,Extract","supportedExportFormats":"filegdb,geojson"},
            {"id":1,"name":"Waterway_Network","geometryType":"esriGeometryPolyline","hasZ":True,"hasM":True,
             "extent":{**source.OBSERVED_COVERAGE,"spatialReference":{"latestWkid":4269}},"fields":layer_fields},
            {"count":source.EXPECTED_COUNT},
            {"objectIds":list(range(1,source.EXPECTED_COUNT+1))},
        )

    def test_exact_official_items_layer_roster_and_population_pass(self):
        docs=self._metadata()
        fields,ids=source._validate_before(*docs)
        self.assertEqual([x["name"] for x in fields],list(source.EXPECTED_FIELDS))
        self.assertEqual(len(ids),source.EXPECTED_COUNT)

    def test_missing_blob_or_renamed_id_field_quarantines_source(self):
        docs=list(self._metadata())
        docs[3]=json.loads(json.dumps(docs[3]))
        docs[3]["fields"]=[field for field in docs[3]["fields"] if field["name"]!="SE_ANNO_CAD_DATA"]
        with self.assertRaisesRegex(ValueError,"field_roster_drift"):
            source._validate_before(*docs)
        docs=list(self._metadata())
        docs[3]=json.loads(json.dumps(docs[3]))
        docs[3]["fields"][0]["name"]="SOURCE_OID"
        with self.assertRaisesRegex(ValueError,"field_roster_drift"):
            source._validate_before(*docs)

    def test_incomplete_or_duplicate_ids_block_candidate(self):
        docs=list(self._metadata()); docs[4]={"count":source.EXPECTED_COUNT-1}
        with self.assertRaisesRegex(ValueError,"population_or_ID_drift"):
            source._validate_before(*docs)
        docs=list(self._metadata()); docs[5]["objectIds"][-1]=docs[5]["objectIds"][-2]
        with self.assertRaisesRegex(ValueError,"population_or_ID_drift"):
            source._validate_before(*docs)

    def test_changed_field_type_and_observed_id_roster_block_candidate(self):
        docs=list(self._metadata());docs[3]=json.loads(json.dumps(docs[3]));docs[3]["fields"][1]["type"]="esriFieldTypeDouble"
        with self.assertRaisesRegex(ValueError,"field_type_drift"):
            source._validate_before(*docs)
        docs=list(self._metadata());docs[5]["objectIds"][-1]=7000
        with self.assertRaisesRegex(ValueError,"observed_ID_roster_drift"):
            source._validate_before(*docs)

    def test_unexpected_zm_or_crs_blocks_candidate(self):
        docs=list(self._metadata()); docs[3]=json.loads(json.dumps(docs[3])); docs[3]["hasM"]=False
        with self.assertRaisesRegex(ValueError,"ZM_contract_drift"):
            source._validate_before(*docs)
        docs=list(self._metadata()); docs[3]=json.loads(json.dumps(docs[3])); docs[3]["extent"]["spatialReference"]["latestWkid"]=4326
        with self.assertRaisesRegex(ValueError,"CRS_drift"):
            source._validate_before(*docs)

    def test_source_check_passes_only_complete_native_candidate_and_reports_all_decisions_false(self):
        result=usace_navigable_waterway_network_lines_complete({"candidate_ready":True,"feature_count":source.EXPECTED_COUNT,"native_Z_M_and_BLOB_preserved":True})
        self.assertTrue(result.passed)
        self.assertTrue(all(result.metadata[key].value is False for key in (
            "source_release_accepted","source_freshness_accepted","schema_contract_accepted",
            "id_contract_accepted","rights_accepted","mapping_approved","release_accepted")))
        self.assertEqual(usace_waterway_lines_draft_schedule.default_status,DefaultScheduleStatus.STOPPED)


if __name__ == "__main__":
    unittest.main()
