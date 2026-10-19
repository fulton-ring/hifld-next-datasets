import hashlib
import json
import pathlib
import tempfile
import unittest

from dagster_hifld.faa_airspace import stage_candidate
from dagster_hifld.resources import StagingStorageResource


class FaaCandidateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=pathlib.Path(self.tmp.name);self.capture=self.root/'capture';self.capture.mkdir()
        self.storage=StagingStorageResource(local_dir=str(self.root/'staging'))
        fields=[{'name':k,'type':v,'nullable':False} for k,v in [('OBJECTID','esriFieldTypeOID'),('GLOBAL_ID','esriFieldTypeString'),('GlobalID','esriFieldTypeGlobalID'),('UPPER_VAL','esriFieldTypeDouble')]]
        attrs={'OBJECTID':7,'GLOBAL_ID':'001234567890123456789','GlobalID':'{a}','UPPER_VAL':1.25}
        polygon={'type':'Polygon','coordinates':[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}
        self.data={'layer':{'fields':fields},'item':{'owner':'AeronauticalInformationServices_FAA','description':'Effective test release'},'count':{'count':1},'ids':{'objectIds':[7]},'source-native':{'features':[{'attributes':attrs}], 'spatialReference':{'wkid':4269}},'source-geojson':{'features':[{'properties':attrs,'geometry':polygon,'id':7}]},'page-end':{'features':[]}}
    def write(self):
        for k,v in self.data.items():
            body=json.dumps(v).encode();(self.capture/(k+'.body')).write_bytes(body)
            (self.capture/(k+'.json')).write_text(json.dumps({'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'status':200}))
    def run_candidate(self):
        self.write();return stage_candidate(self.storage,self.capture)
    def test_all_identifiers_and_physical_types_preserved_idempotent_quarantine(self):
        import pyarrow.parquet as pq
        result=self.run_candidate();again=stage_candidate(self.storage,self.capture)
        self.assertEqual(result,again);self.assertFalse(result['promotion_allowed'])
        p=self.root/'staging'/result['quarantine_prefix']/'candidate/airspace.parquet';t=pq.read_table(p)
        self.assertEqual(t['GLOBAL_ID'].to_pylist(),['001234567890123456789']);self.assertEqual(str(t.schema.field('UPPER_VAL').type),'double');self.assertFalse(t.schema.field('GLOBAL_ID').nullable)
        self.assertEqual(t['__source_geojson_feature_id'].to_pylist(),[7])
        self.assertEqual((p.parent.parent/'raw/source-native.json').read_bytes(),(self.capture/'source-native.body').read_bytes())
        self.assertFalse(list((self.root/'staging').glob('**/v*/geojson/*')))
    def test_missing_identifier_rejected(self):
        for f in self.data['source-native']['features']:f['attributes'].pop('GLOBAL_ID')
        with self.assertRaisesRegex(ValueError,'schema mismatch'):self.run_candidate()
    def test_incomplete_ids_rejected(self):
        self.data['ids']['objectIds']=[8]
        with self.assertRaisesRegex(ValueError,'ID set mismatch'):self.run_candidate()
    def test_transfer_limit_rejected(self):
        self.data['source-native']['exceededTransferLimit']=True
        with self.assertRaisesRegex(ValueError,'pagination'):self.run_candidate()
    def test_integer_fraction_not_truncated(self):
        self.data['source-native']['features'][0]['attributes']['OBJECTID']=7.5
        self.data['source-geojson']['features'][0]['properties']['OBJECTID']=7.5
        self.data['ids']['objectIds']=[7.5]
        with self.assertRaisesRegex(ValueError,'integer source type'):self.run_candidate()
    def test_new_field_retained_only_in_quarantine(self):
        self.data['layer']['fields'].append({'name':'EXTRA_ID','type':'esriFieldTypeString','nullable':False})
        self.data['source-native']['features'][0]['attributes']['EXTRA_ID']='00001'
        result=self.run_candidate();m=json.loads((self.root/'staging'/result['quarantine_prefix']/'metadata/provenance.json').read_text())
        self.assertIn('EXTRA_ID',m['candidate_columns']);self.assertFalse(m['promotion_allowed'])
    def test_checksum_change_rejected(self):
        self.write();(self.capture/'ids.body').write_text('{}')
        with self.assertRaisesRegex(ValueError,'checksum'):stage_candidate(self.storage,self.capture)
    def test_remote_storage_forbidden(self):
        with self.assertRaisesRegex(ValueError,'isolated local'):stage_candidate(StagingStorageResource(use_local=False,bucket='not-used'),self.capture)

    def test_invalid_geometry_preserved_unrepaired_and_flagged(self):
        self.data['source-geojson']['features'][0]['geometry']['coordinates']=[[[0,0],[1,1],[1,0],[0,1],[0,0]]]
        result=self.run_candidate();m=json.loads((self.root/'staging'/result['quarantine_prefix']/'metadata/provenance.json').read_text())
        self.assertEqual(m['invalid_geometry_count'],1);self.assertFalse(m['promotion_allowed'])
