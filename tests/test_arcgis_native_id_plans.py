"""Actual adapters use the shared page owner and keep native refusal evidence."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import arcgis_raw_capture as owner
from dagster_hifld.sources import state_emergency_operations_centers as eoc
from dagster_hifld.sources import state_electric_territories as electric
from dagster_hifld.sources import usace_recommended_tracks as tracks
import test_usace_recommended_tracks_source as tracks_fixtures


class NativeIdPlanTests(unittest.TestCase):
    def test_eoc_metadata_batch_policy_and_refused_schema_preserve_native_prefix(self):
        fields=[{'name':name,'type':kind,'nullable':nullable,'length':length}
                for name,kind,nullable,length in eoc.EXPECTED_FIELDS]
        layer={'objectIdField':'objectid','maxRecordCount':1,'fields':fields}
        seen=[]
        def respond(request):
            params=request.url.params;seen.append(dict(params))
            if 'objectIds' in params:
                oid=int(params['objectIds']);page_fields=[dict(field)for field in fields]
                if oid==2:page_fields[1]['type']='esriFieldTypeInteger'
                value={'spatialReference':{'wkid':4326},'fields':page_fields,
                    'features':[{'attributes':{'objectid':oid,'zip':'00123'},'geometry':None}]}
            elif 'returnIdsOnly' in params:value={'objectIdFieldName':'objectid','objectIds':[2,1]}
            elif 'returnCountOnly' in params:value={'count':2}
            else:value=layer
            return httpx.Response(200,stream=httpx.ByteStream(json.dumps(value).encode()))
        real_client=httpx.Client
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);storage=StagingStorageResource(local_dir=str(root/'stage'))
            with patch.object(eoc.httpx,'Client',side_effect=lambda *a,**kw:real_client(*a,transport=httpx.MockTransport(respond),**kw)),patch.object(
                    owner,'capture_exact_id_population',wraps=owner.capture_exact_id_population)as common:
                with self.assertRaisesRegex(eoc.EOCSourceError,'query_physical_schema_changed'):
                    eoc.stage_state_eoc(storage,version='v1',capture_dir=str(root/'captures'))
                common.assert_called_once()
            self.assertEqual([r['objectIds']for r in seen if 'objectIds'in r],['1','2'])
            self.assertEqual(sum('returnCountOnly'in r for r in seen),1)
            self.assertEqual(sum('returnIdsOnly'in r for r in seen),1)
            retained=root/'captures'/'v1'
            self.assertEqual(json.loads((retained/'features_00000.decoded').read_bytes())['features'][0]['attributes']['zip'],'00123')
            self.assertTrue((retained/'features_00001.receipt.json').is_file())
            self.assertFalse((retained/'layer_after.receipt.json').exists())

    def test_recommended_tracks_preserves_native_projection_and_refuses_second_page_schema(self):
        fixture=tracks_fixtures.IencFixture(expected_count=2)
        seen=[]
        def respond(request):
            seen.append(request)
            response=fixture.handler(request);value=json.loads(response.content)
            if request.url.params.get('objectIds')=='1':
                value['features'][0]['geometry']['spatialReference']={'wkid':4326}
            if request.url.params.get('objectIds')=='2':value['fields'][1]['type']='esriFieldTypeInteger'
            return httpx.Response(200,stream=httpx.ByteStream(json.dumps(value).encode()))
        with tempfile.TemporaryDirectory()as tmp,httpx.Client(transport=httpx.MockTransport(respond))as client:
            root=Path(tmp);capture=root/'captures';storage=StagingStorageResource(local_dir=str(root/'stage'))
            with patch.object(tracks,'PAGE_SIZE',1),patch.object(tracks,'EXPECTED_EXTENT',{key:fixture.extent[key]for key in ('xmin','ymin','xmax','ymax')}),patch.object(
                    owner,'capture_exact_id_population',wraps=owner.capture_exact_id_population)as common:
                with self.assertRaisesRegex(ValueError,'source_page_schema_drift'):
                    tracks.capture(storage,capture_directory=str(capture),client=client)
                common.assert_called_once()
            self.assertEqual([r.url.params['objectIds']for r in seen if 'objectIds'in r.url.params],['1','2'])
            self.assertTrue((capture/'page_000000.body').is_file())
            self.assertTrue((capture/'page_000001.receipt.json').is_file())
            self.assertFalse((capture/'ids_after.body').exists())
            self.assertTrue(storage.list_prefix('_quarantine/'+tracks.DATASET_SLUG))

    def _iowa_capture(self, refuse_page):
        ids=list(range(1,102));fields=[{'name':name,'type':kind,'length':length}for name,kind,length in electric.IOWA_FIELDS]
        layer={'id':14,'geometryType':'esriGeometryPolygon','objectIdField':'OBJECTID','hasZ':True,'hasM':False,
               'extent':{'spatialReference':{'wkid':102004}},'fields':fields}
        seen=[]
        def respond(request):
            params=request.url.params;seen.append(dict(params))
            if 'objectIds'in params:
                wanted=list(map(int,params['objectIds'].split(',')))
                value={'geometryType':'esriGeometryPolygon','spatialReference':{'wkid':102004},
                    'fields':fields if wanted[0]==1 or not refuse_page else fields[:-1],
                    'features':[{'attributes':{'OBJECTID':oid,'Owner':'  Native  '},'geometry':None}for oid in wanted]}
            elif 'returnIdsOnly'in params:value={'objectIds':list(reversed(ids))}
            elif 'returnCountOnly'in params:value={'count':len(ids)+(1 if not refuse_page and sum('returnCountOnly'in item for item in seen)>1 else 0)}
            else:value=layer
            return httpx.Response(200,stream=httpx.ByteStream(json.dumps(value).encode()))
        real_client=httpx.Client
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp)
            with patch.object(electric.httpx,'Client',side_effect=lambda *a,**kw:real_client(*a,transport=httpx.MockTransport(respond),**kw)),patch.object(
                    owner,'capture_exact_id_population',wraps=owner.capture_exact_id_population)as common:
                with self.assertRaisesRegex(ValueError,'iowa_page_incomplete_or_drift' if refuse_page else 'iowa_changed_during_capture'):
                    electric.acquire_iowa(root,False)
                common.assert_called_once()
            pages=[r for r in seen if 'objectIds'in r]
            self.assertEqual([len(r['objectIds'].split(','))for r in pages],[100,1])
            self.assertTrue(all(r['returnZ']=='true'and r['returnM']=='false'and r['returnTrueCurves']=='true'for r in pages))
            self.assertTrue((root/'page_00000.body').is_file())
            self.assertTrue((root/'page_00100.receipt.json').is_file())
            self.assertEqual((root/'count_after.body').exists(),not refuse_page)
            self.assertFalse((root/'ids_after.body').exists())
            self.assertFalse((root/'layer_after.body').exists())

    def test_iowa_keeps_order_fields_crs_and_refuses_second_page_before_witnesses(self):
        self._iowa_capture(True)

    def test_iowa_closing_count_refusal_precedes_later_witness_requests(self):
        self._iowa_capture(False)
