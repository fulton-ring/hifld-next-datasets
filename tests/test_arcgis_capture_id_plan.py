"""The common owner chooses ID pages and closes witnesses only after validation."""
import json
import tempfile
import unittest
from pathlib import Path

import httpx

from dagster_hifld.sources import arcgis_capture_sequence as common
from dagster_hifld.sources.capture_common import capture_get


class ExactIdPlanTests(unittest.TestCase):
    def _run(self, refused=False):
        seen=[]
        ids=[9,2,18]
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp)
            def respond(request):
                seen.append(request.url.path)
                wanted=[int(value)for value in request.url.params.get('ids','').split(',')if value]
                fields=[{'name':'FID','type':'esriFieldTypeOID'},{'name':'native','type':'esriFieldTypeString'}]
                if refused and wanted==[18]:fields[1]['name']='renamed'
                return httpx.Response(200,stream=httpx.ByteStream(json.dumps({'fields':fields,'features':[
                    {'attributes':{'FID':oid,'native':f'00{oid}'},'geometry':None}for oid in wanted]}).encode()))
            def validate(offset,wanted,response):
                page=json.loads(response[0])
                if page['fields']!=[{'name':'FID','type':'esriFieldTypeOID'},{'name':'native','type':'esriFieldTypeString'}]:
                    raise ValueError('native_page_schema_drift')
                self.assertEqual([f['attributes']['FID']for f in page['features']],wanted)
                return page['features']
            def steps():
                return (yield common.ExactIdCapturePlan(ids,2,
                    page_request=lambda offset,wanted:(f'page_{offset}','https://example.test/page_'+str(offset)+'?ids='+','.join(map(str,wanted))),
                    validate_page=validate,
                    read_page=lambda offset:(root/f'page_{offset}.body').read_bytes(),
                    closing_requests=(('ids_after','https://example.test/ids_after'),('count_after','https://example.test/count_after'))))
            with httpx.Client(transport=httpx.MockTransport(respond))as client:
                operation=lambda:common.capture_request_steps(steps(),lambda name,url:capture_get(client,url,root,name))
                if refused:
                    with self.assertRaisesRegex(ValueError,'native_page_schema_drift'):operation()
                    self.assertEqual(seen,['/page_0','/page_2'])
                    self.assertTrue((root/'page_0.body').is_file())
                    self.assertTrue((root/'page_2.receipt.json').is_file())
                    self.assertFalse((root/'ids_after.body').exists())
                else:
                    features,pages,closing=operation()
                    self.assertEqual([f['attributes']['FID']for f in features],ids)
                    self.assertEqual([f['attributes']['native']for f in features],['009','002','0018'])
                    self.assertEqual([p['requested_ids']for p in pages],[[9,2],[18]])
                    self.assertEqual(list(closing),['ids_after','count_after'])
                    self.assertEqual(seen,['/page_0','/page_2','/ids_after','/count_after'])
    def test_exact_native_order_fields_and_closing_witnesses(self):self._run()
    def test_refused_schema_retains_prefix_without_closing_witnesses(self):self._run(True)
