"""Anonymous live acquisition transport proof; raw-only isolation, no promotion."""
from __future__ import annotations
import argparse
import ast
import json
import subprocess
from datetime import datetime,timezone
from pathlib import Path

import httpx
from dagster_hifld.resources import StagingStorageResource
from dagster_hifld.sources import cbrs_map_panels as s

parser=argparse.ArgumentParser();parser.add_argument('--runtime',type=Path,required=True);args=parser.parse_args()
root=args.runtime.resolve();root.mkdir(parents=True,exist_ok=False);repo=Path(__file__).resolve().parents[1]
report={'head':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),'observed_at':datetime.now(timezone.utc).isoformat(),'approved':False,'mode':'actual_live_acquire_capture_dir_None','canonical_or_published_writes':False}
# Separately observe the old decoded/wire mismatch using default HTTPX negotiation.
with httpx.Client(follow_redirects=False,timeout=45) as client:
    with client.stream('GET',s.LANDING) as response:
        decoded=b''
        for block in response.iter_bytes(chunk_size=65536):
            decoded+=block
            assert len(decoded)<=s.MAX_BYTES
        report['default_encoding_control']={'url':str(response.url),'status':response.status_code,'content_encoding':response.headers.get('content-encoding'),'content_length_header':response.headers.get('content-length'),'decoded_bytes':len(decoded),'wire_bytes':response.num_bytes_downloaded,'sha256':s.sha(decoded)}
        (root/'default-encoding-control.body').write_bytes(decoded)
storage=StagingStorageResource(local_dir=str(root/'raw-only'),use_local=True)
captures=s.acquire(storage,None)
assert set(captures)==set(s.URLS)
manifest=s.provenance(captures);rows,audit,_=s.inspect_source(captures)
assert len(rows)==574
capture=root/'captures';capture.mkdir()
report['live_inputs']={}
for name,(body,receipt) in captures.items():
    (capture/(name+'.body')).write_bytes(body);(capture/(name+'.receipt.json')).write_bytes(s.encode(receipt))
    report['live_inputs'][name]=receipt
    assert receipt['status']==200
    if receipt['content_length_header'] is not None:assert int(receipt['content_length_header'])==receipt['wire_byte_length']
reference=repo/'docs/dataset-sources/cbrs-map-panels-1/evidence/captures/page0000.body'
assert captures['page0000'][0]==reference.read_bytes()
originals={n:((reference.parent/(n+'.body')).read_bytes(),json.loads((reference.parent/(n+'.receipt.json')).read_bytes())) for n in s.URLS}
oldrows,oldaudit,_=s.inspect_source(originals)
assert s.encode(rows)==s.encode(oldrows) and audit==oldaudit
report.update(all_live_capture_slots=len(captures),all_live_status200=True,complete_count574=True,before_after_count_and_ids_coherent=True,complete_provenance_inputs=len(manifest['inputs']),live_original_page_sha256=s.sha(captures['page0000'][0]),all574_source_ID_values_types_native_geometry_byte_identical=True,full_original_field_declarations_retained=True,release_date_unknown=True,canonical_namespace_absent=not Path(storage.local_dir,s.SLUG).exists())
assert report['canonical_namespace_absent']
old=ast.parse(subprocess.check_output(['git','show','78e1f02:src/dagster_hifld/sources/cbrs_map_panels.py'],cwd=repo,text=True));new=ast.parse(Path(s.__file__).read_text())
report['unchanged_data_constructor_AST']={}
for name in ['inspect_source','typed_table','write_geopackage']:
    before=ast.dump(next(n for n in old.body if isinstance(n,ast.FunctionDef) and n.name==name));after=ast.dump(next(n for n in new.body if isinstance(n,ast.FunctionDef) and n.name==name));assert before==after;report['unchanged_data_constructor_AST'][name]=s.sha(before.encode())
(root/'source_manifest.json').write_bytes(s.encode(manifest));(root/'source_audit.json').write_bytes(s.encode(audit));(root/'execution.json').write_bytes(s.encode(report))
print(json.dumps({'head':report['head'],'inputs':report['all_live_capture_slots'],'rows':574,'control':report['default_encoding_control'],'live_transport':[{'name':n,'encoding':r['content_encoding'],'decoded':r['byte_length'],'wire':r['wire_byte_length']} for n,r in report['live_inputs'].items()]}))
