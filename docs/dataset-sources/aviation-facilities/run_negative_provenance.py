"""Actual Dagster replay of malformed metadata on complete authentic source data.

Run after run_local.py --source-catalog-only. Deliberate metadata mutations are
fixtures only; original captures and every page/geometry/attribute remain intact.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys

from dagster import AssetSelection, DagsterInstance, Definitions, define_asset_job
from dagster_hifld.assets import hifld_dataset_assets
from dagster_hifld.assets.faa import faa_aviation_facilities
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

capture=Path(sys.argv[1]);runtime=Path(sys.argv[2]);bad=runtime/'bad-provenance-capture'
shutil.copytree(capture,bad)
staging=StagingStorageResource(local_dir=str(runtime/'staging'))
published=PublishedStorageResource(local_dir=str(runtime/'published'))
state=runtime/'dagster';os.environ['DAGSTER_HOME']=str(state)
good=json.loads((runtime/'execution.json').read_bytes());version=good['ingest']['output']['version']
prefix=f'aviation-facilities/aviation-facilities/{version}'
canonical=lambda:{key:hashlib.sha256(staging.read_key(key)).hexdigest() for key in staging.list_prefix('aviation-facilities')}
accepted=lambda:{key:hashlib.sha256(published.read_key(key)).hexdigest() for key in published.list_prefix()}
prior=canonical();prior_accepted=accepted()
item_body=(bad/'item.body').read_bytes();item_receipt=(bad/'item.receipt.json').read_bytes()
original_receipt=json.loads(item_receipt);extra=bad/'page-retained-diagnostic.receipt.json'
proof={'synthetic_mutated_metadata_on_authentic_complete_19427_input':True,'runs':[],'prior_version':version}
with DagsterInstance.from_config(str(state)) as instance:
    job=define_asset_job('aviation_negative_provenance',selection=AssetSelection.assets(faa_aviation_facilities))
    defs=Definitions(assets=hifld_dataset_assets,resources={'staging_storage':staging,'published_storage':published},jobs=[job])
    for case in ('extra-receipt','headers-null','license','attribution'):
        (bad/'item.body').write_bytes(item_body);(bad/'item.receipt.json').write_bytes(item_receipt)
        if extra.exists():extra.unlink()
        if case=='extra-receipt':
            extra.write_text(json.dumps({'observed_at':original_receipt['observed_at'],'headers':None,'role':'diagnostic_fixture'}))
        elif case=='headers-null':
            receipt=dict(original_receipt,headers=None);(bad/'item.receipt.json').write_text(json.dumps(receipt))
        else:
            item=json.loads(item_body);item['licenseInfo' if case=='license' else 'accessInformation']=''
            body=json.dumps(item,sort_keys=True).encode();(bad/'item.body').write_bytes(body)
            receipt=dict(original_receipt,sha256=hashlib.sha256(body).hexdigest(),bytes=len(body))
            (bad/'item.receipt.json').write_text(json.dumps(receipt))
        r=defs.resolve_job_def('aviation_negative_provenance').execute_in_process(instance=instance,
            run_config={'ops':{faa_aviation_facilities.op.name:{'config':{'capture_dir':str(bad)}}}},raise_on_error=False)
        assert not r.success and canonical()==prior and accepted()==prior_accepted
        failures=[e.event_specific_data.error.cause.message.strip() for e in r.get_step_failure_events()]
        assert len(failures)==1
        changed=extra if case=='extra-receipt' else bad/('item.receipt.json' if case=='headers-null' else 'item.body')
        wanted=hashlib.sha256(changed.read_bytes()).hexdigest()
        quarantine=list((runtime/'staging/quarantine').rglob(changed.name))
        assert any(hashlib.sha256(p.read_bytes()).hexdigest()==wanted for p in quarantine)
        proof['runs'].append({'case':case,'run_id':r.run_id,'success':False,'reason':failures[0],
            'no_new_canonical_output':True,'all_prior_canonical_and_accepted_bytes_unchanged':True,'raw_quarantine_retained':True})
(runtime/'negative-provenance.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof,indent=2))
