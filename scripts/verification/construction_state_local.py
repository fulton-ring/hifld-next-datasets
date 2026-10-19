"""Actual isolated Dagster first-construction lifecycle; no dataset acceptance."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path

from dagster import Definitions, DagsterInstance, asset, define_asset_job

from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.release_approval import require_release_approval
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource

KEY = '_construction-state/synthetic/v1.json'
FP = 'a' * 64
VERSION = 'v1'
RAW_KEY = 'synthetic/source/v1/raw/original.body'
RAW = b'complete synthetic original source\n'


def run(work: Path) -> dict:
    work.mkdir(parents=True, exist_ok=False)
    staging = work/'staging'; published = work/'published'; home = work/'dagster'
    for path in (staging, published, home): path.mkdir()
    source_storage=StagingStorageResource(local_dir=str(staging))
    destination=PublishedStorageResource(local_dir=str(published))
    (published/'prior-release.body').write_bytes(b'preserved prior release\n')
    mode={'source':'valid','receipt':'original receipt'}

    @asset
    def synthetic_construction():
        with (work/'common-version.lock').open('a+b') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            if mode['source']!='valid': raise ValueError('synthetic source validation failed before seal')
            body=canonical_construction_bundle(VERSION,FP,{'raw_sha256':hashlib.sha256(RAW).hexdigest(),'receipt':mode['receipt']})
            original=seal_first_construction(source_storage,KEY,VERSION,FP,body)
            raw=staging/RAW_KEY
            if raw.exists():
                if raw.read_bytes()!=RAW: raise ValueError('synthetic original raw changed')
            else: source_storage.write_key_if_unchanged(RAW_KEY,RAW,None)
            return hashlib.sha256(original).hexdigest()

    @asset(deps=[synthetic_construction])
    def synthetic_prepare():
        original=load_first_construction(source_storage,KEY,VERSION,FP)
        if json.loads(original)['payload']['raw_sha256']!=hashlib.sha256((staging/RAW_KEY).read_bytes()).hexdigest():
            raise ValueError('synthetic preparation raw differs')
        return hashlib.sha256(original).hexdigest()

    @asset(deps=[synthetic_prepare])
    def synthetic_promotion():
        # Real release gate, never mocked: absent registry must reject before copy.
        require_release_approval(source_storage,destination,[(RAW_KEY,RAW_KEY)])
        raise AssertionError('synthetic unapproved fixture unexpectedly promotable')

    definitions=Definitions(assets=[synthetic_construction,synthetic_prepare,synthetic_promotion],jobs=[
        define_asset_job('construct_prepare',selection=[synthetic_construction,synthetic_prepare]),
        define_asset_job('prepare_only',selection=[synthetic_prepare]),
        define_asset_job('promote_only',selection=[synthetic_promotion]),
    ])
    runs={}
    with DagsterInstance.local_temp(str(home), overrides={'telemetry': {'enabled': False}}) as instance:
        def execute(name,job,success):
            result=definitions.resolve_job_def(job).execute_in_process(instance=instance,raise_on_error=False)
            runs[name]={'run_id':result.run_id,'success':result.success}
            assert result.success==success, name
        execute('first_construction','construct_prepare',True)
        witness=load_first_construction(source_storage,KEY,VERSION,FP)
        immutable={str(p.relative_to(staging)):p.read_bytes() for p in staging.rglob('*') if p.is_file()}
        published_before={p.name:p.read_bytes() for p in published.iterdir()}
        mode['receipt']='newer refresh receipt'
        execute('refresh_preserves_original','construct_prepare',True)
        assert witness==load_first_construction(source_storage,KEY,VERSION,FP)
        mode['source']='invalid'
        execute('source_failure','construct_prepare',False)
        mode['source']='valid'
        alias=staging/KEY;quarantined=work/'quarantined-original-alias.body';alias.rename(quarantined)
        execute('missing_alias_preparation','prepare_only',False)
        execute('missing_alias_refresh_first','construct_prepare',False)
        execute('missing_alias_refresh_second','construct_prepare',False)
        assert not alias.exists()
        assert quarantined.read_bytes()==witness
        alias.write_bytes(witness) # Explicit exact trusted restoration in this synthetic operator fixture.
        execute('restored_original','construct_prepare',True)
        previous=os.environ.pop('HIFLD_RELEASE_APPROVALS',None)
        try: execute('missing_approval_promotion','promote_only',False)
        finally:
            if previous is not None: os.environ['HIFLD_RELEASE_APPROVALS']=previous
        assert immutable=={str(p.relative_to(staging)):p.read_bytes() for p in staging.rglob('*') if p.is_file()}
        assert published_before=={p.name:p.read_bytes() for p in published.iterdir()}
    result={'classification':'actual isolated registered Dagster synthetic construction; no real acceptance',
            'runs':runs,'original_bundle_sha256':hashlib.sha256(witness).hexdigest(),
            'staged_objects_preserved':len(immutable),'prior_published_objects_preserved':len(published_before),
            'raw_sha256':hashlib.sha256(RAW).hexdigest(),'newer_receipts_not_rebound':True,
            'missing_alias_second_refresh_blocked':True,'operator_restoration':'exact trusted original bytes, fixture only',
            'dagster_home':str(home)}
    (work/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work-dir',type=Path,required=True)
    print(json.dumps(run(parser.parse_args().work_dir.absolute()),indent=2))
