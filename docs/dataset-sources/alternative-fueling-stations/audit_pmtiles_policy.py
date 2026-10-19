"""Verify the final exclusion against preserved real tiles without regenerating.

Usage: uv run python .../audit_pmtiles_policy.py NORMAL_RUN_ROOT NEW_PROBE_ROOT OUTPUT
The probe uses a separate local manifest projection, never edits the immutable
source version, and never approves/publishes. Original generated tiles remain
an unapproved diagnostic; feature IDs and per-zoom coverage are not certified.
"""
import json
import os
import pathlib
import shutil
import sys

from dagster_hifld.assets.fuel_stations import SLUG, UNSUPPORTED_OUTPUT_FORMATS, _digest, _strict_json, processing_identity
from dagster_hifld.assets.publish import _write_and_publish_pmtiles
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.promotion import promote_immutable_objects
from dagster_hifld.release_approval import ReleaseApprovalError


def main(run, probe, output):
    probe.mkdir(parents=True,exist_ok=False)
    os.environ.pop('HIFLD_RELEASE_APPROVALS',None)
    version=_strict_json((run/'validation.json').read_bytes())['version']
    prefix=f'{SLUG}/{SLUG}/{version}'
    original=run/'staging'/prefix
    staging=StagingStorageResource(local_dir=str(probe/'staging'),use_local=True)
    published=PublishedStorageResource(local_dir=str(probe/'published'),use_local=True)
    manifest=_strict_json((original/'metadata/source_manifest.json').read_bytes())
    manifest['unsupported_output_formats']=dict(UNSUPPORTED_OUTPUT_FORMATS)
    manifest['policy_probe_only']={'meaning':'Separate exclusion projection; original immutable source version not modified.',
        'policy_processing':processing_identity()}
    staging.write(SLUG,SLUG,version,'metadata/source_manifest.json',json.dumps(manifest,sort_keys=True).encode())
    tiles=sorted((original/'pmtiles').glob('*.pmtiles'))
    assert len(tiles)==1
    key=f'{prefix}/pmtiles/{SLUG}.pmtiles'
    target=pathlib.Path(staging.local_dir)/key;target.parent.mkdir(parents=True)
    shutil.copyfile(tiles[0],target)
    tile_hash=_digest(target)
    published.write_key('previous-local-fixture/keep.bin',b'preserve previous local fixture')
    before={k:published.sha256_key(k) for k in published.list_prefix('')}
    outputs=_write_and_publish_pmtiles(staging,SLUG,SLUG,version)
    assert len(outputs)==1 and outputs[0].path==''
    reason=outputs[0].source_metadata['skip_reason']
    assert reason.startswith('unsupported_output_format:')
    try:
        promote_immutable_objects(staging,published,[(key,key)],version_prefix=prefix)
    except ReleaseApprovalError as error:
        rejection=str(error)
    else:raise AssertionError('Unapproved diagnostic tiles unexpectedly promoted')
    after={k:published.sha256_key(k) for k in published.list_prefix('')}
    assert rejection=='missing_independent_release_approval_registry'
    assert before==after and tile_hash==_digest(target)==_digest(tiles[0])
    result={'policy_processing':processing_identity(),'local_only':True,
        'original_generated_tiles':{'bytes':tiles[0].stat().st_size,'sha256':tile_hash,'role':'diagnostic only; no rendered-ID or per-zoom coverage proof'},
        'exclusion_before_existing_reuse':True,'skip_reason':reason,'generated_again':False,
        'original_immutable_manifest_unchanged':True,'probe_manifest_only':True,
        'direct_promotion_rejection':rejection,'previous_release_before':before,'previous_release_after':after,
        'previous_release_preserved':True,'diagnostic_bytes_preserved':True,'approval':None}
    output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))


if __name__=='__main__':main(*(pathlib.Path(value).resolve() for value in sys.argv[1:]))
