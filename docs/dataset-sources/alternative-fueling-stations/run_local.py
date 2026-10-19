"""Replay complete preserved source through local Dagster and the real gate.

Usage: ... run_local.py CAPTURE_ROOT ISOLATED_ROOT v0.0.0-research-N
No approvals are created. Publication is expected to fail until independently
accepted, and the local previous-release fixture must remain byte-identical.
"""
import hashlib
import json
import os
import pathlib
import sys

from dagster import AssetKey, DagsterInstance, materialize
from dagster_hifld.assets.fuel_stations import SLUG, bts_alternative_fueling_stations
from dagster_hifld.assets.catalog import catalog_assets
from dagster_hifld.assets.publish import publish_assets
from dagster_hifld.partitions import PUBLISH_PARTITIONS
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.promotion import promote_immutable_objects
from dagster_hifld.release_approval import ReleaseApprovalError


def main(capture, root, version):
    root.mkdir(parents=True, exist_ok=True)
    for key in ('HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS'):
        os.environ.pop(key,None)
    home=root/'dagster';home.mkdir(exist_ok=True)
    (home/'dagster.yaml').write_text('telemetry:\n  enabled: false\n')
    os.environ['DAGSTER_HOME']=str(home.resolve())
    staging=StagingStorageResource(local_dir=str(root/'staging'),use_local=True)
    published=PublishedStorageResource(local_dir=str(root/'published'),use_local=True)
    sentinel='previous-local-fixture/keep.bin'
    published.write_key(sentinel,b'preserve previous local fixture')
    before={k:published.sha256_key(k) for k in published.list_prefix('')}
    prefix=f'{SLUG}/{SLUG}/{version}'
    record={'version':version,'local_only':True,'accepted_fixture_before':before}
    with DagsterInstance.get() as instance:
        ingest=materialize([bts_alternative_fueling_stations],instance=instance,
            resources={'staging_storage':staging},run_config={'ops':{bts_alternative_fueling_stations.op.name:{'config':{'capture_dir':str(capture.resolve()),'version':version}}}},raise_on_error=False)
        record['ingest']={'success':ingest.success,'run_id':ingest.run_id}
        if not ingest.success:raise RuntimeError('Draft source staging failed; retain run logs')
        instance.add_dynamic_partitions(PUBLISH_PARTITIONS.name,[prefix])
        assets=catalog_assets+[a for a in publish_assets if next(iter(a.keys)).path!=['portolan','catalog']]
        catalog=materialize(assets,selection=[AssetKey(['publish','catalog'])],instance=instance,partition_key=prefix,
            resources={'staging_storage':staging,'published_storage':published},raise_on_error=False)
        record['catalog']={'success':catalog.success,'run_id':catalog.run_id,
            'errors':[x.event_specific_data.error.to_string() for x in catalog.get_step_failure_events()]}
        candidate=f'{prefix}/draft_candidate/candidate.parquet'
        try:promote_immutable_objects(staging,published,[(candidate,f'{prefix}/geoparquet/{SLUG}.parquet')],version_prefix=prefix)
        except ReleaseApprovalError as error:record['expected_promotion_rejection']=str(error)
        else:raise AssertionError('Unapproved real source unexpectedly published')
        after={k:published.sha256_key(k) for k in published.list_prefix('')}
        assert after==before and (pathlib.Path(staging.local_dir)/candidate).exists()
        record.update(accepted_fixture_after=after,accepted_preserved=True,candidate_preserved=True)
    (root/'validation.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record,indent=2))


if __name__=='__main__':main(pathlib.Path(sys.argv[1]),pathlib.Path(sys.argv[2]),sys.argv[3])
