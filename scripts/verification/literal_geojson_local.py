"""Actual registered synthetic-only literal JSON catalog/gate preservation.

Run with --runtime an EMPTY isolated directory. Original source acceptance and
production publication are not established by these synthetic negative cases.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

from dagster import AssetSelection, DagsterInstance, Definitions, define_asset_job


parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--runtime',type=Path,required=True)
runtime=parser.parse_args().runtime.absolute()
if runtime.exists() and any(runtime.iterdir()):raise ValueError('Use an empty isolated runtime')
for name in ['staging','published','dagster']:(runtime/name).mkdir(parents=True,exist_ok=True)
(runtime/'dagster/dagster.yaml').write_text('telemetry:\n  enabled: false\n')
for key in ['HIFLD_STAGING_BUCKET','HIFLD_DATASETS_BUCKET','HIFLD_RELEASE_APPROVALS']:
    os.environ.pop(key,None)
os.environ.update(DAGSTER_HOME=str(runtime/'dagster'),HIFLD_STAGING_DIR=str(runtime/'staging'),
    HIFLD_DATASETS_DIR=str(runtime/'published'),HIFLD_SHADOW_MANUAL_ONLY='1')

# Imports remain at module scope after explicit isolated environment setup.
from dagster_hifld.definitions import defs as normal
from dagster_hifld.checks import publish_catalog_quality_manifest
from dagster_hifld.partitions import PUBLISH_PARTITION_NAME
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource


def run(runtime):
    staging=StagingStorageResource(local_dir=str(runtime/'staging'));published=PublishedStorageResource(local_dir=str(runtime/'published'))
    published.write_key('synthetic-prior.bin',b'Prior synthetic output remains exact; no real dataset accepted.')
    prior={key:published.read_key(key)for key in published.list_prefix()}
    job=define_asset_job('literal_geojson_actual_registered',selection=AssetSelection.assets(*[key for key in normal.resolve_asset_graph().get_all_asset_keys()if key.path[0]=='publish'])|AssetSelection.checks(publish_catalog_quality_manifest))
    definitions=Definitions(assets=normal.assets,asset_checks=normal.asset_checks,
        resources={'staging_storage':staging,'published_storage':published},jobs=[job])
    commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
    report={'schema_version':1,'scope':'Synthetic catalog/gate mechanics only, no real source/contract/release acceptance','code_commit':commit,'runs':[]}
    with DagsterInstance.get()as instance:
        for mode in ['valid_mixed','tail_null','tail_invalid','byte_drift']:
            count=6001 if mode.startswith('tail') else 6
            values=[{'type':'Feature','properties':{'ID':2**53+index,'mixed':[1,'',True,1.0,None,{'nested':[False,0,1.0]}][index%6],
                'geometry':'ordinary literal attribute'},'geometry':{'type':'Point','coordinates':[1,2]},
                **({'id':'001'}if index==0 else{})}for index in range(count)]
            values[0]['properties']['sometimes_null']=None
            if mode=='tail_null':values[-1]['geometry']=None
            if mode=='tail_invalid':values[-1]['geometry']={'type':'Polygon','coordinates':[[[0,0],[9,9],[0,9],[9,0],[0,0]]]}
            body=json.dumps({'type':'FeatureCollection','features':values},separators=(',',':')).encode()
            prefix='synthetic-literal/synthetic-literal/v0.0.0-'+mode
            profile={'schema_version':1,'path':'geojson/native.geojson','bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()}
            metadata={'schema_version':1,'processor':'HIFLD Next','code_commit':commit,'transformations':[{'operation':'synthetic_literal_preservation','parameters':{}}],
                'inputs':[{'input_id':'synthetic-'+mode,'producer':'Synthetic Example Agency','host':'example.invalid','role':'producer_publication',
                    'canonical_url':'https://example.invalid/synthetic','retrieval_url':'https://example.invalid/synthetic','retrieved_at':'2026-10-19T00:00:00Z',
                    'sha256':profile['sha256'],'bytes':len(body),'license':'Synthetic test','attribution':'Synthetic example only',
                    'release_date_unknown_reason':'No real source release represented by synthetic fixture'}],
                'literal_geojson_catalog':profile,'unsupported_output_formats':{fmt:'Synthetic literal JSON has no approved homogeneous conversion/mapping'for fmt in ['geopackage','geoparquet','shapefile_zip','pmtiles']}}
            staging.write_key(prefix+'/geojson/native.geojson',body)
            staging.write_key(prefix+'/metadata/source_manifest.json',json.dumps(metadata).encode())
            if mode=='byte_drift':staging.write_key(prefix+'/geojson/native.geojson',body+b' ')
            instance.add_dynamic_partitions(PUBLISH_PARTITION_NAME,[prefix])
            result=definitions.resolve_job_def(job.name).execute_in_process(instance=instance,partition_key=prefix,raise_on_error=False)
            assert not result.success
            errors=[event.event_specific_data.error.to_string()for event in result.get_step_failure_events()]
            checks=[{'name':check.check_name,'passed':check.passed}for check in result.get_asset_check_evaluations()]
            assert prior=={key:published.read_key(key)for key in published.list_prefix()}
            if mode=='valid_mixed':
                assert any('missing_independent_release_approval_registry'in error for error in errors)
                quality=json.loads(staging.read_key(prefix+'/metadata/quality_manifest.json'));assert quality['quality_check_passed']
                dictionary=json.loads(staging.read_key(prefix+'/metadata/data_dictionary.json'))
                columns={entry['name']:entry for entry in dictionary['columns']}
                assert columns['mixed']['nativeJSONTypeCounts']=={'integer':1,'string':1,'boolean':1,'float':1,'null':1,'object':1}
                assert dictionary['featureIDMember']['typedExamples'][0]['value']=='001'
            elif mode.startswith('tail'):
                quality=json.loads(staging.read_key(prefix+'/metadata/quality_manifest.json'));assert not quality['quality_check_passed']
                assert quality['feature_count']==6001 and quality['sampled_feature_count']==5000
                assert any(not check['passed']for check in checks)
            else:assert any('pinned_size'in error for error in errors)
            assert staging.read_key(prefix+'/geojson/native.geojson')==(body+b' 'if mode=='byte_drift'else body)
            report['runs'].append({'case':mode,'run_id':result.run_id,'success':result.success,'errors':errors,'checks':checks,'prior_preserved':True,'native_bytes_preserved':True})
            (runtime/'execution-progress.json').write_text(json.dumps(report,indent=2)+'\n')
    (runtime/'execution.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'cases':len(report['runs']),'all_expected_blocked':True,'prior_preserved':True,'release_approved':False}))


if __name__=='__main__':
    run(runtime)
