"""Offline complete-source/baseline replay. Run with repository src on PYTHONPATH."""
import json
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import shapely

from dagster_hifld.sources import canada_us_border as s
from dagster_hifld.resources import StagingStorageResource

root=Path(__file__).parent
with tempfile.TemporaryDirectory() as directory:
    captures=s.acquire(StagingStorageResource(local_dir=directory),root/'evidence/captures')
    manifest=s.provenance(captures);s.validate_release_provenance(manifest)
    rows,audit,members=s.inspect_archive(captures['ibc-zip'][0],Path(directory))
    baseline=pq.read_table(root/'evidence/baseline.parquet')
    receipt=json.loads((root/'evidence/baseline.receipt.json').read_text())
    assert s.sha((root/'evidence/baseline.parquet').read_bytes())==receipt['sha256']
    assert len(rows)==len(baseline)==30 and len(members)==11
    bm={r['SectionNum']:r for r in baseline.to_pylist()}
    assert len(bm)==30 and set(bm)==set(range(30))
    assert all(all(row['properties'][k]==bm[row['properties']['SectionNum']][k] for k in s.NATIVE_SCHEMA) for row in rows)
    assert all(shapely.from_wkb(bm[row['properties']['SectionNum']]['geometry']).geom_type=='MultiLineString' and len(shapely.from_wkb(bm[row['properties']['SectionNum']]['geometry']).geoms)==1 and shapely.from_wkb(bm[row['properties']['SectionNum']]['geometry']).geoms[0].wkb==shapely.geometry.shape(row['geometry']).wkb for row in rows)
    assert sum(str(bm[r['properties']['SectionNum']]['id'])==r['properties']['__source_fid'] for r in rows)==0
    assert sum(bm[r['properties']['SectionNum']]['id']==r['properties']['SectionNum'] for r in rows)==3
    print(json.dumps({'status':'offline_complete_evidence_replay_pass','approved':False,'source_rows':30,'baseline_rows':30,'shared_fields_exact':5,'native_coordinates_exact_inside_unapproved_geometry_wrapper':30,'original_zip_members':11,'provenance_inputs':len(manifest['inputs'])}))
