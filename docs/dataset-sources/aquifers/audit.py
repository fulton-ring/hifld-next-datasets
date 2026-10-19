"""Replay full source/baseline populations; no ID/CRS/migration acceptance inferred.

PYTHONPATH=src uv run python docs/dataset-sources/aquifers/audit.py --captures DIR
Complete raw ZIP + baseline Parquet and original HTTP receipts remain in DIR.
"""
import argparse
import collections
import json
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import shapely
from dagster_hifld.sources import aquifers as source

BASELINE_SHA = '3897534f901612081bfe1cb47e6bb1e20827c93ccec8d29bb5e128b18c784078'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--captures',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();root=args.captures
    body=root.joinpath('zip.body').read_bytes();receipt=source.strict_json(root.joinpath('zip.json').read_bytes())
    source.validate_receipt(body,receipt)
    baseline_bytes=root.joinpath('baseline.body').read_bytes()
    assert source.sha(baseline_bytes)==BASELINE_SHA
    baseline_receipt=source.strict_json(root.joinpath('baseline.json').read_bytes())
    assert baseline_receipt['headers']['x-goog-generation']=='1791506350105881'
    with tempfile.TemporaryDirectory() as tmp:
        rows,audit,members=source.inspect_archive(body,Path(tmp))
    table=pq.read_table(root/'baseline.body');published=table.to_pylist()
    original=[r['properties'] for r in rows];index={r['OBJECTID']:r for r in published}
    shared=['ROCK_NAME','ROCK_TYPE','AQ_NAME','AQ_CODE','Shape_Leng']
    direct={name:sum(r[name]!=index[r['OBJECTID_1']][name] for r in original) for name in shared}
    fields=['ROCK_TYPE','AQ_CODE','AQ_NAME','Shape_Leng'];key=lambda r:tuple(r[k] for k in fields)
    source_tuple=collections.Counter(key(r) for r in original);published_tuple=collections.Counter(key(r) for r in published)
    assert all(v==1 for v in source_tuple.values()) and all(v==1 for v in published_tuple.values())
    tuple_index={key(r):r for r in published}
    renamed=sum(r['OBJECTID_1']!=tuple_index[key(r)]['OBJECTID'] for r in original if key(r) in tuple_index)
    geometry=[shapely.from_wkb(r['geometry']) for r in published]
    report={'approved':False,'status':'migration_decision_required','complete_populations':True,
        'producer_source':audit,'published_candidate':{'sha256':BASELINE_SHA,'generation':'1791506350105881',
            'bytes':len(baseline_bytes),'feature_count':len(table),'arrow_schema':str(table.schema),
            'geo_metadata':json.loads(table.schema.metadata[b'geo']),
            'ids':{'OBJECTID':{'unique':len(index),'nulls':sum(r['OBJECTID'] is None for r in published)}},
            'invalid_geometry_count':sum(not g.is_valid for g in geometry),
            'bounds':shapely.total_bounds(geometry).tolist(),
            'nulls':{name:sum(r[name] is None for r in published) for name in shared}},
        'source_vs_published':{'missing_source_columns_in_published':['OBJECTID_1','Shape_Area'],
            'published_only_columns':['OBJECTID','bbox'],
            'source_crs':'EPSG:4269','published_crs':'EPSG:3857',
            'physical_integer_widths':'source int32 DBF vs published int64 Arrow',
            'numeric_id_join_attribute_differences':direct,
            'diagnostic_shared_attribute_tuple':{'columns':fields,'unique_source':len(source_tuple),'unique_published':len(published_tuple),
                'matched':sum((source_tuple&published_tuple).values()),'renumbered_ids':renamed,
                'approved_crosswalk':False,'geometry_comparison':'Different nativeCRS/coordinates; no accepted reprojection,tolerance,repair or ID crosswalk. Invalidsource20 vs validpublished0 retained; no geometryequivalence inferred.'},
            'source_null_rock_name_published_fill':{'source_nulls':audit['nulls']['ROCK_NAME'],'published_nulls':sum(r['ROCK_NAME'] is None for r in published),
                'source_null_published_values':dict(collections.Counter(tuple_index[key(r)]['ROCK_NAME'] for r in original if r['ROCK_NAME'] is None))}},
        'acceptance_prerequisites':['DHS crosswalk provider403 or explicit unmatched acceptance',
            'accept static2003producer release scope; not unrelated2023three-aquifer prototype',
            'approve versioned physicalschema/null/IDmapping preserving source and historicalpublishedIDs',
            'decide20nativeinvalid geometries without silently repairing/deleting; explicit quality exception or reviewed derivedmapping',
            'independent exact release acceptance registry; no selfapproval']}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_bytes(source.encode(report));print(args.output)


if __name__=='__main__':main()
