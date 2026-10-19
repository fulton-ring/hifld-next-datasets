"""Replay every captured source/published row; correspondence is not an approved join."""
from pathlib import Path
import collections
import hashlib
import json
import fiona
import pyarrow.parquet as pq
import shapely
import pyproj

R=Path(__file__).resolve().parent
body=(R/'captures/features.body').read_bytes();source=json.loads(body)
original={f['attributes']['STATION']:f for f in source['features']}
parquet=pq.read_table(R/'published-candidate/current-dart-deployments.parquet')
rows=parquet.to_pylist();old={row['STATION']:row for row in rows}
geojson=json.loads((R/'published-candidate/current-dart-deployments.geojson').read_bytes())
oldgeo={f['properties']['STATION']:f for f in geojson['features']}
with fiona.open(R/'published-candidate/current-dart-deployments.gpkg') as stream:
    gpschema=dict(stream.schema);gpcrs=pyproj.CRS(stream.crs).to_string()
    gp={f['properties']['STATION']:{'source_fid':f.id,'properties':dict(f['properties']),'wkb':shapely.geometry.shape(f['geometry']).wkb.hex()} for f in stream}
assert len(original)==len(old)==len(oldgeo)==len(gp)==62
assert set(original)==set(old)==set(oldgeo)==set(gp)
common=['STATION','DESCRIPTION','DEPLOYED','DATA_URL','OID']
result={'approved':False,'classification':'unapproved_published_candidate_diagnostic',
 'source_count':len(original),'published_counts':{'geoparquet':len(old),'geojson':len(oldgeo),'geopackage':len(gp)},
 'source_schema':source['fields'],'published_parquet_schema':str(parquet.schema),'published_geopackage_schema':gpschema,
 'source_crs':source['spatialReference'],'published_geopackage_crs':gpcrs,
 'all_station_values_retained':True,'source_deployment_coverage':dict(collections.Counter(f['attributes']['DEPLOYED'] for f in original.values())),
 'identifier_fields':{'source':['STATION','OBJECTID','OID'],'published_geojson':['STATION','OBJECTID','OID'],'published_parquet':['STATION','OID'],'published_geopackage':['STATION','OID','intrinsic_fid']},
 'common_attribute_changed_counts':{k:sum(original[s]['attributes'][k]!=old[s][k] for s in original) for k in common},
 'source_objectid_differs_published_geojson':sum(original[s]['attributes']['OBJECTID']!=oldgeo[s]['properties']['OBJECTID'] for s in original),
 'original_oid_differs_published_oid':sum(original[s]['attributes']['OID']!=old[s]['OID'] for s in original),
 'native_wkb_exact_count':sum(shapely.geometry.Point(original[s]['geometry']['x'],original[s]['geometry']['y']).wkb==old[s]['geometry'] for s in original),
 'native_xy_max_absolute_difference':max(max(abs(original[s]['geometry'][k]-shapely.from_wkb(old[s]['geometry']).coords[0][i]) for i,k in enumerate(['x','y'])) for s in original),
 'ids':[{ 'station':s,'source_OBJECTID':original[s]['attributes']['OBJECTID'],'source_OID':original[s]['attributes']['OID'],'published_geojson_OBJECTID':oldgeo[s]['properties']['OBJECTID'],'published_OID':old[s]['OID'],'published_gp_fid':gp[s]['source_fid']} for s in sorted(original)],
 'files':{str(p.relative_to(R)):{'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for folder in ['captures','published-candidate'] for p in sorted((R/folder).rglob('*')) if p.is_file()},
 'release_gate':'No source, baseline, schema/nullability/int32-vs-int64/ID mapping/format approval. All published identifiers remain in full raw candidates; no join or destructive schema migration applied.'}
(R/'baseline-audit.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k not in ['ids','files','source_schema','published_parquet_schema','published_geopackage_schema']}))
