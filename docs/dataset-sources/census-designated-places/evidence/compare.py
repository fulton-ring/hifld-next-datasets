import pathlib,json,collections,pyogrio,pyarrow.parquet as pq
p=pathlib.Path('/workspace/work/hifld-coordinator/34')
m,s=pyogrio.read_arrow('/vsizip/'+str(p/'official2024_substategeo.zip')+'/tlgdb_2024_a_us_substategeo.gdb',layer='Census_Designated_Place',return_fids=True);b=pq.read_table(p/'baseline.parquet');sr={r['GEOID']:r for r in s.to_pylist()};br={r['GEOID']:r for r in b.to_pylist()};common=sorted(sr.keys()&br.keys())
fields={'GEOID':'GEOID','PLACENS':'PLACENS','NAMELSAD':'NAME','CLASSFP':'PLACECC','FUNCSTAT':'FUNCSTAT','INTPTLAT':'INTPTLAT','INTPTLON':'INTPTLON'}
report={'approved':False,'classification':'complete unapproved published baseline candidate versus exact inventory2024 producer layer; compatibility and mapping pending',
 'source_rows':s.num_rows,'baseline_rows':b.num_rows,'source_crs':m['crs'],'baseline_geo_metadata':json.loads(b.schema.metadata[b'geo']),
 'source_schema':str(s.schema),'baseline_schema':str(b.schema),'source_GEOID_unique':len(sr)==s.num_rows,'baseline_GEOID_unique':len(br)==b.num_rows,'diagnostic_GEOID_common':len(common),
 'source_only_GEOIDs':sorted(sr.keys()-br.keys()),'baseline_only_GEOIDs':sorted(br.keys()-sr.keys()),
 'source_original_OBJECTID_range':[min(s['OBJECTID'].to_pylist()),max(s['OBJECTID'].to_pylist())],'baseline_OBJECTID_range':[min(b['OBJECTID'].to_pylist()),max(b['OBJECTID'].to_pylist())],
 'diagnostic_common_OBJECTID_equal':sum(sr[g]['OBJECTID']==br[g]['OBJECTID'] for g in common),
 'diagnostic_proposed_field_value_difference_counts':{f'{sf}->{bf}':sum(sr[g][sf]!=br[g][bf] for g in common) for sf,bf in fields.items()},
 'no_accepted_join':'GEOID is only a diagnostic semantic cross-check, not an approved replacement ID mapping. All original published OBJECTID/id/OID/STATE/PLACE and all source IDs remain retained independently.',
 'geometry':'Source native4269 and published3857 differ. No coordinate conversion or geometry equivalence claim is made by the pipeline; any diagnostic transformation and historical geometry mapping needs review.',
 'areas':'Source ALAND/AWATER native doubles and published AREALAND/AREAWATER strings are not silently cast or renamed.'}
p.joinpath('baseline-comparison.json').write_text(json.dumps(report,indent=2));print({k:v for k,v in report.items() if k in ['source_rows','baseline_rows','diagnostic_GEOID_common','source_only_GEOIDs','baseline_only_GEOIDs','diagnostic_common_OBJECTID_equal','diagnostic_proposed_field_value_difference_counts']})
