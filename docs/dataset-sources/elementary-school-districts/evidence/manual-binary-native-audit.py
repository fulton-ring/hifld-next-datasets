"""Independent physical DBF/SHP audit of actually materialized native outputs.

Uses original binary records directly; never calls the adapter's source reader,
expected-table builder, writer, or fidelity comparator.
"""
import collections,hashlib,json,struct,zipfile
from pathlib import Path
import pyogrio,pyarrow.parquet as pq,pyproj,shapely
R=Path('/workspace/work/hifld-coordinator/64');C=R/'capture';runtime=R/'cloud-c0-registered';SLUG='elementary-school-districts'
progress=json.loads((runtime/'execution-progress.json').read_bytes());first=next(x for x in progress['runs']if x['label']=='source');assert first['success']
root=runtime/'staging'/SLUG/SLUG/first['output']['version']
gm,gt=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer='Elementary_School_Districts')
pt=pq.read_table(root/'geoparquet'/f'{SLUG}.parquet');gr=gt.to_pylist();pr=pt.to_pylist()
assert len(gr)==len(pr)==2030
gmap={r['GEOID']:r for r in gr};assert len(gmap)==2030
assert gr==pr
manifest=json.loads((root/'metadata/source_manifest.json').read_bytes());assert manifest['code_commit']=='a6391d662fd43380ffbc5fed4f1b73c7053f495e'
lineage=[json.loads(line)for line in(root/'metadata/source_record_lineage.jsonl').read_bytes().splitlines()]
lm={r['original_identifiers']['GEOID']:r for r in lineage};assert len(lineage)==len(lm)==2030
pins=json.loads((R/'source-pins.json').read_bytes())
rows=0;coords=0;rings=0;all_geoids=[];states=collections.Counter();geotypes=collections.Counter();nulls=collections.Counter();archives={};raw_seen={}
def ring_sha(sequence):
    return hashlib.sha256(b''.join(struct.pack('<dd',x,y)for x,y in sequence)).hexdigest()
for name,pin in pins.items():
    raw=(C/(name+'.body')).read_bytes();assert len(raw)==pin['bytes']and hashlib.sha256(raw).hexdigest()==pin['sha256']
    with zipfile.ZipFile(C/(name+'.body'))as z:
        assert z.testzip()is None
        dbf=z.read(name+'.dbf');shp=z.read(name+'.shp');prj=z.read(name+'.prj').decode()
    assert pyproj.CRS.from_wkt(prj).equals(pyproj.CRS(4269),ignore_axis_order=True)
    count,header,width=struct.unpack('<IHH',dbf[4:12]);assert len(dbf)in [header+count*width,header+count*width+1]
    fields=[];offset=1
    for start in range(32,header-1,32):
        f=dbf[start:start+32];n=f[:11].split(b'\0')[0].decode('ascii');kind=chr(f[11]);size=f[16];decimals=f[17]
        fields.append((n,kind,size,decimals,offset));offset+=size
    assert len(fields)==15 and offset==width
    assert tuple(gt.column_names)==('OGC_FID',*[f[0]for f in fields],'wkb_geometry')
    for n,kind,_,_,_ in fields:
        assert str(pt[n].type)==('int64'if kind=='N'else'string')
    assert 2*struct.unpack('>i',shp[24:28])[0]==len(shp)and struct.unpack('<i',shp[32:36])[0]==5
    pos=100;filecoords=0
    for fid in range(count):
        record=dbf[header+fid*width:header+(fid+1)*width];assert record[0]==32
        expected={}
        for n,kind,size,decimals,offset in fields:
            value=record[offset:offset+size]
            if kind=='C':
                v=value.rstrip(b' \0').decode('utf8');expected[n]=v if v else None
            else:
                assert kind=='N'and decimals==0
                expected[n]=int(value.decode('ascii').strip())
        geoid=expected['GEOID'];actual=gmap[geoid]
        assert {n:actual[n]for n in expected}==expected,(name,fid,'attributes')
        assert type(actual['OGC_FID'])is int and actual['OGC_FID']==fid
        states[expected['STATEFP']]+=1;nulls.update(n for n,v in expected.items()if v is None)
        all_geoids.append(geoid)
        number,words=struct.unpack('>ii',shp[pos:pos+8]);body=shp[pos+8:pos+8+2*words];pos+=8+2*words
        assert number==fid+1 and struct.unpack('<i',body[:4])[0]==5
        nparts,npoints=struct.unpack('<ii',body[36:44]);parts=list(struct.unpack('<'+'i'*nparts,body[44:44+4*nparts]))
        assert parts[0]==0 and sorted(set(parts))==parts
        point_start=44+4*nparts;assert len(body)==point_start+16*npoints
        original_ring_sha=collections.Counter()
        for begin,end in zip(parts,parts[1:]+[npoints]):
            original_ring_sha[hashlib.sha256(body[point_start+16*begin:point_start+16*end]).hexdigest()]+=1
        geom=shapely.from_wkb(actual['wkb_geometry']);assert geom.is_valid and not geom.is_empty
        geotypes[geom.geom_type]+=1;produced_ring_sha=collections.Counter()
        polygons=[geom]if geom.geom_type=='Polygon'else list(geom.geoms)
        for polygon in polygons:
            for ring in [polygon.exterior,*polygon.interiors]:produced_ring_sha[ring_sha(ring.coords)]+=1
        assert produced_ring_sha==original_ring_sha,(name,fid,'original ordered ring bytes')
        assert int(shapely.get_num_coordinates(geom))==npoints
        assert tuple(geom.bounds)==struct.unpack('<dddd',body[4:36])
        line=lm[geoid]
        assert line['source_archive']==name+'.zip'and line['source_member']==name+'.dbf'
        assert line['original_file_local_OGC_FID']==fid
        assert line['original_identifiers']=={n:expected[n]for n in ['STATEFP','ELSDLEA','GEOID','GEOIDFQ']}
        assert line['original_input_sha256']==pin['sha256']
        key=f"_raw-captures/{SLUG}/{pin['sha256']}/{name}.body"
        assert line['raw_body_key']==key
        if key not in raw_seen:
            b=(runtime/'staging'/key).read_bytes();assert b==raw;raw_seen[key]=hashlib.sha256(b).hexdigest()
        rows+=1;coords+=npoints;rings+=nparts;filecoords+=npoints
    assert pos==len(shp)
    archives[name]={'rows':count,'original_ordered_coordinates':filecoords,'original_DBF_declarations':[list(f[:4])for f in fields]}
assert rows==2030 and coords==1455682 and geotypes=={'Polygon':1930,'MultiPolygon':100}
assert gt['GEOID'].to_pylist()==all_geoids
assert gm['crs']=='EPSG:4269'and pyproj.CRS(json.loads(pt.schema.metadata[b'geo'])['columns']['wkb_geometry']['crs']).equals(pyproj.CRS(4269))
assert 'source_archive'not in gt.column_names and 'source_archive'not in pt.column_names
report={'head':manifest['code_commit'],'source_run_id':first['run_id'],'method':'Independent direct DBF/SHP binary parser to actual produced GP/Parquet; no adapter source/expected/writer functions called','archives':archives,'rows':rows,'all_original_DBF_attribute_values_checked':rows*15,'all_original_FIDs_checked':rows,'lineage_records_checked':rows,'original_ordered_ring_coordinate_bytes_exact':True,'original_rings':rings,'original_coordinates':coords,'native_geometry_types':dict(geotypes),'state_counts':dict(states),'null_counts':dict(nulls),'native_crs':'EPSG:4269','CRS_equivalence':'Original ESRI PRJ NAD83 GIS XY equals EPSG4269 ignoring axis-descriptor order; every raw XY coordinate byte is separately checked against terminal WKB, with no axis swap or reprojection.','terminal_columns':gt.column_names,'no_constructed_main_column':True,'GP_Parquet_all_rows_fields_values_WKB_exact':True,'source_raw_copies_identical':len(raw_seen),'approval':False}
(R/'manual-binary-native-audit.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items()if k!='archives'},indent=2))
