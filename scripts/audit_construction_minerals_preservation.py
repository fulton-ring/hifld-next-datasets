"""Read-only complete baseline/preservation comparison, no acceptance tolerance."""
import argparse, base64, collections, hashlib, json, math, pathlib, struct, xml.etree.ElementTree as ET
import fiona
from fiona.io import ZipMemoryFile
import pyarrow.parquet as pq
from pyproj import Geod, Transformer
from shapely import from_wkb


def dbf(body):
    n, head, width = struct.unpack_from('<IHH', body, 4)
    spec = []; offset = 32
    while body[offset] != 13:
        desc = body[offset:offset+32]
        spec.append({'name': desc[:11].split(b'\0')[0].decode('ascii'), 'type': chr(desc[11]), 'width': desc[16], 'decimals': desc[17]})
        offset += 32
    assert len(body) in (head+n*width, head+n*width+1)
    rows = []
    for fid in range(n):
        rec = body[head+fid*width:head+(fid+1)*width]; assert rec[:1] == b' '
        offset = 1; row = {}
        for field in spec:
            raw = rec[offset:offset+field['width']]; offset += field['width']
            value = raw.rstrip(b' ').decode('cp1252') if field['type']=='C' else (int(raw) if field['decimals']==0 else float(raw))
            row[field['name']] = value
        rows.append(row)
    return spec, rows


def points(body):
    assert struct.unpack_from('>i', body)[0] == 9994
    assert struct.unpack_from('>i', body,24)[0]*2 == len(body)
    assert struct.unpack_from('<ii', body,28)==(1000,1)
    out=[]; off=100
    while off<len(body):
        record, words = struct.unpack_from('>ii',body,off)
        typ,x,y=struct.unpack_from('<idd',body,off+8)
        assert typ==1 and words==10 and record==len(out)+1 and math.isfinite(x) and math.isfinite(y)
        out.append((x,y));off+=8+words*2
    assert off==len(body)
    return out


def audit(root):
    root=pathlib.Path(root); read=lambda name:(root/(name+'.body')).read_bytes()
    collection=json.loads(read('baseline_version')); provider=json.loads(read('baseline_current_metadata')); provider_pins={}
    for key,asset in collection['assets'].items():
        format_name=key.split('-')[0];name=asset['href'].split('/hifld-next-datasets-prod/',1)[1]
        obj=next(i for i in provider['items'] if i['name']==name);body=read('baseline_'+format_name)
        assert int(obj['size'])==len(body) and base64.b64encode(hashlib.md5(body).digest()).decode()==obj['md5Hash']
        provider_pins[format_name]={'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'MD5_matches':True,'generation':obj['generation']}
    assert len(provider_pins)==5
    xml=ET.fromstring(read('stanford_publicxml')); masters=[]
    for file in xml.iter('file'):
        if file.attrib.get('role')!='master':continue
        name=file.attrib['id']; b=read(name)
        assert len(b)==int(file.attrib['size'])
        for checksum in file.findall('checksum'):
            assert hashlib.new(checksum.attrib['type'].lower().replace('-',''),b).hexdigest()==checksum.text
        masters.append({'name':name,'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()})
    assert len(masters)==5
    spec,rawrows=dbf(read('minop2x020.dbf')); rawpts=points(read('minop2x020.shp'))
    assert len(rawrows)==len(rawpts)==766
    shx=read('minop2x020.shx');assert len(shx)==100+8*766
    for i in range(766):assert struct.unpack_from('>ii',shx,100+8*i)==((100+i*28)//2,10)
    raw={row['MINOP2X020']:(row,rawpts[i]) for i,row in enumerate(rawrows)};assert set(raw)==set(range(1,767))
    parquet=pq.read_table(root/'baseline_geoparquet.body'); pub={r['MINOP2X020']:r for r in parquet.to_pylist()};assert len(pub)==766 and set(pub)==set(raw)
    fields=[f['name'] for f in spec]; common=['OBJECTID']+fields
    geo=json.loads(read('baseline_geojson'));gpkg=[]
    with fiona.open(root/'baseline_geopackage.body') as c:
        gpkg_schema=c.schema;gpkg_crs=c.crs.to_string();gpkg=[{'id':int(f.id),'attrs':dict(f.properties),'xy':tuple(f.geometry.coordinates)} for f in c]
    with ZipMemoryFile(read('baseline_shapefile')) as mem:
        with mem.open() as c:
            shp_schema=c.schema;shp_crs=c.crs.to_string();shprows=[{'id':int(f.id),'attrs':dict(f.properties),'xy':tuple(f.geometry.coordinates)} for f in c]
    baseline_rows={'geojson':[f['properties'] for f in geo['features']],'geopackage':[f['attrs'] for f in gpkg],'shapefile':[f['attrs'] for f in shprows]}
    baseline_comparison={}
    ref=collections.Counter(tuple(row[k] for k in common) for row in pub.values())
    for name,rows in baseline_rows.items():
        assert len(rows)==766 and collections.Counter(tuple(row[k] for k in common) for row in rows)==ref
        baseline_comparison[name]={'count':len(rows),'all_10_attributes_exact_multiset_equal_parquet':True}
    baseline_comparison['geojson']['extra_published_FID_values']={'min':min(f['properties']['FID'] for f in geo['features']),'max':max(f['properties']['FID'] for f in geo['features']),'equals_feature_id':all(f['id']==f['properties']['FID']==f['properties']['OBJECTID'] for f in geo['features'])}
    baseline_comparison['geopackage'].update({'native_fid_range':[min(f['id'] for f in gpkg),max(f['id'] for f in gpkg)],'schema':gpkg_schema,'crs':gpkg_crs})
    baseline_comparison['shapefile'].update({'native_fid_range':[min(f['id'] for f in shprows),max(f['id'] for f in shprows)],'schema':shp_schema,'crs':shp_crs})
    pasda=json.loads(read('pasda_features_native')); assert pasda.get('exceededTransferLimit',False) is False
    ps={f['attributes']['MINOP2X020']:f for f in pasda['features']};assert len(ps)==766
    ids=json.loads(read('pasda_ids'))['objectIds'];ids_after=json.loads(read('pasda_ids_after'))['objectIds']
    assert set(ids)==set(ids_after)=={f['attributes']['OBJECTID_1'] for f in ps.values()}
    assert json.loads(read('pasda_count'))['count']==json.loads(read('pasda_count_after'))['count']==766
    assert json.loads(read('pasda_layer'))==json.loads(read('pasda_layer_after'))
    assert all(all(f['attributes'][key]==pub[k][key] for key in common) for k,f in ps.items())
    per_field={};examples=[]
    for key in fields:
        mismatch=[k for k,(r,_) in raw.items() if r[key]!=pub[k][key]]
        uppercase_mismatch=[k for k,(r,_) in raw.items() if (r[key].upper() if isinstance(r[key],str) else r[key])!=pub[k][key]]
        per_field[key]={'exact_mismatched_rows':len(mismatch),'mismatched_after_hypothetical_uppercase_diagnostic_only':len(uppercase_mismatch)}
        if uppercase_mismatch:examples.extend({'MINOP2X020':k,'field':key,'preserved':raw[k][0][key],'published':pub[k][key]} for k in uppercase_mismatch[:3])
    transform=Transformer.from_crs(3857,4326,always_xy=True);geod=Geod(ellps='WGS84');dist=[];psdist=[];baselinegeom={}
    for k,(row,(x,y)) in raw.items():
        p=from_wkb(pub[k]['geometry']); lon,lat=transform.transform(p.x,p.y)
        dist.append(geod.inv(x,y,lon,lat)[2]);q=ps[k]['geometry'];psdist.append(geod.inv(q['x'],q['y'],lon,lat)[2])
    for name,features in [('geojson',[{'id':f['properties']['MINOP2X020'],'xy':f['geometry']['coordinates']} for f in geo['features']]),('geopackage',[{'id':f['attrs']['MINOP2X020'],'xy':f['xy']} for f in gpkg]),('shapefile',[{'id':f['attrs']['MINOP2X020'],'xy':f['xy']} for f in shprows])]:
        errors=[]
        for f in features:
            p=from_wkb(pub[f['id']]['geometry']);errors.append(max(abs(f['xy'][0]-p.x),abs(f['xy'][1]-p.y)))
        baselinegeom[name]={'exact_equal_rows':sum(e==0 for e in errors),'max_absolute_coordinate_difference_meters':max(errors)}
    return {'schema_version':1,'accepted_source':False,'accepted_baseline':False,'accepted_tolerance':None,'raw_master_pins':masters,'baseline_provider_pins':provider_pins,'native_count':766,'native_physical_DBF':spec,'native_FID_range':[0,765],'native_MINOP2X020_range':[1,766],'native_states':len({r['STATE_LOCA'] for r in rawrows}),'native_commodities':dict(collections.Counter(r['COMMODITY'] for r in rawrows)),'preserved_prj':read('minop2x020.prj').decode(),'official_FGDC_datum':'North American Datum of 1983','baseline_schema':str(parquet.schema),'baseline_all_formats':baseline_comparison,'baseline_geometry_comparisons_no_tolerance':baselinegeom,'pmtiles':'full bytes/provider MD5 verified; features not independently decoded','preservation_vs_baseline_by_MINOP2X020':per_field,'non_case_difference_examples':examples,'preservation_vs_baseline_geometry':{'comparison':'preserved PRJ interpreted EPSG4326; published nativeEPSG3857 transformed EPSG4326; not an approved transform/mapping','different_all_rows':all(d>0 for d in dist),'min_geodesic_m':min(dist),'max_geodesic_m':max(dist)},'pasda':{'classification':'third-party national HIFLD mirror; no agency delegation established','count':766,'before_after_count_ids_metadata_unchanged':True,'all_10_attributes_exact_equal_baseline':True,'extra_native_ids':['OBJECTID_1','FID_1'],'physical_fields':pasda['fields'],'native_crs':pasda['spatialReference'],'vs_baseline_geometry_minmax_m':[min(psdist),max(psdist)]}}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('capture_dir');p.add_argument('output');a=p.parse_args()
    result=audit(a.capture_dir);pathlib.Path(a.output).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:result[k] for k in ['native_count','native_states','preservation_vs_baseline_by_MINOP2X020','preservation_vs_baseline_geometry']}))
