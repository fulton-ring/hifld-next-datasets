"""Shared decoders for the USGS-owned MRDATA mineplant 2003 survey distribution.

The construction-minerals and crushed-stone explicit historical
reconstructions both read this single USGS producer package. These helpers
only decode already-acquired bytes; they fetch nothing, poll nothing and grant
no source, datum, schema/ID mapping, contract or release acceptance.
"""
from __future__ import annotations
import math
import re
import struct
import zipfile
import io
from decimal import Decimal
import shapely

class MineplantQuarantineError(ValueError): pass

MINEPLANT_MEMBERS={'mineplant.dbf':1655861,'mineplant.shp':190080,'mineplant.shx':54380,'mineplant.txt':670146,'mineplant.prj':368}
MINEPLANT_DBF_FIELDS=[('id','N',8,0),('COMMODITY','C',30,0),('SITE_NAME','C',50,0),('COMPANY_NA','C',50,0),('STATE_LOCA','C',40,0),('COUNTY','C',40,0),('LATITUDE','N',10,5),('LONGITUDE','N',10,5),('PLANT_MIN','C',5,0)]
MINEPLANT_RECORDS=6785
MINEPLANT_PRJ='GEOGCS["GCS_North_American_1927",DATUM["D_North_American_1927",SPHEROID["Clarke_1866", 6378206.4, 294.9786982138982, AUTHORITY["EPSG","7008"]],TOWGS84[-9.0, 151.0, 185.0, 0.0, 0.0, 0.0, 0.0],AUTHORITY["EPSG","6267"]],PRIMEM["Greenwich", 0.0, AUTHORITY["EPSG","8901"]],UNIT["degree", 0.017453292519943295],AXIS["Lon", EAST],AXIS["Lat", NORTH],AUTHORITY["EPSG","4267"]]'


def decode_mineplant_zip(body):
    try:archive=zipfile.ZipFile(io.BytesIO(body))
    except (OSError,ValueError,zipfile.BadZipFile,EOFError) as exc:raise MineplantQuarantineError('invalid_usgs_package_container') from exc
    names=archive.namelist()
    if len(names)!=len(set(names)) or set(names)!=set(MINEPLANT_MEMBERS) or any(archive.getinfo(name).file_size!=MINEPLANT_MEMBERS[name] for name in names):raise MineplantQuarantineError('usgs_package_member_roster_or_size_drift')
    return {name:archive.read(name) for name in MINEPLANT_MEMBERS}


def decode_mineplant_dbf(body):
    if len(body)<321:raise MineplantQuarantineError('incomplete_usgs_dbf')
    count,header,width=struct.unpack_from('<IHH',body,4);fields=[];offset=32
    while offset+32<=header and body[offset]!=13:
        desc=body[offset:offset+32];fields.append((desc[:11].split(b'\0')[0].decode('ascii'),chr(desc[11]),desc[16],desc[17]));offset+=32
    if fields!=MINEPLANT_DBF_FIELDS or count!=MINEPLANT_RECORDS or width!=244 or header!=321 or body[offset]!=13 or len(body) not in (header+count*width,header+count*width+1):raise MineplantQuarantineError('missing_renamed_extra_ID_type_width_count_or_DBF_completeness_drift')
    if len(body)==header+count*width+1 and body[-1]!=26:raise MineplantQuarantineError('unexpected_dbf_tail')
    rows=[];seen=set()
    for fid in range(count):
        rec=body[header+fid*width:header+(fid+1)*width]
        if rec[:1]!=b' ':raise MineplantQuarantineError('deleted_or_unknown_dbf_record')
        offset=1;row={'source_fid':fid,'source_shp_record_number':fid+1}
        for name,kind,size,scale in fields:
            raw=rec[offset:offset+size];offset+=size
            if kind=='C':
                if any(c>127 or c<32 for c in raw):raise MineplantQuarantineError('undeclared_nonASCII_source_encoding')
                row[name]=raw.rstrip(b' ').decode('ascii')
            else:
                text=raw.decode('ascii').strip()
                if not re.fullmatch(r'-?\d+(?:\.\d+)?',text):raise MineplantQuarantineError('null_nonfinite_or_unparseable_DBF_number')
                value=Decimal(text)
                if value.as_tuple().exponent<-scale:raise MineplantQuarantineError('declared_DBF_scale_drift')
                row[name]=value
        ident=int(row['id'])
        if ident<1 or ident in seen:raise MineplantQuarantineError('missing_duplicate_or_nonpositive_usgs_mineplant_id')
        seen.add(ident);rows.append(row)
    return rows


def crosscheck_mineplant_txt(body,rows):
    lines=body.decode('ascii').splitlines()
    if len(lines)!=len(rows)+1 or lines[0].split('\t')!=['ID','COMMODITY','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN']:raise MineplantQuarantineError('usgs_txt_roster_or_header_drift')
    def norm(value): return '' if value.strip().upper()=='NULL' else value.rstrip()
    for line,row in zip(lines[1:],rows):
        cells=line.split('\t')
        if len(cells)!=9 or int(cells[0])!=int(row['id']) or norm(cells[1])!=norm(row['COMMODITY']) or norm(cells[2])!=norm(row['SITE_NAME']) or norm(cells[3])!=norm(row['COMPANY_NA']) or norm(cells[4])!=norm(row['STATE_LOCA']) or norm(cells[5])!=norm(row['COUNTY']) or Decimal(cells[6])!=row['LATITUDE'] or Decimal(cells[7])!=row['LONGITUDE'] or norm(cells[8])!=norm(row['PLANT_MIN']):raise MineplantQuarantineError('usgs_txt_dbf_value_drift')


def decode_points(body,index,count):
    if len(body)<100 or len(index)!=100+8*count:raise MineplantQuarantineError('incomplete_SHP_SHX')
    for data in (body,index):
        if struct.unpack_from('>i',data)[0]!=9994 or struct.unpack_from('>i',data,24)[0]*2!=len(data) or struct.unpack_from('<ii',data,28)!=(1000,1):raise MineplantQuarantineError('native_geometry_header_type_or_length_drift')
    out=[];offset=100
    for fid in range(count):
        if offset+28>len(body):raise MineplantQuarantineError('incomplete_point_records')
        record,words=struct.unpack_from('>ii',body,offset);typ,x,y=struct.unpack_from('<idd',body,offset+8)
        if record!=fid+1 or words!=10 or typ!=1 or struct.unpack_from('>ii',index,100+8*fid)!=(offset//2,10) or not all(math.isfinite(v) for v in (x,y)):raise MineplantQuarantineError('SHP_SHX_record_ID_offset_type_or_coordinate_drift')
        out.append(shapely.Point(x,y).wkb);offset+=28
    if offset!=len(body):raise MineplantQuarantineError('extra_point_records')
    geometry=shapely.from_wkb(out)
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):raise MineplantQuarantineError('invalid_empty_native_point')
    bounds=shapely.total_bounds(geometry).tolist()
    if list(struct.unpack_from('<4d',body,36))!=bounds or index[36:68]!=body[36:68]:raise MineplantQuarantineError('native_bounds_or_index_drift')
    return out


def verify_mineplant_geometry_values(rows,geometry):
    if len(rows)!=len(geometry):raise MineplantQuarantineError('usgs_record_geometry_alignment_drift')
    for row,wkb in zip(rows,geometry):
        point=shapely.from_wkb(wkb)
        if (point.x,point.y)!=(float(row['LONGITUDE']),float(row['LATITUDE'])):raise MineplantQuarantineError('usgs_geometry_attribute_value_drift')
