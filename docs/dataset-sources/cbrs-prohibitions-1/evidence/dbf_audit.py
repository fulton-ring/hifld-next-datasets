import struct,json,zipfile,tempfile,hashlib,datetime
from pathlib import Path
import fiona
root=Path(__file__).resolve().parent
with zipfile.ZipFile(root/'captures/official_zip.body') as z:
 body=z.read('20241219_CBRS_Prohibitions.dbf');header,record=struct.unpack_from('<HH',body,8);count=struct.unpack_from('<I',body,4)[0]
 fields=[];offset=1
 for o in range(32,header-1,32):
  d=body[o:o+32];field={'name':d[:11].split(b'\0')[0].decode(),'physical_type':chr(d[11]),'width':d[16],'decimal_count':d[17],'record_offset':offset};fields.append(field);offset+=d[16]
 assert count==1693 and offset==record
 decoded=[];rawrows=[]
 for n in range(count):
  row=body[header+n*record:header+(n+1)*record];assert row[0:1]==b' '
  values={};raw={}
  for f in fields:
   text=row[f['record_offset']:f['record_offset']+f['width']].decode('utf-8');raw[f['name']]=text;t=text.strip()
   if f['physical_type']=='C':value=text.rstrip()
   elif f['physical_type']=='D':value=None if not t or t=='00000000' else datetime.datetime.strptime(t,'%Y%m%d').date().isoformat()
   elif f['physical_type']=='F':value=float(t)
   else:raise ValueError(f)
   values[f['name']]=value
  decoded.append(values);rawrows.append(raw)
 with tempfile.TemporaryDirectory() as d:
  z.extractall(d)
  with fiona.open(Path(d)/'20241219_CBRS_Prohibitions.shp') as f:
   rows=list(f);assert [dict(r['properties']) for r in rows]==decoded;assert [r.id for r in rows]==[str(n) for n in range(count)]
 report={'rows':count,'dbf_sha256':hashlib.sha256(body).hexdigest(),'record_bytes':record,'header_bytes':header,'field_descriptors':fields,'all_1693_manually_decoded_source_values_equal_Fiona':True,'all_original_record_ordinals_equal_retained_Fiona_intrinsic_FIDs':True,'all_original_field_strings':rawrows,'float_physical_type':'DBF F with scientific-notation ASCII payload, width19/decimal-count11; not DBF N or an API binary-double copy. Raw fields retained exactly; float64 is explicit decoded driver representation.'}
 (root/'physical-dbf-audit.json').write_text(json.dumps(report,indent=2)+'\n');print(count,'all physical DBF fields independently decoded equal Fiona')
