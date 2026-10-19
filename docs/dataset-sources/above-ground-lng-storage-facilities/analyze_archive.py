"""Offline, bounded PASDA snapshot analysis. Not an accepted published baseline."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import struct
import zipfile
import fiona
from shapely.geometry import shape

ROOT = Path(__file__).parent
p = ROOT / 'evidence' / 'pasda-zip.zip'
member = 'Above_Ground_LNG_Storage_Facilities/Above_Ground_LNG_Storage_Facilities_pt.shp'
with zipfile.ZipFile(p) as z:
    members = {n: {'bytes': len(z.read(n)), 'sha256': hashlib.sha256(z.read(n)).hexdigest()} for n in z.namelist() if not n.endswith('/')}
    dbf = z.read(member.replace('.shp', '.dbf'))
    nrows, header_length, record_length = struct.unpack_from('<IHH', dbf, 4)
    fields = []
    offset = 32
    while dbf[offset] != 13:
        descriptor = dbf[offset:offset+32]
        fields.append({'name': descriptor[:11].split(b'\0')[0].decode(), 'type': chr(descriptor[11]), 'width': descriptor[16], 'decimal_places': descriptor[17]})
        offset += 32
    raw_ids = []
    for i in range(nrows):
        record = dbf[header_length+i*record_length:header_length+(i+1)*record_length]
        start = 1
        ids = {}
        for field in fields:
            value = record[start:start+field['width']]
            if field['name'] in ['AGLNGID', 'FAC_GOVID', 'COUNTYFIPS', 'ZIP', 'ZIP4', 'NAICS_CODE']:
                ids[field['name']] = {'raw_ascii': value.decode('ascii'), 'raw_hex': value.hex()}
            start += field['width']
        raw_ids.append(ids)
with fiona.open('zip://' + str(p.resolve()) + '!' + member) as collection:
    features = list(collection)
    props = [dict(f.properties) for f in features]
    schema = dict(collection.schema)
    geometry = {'type': schema['geometry'], 'crs': str(collection.crs), 'bounds': list(collection.bounds), 'null': sum(f.geometry is None for f in features), 'invalid': sum(not shape(f.geometry).is_valid for f in features if f.geometry), 'empty': sum(shape(f.geometry).is_empty for f in features if f.geometry)}
report = {'snapshot_role': 'mirror; PA-clipped/deduplicated, NOT accepted baseline or national continuation', 'zip_sha256': hashlib.sha256(p.read_bytes()).hexdigest(), 'members': members, 'dbf_record_count': nrows, 'feature_count': len(features), 'dbf_fields': fields, 'fiona_schema': schema, 'geometry': geometry, 'columns': {k: {'null_count':sum(row[k] is None for row in props), 'distinct_count':len({row[k] for row in props})} for k in schema['properties']}, 'coverage': {k:dict(Counter(str(row[k]) for row in props)) for k in ['STATE','COUNTYFIPS','TYPE','STATUS']}, 'ids': {k:{'values':[row[k] for row in props], 'null_count':sum(row[k] is None for row in props), 'duplicate_nonnull_count':sum(v-1 for v in Counter(row[k] for row in props if row[k] is not None).values())} for k in ['AGLNGID','FAC_GOVID']}, 'raw_id_cells':raw_ids, 'source_date_values': sorted({str(row['SOURCEDATE']) for row in props})}
output = ROOT / 'evidence' / 'archive-analysis.json'
text = json.dumps(report, indent=2, sort_keys=True) + '\n'
if output.exists():
    assert output.read_text() == text, 'offline replay differs'
    print('PASS: byte checksums, schema, 13 features, IDs, geometry and coverage replay exactly')
else:
    output.write_text(text)
    print('Created bounded archive analysis')
