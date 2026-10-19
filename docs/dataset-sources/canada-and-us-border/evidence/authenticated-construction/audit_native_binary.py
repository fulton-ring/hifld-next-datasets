"""Coordinator direct binary DBF/SHP audit against actual produced rows."""
import argparse
import hashlib
import io
import json
import struct
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
from shapely.geometry import LineString

parser = argparse.ArgumentParser()
parser.add_argument('runtime', type=Path)
args = parser.parse_args()
base = args.runtime
execution = json.loads((base / 'execution.json').read_text())
out = execution['output']
root = base / 'staging' / out['dataset_slug'] / out['file_slug'] / out['version']
rows = pq.read_table(root / 'candidate/typed-source.parquet').to_pylist()
by_fid = {int(row['__source_fid']): row for row in rows}
body = (root / 'raw/us-canada-boundary-v1-3.zip').read_bytes()
assert hashlib.sha256(body).hexdigest() == 'eb327459528b87cbc27e55ccc6bfd6982562c75559823a00b6dc50c04abcaab1'
with zipfile.ZipFile(io.BytesIO(body)) as archive:
    dbf = archive.read('US-Canada_Boundary_v1.3.dbf')
    shp = archive.read('US-Canada_Boundary_v1.3.shp')
    codepage = archive.read('US-Canada_Boundary_v1.3.cpg').strip()
    assert codepage == b'ANSI 1252'
count = struct.unpack_from('<I', dbf, 4)[0]
header_bytes, record_bytes = struct.unpack_from('<HH', dbf, 8)
fields = []
offset = 32
while dbf[offset] != 13:
    field = dbf[offset:offset + 32]
    fields.append((field[:11].split(b'\0')[0].decode(), chr(field[11]), field[16], field[17]))
    offset += 32
leading = []
for index in range(count):
    record = dbf[header_bytes + index * record_bytes:header_bytes + (index + 1) * record_bytes]
    assert record[:1] == b' '
    offset = 1
    for name, physical_type, width, precision in fields:
        raw = record[offset:offset + width]
        offset += width
        if physical_type == 'C':
            value = raw.decode('cp1252').rstrip(' \0')
            if value.startswith(' '):
                leading.append((index, name, value))
        else:
            assert physical_type in ('N', 'F')
            value = float(raw) if precision else int(raw)
        assert by_fid[index][name] == value, (index, name)
offset = 100
shapes = 0
vertices = 0
while offset < len(shp):
    record_number, words = struct.unpack_from('>II', shp, offset)
    record = shp[offset + 8:offset + 8 + words * 2]
    assert record_number == shapes + 1
    assert struct.unpack_from('<I', record, 0)[0] == 3
    parts, points = struct.unpack_from('<II', record, 36)
    assert parts == 1 and struct.unpack_from('<I', record, 44)[0] == 0
    coordinates = [struct.unpack_from('<dd', record, 48 + index * 16) for index in range(points)]
    assert LineString(coordinates).wkb == by_fid[shapes]['geometry']
    vertices += points
    shapes += 1
    offset += 8 + words * 2
assert offset == len(shp) and shapes == count == len(rows) == 30
report = {
    'source_code_head': execution['code_head'],
    'classification': 'Coordinator direct binary audit; independent native reviewer checks separately',
    'source_rows': count,
    'original_fields': fields,
    'original_codepage': codepage.decode('ascii'),
    'all_30_by_5_native_attribute_values_exact': True,
    'leading_character_values': leading,
    'all_original_binary_polyline_coordinates_wkb_exact': True,
    'raw_vertex_count': vertices,
    'complete_zip_sha256': hashlib.sha256(body).hexdigest(),
    'approved': False,
}
(base / 'manual-binary-audit.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
