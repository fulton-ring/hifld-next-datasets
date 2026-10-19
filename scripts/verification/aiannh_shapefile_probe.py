"""Retain an incompatible regenerated AIANNH Shapefile outside candidate storage."""
import argparse
import json
import zipfile
from pathlib import Path

import fiona
import geopandas as gpd

from dagster_hifld.conversion import ShapefileZipPolicy, write_shapefile_zip
from dagster_hifld.sources.aiannh import PIN, SCHEMA, SLUG
from dagster_hifld.validation import sha256_file

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--geopackage', type=Path, required=True)
parser.add_argument('--workdir', type=Path, required=True)
args = parser.parse_args()
args.workdir.mkdir(parents=True, exist_ok=True)
result = write_shapefile_zip(gpd.read_file(args.geopackage), args.workdir, SLUG, ShapefileZipPolicy())
if not result.created:
    raise ValueError('Probe did not generate a local derivative')
with fiona.open(f'/vsizip/{result.path.resolve()}/{SLUG}.shp') as source:
    schema, count, crs = dict(source.schema), len(source), source.crs.to_string()
with zipfile.ZipFile(result.path) as archive:
    members = archive.namelist()
report = {'source_archive_sha256': PIN, 'regenerated_zip_sha256': sha256_file(result.path),
          'quarantined_path': str(result.path), 'feature_count': count, 'crs': crs,
          'physical_schema': schema, 'source_physical_schema': SCHEMA,
          'unapproved_physical_type_width_changes': {k: {'source': v, 'regenerated': schema['properties'].get(k)}
              for k, v in SCHEMA.items() if schema['properties'].get(k) != v},
          'regenerated_members': members, 'producer_iso_sidecars_preserved_in_derivative': False,
          'outcome': 'regenerated derivative excluded; original ZIP and all7members remain authoritative raw capture; no publication approved'}
(args.workdir / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
