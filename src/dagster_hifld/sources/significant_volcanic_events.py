"""Complete official NOAA significant volcanic event draft; no release accepted.

All59 original attributes, distinct IDs, BCE/date components, physical types,
literal strings/nulls and native EPSG3857 Point WKB remain preserved. Completeness
comes from the producer's own count and OBJECTID witnesses captured before and
after the ID-targeted pages (shared validate_capture_witness), sized and paged at
capture time from the observed roster; a plausibility band quarantines a truncated
or replaced service instead of an exact snapshot pin. Live capture evidence lives
under the shared HIFLD_CAPTURE_ROOT via default_capture_dir.
"""
from __future__ import annotations
import fcntl
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import stat
import struct
import math
from collections import Counter
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.capture_completeness import validate_capture_witness
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle,load_first_construction,seal_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource,StagingStorageResource,default_capture_dir
from dagster_hifld.validation import sha256_file
from dagster_hifld.sources.arcgis_capture_sequence import capture_request_sequence

SLUG='historical-significant-volcanic-event-locations'
LANDING='https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/volcanoes'
SERVICE='https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer'
BASE=SERVICE+'/0'
LICENSE='https://dx.doi.org/10.7289/V5JW8BSH'
READY='metadata/significant_volcanic_candidate_ready.json'
SPACE_FLOOR=300_000_000
MAX_AUX_BYTES=3_000_000
MAX_BODY_BYTES=4_000_000
PAGE_SIZE=250
# The 2026-11-11 capture observed 903 features with OBJECTIDs 7805..8707. The
# maintained layer re-imports with continuing OBJECTIDs and appends new
# significant events (a few per year), so the observed count is a capture
# witness, not a pin. The band quarantines a truncated or replaced service
# without rejecting legitimate refresh: below 500 the complete historical
# record would have lost nearly half its events; above 2000 the layer would
# serve a different population than the Significant Volcanic Events database.
COUNT_FLOOR=500
COUNT_CEIL=2000
# Pin existing native EPSG3857 serialization; global PROJ lookup caches must not change candidate bytes.
NATIVE_CRS_JSON={'$schema': 'https://proj.org/schemas/v0.7/projjson.schema.json', 'area': 'World between 85.06°S and 85.06°N.', 'base_crs': {'coordinate_system': {'axis': [{'abbreviation': 'Lat', 'direction': 'north', 'name': 'Geodetic latitude', 'unit': 'degree'}, {'abbreviation': 'Lon', 'direction': 'east', 'name': 'Geodetic longitude', 'unit': 'degree'}], 'subtype': 'ellipsoidal'}, 'datum_ensemble': {'accuracy': '2.0', 'ellipsoid': {'inverse_flattening': 298.257223563, 'name': 'WGS 84', 'semi_major_axis': 6378137}, 'id': {'authority': 'EPSG', 'code': 6326}, 'members': [{'id': {'authority': 'EPSG', 'code': 1166}, 'name': 'World Geodetic System 1984 (Transit)'}, {'id': {'authority': 'EPSG', 'code': 1152}, 'name': 'World Geodetic System 1984 (G730)'}, {'id': {'authority': 'EPSG', 'code': 1153}, 'name': 'World Geodetic System 1984 (G873)'}, {'id': {'authority': 'EPSG', 'code': 1154}, 'name': 'World Geodetic System 1984 (G1150)'}, {'id': {'authority': 'EPSG', 'code': 1155}, 'name': 'World Geodetic System 1984 (G1674)'}, {'id': {'authority': 'EPSG', 'code': 1156}, 'name': 'World Geodetic System 1984 (G1762)'}, {'id': {'authority': 'EPSG', 'code': 1309}, 'name': 'World Geodetic System 1984 (G2139)'}, {'id': {'authority': 'EPSG', 'code': 1383}, 'name': 'World Geodetic System 1984 (G2296)'}], 'name': 'World Geodetic System 1984 ensemble'}, 'id': {'authority': 'EPSG', 'code': 4326}, 'name': 'WGS 84'}, 'bbox': {'east_longitude': 180, 'north_latitude': 85.06, 'south_latitude': -85.06, 'west_longitude': -180}, 'conversion': {'method': {'id': {'authority': 'EPSG', 'code': 1024}, 'name': 'Popular Visualisation Pseudo Mercator'}, 'name': 'Popular Visualisation Pseudo-Mercator', 'parameters': [{'id': {'authority': 'EPSG', 'code': 8801}, 'name': 'Latitude of natural origin', 'unit': 'degree', 'value': 0}, {'id': {'authority': 'EPSG', 'code': 8802}, 'name': 'Longitude of natural origin', 'unit': 'degree', 'value': 0}, {'id': {'authority': 'EPSG', 'code': 8806}, 'name': 'False easting', 'unit': 'metre', 'value': 0}, {'id': {'authority': 'EPSG', 'code': 8807}, 'name': 'False northing', 'unit': 'metre', 'value': 0}]}, 'coordinate_system': {'axis': [{'abbreviation': 'X', 'direction': 'east', 'name': 'Easting', 'unit': 'metre'}, {'abbreviation': 'Y', 'direction': 'north', 'name': 'Northing', 'unit': 'metre'}], 'subtype': 'Cartesian'}, 'id': {'authority': 'EPSG', 'code': 3857}, 'name': 'WGS 84 / Pseudo-Mercator', 'scope': 'Web mapping and visualisation.', 'type': 'ProjectedCRS'}
FIELD_DEFINITIONS=[{'name': 'YEAR', 'type': 'esriFieldTypeSmallInteger', 'alias': 'YEAR', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MO', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MO', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAY', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'VEI', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VEI', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'VOL_ID', 'type': 'esriFieldTypeDouble', 'alias': 'VOL_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'FATALITIES', 'type': 'esriFieldTypeInteger', 'alias': 'FATALITIES', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_EQ', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_EQ', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_TSU', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_TSU', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'COMMENTS', 'type': 'esriFieldTypeString', 'alias': 'COMMENTS', 'sqlType': 'sqlTypeOther', 'length': 4000, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'SIGNIF', 'type': 'esriFieldTypeSmallInteger', 'alias': 'SIGNIF', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'VALIDITY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VALIDITY', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'NUM', 'type': 'esriFieldTypeString', 'alias': 'NUM', 'sqlType': 'sqlTypeOther', 'length': 8, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'NAME', 'type': 'esriFieldTypeString', 'alias': 'NAME', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LOCATION', 'type': 'esriFieldTypeString', 'alias': 'LOCATION', 'sqlType': 'sqlTypeOther', 'length': 20, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LATITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LATITUDE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LONGITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LONGITUDE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ELEVATION', 'type': 'esriFieldTypeInteger', 'alias': 'ELEVATION', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MORPHOLOGY', 'type': 'esriFieldTypeString', 'alias': 'MORPHOLOGY', 'sqlType': 'sqlTypeOther', 'length': 19, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STATUS', 'type': 'esriFieldTypeString', 'alias': 'STATUS', 'sqlType': 'sqlTypeOther', 'length': 19, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TIME_ERUPT', 'type': 'esriFieldTypeString', 'alias': 'TIME_ERUPT', 'sqlType': 'sqlTypeOther', 'length': 12, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'COUNTRY', 'type': 'esriFieldTypeString', 'alias': 'COUNTRY', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'NUM_SLIDES', 'type': 'esriFieldTypeDouble', 'alias': 'NUM_SLIDES', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'SIG_ID', 'type': 'esriFieldTypeInteger', 'alias': 'SIG_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TSU_ID', 'type': 'esriFieldTypeInteger', 'alias': 'TSU_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HAZ_EVENT_ID', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HAZ_EVENT_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None}, {'name': 'PUBLISH', 'type': 'esriFieldTypeString', 'alias': 'PUBLISH', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'OID', 'type': 'esriFieldTypeInteger', 'alias': 'OID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ID', 'type': 'esriFieldTypeInteger', 'alias': 'ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}]
PAGE_FIELDS=[{'name': 'YEAR', 'type': 'esriFieldTypeSmallInteger', 'alias': 'YEAR', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MO', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MO', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAY', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'VEI', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VEI', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'VOL_ID', 'type': 'esriFieldTypeDouble', 'alias': 'VOL_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'FATALITIES', 'type': 'esriFieldTypeInteger', 'alias': 'FATALITIES', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_EQ', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_EQ', 'sqlType': 'sqlTypeOther', 'length': 2, 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_TSU', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_TSU', 'sqlType': 'sqlTypeOther', 'length': 2, 'domain': None, 'defaultValue': None}, {'name': 'COMMENTS', 'type': 'esriFieldTypeString', 'alias': 'COMMENTS', 'sqlType': 'sqlTypeOther', 'length': 4000, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'MISSING', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'SIGNIF', 'type': 'esriFieldTypeSmallInteger', 'alias': 'SIGNIF', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'VALIDITY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VALIDITY', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'NUM', 'type': 'esriFieldTypeString', 'alias': 'NUM', 'sqlType': 'sqlTypeOther', 'length': 8, 'domain': None, 'defaultValue': None}, {'name': 'NAME', 'type': 'esriFieldTypeString', 'alias': 'NAME', 'sqlType': 'sqlTypeOther', 'length': 50, 'domain': None, 'defaultValue': None}, {'name': 'LOCATION', 'type': 'esriFieldTypeString', 'alias': 'LOCATION', 'sqlType': 'sqlTypeOther', 'length': 20, 'domain': None, 'defaultValue': None}, {'name': 'LATITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LATITUDE', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'LONGITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LONGITUDE', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'ELEVATION', 'type': 'esriFieldTypeInteger', 'alias': 'ELEVATION', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MORPHOLOGY', 'type': 'esriFieldTypeString', 'alias': 'MORPHOLOGY', 'sqlType': 'sqlTypeOther', 'length': 19, 'domain': None, 'defaultValue': None}, {'name': 'STATUS', 'type': 'esriFieldTypeString', 'alias': 'STATUS', 'sqlType': 'sqlTypeOther', 'length': 19, 'domain': None, 'defaultValue': None}, {'name': 'TIME_ERUPT', 'type': 'esriFieldTypeString', 'alias': 'TIME_ERUPT', 'sqlType': 'sqlTypeOther', 'length': 12, 'domain': None, 'defaultValue': None}, {'name': 'COUNTRY', 'type': 'esriFieldTypeString', 'alias': 'COUNTRY', 'sqlType': 'sqlTypeOther', 'length': 50, 'domain': None, 'defaultValue': None}, {'name': 'NUM_SLIDES', 'type': 'esriFieldTypeDouble', 'alias': 'NUM_SLIDES', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'SIG_ID', 'type': 'esriFieldTypeInteger', 'alias': 'SIG_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'TSU_ID', 'type': 'esriFieldTypeInteger', 'alias': 'TSU_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HAZ_EVENT_ID', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HAZ_EVENT_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'PUBLISH', 'type': 'esriFieldTypeString', 'alias': 'PUBLISH', 'sqlType': 'sqlTypeOther', 'length': 1, 'domain': None, 'defaultValue': None}, {'name': 'OID', 'type': 'esriFieldTypeInteger', 'alias': 'OID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'ID', 'type': 'esriFieldTypeInteger', 'alias': 'ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}]
PRE_PAGE_URLS={'ncei': LANDING, 'hazards_viewer': 'https://www.ncei.noaa.gov/maps/hazards/?layers=0', 'hazards_js': 'https://www.ncei.noaa.gov/maps/hazards/assets/index-Cyvq2L9n.js', 'volcano_item': 'https://www.arcgis.com/sharing/rest/content/items/e991eccebfa5425e9180d884af76f16a?f=json', 'volcano_service': SERVICE+'?f=pjson', 'volcano_layer': BASE+'?f=pjson', 'count_before': BASE+'/query?where=1%3D1&returnCountOnly=true&f=json', 'ids_before': BASE+'/query?where=1%3D1&returnIdsOnly=true&f=json'}
POST_PAGE_URLS={'layer_after': BASE+'?f=pjson', 'count_after': BASE+'/query?where=1%3D1&returnCountOnly=true&f=json', 'ids_after': BASE+'/query?where=1%3D1&returnIdsOnly=true&f=json'}

# The live service answers an objectIds query with only the NAME attribute
# unless outFields is explicit; the exact wire form is also what the previously
# retained captures recorded, so those captures replay unchanged.
def page_url(ids):
    return (BASE+'/query?f=json&objectIds='+",".join(map(str,ids)).replace(",","%2C")
        +'&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&orderByFields=OBJECTID+ASC')

def derive_pages(ids):
    pages={}
    for start in range(0,len(ids),PAGE_SIZE):
        pages[f'page_{len(pages):04d}']=page_url(ids[start:start+PAGE_SIZE])
    return pages

def capture_urls(pages):
    return {**PRE_PAGE_URLS,**pages,**POST_PAGE_URLS}


def witness_urls(captures):
    count=strict_json(captures['count_before'][0]).get('count')
    if type(count)is not int or count<0:raise VolcanicQuarantineError('invalid_count_witness')
    return capture_urls(derive_pages(id_roster(captures['ids_before'][0],count)))
class VolcanicQuarantineError(ValueError):pass
class IncidentWriteError(Exception):pass


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()


def digest(body):
    return hashlib.sha256(body).hexdigest()


def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise VolcanicQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise VolcanicQuarantineError('metadata_must_be_object_without_error')
    return value


def local_only(storage):
    if isinstance(storage,PublishedStorageResource)or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise VolcanicQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..'in root.parts or any(p.is_symlink()for p in [root,*root.parents]):raise VolcanicQuarantineError('staging_path_symlink_refused')
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<SPACE_FLOOR:raise VolcanicQuarantineError('disk_floor_before_acquisition')
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise VolcanicQuarantineError('staging_alias_or_special_file_refused')


def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise VolcanicQuarantineError('immutable_capture_changed:'+key)


def quarantine(storage,captures,reason):
    key=f'_quarantine/{SLUG}/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,key+'/'+name+'.body',body)
        immutable(storage,key+'/'+name+'.receipt.json',encode(receipt))
    immutable(storage,key+'/reason.json',encode({'approved':False,'reason':reason}))
    return key


def processing_identity():
    root=Path(__file__).parents[3]
    return {'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}


def bounded_input(path,limit):
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(descriptor,'rb')as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise VolcanicQuarantineError('unsafe_or_oversized_original_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise VolcanicQuarantineError('changed_or_incomplete_original_input')
    return body


def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise VolcanicQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names


def construction_record_key(version):return f'_construction-state/{SLUG}/{version}.json'


def incident_directory(version):return f'_construction-state/{SLUG}/incident-{version}'


def state_present(storage,version):
    key=construction_record_key(version);root=Path(storage.local_dir).absolute()
    return any((root/value).exists()for value in(key,key+'.first-construction.json',key+'.evidence'))


def refuse_observed_loss(storage,version):
    if(Path(storage.local_dir).absolute()/incident_directory(version)).exists():raise VolcanicQuarantineError('permanent_observed_original_trust_loss_requires_explicit_reviewed_restore')


def record_observed_loss(storage,version,fingerprint,reason):
    # The reviewed shared protocol fsyncs each created directory and the final
    # records before returning. Any failure preserves the existing candidate;
    # even an incomplete incident directory blocks subsequent cold retries.
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason,
        'operator_action':'Restore exact original witnesses and explicitly retain/review this negative incident; no automatic bootstrap.'})
    seal_first_construction(storage,key,version,fingerprint,body)


def write_private(root,name,body):
    path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        info=os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise VolcanicQuarantineError('private_write_alias_or_special_file_refused')
        os.ftruncate(descriptor,0)
        if os.write(descriptor,body)!=len(body):raise VolcanicQuarantineError('incomplete_private_write:'+name)
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if candidate_bytes(root,name)!=body:raise VolcanicQuarantineError('incomplete_private_write:'+name)


def validate_receipt(name,body,receipt,url):
    if (receipt.get('url')!=url or receipt.get('final_url')!=url
        or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body)or receipt.get('complete_body')is not True):raise VolcanicQuarantineError('invalid_original_receipt:'+name)
    if not isinstance(receipt.get('observed_at'),str):raise VolcanicQuarantineError('missing_retrieval_time')
    if datetime.fromisoformat(receipt['observed_at']).tzinfo is None:raise VolcanicQuarantineError('retrieval_timezone_required')
    return {'input_id':name+':'+digest(body),'producer':'NOAA National Centers for Environmental Information','host':httpx.URL(url).host,
        'role':'official_distributor'if name.startswith(('volcano_','page_','count_','ids_','layer_'))else'producer_publication',
        'distributor':'NOAA NCEI ArcGIS organization C8EMgrsFcRFL6LrL, ncei_noaa-owned service item',
        'delegation_evidence':{'producer_page_url':LANDING,'viewer_url':PRE_PAGE_URLS['hazards_viewer'],'viewer_javascript_url':PRE_PAGE_URLS['hazards_js'],'portal_item_url':PRE_PAGE_URLS['volcano_item'],'portal_owner':'ncei_noaa','hosted_service_url':SERVICE},
        'canonical_url':url,'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],
        'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,
        'attribution':'NOAA NCEI/WDS Significant Volcanic Eruptions Database, DOI10.7289/V5JW8BSH; original source item attribution and rights retained.',
        'release_date_unknown_reason':'No verified dated data release. Event YEAR/MO/DAY, service editingInfo, portal modification and retrieval are separate values, never invented release dates.'}


def retain_capture_fragment(path,body):
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        if os.write(descriptor,body)!=len(body):raise OSError('short_pending_capture_write')
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if bounded_input(path,len(body))!=body:raise OSError('pending_capture_readback_mismatch')


def preserve_transfer_result(path,result):
    transfer={key:value for key,value in vars(result).items()if key not in('pending_wire','pending_decoded')}
    for label in('wire','decoded'):
        pending=getattr(result,'pending_'+label)
        if pending:retain_capture_fragment(path/('pending_'+label+'.body'),pending)
        transfer['pending_'+label+'_artifact']={'bytes':len(pending),'sha256':digest(pending),'path':'pending_'+label+'.body'if pending else None}
        observed=path/(label+'.body')
        transfer['physical_'+label]={'bytes':observed.stat().st_size,'sha256':sha256_file(observed)}
    readback=(transfer['physical_wire']['bytes']==result.wire_bytes_retained
        and transfer['physical_wire']['sha256']==result.wire_sha256
        and transfer['physical_decoded']['bytes']==result.decoded_bytes_retained
        and transfer['physical_decoded']['sha256']==result.decoded_sha256)
    transfer['independent_physical_readback_matches_acknowledged_sinks']=readback
    retain_capture_fragment(path/'transfer.json',encode(transfer))
    return readback


def replay_witness_pages(root):
    count_body=bounded_input(root/'count_before.body',MAX_BODY_BYTES)
    ids_body=bounded_input(root/'ids_before.body',MAX_BODY_BYTES)
    count=strict_json(count_body).get('count')
    if type(count)is not int or count<0:raise VolcanicQuarantineError('invalid_count_witness')
    return derive_pages(id_roster(ids_body,count))


def replay_roster_guard(root,names):
    expected={n+'.body'for n in names}|{n+'.receipt.json'for n in names}
    actual=set()
    for entry in root.iterdir():
        info=entry.lstat()
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise VolcanicQuarantineError('retained_capture_unsafe_or_special_file:'+entry.name)
        actual.add(entry.name)
    if actual!=expected:raise VolcanicQuarantineError('retained_capture_roster_drift')


def capture_one(storage,attempt,name,url,root):
    if root is not None:
        body=bounded_input(root/(name+'.body'),MAX_BODY_BYTES)
        raw=bounded_input(root/(name+'.receipt.json'),65536)
    else:
        path=attempt/name;path.mkdir(parents=True)
        with(path/'wire.body').open('xb')as wire,(path/'decoded.body').open('xb')as decoded:
            def retain(stream,block,target):
                before=os.fstat(stream.fileno());written=stream.write(block)
                stream.flush();os.fsync(stream.fileno())
                if written!=len(block):raise OSError('short_capture_write')
                reader=os.open(target,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                try:
                    actual=os.fstat(reader)
                    if(not stat.S_ISREG(actual.st_mode)or actual.st_nlink!=1
                        or(actual.st_dev,actual.st_ino)!=(before.st_dev,before.st_ino)
                        or actual.st_size!=before.st_size+len(block)
                        or os.pread(reader,len(block),before.st_size)!=block):raise OSError('capture_block_readback_mismatch')
                finally:os.close(reader)
            def reserve(progress):
                if shutil.disk_usage(path).free<SPACE_FLOOR+progress.reserve_bytes:raise OSError('disk_floor_before_read')
            receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat()};result=None
            try:
                with httpx.Client(follow_redirects=False,timeout=60,headers={'Accept-Encoding':'identity'})as client,client.stream('GET',url)as response:
                    result=capture_http_response(response,max_wire_bytes=MAX_BODY_BYTES,max_decoded_bytes=MAX_BODY_BYTES,
                        reserve=reserve,retain_wire=lambda b:retain(wire,b,path/'wire.body'),retain_decoded=lambda b:retain(decoded,b,path/'decoded.body'))
                    receipt.update(final_url=str(response.url),status=response.status_code,complete_body=result.complete,
                        bytes=result.decoded_bytes_retained,sha256=result.decoded_sha256,wire_bytes=result.wire_bytes_received,error_code=result.error_code)
            finally:
                if result is not None:readback=preserve_transfer_result(path,result)
                wire.flush();decoded.flush()
        receipt['complete_body']=result.complete and readback
        receipt['independent_sink_readback_matches']=readback
        body=bounded_input(path/'decoded.body',MAX_BODY_BYTES+1024*1024)
        raw=encode(receipt)
    write_private(attempt,name+'.raw-receipt.body',raw)
    immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body)
    receipt=strict_json(raw);validate_receipt(name,body,receipt,url);return(body,receipt)


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempt=default_capture_dir(SLUG,SLUG,uuid4().hex)
    if '..'in attempt.parts or any(p.is_symlink()for p in[attempt,*attempt.parents]):raise VolcanicQuarantineError('unsafe_capture_root')
    attempt.mkdir(parents=True,exist_ok=True)
    root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or any(p.is_symlink()for p in[root,*root.parents])or not root.is_dir()):raise VolcanicQuarantineError('unsafe_capture_root')
    pages=None
    try:
        if root is not None:
            pages=replay_witness_pages(root)
            replay_roster_guard(root,tuple(capture_urls(pages)))
        def derive_requests(retained):
            if pages is not None:
                planned_pages = pages
            else:
                count_body, _ = retained['count_before']; ids_body, _ = retained['ids_before']
                count = strict_json(count_body).get('count')
                if type(count) is not int or count < 0:
                    raise VolcanicQuarantineError('invalid_count_witness')
                planned_pages = derive_pages(id_roster(ids_body, count))
            return {**planned_pages, **POST_PAGE_URLS}.items()
        capture_request_sequence(captures, PRE_PAGE_URLS.items(), derive_requests,
            lambda name, url: capture_one(storage, attempt, name, url, root))
        return captures
    except Exception as exc:
        write_private(attempt,'failure.json',encode({'approved':False,'reason_type':type(exc).__name__}))
        where=quarantine(storage,captures,str(exc));raise VolcanicQuarantineError('source_acquisition_failed:'+str(exc)+';retained:'+where)from exc


def parquet_table(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':NATIVE_CRS_JSON}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def validate_output(root,table):
    path=root/'geoparquet'/f'{SLUG}.parquet';actual=pq.read_table(path)
    if not actual.equals(parquet_table(table),check_metadata=True):raise VolcanicQuarantineError('full_typed_native_output_drift')
    for w in actual['geometry'].to_pylist():
        p=shapely.from_wkb(w)
        if p.geom_type!='Point'or not p.is_valid or p.is_empty:raise VolcanicQuarantineError('invalid_native_output_geometry')


def manifest_for(captures,identity,output_pin):
    urls=witness_urls(captures)
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Historical Significant Volcanic Event Locations — complete current official NOAA NCEI draft','publisher':'NOAA National Centers for Environmental Information','agency':'National Oceanic and Atmospheric Administration','source_url':LANDING,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(n,*captures[n],urls[n])for n in captures],
        'transformations':[{'operation':'complete_official_ID_targeted_feature_extraction','parameters':{'source_ids':'OBJECTID, VOL_ID, SIG_ID, TSU_ID, HAZ_EVENT_ID, ID, OID and literal NUM remain distinct original columns; no historical ID join accepted.','types':'Original SmallInteger int16/Integer int32/Double float64/string/nullability/width metadata retained. OID Arrow int32 is a proposed bounded staging representation; source OID physical width is not explicitly declared. Original ID values/declarations/JSON lexical bytes remain immutable; no width or published schema mapping approved.','dates':'Original YEAR/MO/DAY components including BCE and nulls remain literal integers; TIME_ERUPT remains original coded string, never converted to invented timestamp.','geometry':'Native3857 Point WKB from original binary64 x/y, no projection, precision change or repair.','relationship':'Complete current official significant event population; count and OBJECTID roster are capture-time witnesses with a plausibility band, not pins. Historical893-row baseline and schema/ID/geometry differences remain unapproved.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all59 original attributes/IDs/types/nulls/metadata and exact native WKB; generic schema regeneration unapproved.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**output_pin}]}}},
        'unsupported_output_formats':{'geopackage':'Original source field widths/types and container mapping have no approved GeoPackage contract.','shapefile_zip':'Original long names/string/null/physical widths cannot be silently coerced.','pmtiles':'Reprojection/quantization and feature ID mapping remain unapproved.'}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def candidate_bytes(root,name):
    if Path(name).is_absolute()or'..'in Path(name).parts:raise VolcanicQuarantineError('unsafe_candidate_key')
    return bounded_input(root/name,MAX_BODY_BYTES if name.endswith('.body')else(8_000_000 if name.endswith('.parquet')else MAX_AUX_BYTES))


def source_payloads(captures,identity,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/significant_volcanic_source_audit.json':encode(audit)}
    for n,(b,r)in captures.items():values['raw/'+n+'.body']=b;values['raw/'+n+'.receipt.json']=encode(r)
    return values


def original_context(storage,version,fingerprint,captures,identity,manifest,*,create=False):
    key=construction_record_key(version)
    if create:
        inspect_captures(captures);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,{'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(b)}/{n}.body','receipt':r}for n,(b,r)in captures.items()}}))
    else:body=load_first_construction(storage,key,version,fingerprint)
    payload=strict_json(body)['payload'];original={}
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(captures):raise VolcanicQuarantineError('invalid_original_context')
    for n,item in payload['captures'].items():
        key=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{n}.body"
        if item['body_key']!=key:raise VolcanicQuarantineError('invalid_original_raw_key')
        original[n]=(bounded_input(Path(storage.local_dir)/key,MAX_BODY_BYTES),item['receipt'])
    a,_=inspect_captures(original);b,_=inspect_captures(captures)
    if not a.equals(b,check_metadata=True):raise VolcanicQuarantineError('original_source_population_drift')
    old=payload['manifest'];obj=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:obj[k]for k in('bytes','sha256')})
    if encode({k:v for k,v in old.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise VolcanicQuarantineError('original_manifest_context_drift')
    validate_release_provenance(old);validate_output_policy(old);validate_construction_code_identity(Path(__file__).parents[3],old['code_commit'],identity['files']);return original,old


def validate_candidate(root,table,identity,captures,audit,manifest,version,*,cached=False,storage=None):
    roster=candidate_roster(root);expected=source_payloads(captures,identity,audit,manifest);names=set(expected)|{f'geoparquet/{SLUG}.parquet'}
    ready={'approved':False,'version':version,'processing_fingerprint':digest(encode(identity)),'objects':{n:sha256_file(root/n)for n in names}}
    if not(names|{READY}).issubset(roster):raise VolcanicQuarantineError('missing_candidate_object')
    if not cached and roster!=names|{READY}:raise VolcanicQuarantineError('private_candidate_roster_drift')
    if cached:
        allowed={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json','metadata/source/source_manifest.json','metadata/source/data_dictionary.json','metadata/source/quality_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'}
        if roster-(names|{READY}|allowed):raise VolcanicQuarantineError('unknown_cached_object')
        for name in roster-(names|{READY}):strict_json(candidate_bytes(root,name))
        for name in('metadata/source/source_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'):
            if name in roster and candidate_bytes(root,name)!=encode(manifest):raise VolcanicQuarantineError('cached_source_snapshot_drift')
        if 'metadata/resolved_provenance.json'in roster:
            if storage is None:raise VolcanicQuarantineError('resolved_provenance_requires_explicit_storage_context')
            projected=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata)
            if encode(strict_json(candidate_bytes(root,'metadata/resolved_provenance.json')))!=encode(projected):raise VolcanicQuarantineError('cached_resolved_provenance_drift')
    for n,b in expected.items():
        if candidate_bytes(root,n)!=b:raise VolcanicQuarantineError('candidate_source_bytes_drift:'+n)
    if candidate_bytes(root,READY)!=encode(ready):raise VolcanicQuarantineError('candidate_ready_drift')
    validate_output(root,table)


def stage_significant_volcanic(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir);table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));version='v0.0.0-draft-noaa-volcanic-'+fingerprint[:16]
    base=Path(storage.local_dir).absolute();prefix=f'{SLUG}/{SLUG}/{version}';root=base/prefix;locks=base/'_locks';locks.mkdir(exist_ok=True)
    descriptor=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b')as lock:
        if os.fstat(lock.fileno()).st_nlink!=1:raise VolcanicQuarantineError('linked_lock')
        fcntl.flock(lock,fcntl.LOCK_EX);refuse_observed_loss(storage,version)
        # A private deterministic output provides the pin before any trust seal.
        build=base/'_building'/SLUG;build.mkdir(parents=True,exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='candidate-',dir=build)as temporary:
            temp=Path(temporary);(temp/'geoparquet').mkdir();path=temp/'geoparquet'/f'{SLUG}.parquet';pq.write_table(parquet_table(table),path,compression='zstd');validate_output(temp,table)
            manifest=manifest_for(captures,identity,{'bytes':path.stat().st_size,'sha256':sha256_file(path)})
            if root.exists()or state_present(storage,version):
                try:
                    captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest)
                    table,audit=inspect_captures(captures)
                except Exception as exc:
                    try:record_observed_loss(storage,version,fingerprint,type(exc).__name__)
                    except Exception as write:raise IncidentWriteError('incident_failure_candidate_preserved')from write
                    if root.exists():
                        q=base/'_quarantine'/SLUG/('observed-loss-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q)
                    raise VolcanicQuarantineError('observed_original_trust_loss')from exc
                if root.exists():
                    try:validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    except Exception as exc:
                        q=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q);raise VolcanicQuarantineError('invalid_cache_moved_intact:'+str(q))from exc
                    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True,
                        'source_reported_count':audit['source_reported_count'],'source_id_sha256':audit['source_id_sha256'],'source_population_complete':True}
            for n,b in source_payloads(captures,identity,audit,manifest).items():write_private(temp,n,b)
            ready={'approved':False,'version':version,'processing_fingerprint':fingerprint,'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));validate_candidate(temp,table,identity,captures,audit,manifest,version)
            captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest,create=True)
            validate_candidate(temp,table,identity,captures,audit,manifest,version);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False,
        'source_reported_count':audit['source_reported_count'],'source_id_sha256':audit['source_id_sha256'],'source_population_complete':True}


def id_roster(body,expected_count):
    data=strict_json(body);ids=data.get('objectIds')
    if data.get('objectIdFieldName')!='OBJECTID'or type(ids)is not list or len(ids)!=expected_count or any(type(x)is not int for x in ids)or len(set(ids))!=expected_count:raise VolcanicQuarantineError('invalid_complete_OID_witness')
    return sorted(ids)


def decode_pages(pages,expected_ids):
    fields=[x['name']for x in FIELD_DEFINITIONS];names=fields+['geometry'];rows=[];seen=set()
    type_map={'esriFieldTypeSmallInteger':pa.int16(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeOID':pa.int32(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
    for page in pages:
        if(page.get('objectIdFieldName')!='OBJECTID'or page.get('geometryType')!='esriGeometryPoint'or encode(page.get('spatialReference'))!=encode({'wkid':102100,'latestWkid':3857})
            or encode(page.get('fields'))!=encode(PAGE_FIELDS)or page.get('exceededTransferLimit')not in(None,False)
            or('exceededTransferLimit'in page and type(page['exceededTransferLimit'])is not bool)):
            raise VolcanicQuarantineError('page_schema_CRS_or_transfer_limit_drift')
        if type(page.get('features'))is not list:raise VolcanicQuarantineError('missing_feature_population')
        for feature in page['features']:
            attrs=feature.get('attributes');geom=feature.get('geometry');values=[]
            if type(attrs)is not dict or list(attrs)!=fields:raise VolcanicQuarantineError('missing_extra_or_reordered_source_fields')
            oid=attrs['OBJECTID']
            if type(oid)is not int or oid in seen:raise VolcanicQuarantineError('invalid_or_duplicate_original_OBJECTID')
            seen.add(oid)
            for decl in FIELD_DEFINITIONS:
                name=decl['name'];kind=decl['type'];v=attrs[name]
                if v is None:
                    if decl['nullable']is not True:raise VolcanicQuarantineError('nonnull_source_field:'+name)
                elif kind=='esriFieldTypeString':
                    if type(v)is not str or len(v)>decl['length']:raise VolcanicQuarantineError('source_string_width_type:'+name)
                elif kind in('esriFieldTypeSmallInteger','esriFieldTypeInteger','esriFieldTypeOID'):
                    bound=2**(15 if kind=='esriFieldTypeSmallInteger'else 31)
                    if type(v)is not int or not-bound<=v<bound:raise VolcanicQuarantineError('source_integer_width:'+name)
                elif kind=='esriFieldTypeDouble':
                    if type(v)not in(int,float)or not math.isfinite(v)or float(v)!=v:raise VolcanicQuarantineError('nonfinite_or_lossy_source_double:'+name)
                    v=float(v)
                else:raise VolcanicQuarantineError('unknown_source_physical_type')
                values.append(v)
            if type(geom)is not dict or set(geom)!={'x','y'}or any(type(geom[k])not in(int,float)or not math.isfinite(geom[k])or float(geom[k])!=geom[k]for k in('x','y')):raise VolcanicQuarantineError('invalid_or_extended_native_point')
            values.append(struct.pack('<BIdd',1,1,float(geom['x']),float(geom['y'])));rows.append(values)
    if sorted(seen)!=sorted(expected_ids):raise VolcanicQuarantineError('incomplete_or_extra_source_ID_coverage')
    rows.sort(key=lambda v:v[fields.index('OBJECTID')]);schema=[pa.field(d['name'],type_map[d['type']],nullable=d['nullable'],metadata={b'original_esri_field':encode(d)})for d in FIELD_DEFINITIONS]+[pa.field('geometry',pa.binary(),nullable=True,metadata={b'original_geometry_type':b'esriGeometryPoint',b'original_crs':b'EPSG:3857'})]
    return pa.Table.from_arrays([pa.array([row[i]for row in rows],type=d.type)for i,d in enumerate(schema)],schema=pa.schema(schema))


def inspect_captures(captures):
    if 'count_before' not in captures or'ids_before'not in captures:raise VolcanicQuarantineError('capture_roster_drift')
    count=strict_json(captures['count_before'][0]).get('count')
    if type(count)is not int or count<0:raise VolcanicQuarantineError('invalid_count_witness')
    ids=id_roster(captures['ids_before'][0],count)
    page_urls=derive_pages(ids)
    urls=capture_urls(page_urls)
    if set(captures)!=set(urls):raise VolcanicQuarantineError('capture_roster_drift')
    for n,(b,r)in captures.items():validate_receipt(n,b,r,urls[n])
    if b'https://www.ncei.noaa.gov/maps/hazards/?layers=0'not in captures['ncei'][0]or b'./assets/index-Cyvq2L9n.js'not in captures['hazards_viewer'][0]or b'e991eccebfa5425e9180d884af76f16a'not in captures['hazards_js'][0]:raise VolcanicQuarantineError('missing_official_viewer_delegation')
    item=strict_json(captures['volcano_item'][0]);service=strict_json(captures['volcano_service'][0])
    if item.get('owner')!='ncei_noaa'or item.get('url')!=SERVICE or not any(x.get('id')==0 and type(x.get('id'))is int for x in service.get('layers',[])):raise VolcanicQuarantineError('official_item_service_delegation_drift')
    before=strict_json(captures['volcano_layer'][0]);after=strict_json(captures['layer_after'][0])
    for layer in[before,after]:
        if(encode(layer.get('fields'))!=encode(FIELD_DEFINITIONS)or layer.get('geometryType')!='esriGeometryPoint'or layer.get('objectIdField')!='OBJECTID'
            or type(layer.get('id'))is not int or layer['id']!=0 or layer.get('hasZ')is not False or layer.get('hasM')is not False
            or encode(layer.get('extent',{}).get('spatialReference'))!=encode({'wkid':102100,'latestWkid':3857})
            or type(layer.get('maxRecordCount'))is not int or layer['maxRecordCount']<PAGE_SIZE):raise VolcanicQuarantineError('physical_source_schema_geometry_or_transfer_drift')
    if encode(before.get('editingInfo'))!=encode(after.get('editingInfo')):raise VolcanicQuarantineError('source_changed_during_extraction')
    if not COUNT_FLOOR<=count<=COUNT_CEIL:raise VolcanicQuarantineError('observed_count_outside_plausibility_band')
    count_after=strict_json(captures['count_after'][0]).get('count')
    if type(count_after)is not int or count_after<0:raise VolcanicQuarantineError('invalid_count_witness')
    ids_after=id_roster(captures['ids_after'][0],count_after)
    validate_capture_witness(count,ids,count_after,ids_after)
    pages=[]
    for index,(name,_url)in enumerate(page_urls.items()):
        expected=ids[index*PAGE_SIZE:(index+1)*PAGE_SIZE];page=strict_json(captures[name][0])
        oids=[]
        for x in page.get('features',[]):
            attrs=x.get('attributes')if type(x)is dict else None
            oid=attrs.get('OBJECTID')if type(attrs)is dict else None
            if type(oid)is not int:raise VolcanicQuarantineError('targeted_page_feature_without_integer_OBJECTID')
            oids.append(oid)
        if sorted(oids)!=expected:raise VolcanicQuarantineError('targeted_page_missing_extra_ID')
        pages.append(page)
    table=decode_pages(pages,ids);years=[x for x in table['YEAR'].to_pylist()if x is not None];countries=Counter(table['COUNTRY'].to_pylist());audit={'count':table.num_rows,'source_reported_count':count,'source_id_sha256':digest(encode(ids)),'original_OBJECTIDs':ids,'original_SIG_IDs':table['SIG_ID'].to_pylist(),'original_event_IDs':table['ID'].to_pylist(),'original_VOL_IDs':table['VOL_ID'].to_pylist(),'countries':sorted(countries.items(),key=lambda x:repr(x[0])),'crs':'EPSG3857','field_count':59,'BCE_count':sum(y<0 for y in years),'unknown_years':table['YEAR'].null_count,'minimum_YEAR':min(years),'maximum_YEAR':max(years),'date_semantics':'YEAR/MO/DAY components and literal TIME_ERUPT codes retained; no BCE or unknown dates inferred.','editingInfo':before.get('editingInfo'),'release_date_established':False}
    return table,audit
