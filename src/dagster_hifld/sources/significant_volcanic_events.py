"""Complete official NOAA significant volcanic event draft; no release accepted.

All59 original attributes, distinct IDs, BCE/date components, physical types,
literal strings/nulls and native EPSG3857 Point WKB remain preserved.
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
from urllib.parse import urlparse, parse_qs
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
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.http_capture import capture_http_response
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.validation import sha256_file

SLUG='historical-significant-volcanic-event-locations'
LANDING='https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/volcanoes'
SERVICE='https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer'
BASE=SERVICE+'/0'
LICENSE='https://dx.doi.org/10.7289/V5JW8BSH'
READY='metadata/significant_volcanic_candidate_ready.json'
SPACE_FLOOR=300_000_000
MAX_AUX_BYTES=3_000_000
MAX_BODY_BYTES=4_000_000
INITIAL_COUNT=903
INITIAL_SOURCE_ID_SHA256='3700ad4ca7bfc2cbe94ac87478074aac49f4e0d27d14cc56dcdca7b75f2fb016'
FIELD_DEFINITIONS=[{'name': 'YEAR', 'type': 'esriFieldTypeSmallInteger', 'alias': 'YEAR', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MO', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MO', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAY', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'VEI', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VEI', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'VOL_ID', 'type': 'esriFieldTypeDouble', 'alias': 'VOL_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'FATALITIES', 'type': 'esriFieldTypeInteger', 'alias': 'FATALITIES', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_EQ', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_EQ', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_TSU', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_TSU', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'COMMENTS', 'type': 'esriFieldTypeString', 'alias': 'COMMENTS', 'sqlType': 'sqlTypeOther', 'length': 4000, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'SIGNIF', 'type': 'esriFieldTypeSmallInteger', 'alias': 'SIGNIF', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'VALIDITY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VALIDITY', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'NUM', 'type': 'esriFieldTypeString', 'alias': 'NUM', 'sqlType': 'sqlTypeOther', 'length': 8, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'NAME', 'type': 'esriFieldTypeString', 'alias': 'NAME', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LOCATION', 'type': 'esriFieldTypeString', 'alias': 'LOCATION', 'sqlType': 'sqlTypeOther', 'length': 20, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LATITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LATITUDE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LONGITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LONGITUDE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ELEVATION', 'type': 'esriFieldTypeInteger', 'alias': 'ELEVATION', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MORPHOLOGY', 'type': 'esriFieldTypeString', 'alias': 'MORPHOLOGY', 'sqlType': 'sqlTypeOther', 'length': 19, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'STATUS', 'type': 'esriFieldTypeString', 'alias': 'STATUS', 'sqlType': 'sqlTypeOther', 'length': 19, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TIME_ERUPT', 'type': 'esriFieldTypeString', 'alias': 'TIME_ERUPT', 'sqlType': 'sqlTypeOther', 'length': 12, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'COUNTRY', 'type': 'esriFieldTypeString', 'alias': 'COUNTRY', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'NUM_SLIDES', 'type': 'esriFieldTypeDouble', 'alias': 'NUM_SLIDES', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'SIG_ID', 'type': 'esriFieldTypeInteger', 'alias': 'SIG_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TSU_ID', 'type': 'esriFieldTypeInteger', 'alias': 'TSU_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HAZ_EVENT_ID', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HAZ_EVENT_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None}, {'name': 'PUBLISH', 'type': 'esriFieldTypeString', 'alias': 'PUBLISH', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'OID', 'type': 'esriFieldTypeInteger', 'alias': 'OID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ID', 'type': 'esriFieldTypeInteger', 'alias': 'ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}]
PAGE_FIELDS=[{'name': 'YEAR', 'type': 'esriFieldTypeSmallInteger', 'alias': 'YEAR', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MO', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MO', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAY', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'VEI', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VEI', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'VOL_ID', 'type': 'esriFieldTypeDouble', 'alias': 'VOL_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'FATALITIES', 'type': 'esriFieldTypeInteger', 'alias': 'FATALITIES', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_EQ', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_EQ', 'sqlType': 'sqlTypeOther', 'length': 2, 'domain': None, 'defaultValue': None}, {'name': 'ASSOC_TSU', 'type': 'esriFieldTypeString', 'alias': 'ASSOC_TSU', 'sqlType': 'sqlTypeOther', 'length': 2, 'domain': None, 'defaultValue': None}, {'name': 'COMMENTS', 'type': 'esriFieldTypeString', 'alias': 'COMMENTS', 'sqlType': 'sqlTypeOther', 'length': 4000, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'MISSING', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'MISSING_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MISSING_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MISSING_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'MISSING_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER_TOTAL', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_TOTAL_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'domain': None, 'defaultValue': None}, {'name': 'SIGNIF', 'type': 'esriFieldTypeSmallInteger', 'alias': 'SIGNIF', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'VALIDITY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'VALIDITY', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'NUM', 'type': 'esriFieldTypeString', 'alias': 'NUM', 'sqlType': 'sqlTypeOther', 'length': 8, 'domain': None, 'defaultValue': None}, {'name': 'NAME', 'type': 'esriFieldTypeString', 'alias': 'NAME', 'sqlType': 'sqlTypeOther', 'length': 50, 'domain': None, 'defaultValue': None}, {'name': 'LOCATION', 'type': 'esriFieldTypeString', 'alias': 'LOCATION', 'sqlType': 'sqlTypeOther', 'length': 20, 'domain': None, 'defaultValue': None}, {'name': 'LATITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LATITUDE', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'LONGITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LONGITUDE', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'ELEVATION', 'type': 'esriFieldTypeInteger', 'alias': 'ELEVATION', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'MORPHOLOGY', 'type': 'esriFieldTypeString', 'alias': 'MORPHOLOGY', 'sqlType': 'sqlTypeOther', 'length': 19, 'domain': None, 'defaultValue': None}, {'name': 'STATUS', 'type': 'esriFieldTypeString', 'alias': 'STATUS', 'sqlType': 'sqlTypeOther', 'length': 19, 'domain': None, 'defaultValue': None}, {'name': 'TIME_ERUPT', 'type': 'esriFieldTypeString', 'alias': 'TIME_ERUPT', 'sqlType': 'sqlTypeOther', 'length': 12, 'domain': None, 'defaultValue': None}, {'name': 'COUNTRY', 'type': 'esriFieldTypeString', 'alias': 'COUNTRY', 'sqlType': 'sqlTypeOther', 'length': 50, 'domain': None, 'defaultValue': None}, {'name': 'NUM_SLIDES', 'type': 'esriFieldTypeDouble', 'alias': 'NUM_SLIDES', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'SIG_ID', 'type': 'esriFieldTypeInteger', 'alias': 'SIG_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'TSU_ID', 'type': 'esriFieldTypeInteger', 'alias': 'TSU_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'HAZ_EVENT_ID', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HAZ_EVENT_ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'OBJECTID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'PUBLISH', 'type': 'esriFieldTypeString', 'alias': 'PUBLISH', 'sqlType': 'sqlTypeOther', 'length': 1, 'domain': None, 'defaultValue': None}, {'name': 'OID', 'type': 'esriFieldTypeInteger', 'alias': 'OID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}, {'name': 'ID', 'type': 'esriFieldTypeInteger', 'alias': 'ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None}]
URLS={'ncei': 'https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/volcanoes', 'hazards_viewer': 'https://www.ncei.noaa.gov/maps/hazards/?layers=0', 'hazards_js': 'https://www.ncei.noaa.gov/maps/hazards/assets/index-Cyvq2L9n.js', 'volcano_item': 'https://www.arcgis.com/sharing/rest/content/items/e991eccebfa5425e9180d884af76f16a?f=json', 'volcano_service': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer?f=pjson', 'volcano_layer': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0?f=pjson', 'count_before': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?where=1%3D1&returnCountOnly=true&f=json', 'ids_before': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?where=1%3D1&returnIdsOnly=true&f=json', 'page_0000': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?f=json&objectIds=7805%2C7806%2C7807%2C7808%2C7809%2C7810%2C7811%2C7812%2C7813%2C7814%2C7815%2C7816%2C7817%2C7818%2C7819%2C7820%2C7821%2C7822%2C7823%2C7824%2C7825%2C7826%2C7827%2C7828%2C7829%2C7830%2C7831%2C7832%2C7833%2C7834%2C7835%2C7836%2C7837%2C7838%2C7839%2C7840%2C7841%2C7842%2C7843%2C7844%2C7845%2C7846%2C7847%2C7848%2C7849%2C7850%2C7851%2C7852%2C7853%2C7854%2C7855%2C7856%2C7857%2C7858%2C7859%2C7860%2C7861%2C7862%2C7863%2C7864%2C7865%2C7866%2C7867%2C7868%2C7869%2C7870%2C7871%2C7872%2C7873%2C7874%2C7875%2C7876%2C7877%2C7878%2C7879%2C7880%2C7881%2C7882%2C7883%2C7884%2C7885%2C7886%2C7887%2C7888%2C7889%2C7890%2C7891%2C7892%2C7893%2C7894%2C7895%2C7896%2C7897%2C7898%2C7899%2C7900%2C7901%2C7902%2C7903%2C7904%2C7905%2C7906%2C7907%2C7908%2C7909%2C7910%2C7911%2C7912%2C7913%2C7914%2C7915%2C7916%2C7917%2C7918%2C7919%2C7920%2C7921%2C7922%2C7923%2C7924%2C7925%2C7926%2C7927%2C7928%2C7929%2C7930%2C7931%2C7932%2C7933%2C7934%2C7935%2C7936%2C7937%2C7938%2C7939%2C7940%2C7941%2C7942%2C7943%2C7944%2C7945%2C7946%2C7947%2C7948%2C7949%2C7950%2C7951%2C7952%2C7953%2C7954%2C7955%2C7956%2C7957%2C7958%2C7959%2C7960%2C7961%2C7962%2C7963%2C7964%2C7965%2C7966%2C7967%2C7968%2C7969%2C7970%2C7971%2C7972%2C7973%2C7974%2C7975%2C7976%2C7977%2C7978%2C7979%2C7980%2C7981%2C7982%2C7983%2C7984%2C7985%2C7986%2C7987%2C7988%2C7989%2C7990%2C7991%2C7992%2C7993%2C7994%2C7995%2C7996%2C7997%2C7998%2C7999%2C8000%2C8001%2C8002%2C8003%2C8004%2C8005%2C8006%2C8007%2C8008%2C8009%2C8010%2C8011%2C8012%2C8013%2C8014%2C8015%2C8016%2C8017%2C8018%2C8019%2C8020%2C8021%2C8022%2C8023%2C8024%2C8025%2C8026%2C8027%2C8028%2C8029%2C8030%2C8031%2C8032%2C8033%2C8034%2C8035%2C8036%2C8037%2C8038%2C8039%2C8040%2C8041%2C8042%2C8043%2C8044%2C8045%2C8046%2C8047%2C8048%2C8049%2C8050%2C8051%2C8052%2C8053%2C8054&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&orderByFields=OBJECTID+ASC', 'page_0250': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?f=json&objectIds=8055%2C8056%2C8057%2C8058%2C8059%2C8060%2C8061%2C8062%2C8063%2C8064%2C8065%2C8066%2C8067%2C8068%2C8069%2C8070%2C8071%2C8072%2C8073%2C8074%2C8075%2C8076%2C8077%2C8078%2C8079%2C8080%2C8081%2C8082%2C8083%2C8084%2C8085%2C8086%2C8087%2C8088%2C8089%2C8090%2C8091%2C8092%2C8093%2C8094%2C8095%2C8096%2C8097%2C8098%2C8099%2C8100%2C8101%2C8102%2C8103%2C8104%2C8105%2C8106%2C8107%2C8108%2C8109%2C8110%2C8111%2C8112%2C8113%2C8114%2C8115%2C8116%2C8117%2C8118%2C8119%2C8120%2C8121%2C8122%2C8123%2C8124%2C8125%2C8126%2C8127%2C8128%2C8129%2C8130%2C8131%2C8132%2C8133%2C8134%2C8135%2C8136%2C8137%2C8138%2C8139%2C8140%2C8141%2C8142%2C8143%2C8144%2C8145%2C8146%2C8147%2C8148%2C8149%2C8150%2C8151%2C8152%2C8153%2C8154%2C8155%2C8156%2C8157%2C8158%2C8159%2C8160%2C8161%2C8162%2C8163%2C8164%2C8165%2C8166%2C8167%2C8168%2C8169%2C8170%2C8171%2C8172%2C8173%2C8174%2C8175%2C8176%2C8177%2C8178%2C8179%2C8180%2C8181%2C8182%2C8183%2C8184%2C8185%2C8186%2C8187%2C8188%2C8189%2C8190%2C8191%2C8192%2C8193%2C8194%2C8195%2C8196%2C8197%2C8198%2C8199%2C8200%2C8201%2C8202%2C8203%2C8204%2C8205%2C8206%2C8207%2C8208%2C8209%2C8210%2C8211%2C8212%2C8213%2C8214%2C8215%2C8216%2C8217%2C8218%2C8219%2C8220%2C8221%2C8222%2C8223%2C8224%2C8225%2C8226%2C8227%2C8228%2C8229%2C8230%2C8231%2C8232%2C8233%2C8234%2C8235%2C8236%2C8237%2C8238%2C8239%2C8240%2C8241%2C8242%2C8243%2C8244%2C8245%2C8246%2C8247%2C8248%2C8249%2C8250%2C8251%2C8252%2C8253%2C8254%2C8255%2C8256%2C8257%2C8258%2C8259%2C8260%2C8261%2C8262%2C8263%2C8264%2C8265%2C8266%2C8267%2C8268%2C8269%2C8270%2C8271%2C8272%2C8273%2C8274%2C8275%2C8276%2C8277%2C8278%2C8279%2C8280%2C8281%2C8282%2C8283%2C8284%2C8285%2C8286%2C8287%2C8288%2C8289%2C8290%2C8291%2C8292%2C8293%2C8294%2C8295%2C8296%2C8297%2C8298%2C8299%2C8300%2C8301%2C8302%2C8303%2C8304&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&orderByFields=OBJECTID+ASC', 'page_0500': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?f=json&objectIds=8305%2C8306%2C8307%2C8308%2C8309%2C8310%2C8311%2C8312%2C8313%2C8314%2C8315%2C8316%2C8317%2C8318%2C8319%2C8320%2C8321%2C8322%2C8323%2C8324%2C8325%2C8326%2C8327%2C8328%2C8329%2C8330%2C8331%2C8332%2C8333%2C8334%2C8335%2C8336%2C8337%2C8338%2C8339%2C8340%2C8341%2C8342%2C8343%2C8344%2C8345%2C8346%2C8347%2C8348%2C8349%2C8350%2C8351%2C8352%2C8353%2C8354%2C8355%2C8356%2C8357%2C8358%2C8359%2C8360%2C8361%2C8362%2C8363%2C8364%2C8365%2C8366%2C8367%2C8368%2C8369%2C8370%2C8371%2C8372%2C8373%2C8374%2C8375%2C8376%2C8377%2C8378%2C8379%2C8380%2C8381%2C8382%2C8383%2C8384%2C8385%2C8386%2C8387%2C8388%2C8389%2C8390%2C8391%2C8392%2C8393%2C8394%2C8395%2C8396%2C8397%2C8398%2C8399%2C8400%2C8401%2C8402%2C8403%2C8404%2C8405%2C8406%2C8407%2C8408%2C8409%2C8410%2C8411%2C8412%2C8413%2C8414%2C8415%2C8416%2C8417%2C8418%2C8419%2C8420%2C8421%2C8422%2C8423%2C8424%2C8425%2C8426%2C8427%2C8428%2C8429%2C8430%2C8431%2C8432%2C8433%2C8434%2C8435%2C8436%2C8437%2C8438%2C8439%2C8440%2C8441%2C8442%2C8443%2C8444%2C8445%2C8446%2C8447%2C8448%2C8449%2C8450%2C8451%2C8452%2C8453%2C8454%2C8455%2C8456%2C8457%2C8458%2C8459%2C8460%2C8461%2C8462%2C8463%2C8464%2C8465%2C8466%2C8467%2C8468%2C8469%2C8470%2C8471%2C8472%2C8473%2C8474%2C8475%2C8476%2C8477%2C8478%2C8479%2C8480%2C8481%2C8482%2C8483%2C8484%2C8485%2C8486%2C8487%2C8488%2C8489%2C8490%2C8491%2C8492%2C8493%2C8494%2C8495%2C8496%2C8497%2C8498%2C8499%2C8500%2C8501%2C8502%2C8503%2C8504%2C8505%2C8506%2C8507%2C8508%2C8509%2C8510%2C8511%2C8512%2C8513%2C8514%2C8515%2C8516%2C8517%2C8518%2C8519%2C8520%2C8521%2C8522%2C8523%2C8524%2C8525%2C8526%2C8527%2C8528%2C8529%2C8530%2C8531%2C8532%2C8533%2C8534%2C8535%2C8536%2C8537%2C8538%2C8539%2C8540%2C8541%2C8542%2C8543%2C8544%2C8545%2C8546%2C8547%2C8548%2C8549%2C8550%2C8551%2C8552%2C8553%2C8554&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&orderByFields=OBJECTID+ASC', 'page_0750': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?f=json&objectIds=8555%2C8556%2C8557%2C8558%2C8559%2C8560%2C8561%2C8562%2C8563%2C8564%2C8565%2C8566%2C8567%2C8568%2C8569%2C8570%2C8571%2C8572%2C8573%2C8574%2C8575%2C8576%2C8577%2C8578%2C8579%2C8580%2C8581%2C8582%2C8583%2C8584%2C8585%2C8586%2C8587%2C8588%2C8589%2C8590%2C8591%2C8592%2C8593%2C8594%2C8595%2C8596%2C8597%2C8598%2C8599%2C8600%2C8601%2C8602%2C8603%2C8604%2C8605%2C8606%2C8607%2C8608%2C8609%2C8610%2C8611%2C8612%2C8613%2C8614%2C8615%2C8616%2C8617%2C8618%2C8619%2C8620%2C8621%2C8622%2C8623%2C8624%2C8625%2C8626%2C8627%2C8628%2C8629%2C8630%2C8631%2C8632%2C8633%2C8634%2C8635%2C8636%2C8637%2C8638%2C8639%2C8640%2C8641%2C8642%2C8643%2C8644%2C8645%2C8646%2C8647%2C8648%2C8649%2C8650%2C8651%2C8652%2C8653%2C8654%2C8655%2C8656%2C8657%2C8658%2C8659%2C8660%2C8661%2C8662%2C8663%2C8664%2C8665%2C8666%2C8667%2C8668%2C8669%2C8670%2C8671%2C8672%2C8673%2C8674%2C8675%2C8676%2C8677%2C8678%2C8679%2C8680%2C8681%2C8682%2C8683%2C8684%2C8685%2C8686%2C8687%2C8688%2C8689%2C8690%2C8691%2C8692%2C8693%2C8694%2C8695%2C8696%2C8697%2C8698%2C8699%2C8700%2C8701%2C8702%2C8703%2C8704%2C8705%2C8706%2C8707&outFields=%2A&returnGeometry=true&returnZ=true&returnM=true&orderByFields=OBJECTID+ASC', 'layer_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0?f=pjson', 'count_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?where=1%3D1&returnCountOnly=true&f=json', 'ids_after': 'https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/Significant_Volcanic_Events/FeatureServer/0/query?where=1%3D1&returnIdsOnly=true&f=json'}
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


def validate_receipt(name,body,receipt):
    if (receipt.get('url')!=URLS[name]or receipt.get('final_url')!=URLS[name]
        or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)
        or receipt.get('sha256')!=digest(body)or receipt.get('complete_body')is not True):raise VolcanicQuarantineError('invalid_original_receipt:'+name)
    if not isinstance(receipt.get('observed_at'),str):raise VolcanicQuarantineError('missing_retrieval_time')
    if datetime.fromisoformat(receipt['observed_at']).tzinfo is None:raise VolcanicQuarantineError('retrieval_timezone_required')
    return {'input_id':name+':'+digest(body),'producer':'NOAA National Centers for Environmental Information','host':httpx.URL(URLS[name]).host,
        'role':'official_distributor'if name.startswith(('volcano_','page_','count_','ids_','layer_'))else'producer_publication',
        'distributor':'NOAA NCEI ArcGIS organization C8EMgrsFcRFL6LrL, ncei_noaa-owned service item',
        'delegation_evidence':{'producer_page_url':LANDING,'viewer_url':URLS['hazards_viewer'],'viewer_javascript_url':URLS['hazards_js'],'portal_item_url':URLS['volcano_item'],'portal_owner':'ncei_noaa','hosted_service_url':SERVICE},
        'canonical_url':URLS[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],
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


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempt=f'_acquisition-attempts/{SLUG}/{uuid4().hex}';root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or any(p.is_symlink()for p in[root,*root.parents])):raise VolcanicQuarantineError('unsafe_capture_root')
    try:
        for name,url in URLS.items():
            if root is not None:
                body=bounded_input(root/(name+'.body'),MAX_BODY_BYTES)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body)
                raw=bounded_input(root/(name+'.receipt.json'),65536)
            else:
                path=Path(storage.local_dir)/attempt/name;path.mkdir(parents=True)
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
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body);raw=encode(receipt)
            immutable(storage,f'{attempt}/{name}.raw-receipt.body',raw)
            receipt=strict_json(raw);validate_receipt(name,body,receipt);captures[name]=(body,receipt)
        return captures
    except Exception as exc:
        immutable(storage,attempt+'/failure.json',encode({'approved':False,'reason_type':type(exc).__name__}))
        where=quarantine(storage,captures,str(exc));raise VolcanicQuarantineError('source_acquisition_failed:'+str(exc)+';retained:'+where)from exc


def parquet_table(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS.from_epsg(3857).to_json_dict()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})


def validate_output(root,table):
    path=root/'geoparquet'/f'{SLUG}.parquet';actual=pq.read_table(path)
    if not actual.equals(parquet_table(table),check_metadata=True):raise VolcanicQuarantineError('full_typed_native_output_drift')
    for w in actual['geometry'].to_pylist():
        p=shapely.from_wkb(w)
        if p.geom_type!='Point'or not p.is_valid or p.is_empty:raise VolcanicQuarantineError('invalid_native_output_geometry')


def manifest_for(captures,identity,output_pin):
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Historical Significant Volcanic Event Locations — complete current official NOAA NCEI draft','publisher':'NOAA National Centers for Environmental Information','agency':'National Oceanic and Atmospheric Administration','source_url':LANDING,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(n,*captures[n])for n in URLS],
        'transformations':[{'operation':'complete_official_ID_targeted_feature_extraction','parameters':{'source_ids':'OBJECTID, VOL_ID, SIG_ID, TSU_ID, HAZ_EVENT_ID, ID, OID and literal NUM remain distinct original columns; no historical ID join accepted.','types':'Original SmallInteger int16/Integer int32/Double float64/string/nullability/width metadata retained. OID Arrow int32 is a proposed bounded staging representation; source OID physical width is not explicitly declared. Original ID values/declarations/JSON lexical bytes remain immutable; no width or published schema mapping approved.','dates':'Original YEAR/MO/DAY components including BCE and nulls remain literal integers; TIME_ERUPT remains original coded string, never converted to invented timestamp.','geometry':'Native3857 Point WKB from original binary64 x/y, no projection, precision change or repair.','relationship':'903 current official significant event records; historical893-row baseline and schema/ID/geometry differences remain unapproved.'}}],
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
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(URLS):raise VolcanicQuarantineError('invalid_original_context')
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
                    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            for n,b in source_payloads(captures,identity,audit,manifest).items():write_private(temp,n,b)
            ready={'approved':False,'version':version,'processing_fingerprint':fingerprint,'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));validate_candidate(temp,table,identity,captures,audit,manifest,version)
            captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest,create=True)
            validate_candidate(temp,table,identity,captures,audit,manifest,version);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}


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
    if set(captures)!=set(URLS):raise VolcanicQuarantineError('capture_roster_drift')
    for n,(b,r)in captures.items():validate_receipt(n,b,r)
    if b'https://www.ncei.noaa.gov/maps/hazards/?layers=0'not in captures['ncei'][0]or b'./assets/index-Cyvq2L9n.js'not in captures['hazards_viewer'][0]or b'e991eccebfa5425e9180d884af76f16a'not in captures['hazards_js'][0]:raise VolcanicQuarantineError('missing_official_viewer_delegation')
    item=strict_json(captures['volcano_item'][0]);service=strict_json(captures['volcano_service'][0])
    if item.get('owner')!='ncei_noaa'or item.get('url')!=SERVICE or not any(x.get('id')==0 and type(x.get('id'))is int for x in service.get('layers',[])):raise VolcanicQuarantineError('official_item_service_delegation_drift')
    before=strict_json(captures['volcano_layer'][0]);after=strict_json(captures['layer_after'][0])
    for layer in[before,after]:
        if(encode(layer.get('fields'))!=encode(FIELD_DEFINITIONS)or layer.get('geometryType')!='esriGeometryPoint'or layer.get('objectIdField')!='OBJECTID'
            or type(layer.get('id'))is not int or layer['id']!=0 or layer.get('hasZ')is not False or layer.get('hasM')is not False
            or encode(layer.get('extent',{}).get('spatialReference'))!=encode({'wkid':102100,'latestWkid':3857})
            or type(layer.get('maxRecordCount'))is not int or layer['maxRecordCount']<250):raise VolcanicQuarantineError('physical_source_schema_geometry_or_transfer_drift')
    if encode(before.get('editingInfo'))!=encode(after.get('editingInfo')):raise VolcanicQuarantineError('source_changed_during_extraction')
    for n in['count_before','count_after']:
        count=strict_json(captures[n][0]).get('count')
        if type(count)is not int or count!=INITIAL_COUNT:raise VolcanicQuarantineError('complete_count_drift')
    ids=id_roster(captures['ids_before'][0],INITIAL_COUNT)
    if id_roster(captures['ids_after'][0],INITIAL_COUNT)!=ids or digest(encode(ids))!=INITIAL_SOURCE_ID_SHA256:raise VolcanicQuarantineError('original_ID_roster_drift_requires_review')
    pages=[]
    for name,url in URLS.items():
        if not name.startswith('page_'):continue
        expected=[int(x)for x in parse_qs(urlparse(url).query)['objectIds'][0].split(',')];page=strict_json(captures[name][0])
        if sorted(x['attributes']['OBJECTID']for x in page.get('features',[]))!=expected:raise VolcanicQuarantineError('targeted_page_missing_extra_ID')
        pages.append(page)
    table=decode_pages(pages,ids);years=[x for x in table['YEAR'].to_pylist()if x is not None];countries=Counter(table['COUNTRY'].to_pylist());audit={'count':table.num_rows,'original_OBJECTIDs':ids,'original_SIG_IDs':table['SIG_ID'].to_pylist(),'original_event_IDs':table['ID'].to_pylist(),'original_VOL_IDs':table['VOL_ID'].to_pylist(),'countries':sorted(countries.items(),key=lambda x:repr(x[0])),'crs':'EPSG3857','field_count':59,'BCE_count':sum(y<0 for y in years),'unknown_years':table['YEAR'].null_count,'minimum_YEAR':min(years),'maximum_YEAR':max(years),'date_semantics':'YEAR/MO/DAY components and literal TIME_ERUPT codes retained; no BCE or unknown dates inferred.','editingInfo':before.get('editingInfo'),'release_date_established':False}
    return table,audit
