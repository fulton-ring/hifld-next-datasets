"""NOAA NCEI delegated historical tsunami EVENT layer, local draft only.

Original query JSON, all source IDs/types and native3857 Point coordinates are
retained. Observations/runups are a distinct layer and are never substituted.
"""
from __future__ import annotations
import collections
import fcntl
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import sqlite3
import stat
import struct
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pyogrio
import pyproj
import shapely
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.construction_state import canonical_construction_bundle, load_first_construction, seal_first_construction
from dagster_hifld.output_policy import validate_output_policy
from dagster_hifld.provenance import provenance_projection
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.source_manifest import load_resolved_source_manifest
from dagster_hifld.validation import sha256_file
SLUG='historical-tsunami-event-locations-'
LAYER='historical_tsunami_events'
BASE='https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/tsunami_events/FeatureServer/0'
WEBMAP='8bed3e9c40114d6a8b2708ceaee0b329'
LANDING='https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis'
WEBMAP_URL=f'https://www.arcgis.com/sharing/rest/content/items/{WEBMAP}?f=json'
WEBMAP_DATA_URL=f'https://www.arcgis.com/sharing/rest/content/items/{WEBMAP}/data?f=json'
LICENSE='NOAA NCEI source use/citation terms; cite NOAA/WDS Global Historical Tsunami Database DOI10.7289/V5PN93H7. No license exception or acceptance is established.'
READY='metadata/tsunami_events_candidate_ready.json'
BATCH=100
MAX_ROWS=10000
MAX_CAPTURE_BYTES=48*1024*1024
FIELD_DEFINITIONS=[{'name': 'UNIQUE_ID', 'type': 'esriFieldTypeOID', 'alias': 'Unique ID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None, 'description': '{"value":"Unique identifier for each tsunami event record.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'YEAR', 'type': 'esriFieldTypeInteger', 'alias': 'Year', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Year when the tsunami event occurred.","fieldValueType":"dateAndTime"}'}, {'name': 'MONTH', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Month', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Month when the tsunami event occurred, represented as a number from 1 to 12.","fieldValueType":"dateAndTime"}'}, {'name': 'DAY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Day', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Day of the month when the tsunami event occurred.","fieldValueType":"dateAndTime"}'}, {'name': 'HOUR', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Hour', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Hour of the day (0-23) when the tsunami event occurred.","fieldValueType":"dateAndTime"}'}, {'name': 'MINUTE', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Minute', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Minute of the hour (0-59) when the tsunami event occurred.","fieldValueType":"dateAndTime"}'}, {'name': 'SECOND', 'type': 'esriFieldTypeSingle', 'alias': 'Second', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Second of the minute (0-59.9) when the tsunami event occurred.","fieldValueType":"dateAndTime"}'}, {'name': 'DATE_STRING', 'type': 'esriFieldTypeString', 'alias': 'Date String', 'sqlType': 'sqlTypeOther', 'length': 13, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Date of the tsunami event formatted as a string (YYYY/MM/DD).","fieldValueType":"dateAndTime"}'}, {'name': 'EVENT_DATE', 'type': 'esriFieldTypeDate', 'alias': 'Event Date', 'sqlType': 'sqlTypeOther', 'length': 8, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Date and time of the tsunami event (stored as a date/time value).","fieldValueType":"dateAndTime"}', 'precision': 1}, {'name': 'LATITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'Latitude', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Latitude coordinate of the tsunami event\'s location in decimal degrees.","fieldValueType":"coordinate"}'}, {'name': 'LONGITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'Longitude', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Longitude coordinate of the tsunami event\'s location in decimal degrees.","fieldValueType":"coordinate"}'}, {'name': 'LOCATION_NAME', 'type': 'esriFieldTypeString', 'alias': 'Location Name', 'sqlType': 'sqlTypeOther', 'length': 35, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Name of the specific location where the tsunami event occurred.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'AREA', 'type': 'esriFieldTypeString', 'alias': 'Area', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Area code or abbreviation related to the tsunami event\'s location.","fieldValueType":"typeOrCategory"}'}, {'name': 'COUNTRY', 'type': 'esriFieldTypeString', 'alias': 'Country', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Name of the country where the tsunami event occurred.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'REGION_CODE', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Region Code', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Numeric code representing the region for the tsunami event.","fieldValueType":"typeOrCategory"}'}, {'name': 'REGION', 'type': 'esriFieldTypeString', 'alias': 'Region', 'sqlType': 'sqlTypeOther', 'length': 54, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Name of the broader geographic region associated with the tsunami event.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'CAUSE_CODE', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Cause Code', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Numeric code indicating the cause of the tsunami event.","fieldValueType":"typeOrCategory"}'}, {'name': 'CAUSE', 'type': 'esriFieldTypeString', 'alias': 'Cause', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Primary cause of the tsunami event (e.g., Earthquake, Landslide, Volcano).","fieldValueType":"typeOrCategory"}'}, {'name': 'EVENT_VALIDITY_CODE', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Event Validity Code', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Numeric code representing the validity or reliability of the tsunami event record.","fieldValueType":"orderedOrRanked"}'}, {'name': 'EVENT_VALIDITY', 'type': 'esriFieldTypeString', 'alias': 'Event Validity', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Assessment of the tsunami event\'s validity (e.g., Definite, Probable, Questionable).","fieldValueType":"orderedOrRanked"}'}, {'name': 'EQ_MAG_UNK', 'type': 'esriFieldTypeDouble', 'alias': 'Unknown Magnitude', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Earthquake magnitude with unknown scale, representing the strength of the earthquake associated with the tsunami event.","fieldValueType":"measurement"}'}, {'name': 'EQ_MAG_MB', 'type': 'esriFieldTypeDouble', 'alias': 'Body Wave Magnitude (MB)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Earthquake magnitude measured using the Body Wave Magnitude (MB) scale; indicates earthquake strength.","fieldValueType":"measurement"}'}, {'name': 'EQ_MAG_MS', 'type': 'esriFieldTypeDouble', 'alias': 'Surface Wave Magnitude (MS)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Earthquake magnitude measured using the Surface Wave Magnitude (MS) scale; indicates earthquake strength.","fieldValueType":"measurement"}'}, {'name': 'EQ_MAG_MW', 'type': 'esriFieldTypeDouble', 'alias': 'Moment Magnitude (MW)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Earthquake magnitude measured using the Moment Magnitude (MW) scale; widely used for measuring large earthquakes.","fieldValueType":"measurement"}'}, {'name': 'EQ_MAG_ML', 'type': 'esriFieldTypeDouble', 'alias': 'Local Magnitude (ML)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Earthquake magnitude measured using the Local Magnitude (ML) scale, often referred to as the Richter scale.","fieldValueType":"measurement"}'}, {'name': 'EQ_MAG_MFA', 'type': 'esriFieldTypeDouble', 'alias': 'Magnitude from Amplitude (MFA)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Earthquake magnitude measured using the Magnitude from Amplitude (MFA) scale; indicates earthquake strength.","fieldValueType":"measurement"}'}, {'name': 'EQ_MAGNITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'Earthquake Magnitude', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"General earthquake magnitude representing the strength of the earthquake triggering the tsunami event, regardless of scale.","fieldValueType":"measurement"}'}, {'name': 'EQ_MAGNITUDE_RANK', 'type': 'esriFieldTypeDouble', 'alias': 'Earthquake Magnitude Rank', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Rank or order of earthquake magnitude for the event, reflecting its relative size or strength among events.","fieldValueType":"orderedOrRanked"}'}, {'name': 'EQ_DEPTH', 'type': 'esriFieldTypeInteger', 'alias': 'Earthquake Depth (km)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Depth of the earthquake focus in kilometers, indicating how deep below the earth\'s surface the earthquake occurred.","fieldValueType":"measurement"}'}, {'name': 'MAX_EVENT_RUNUP', 'type': 'esriFieldTypeDouble', 'alias': 'Maximum Tsunami Runup (m)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Maximum height of the tsunami wave runup above sea level measured in meters during the event.","fieldValueType":"measurement"}'}, {'name': 'TS_MT_ABE', 'type': 'esriFieldTypeDouble', 'alias': 'Tsunami Magnitude Abe (TS_MT_ABE)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Tsunami magnitude measured by the Abe method; quantifies tsunami wave strength.","fieldValueType":"measurement"}'}, {'name': 'TS_MT_II', 'type': 'esriFieldTypeDouble', 'alias': 'Tsunami Magnitude II (TS_MT_II)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Tsunami magnitude according to the Imamura-Iwasaki method; used to assess tsunami intensity.","fieldValueType":"measurement"}'}, {'name': 'TS_INTENSITY', 'type': 'esriFieldTypeDouble', 'alias': 'Tsunami Intensity', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Intensity rating of the tsunami event, indicating its strength or impact severity on a standardized scale.","fieldValueType":"measurement"}'}, {'name': 'DAMAGE_MILLIONS_DOLLARS', 'type': 'esriFieldTypeDouble', 'alias': 'Damage (Million USD)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Estimated property damage caused by the tsunami event, measured in millions of US dollars.","fieldValueType":"currency"}'}, {'name': 'DAMAGE_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Damage Amount Rank', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Rank or categorical order of the damage amount, scaling damage severity from limited to extreme.","fieldValueType":"orderedOrRanked"}'}, {'name': 'DAMAGE_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Damage Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Text description categorizing the extent of property damage caused by the tsunami event.","fieldValueType":"description"}'}, {'name': 'HOUSES_DESTROYED', 'type': 'esriFieldTypeInteger', 'alias': 'Houses Destroyed', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Total number of houses destroyed due to the tsunami event.","fieldValueType":"countOrAmount"}'}, {'name': 'HOUSES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Houses Destroyed Rank', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Rank or category representing the severity of house destruction, from few to very many houses.","fieldValueType":"orderedOrRanked"}'}, {'name': 'HOUSES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Houses Destroyed Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative description of the extent of house destruction caused by the tsunami, e.g., few, some, many, very many houses.","fieldValueType":"description"}'}, {'name': 'DEATHS', 'type': 'esriFieldTypeInteger', 'alias': 'Deaths', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Total number of confirmed fatalities resulting from the tsunami event.","fieldValueType":"countOrAmount"}'}, {'name': 'DEATHS_AMOUNT_ORDER', 'type': 'esriFieldTypeDouble', 'alias': 'Deaths Amount Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"An ordinal scale representing the number of deaths caused by the tsunami event, ranging from none to a very large number.","fieldValueType":"orderedOrRanked"}'}, {'name': 'DEATHS_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Deaths Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative description of the number of deaths resulting from the tsunami, categorized by approximate ranges.","fieldValueType":"description"}'}, {'name': 'INJURIES', 'type': 'esriFieldTypeInteger', 'alias': 'Injuries', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The total count of injuries reported due to the tsunami event.","fieldValueType":"countOrAmount"}'}, {'name': 'INJURIES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Injuries Amount Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"An ordinal ranking describing the quantity of injuries sustained, from few to very many.","fieldValueType":"orderedOrRanked"}'}, {'name': 'INJURIES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Injuries Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Descriptive category indicating the approximate number of injuries caused by the tsunami.","fieldValueType":"description"}'}, {'name': 'MISSING', 'type': 'esriFieldTypeInteger', 'alias': 'Missing Persons', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Number of people reported missing as a result of the tsunami event.","fieldValueType":"countOrAmount"}'}, {'name': 'MISSING_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Missing Persons Amount Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Ranked category indicating the scale of missing persons, from few to many.","fieldValueType":"orderedOrRanked"}'}, {'name': 'MISSING_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Missing Persons Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative description of the number of missing persons associated with the tsunami event.","fieldValueType":"description"}'}, {'name': 'NUM_RUNUP', 'type': 'esriFieldTypeInteger', 'alias': 'Number of Runup Measurements', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Count of measured tsunami wave runup height observations taken for the event.","fieldValueType":"countOrAmount"}'}, {'name': 'DAMAGE_MILLIONS_DOLLARS_TOTAL', 'type': 'esriFieldTypeDouble', 'alias': 'Total Damage (Million USD)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Total estimated property and economic damage from the tsunami expressed in millions of US dollars.","fieldValueType":"currency"}'}, {'name': 'DAMAGE_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Damage Severity Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Ordinal value indicating the severity level of total damage, from limited to extreme.","fieldValueType":"orderedOrRanked"}'}, {'name': 'DAMAGE_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Damage Severity Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Descriptive category of the overall damage extent with approximate monetary ranges.","fieldValueType":"description"}'}, {'name': 'HOUSES_DESTROYED_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'Total Houses Destroyed', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Total quantity of houses destroyed or severely damaged by the tsunami event.","fieldValueType":"countOrAmount"}'}, {'name': 'HOUSES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Houses Destroyed Amount Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Ordinal ranking of the number of houses destroyed, from few to very many.","fieldValueType":"orderedOrRanked"}'}, {'name': 'HOUSES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Houses Destroyed Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Descriptive category of houses destroyed indicating approximate scale of housing loss.","fieldValueType":"description"}'}, {'name': 'DEATHS_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'Total Deaths', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Aggregate number of deaths attributed to the tsunami event globally or for all affected areas combined.","fieldValueType":"countOrAmount"}'}, {'name': 'DEATHS_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Total Deaths Amount Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Overall ordinal ranking of total deaths, describing severity levels from few to very many.","fieldValueType":"orderedOrRanked"}'}, {'name': 'DEATHS_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Total Deaths Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative summary describing total deaths in categories based on approximate counts.","fieldValueType":"description"}'}, {'name': 'INJURIES_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'Total Injuries', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Cumulative number of injuries reported across all affected areas due to the tsunami.","fieldValueType":"countOrAmount"}'}, {'name': 'INJURIES_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Total Injuries Amount Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Ranked level indicating the severity of total injuries, from few to very many.","fieldValueType":"orderedOrRanked"}'}, {'name': 'INJURIES_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Total Injuries Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative description of the total number of injuries caused by the tsunami event, ranging from \'Few\' to \'Very Many\'.","fieldValueType":"orderedOrRanked"}'}, {'name': 'MISSING_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'Total Missing People', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Total number of people reported missing as a result of the tsunami event.","fieldValueType":"countOrAmount"}'}, {'name': 'MISSING_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Missing Persons Scale Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Rank order indicating the scale of missing persons for the event, with higher values representing larger numbers.","fieldValueType":"orderedOrRanked"}'}, {'name': 'MISSING_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Total Missing Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative description of the total number of people reported missing, such as \'Few\', \'Some\', or \'Very Many\'.","fieldValueType":"orderedOrRanked"}'}, {'name': 'HOUSES_DAMAGED', 'type': 'esriFieldTypeInteger', 'alias': 'Houses Damaged', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Number of individual houses damaged by the tsunami event in the affected area.","fieldValueType":"countOrAmount"}'}, {'name': 'HOUSES_DAMAGED_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Houses Damaged Scale Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Rank order indicating the scale of damaged houses, with higher values denoting greater damage.","fieldValueType":"orderedOrRanked"}'}, {'name': 'HOUSES_DAM_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Houses Damaged Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative description of the number of houses damaged during the event, e.g., \'Few\', \'Many\', or \'Very Many\'.","fieldValueType":"orderedOrRanked"}'}, {'name': 'HOUSES_DAMAGED_TOTAL', 'type': 'esriFieldTypeInteger', 'alias': 'Total Houses Damaged', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Total count of houses damaged from all related impacts of the tsunami event.","fieldValueType":"countOrAmount"}'}, {'name': 'HOUSES_DAM_AMOUNT_ORDER_TOTAL', 'type': 'esriFieldTypeSmallInteger', 'alias': 'Total Houses Damaged Scale Order', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Rank order for the total amount of houses damaged, indicating damage severity on an ordered scale.","fieldValueType":"orderedOrRanked"}'}, {'name': 'HOUSES_DAM_TOTAL_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'Total Houses Damaged Description', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Qualitative description summarizing the total houses damaged, such as \'Few\', \'Some\', or \'Very Many\'.","fieldValueType":"orderedOrRanked"}'}, {'name': 'CAUSE_SYMBOL', 'type': 'esriFieldTypeDouble', 'alias': 'Cause Code', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Numeric code representing the cause type of the tsunami event, such as earthquake, volcanic activity, or landslide.","fieldValueType":"typeOrCategory"}'}, {'name': 'URL', 'type': 'esriFieldTypeString', 'alias': 'Reference URL', 'sqlType': 'sqlTypeOther', 'length': 109, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Web link to detailed information or data source about the tsunami event.","fieldValueType":"description"}'}, {'name': 'COMMENTS', 'type': 'esriFieldTypeString', 'alias': 'Comments', 'sqlType': 'sqlTypeOther', 'length': 4000, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Additional notes or remarks providing context, observations, or supplementary details about the tsunami event.","fieldValueType":"description"}'}, {'name': 'ID', 'type': 'esriFieldTypeInteger', 'alias': 'Event ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Unique identifier number for each tsunami event record in the dataset.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'MAP_SYMBOL', 'type': 'esriFieldTypeInteger', 'alias': 'Map Symbol Code', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Symbol code used in mapping applications to graphically represent the tsunami event location or type.","fieldValueType":"typeOrCategory"}'}, {'name': 'OCEANIC_TSUNAMI', 'type': 'esriFieldTypeString', 'alias': 'Oceanic Tsunami', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Indicates whether the record is associated with an oceanic tsunami event.","fieldValueType":"binary"}'}]
FIELDS=tuple(f['name'] for f in FIELD_DEFINITIONS)+('geometry',)
TYPE_MAP={'esriFieldTypeOID':pa.int64(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeSmallInteger':pa.int16(),'esriFieldTypeSingle':pa.float32(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeDate':pa.timestamp('ms',tz='UTC'),'esriFieldTypeString':pa.string()}
TYPES=tuple(TYPE_MAP[f['type']] for f in FIELD_DEFINITIONS)+(pa.binary(),)
INITIAL_COUNT=2590

class TsunamiQuarantineError(ValueError):
    """Untrusted/incomplete input or cache requires review; never release."""

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body): return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise TsunamiQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda _: (_ for _ in ()).throw(TsunamiQuarantineError('nonfinite_json')))
    if not isinstance(value,dict):raise TsunamiQuarantineError('json_object_required')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise TsunamiQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir)
    if storage.prefix or any(p.is_symlink() for p in [root,*root.parents]):raise TsunamiQuarantineError('aliased_staging_refused')
    root.mkdir(parents=True,exist_ok=True)
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise TsunamiQuarantineError('linked_staging_object')

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None:storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:raise TsunamiQuarantineError('immutable_capture_changed:'+key)

def processing_identity():
    root=Path(__file__).parents[3]
    return {'python':platform.python_version(),'implementation':platform.python_implementation(),'platform':platform.platform(),
        'distributions':sorted((d.metadata['Name'],d.version) for d in importlib.metadata.distributions()),
        'gdal':pyogrio.__gdal_version_string__,'geos':shapely.geos_version_string,'proj':pyproj.proj_version_str,
        'files':{str(p.relative_to(root)):sha256_file(p) for p in sorted((root/'src/dagster_hifld').rglob('*.py'))+[root/'uv.lock',root/'pyproject.toml']},
        'verifier_sha256':verifier_implementation_sha256()}

def query_url(**params):
    return str(httpx.URL(BASE+'/query',params={'f':'json',**params}))

def request_plan(ids=None):
    urls={'authority_landing':LANDING,'authority_webmap':WEBMAP_URL,'authority_webmap_data':WEBMAP_DATA_URL,
        'metadata_before':BASE+'?f=json','count_before':query_url(where='1=1',returnCountOnly='true'),
        'ids_before':query_url(where='1=1',returnIdsOnly='true')}
    if ids is not None:
        for offset in range(0,len(ids),BATCH):
            urls[f'page_{offset:05}']=query_url(objectIds=','.join(map(str,ids[offset:offset+BATCH])),outFields='*',returnGeometry='true',orderByFields='UNIQUE_ID')
        urls.update(metadata_after=BASE+'?f=json',count_after=query_url(where='1=1',returnCountOnly='true'),ids_after=query_url(where='1=1',returnIdsOnly='true'))
    return urls

def retain_failure(storage,captures,reason):
    prefix=f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}'
    for name,(body,receipt) in captures.items():
        immutable(storage,prefix+'/'+name+'.body',body);immutable(storage,prefix+'/'+name+'.receipt.json',encode(receipt))
    immutable(storage,prefix+'/reason.json',encode({'approved':False,'reason':reason}))
    return prefix

def fetch_one(url):
    body=bytearray();receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat(),'complete_body':False}
    try:
        with httpx.Client(follow_redirects=False,timeout=90,headers={'Accept-Encoding':'identity'}) as client,client.stream('GET',url) as response:
            receipt.update(status=response.status_code,final_url=str(response.url),content_length=response.headers.get('content-length'),content_encoding=response.headers.get('content-encoding'),requested_accept_encoding='identity')
            try:
                for block in response.iter_bytes(65536):
                    body+=block
                    if len(body)>6*1024*1024:raise TsunamiQuarantineError('bounded_response_size_exceeded')
            finally:receipt['wire_bytes_downloaded']=response.num_bytes_downloaded
            if response.status_code!=200 or (receipt['content_length'] is not None and int(receipt['content_length'])!=receipt['wire_bytes_downloaded']):raise TsunamiQuarantineError('non200_or_incomplete_encoded_transport')
        receipt.update(bytes=len(body),sha256=digest(body),complete_body=True)
        return bytes(body),receipt
    except (httpx.HTTPError,OSError,ValueError) as exc:
        receipt.update(bytes=len(body),sha256=digest(body),error=str(exc))
        raise TsunamiFetchError(str(exc),bytes(body),receipt) from exc

class TsunamiFetchError(TsunamiQuarantineError):
    def __init__(self,message,body,receipt):
        super().__init__(message);self.body=body;self.receipt=receipt

def validate_capture(name,body,receipt,url):
    if (receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status')) is not int or receipt['status']!=200
        or receipt.get('complete_body') is not True or receipt.get('bytes')!=len(body) or receipt.get('sha256')!=digest(body)):
        raise TsunamiQuarantineError('capture_url_status_length_hash_or_completeness_drift:'+name)

def source_ids(document):
    if set(document)!={'objectIdFieldName','objectIds'} or document['objectIdFieldName']!='UNIQUE_ID':raise TsunamiQuarantineError('missing_or_renamed_original_OID')
    ids=document['objectIds']
    if not isinstance(ids,list) or not ids or len(ids)>MAX_ROWS or any(type(v)is not int or v<=0 or v>2**63-1 for v in ids) or len(set(ids))!=len(ids):raise TsunamiQuarantineError('invalid_duplicate_or_unbounded_source_ids')
    return sorted(ids)

def acquire(storage,capture_dir=None):
    local_only(storage);captures={};total=0
    def read(name,url):
        nonlocal total
        if capture_dir is None:
            try:body,receipt=fetch_one(url)
            except TsunamiFetchError as exc:
                captures[name]=(exc.body,exc.receipt);raise
        else:
            path=Path(capture_dir);body=(path/(name+'.body')).read_bytes();receipt=strict_json((path/(name+'.receipt.json')).read_bytes())
        captures[name]=(body,receipt);total+=len(body)
        if total>MAX_CAPTURE_BYTES:raise TsunamiQuarantineError('cumulative_complete_capture_budget_exceeded')
        validate_capture(name,body,receipt,url)
    try:
        for name,url in request_plan().items():read(name,url)
        ids=source_ids(strict_json(captures['ids_before'][0]))
        for name,url in request_plan(ids).items():
            if name not in captures:read(name,url)
        if capture_dir is not None:
            actual={f.name for f in Path(capture_dir).iterdir() if f.is_file()}
            if actual!={name+suffix for name in captures for suffix in ('.body','.receipt.json')}:raise TsunamiQuarantineError('capture_directory_extra_or_missing_object')
        return captures
    except (ValueError,OSError,KeyError,TypeError) as exc:
        retained=retain_failure(storage,captures,str(exc));raise TsunamiQuarantineError(str(exc)+'; retained:'+retained) from exc

def validate_metadata(meta):
    if (meta.get('fields')!=FIELD_DEFINITIONS or meta.get('objectIdField')!='UNIQUE_ID' or meta.get('geometryType')!='esriGeometryPoint'
        or meta.get('hasZ') is not False or meta.get('hasM') is not False or meta.get('extent',{}).get('spatialReference',{}).get('latestWkid')!=3857
        or meta.get('serviceItemId')!='5a44c3d4d465498993120b70ab568876' or meta.get('maxRecordCount',0)<BATCH):
        raise TsunamiQuarantineError('source_schema_physical_types_geometry_or_service_identity_drift')

def physical_value(field,value):
    if value is None:
        if field['nullable'] is not True:raise TsunamiQuarantineError('nonnull_source_identifier_missing')
        return None
    kind=field['type']
    if kind=='esriFieldTypeString':
        if not isinstance(value,str):raise TsunamiQuarantineError('source_string_type_drift')
        return value # Never strip either leading/trailing text or rewrite HTML.
    if kind in {'esriFieldTypeInteger','esriFieldTypeSmallInteger','esriFieldTypeOID','esriFieldTypeDate'}:
        bounds={'esriFieldTypeInteger':(-2**31,2**31-1),'esriFieldTypeSmallInteger':(-32768,32767),'esriFieldTypeOID':(1,2**63-1),'esriFieldTypeDate':(-2**63,2**63-1)}[kind]
        if type(value)is not int or not bounds[0]<=value<=bounds[1]:raise TsunamiQuarantineError('source_integer_or_date_physical_type_drift')
        return value
    if type(value)not in (int,float) or not math.isfinite(value):raise TsunamiQuarantineError('nonfinite_or_nonnumeric_source_value')
    if kind=='esriFieldTypeSingle':
        result=struct.unpack('<f',struct.pack('<f',value))[0]
        if not math.isfinite(result):raise TsunamiQuarantineError('source_single_overflow')
        return result # Explicit original declared IEEE754binary32 decode, not widening.
    return float(value)

def inspect_captures(captures):
    ids=source_ids(strict_json(captures['ids_before'][0]));plan=request_plan(ids)
    if set(captures)!=set(plan):raise TsunamiQuarantineError('incomplete_or_extra_capture_roster')
    for name,url in plan.items():validate_capture(name,*captures[name],url)
    landing=captures['authority_landing'][0].decode('utf8');webmap=strict_json(captures['authority_webmap'][0]);data=strict_json(captures['authority_webmap_data'][0])
    if '/maps/tsunami-events/' not in landing or '10.7289/V5PN93H7' not in landing or webmap.get('id')!=WEBMAP or webmap.get('owner')!='ncei_noaa' or not any(l.get('url')==BASE for l in data.get('operationalLayers',[])):
        raise TsunamiQuarantineError('producer_landing_webmap_or_explicit_delegation_drift')
    before=strict_json(captures['metadata_before'][0]);after=strict_json(captures['metadata_after'][0]);validate_metadata(before);validate_metadata(after)
    if before!=after or source_ids(strict_json(captures['ids_after'][0]))!=ids:raise TsunamiQuarantineError('source_changed_during_complete_extraction')
    for name in ('count_before','count_after'):
        count=strict_json(captures[name][0])
        if set(count)!={'count'} or type(count['count'])is not int or count['count']!=len(ids):raise TsunamiQuarantineError('source_count_roster_or_completeness_mismatch')
    rows=[];single_rounding={};raw_date_values=[]
    for offset in range(0,len(ids),BATCH):
        name=f'page_{offset:05}';page=strict_json(captures[name][0]);expected_ids=ids[offset:offset+BATCH]
        if page.get('exceededTransferLimit') is True or page.get('geometryType')!='esriGeometryPoint' or page.get('spatialReference',{}).get('latestWkid')!=3857:raise TsunamiQuarantineError('page_limit_geometry_crs_or_completeness_drift')
        if [(f['name'],f['type']) for f in page.get('fields',[])]!=[(f['name'],f['type']) for f in FIELD_DEFINITIONS]:raise TsunamiQuarantineError('query_field_roster_or_physical_types_drift')
        features=page.get('features')
        if not isinstance(features,list) or len(features)!=len(expected_ids) or [f.get('attributes',{}).get('UNIQUE_ID') for f in features]!=expected_ids:raise TsunamiQuarantineError('page_missing_duplicate_unrequested_or_reordered_IDs')
        for feature in features:
            attrs=feature['attributes']
            if set(attrs)!={f['name'] for f in FIELD_DEFINITIONS}:raise TsunamiQuarantineError('source_attribute_rename_missing_or_unexpected_column')
            row={f['name']:physical_value(f,attrs[f['name']]) for f in FIELD_DEFINITIONS}
            for field in FIELD_DEFINITIONS:
                key=field['name']
                if field['type']=='esriFieldTypeSingle' and attrs[key]!=row[key]:single_rounding[key]=single_rounding.get(key,0)+1
            raw_date_values.append(row['EVENT_DATE'])
            geometry=feature.get('geometry')
            if not isinstance(geometry,dict) or set(geometry)!={'x','y'} or any(type(geometry[v])not in(int,float) or not math.isfinite(geometry[v]) for v in ('x','y')):raise TsunamiQuarantineError('missing_or_changed_native_point')
            row['geometry']=shapely.Point(geometry['x'],geometry['y']).wkb;rows.append(row)
    schema=pa.schema([pa.field(f['name'],TYPE_MAP[f['type']],nullable=f['nullable'],metadata={b'noaa:esri_field':encode(f)}) for f in FIELD_DEFINITIONS]+[pa.field('geometry',pa.binary())])
    table=pa.Table.from_pylist(rows,schema=schema)
    if table.num_rows!=len(ids) or len(set(table['ID'].to_pylist()))!=table.num_rows:raise TsunamiQuarantineError('event_ID_missing_duplicate_or_coverage_loss')
    geometry=shapely.from_wkb(table['geometry'].to_pylist())
    if not all(shapely.is_valid(geometry)) or any(shapely.is_empty(geometry)):raise TsunamiQuarantineError('invalid_or_empty_native_geometry')
    # Source-reported timestamp physical precision must round-trip all original ms.
    if table['EVENT_DATE'].cast(pa.int64()).to_pylist()!=raw_date_values:raise TsunamiQuarantineError('source_date_millisecond_loss')
    audit={'approved':False,'feature_count':len(ids),'original_field_definitions':FIELD_DEFINITIONS,'source_original_OID':'UNIQUE_ID is the system-maintained service OID; ID remains the separate source event identifier. Neither replaces a published ID. OID width not declared by sqlTypeOther; Arrowint64 representation is proposed, not accepted.',
        'physical_decode':'SmallInteger=int16; Integer=int32; Single=IEEE754binary32; Double=float64; Date=timestamp(ms,UTC) exactly retaining original wire epochms. Strings unchanged, all nulls preserved.',
        'Single_wire_decimal_to_declared_binary32_differences':single_rounding,'source_crs':'EPSG:3857','geometry':'Point','invalid_geometries':0,'country_counts':[{'value':k,'count':v} for k,v in sorted(collections.Counter(table['COUNTRY'].to_pylist()).items(),key=lambda item:str(item[0]))],
        'year_min':min(table['YEAR'].to_pylist()),'year_max':max(table['YEAR'].to_pylist()),'current_source_count':len(ids),'initial_verified_count':INITIAL_COUNT,'roster_change_from_initial_requires_review':len(ids)!=INITIAL_COUNT,
        'source_editingInfo':before.get('editingInfo'),'release_day':'unknown; service lastEdit is an upstream edit observation, not a verified release day','coverage':'Complete current delegated FeatureServer event-location layer, not a claim to include every nonspatial/invalid underlying database record. No observation/runup rows or volcano-only subset substituted.'}
    return table,audit

def expected_parquet(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS(3857).to_json_dict(),'bbox':shapely.total_bounds(shapely.from_wkb(table['geometry'].to_pylist())).tolist()}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})

def canonical_values(table):
    # Avoid Python datetime coercion; the actual source timestamp is integral ms.
    index=table.schema.get_field_index('EVENT_DATE');table=table.set_column(index,'EVENT_DATE',table['EVENT_DATE'].cast(pa.int64()))
    return table.to_pylist()

def records_hash(table):
    h=hashlib.sha256()
    for row in canonical_values(table):row['geometry']=row['geometry'].hex();h.update(encode(row))
    return h.hexdigest()

def write_gpkg(table,path):
    pyogrio.write_arrow(table,path,layer=LAYER,driver='GPKG',geometry_name='geometry',geometry_type='Point',crs='EPSG:3857',layer_options={'FID':'_gpkg_storage_fid','GEOMETRY_NAME':'geometry'})
    with sqlite3.connect(path) as db:db.execute("UPDATE gpkg_contents SET last_change='2000-01-01T00:00:00.000Z'")

def verify_outputs(root,table):
    for relative in ('candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'):
        if not pq.read_table(root/relative).equals(expected_parquet(table),check_metadata=True):raise TsunamiQuarantineError('parquet_full_values_physical_types_ids_wkb_or_metadata_drift')
    meta,actual=pyogrio.read_arrow(root/'geopackage'/f'{SLUG}.gpkg',layer=LAYER)
    if meta['crs']!='EPSG:3857' or tuple(actual.column_names)!=FIELDS or tuple(f.type for f in actual.schema)!=TYPES or canonical_values(actual)!=canonical_values(table):raise TsunamiQuarantineError('gpkg_full_values_physical_types_ids_or_native_geometry_loss')
    with tempfile.TemporaryDirectory(prefix='tsunami-events-fresh-container-') as tmp:
        fresh=Path(tmp)/'fresh.gpkg';write_gpkg(table,fresh)
        if sha256_file(fresh)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):raise TsunamiQuarantineError('gpkg_full_container_srs_headers_sql_or_index_drift')
def manifest_for(captures,identity,parquet_pin):
    ids=source_ids(strict_json(captures['ids_before'][0]));urls=request_plan(ids);inputs=[]
    for name in sorted(captures):
        body,receipt=captures[name]
        validate_capture(name,body,receipt,urls[name]);host=urlsplit(urls[name]).hostname
        item={'input_id':name+':'+digest(body),'producer':'NOAA National Centers for Environmental Information','host':host,'role':'producer_publication' if host=='www.ncei.noaa.gov' else 'official_distributor',
            'canonical_url':urls[name],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,
            'attribution':'NOAA/WDS Global Historical Tsunami Database, DOI10.7289/V5PN93H7; complete delegated tsunami_events spatial feature layer.',
            'release_date_unknown_reason':'No verified release day. Source service editingInfo and retrieval are distinct observations, not invented publication dates.'}
        if item['role']=='official_distributor':item.update(distributor='NOAA NCEI ArcGIS organization C8EMgrsFcRFL6LrL, ncei_noaa-owned webmap',delegation_evidence={'producer_url':LANDING,'webmap_url':WEBMAP_URL,'webmap_data_url':WEBMAP_DATA_URL,'webmap_id':WEBMAP,'webmap_owner':'ncei_noaa','explicit_service_url':BASE,'captured_webmap_data_sha256':digest(captures['authority_webmap_data'][0])})
        inputs.append(item)
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Historical Tsunami Event Locations — complete current delegated NOAA event layer, unapproved draft','publisher':'NOAA National Centers for Environmental Information','agency':'National Oceanic and Atmospheric Administration','source_url':BASE,'license':LICENSE,'processing_fingerprint':digest(encode(identity)),'inputs':inputs,
        'transformations':[{'operation':'complete_native_event_feature_layer_extraction','parameters':{'authority':'Official NCEI page/viewer→ncei_noaa webmap→explicit hosted NOAA service; not a third-party mirror','completeness':'Before/after full original OID rosters, counts and metadata; exact ID-targeted batches, no offsets or omitted pages','IDs':'UNIQUE_ID and ID retained distinctly; no published ID substitution','types':'Int16/Int32/Float32/Float64/timestamp(ms,UTC), exact original nulls and strings retained; all producer declarations in audit and Arrow field metadata','Single':'Explicit binary32 decode of wire JSON under declared esriFieldTypeSingle, all original JSON losslessly retained','Date':'Original epoch milliseconds retained exactly; unknown/BCE dates remain source nulls, no inferred dates','geometry':'native3857 Point x/y doubles, no projection, repair, quantization or coordinate substitution'}}],
        'unsupported_output_formats':{'shapefile_zip':'Original field names exceed DBF limits and physical dates/nulls/numeric types cannot be silently mapped; no destructive schema mapping approved.','pmtiles':'Reprojection/quantization changes native coordinates; no visualization mapping approved.'},
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve every original physical field type/nullability/metadata and exact native WKB. Generic bbox/typing additions remain unapproved; this capability is not acceptance.','objects':[{'path':f'geoparquet/{SLUG}.parquet',**parquet_pin}]}}}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest

@contextmanager
def candidate_lock(base,prefix):
    target=base/prefix;target.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:raise TsunamiQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX);yield target

def construction_record_key(version):
    return f'_construction-state/{SLUG}/{version}.json'


def original_bundle(storage,version,captures,identity,manifest,*,create=False):
    """Caller holds common lock; create only after complete private validation.

    Complete first manifest/receipts are bound by the shared outside CAS/marker.
    Old unbound records remain evidence and cannot bootstrap this trust scope.
    """
    fingerprint=digest(encode(identity))
    payload={'manifest':manifest,'captures':{name:{'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body','receipt':receipt} for name,(body,receipt) in captures.items()}}
    envelope=canonical_construction_bundle(version,fingerprint,payload)
    legacy=f'_source-provenance/{SLUG}/{version}/bundle.json'
    if storage.object_exists(legacy) or storage.object_exists(legacy+'.first-construction.json'):
        raise TsunamiQuarantineError('legacy_unbound_original_requires_explicit_reviewed_migration')
    key=construction_record_key(version)
    if create:
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        retained=seal_first_construction(storage,key,version,fingerprint,envelope)
    else:
        retained=load_first_construction(storage,key,version,fingerprint)
    bundle=strict_json(retained)['payload']
    if set(bundle)!={'manifest','captures'} or set(bundle['captures'])!=set(captures):raise TsunamiQuarantineError('original_binding_capture_roster_drift')
    original={}
    for name,item in bundle['captures'].items():
        if set(item)!={'body_key','receipt'} or item['body_key']!=f"_raw-captures/{SLUG}/{item['receipt'].get('sha256')}/{name}.body":raise TsunamiQuarantineError('original_capture_body_key_drift')
        body=storage.read_key(item['body_key'])
        if body!=captures[name][0]:raise TsunamiQuarantineError('original_capture_bytes_differ')
        original[name]=(body,item['receipt'])
    pin=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:pin[k] for k in ('sha256','bytes')});actual=bundle['manifest']
    validate_release_provenance(actual);validate_output_policy(actual);validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'])
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):raise TsunamiQuarantineError('original_construction_manifest_or_receipts_changed')
    return original,actual

def source_payloads(captures,identity,audit,manifest,table):
    proof={'status':'pass','approved':False,'rows':table.num_rows,'source_records_sha256':records_hash(table),'method':'Complete original JSON→declared native physical decode→every output column/value/null/identifier/timestamp-ms/native WKB equality; whole deterministic GPKG container and exact Arrow field/schema metadata.'}
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/tsunami_events_source_audit.json':encode(audit),'metadata/geopackage_source_fidelity.json':encode(proof),'metadata/parquet_source_fidelity.json':encode(proof),
        'metadata/baseline_comparison.json':encode({'schema_version':1,'approved':False,'status':'blocked','reason_codes':['unapproved_current_source_published_baseline_original_OID_event_ID_physical_schema_and_coverage_contract']}),'metadata/baseline_differences.jsonl':b'','metadata/baseline_field_changes.jsonl':b''}
    for name,(body,receipt) in captures.items():values['raw/captures/'+name+'.body']=body;values['raw/captures/'+name+'.receipt.json']=encode(receipt)
    return values

def reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin):
    root=Path(storage.local_dir)/prefix
    if not (root/READY).is_file():raise TsunamiQuarantineError('incomplete_existing_candidate_missing_ready')
    captures,manifest=original_bundle(storage,version,captures,identity,manifest_for(captures,identity,pin))
    expected=source_payloads(captures,identity,audit,manifest,table);names={*expected,f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet',f'geoparquet/{SLUG}.parquet'}
    ready=strict_json((root/READY).read_bytes())
    if ready.get('version')!=version or ready.get('approved')is not False or ready.get('processing_fingerprint')!=digest(encode(identity)) or set(ready.get('objects',{}))!=names:raise TsunamiQuarantineError('ready_identity_or_closed_source_inventory_drift')
    snapshot='metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'
    derivatives={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/source/source_manifest.json','metadata/source/quality_manifest.json','metadata/source/data_dictionary.json','metadata/resolved_provenance.json',snapshot}
    actual={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file()}
    if actual-(names|{READY}|derivatives) or not (names|{READY}).issubset(actual) or any(p.is_symlink() or(p.is_file() and p.stat().st_nlink!=1) for p in root.rglob('*')):raise TsunamiQuarantineError('unknown_missing_or_linked_candidate_object')
    for name,body in expected.items():
        if (root/name).read_bytes()!=body:raise TsunamiQuarantineError('source_owned_report_wrapper_placeholder_or_raw_provenance_drift:'+name)
    for name,sha in ready['objects'].items():
        if sha256_file(root/name)!=sha:raise TsunamiQuarantineError('source_owned_object_changed:'+name)
    for name in ('metadata/source/source_manifest.json',snapshot):
        if (root/name).exists() and (root/name).read_bytes()!=encode(manifest):raise TsunamiQuarantineError('catalog_source_snapshot_drift')
    if (root/'metadata/resolved_provenance.json').exists() and strict_json((root/'metadata/resolved_provenance.json').read_bytes())!=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata):raise TsunamiQuarantineError('catalog_resolved_provenance_drift')
    verify_outputs(root,table)

def stage_tsunami_events(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir)
    try:
        table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));capture_hash=digest(encode({name:digest(body) for name,(body,_) in captures.items()}))
        version=f'v0.0.0-draft-tsunami-{capture_hash[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}';base=Path(storage.local_dir)
        for name,(body,receipt) in captures.items():
            key=f'_raw-captures/{SLUG}/{digest(body)}/{name}.body';immutable(storage,key,body);immutable(storage,key+'.receipt-'+digest(encode(receipt))+'.json',encode(receipt))
        with candidate_lock(base,prefix) as target:
            if target.exists() or target.is_symlink():
                try:
                    if target.is_symlink():raise TsunamiQuarantineError('linked_candidate')
                    with tempfile.TemporaryDirectory(prefix='tsunami-native-prebuilt-') as tmp:
                        fresh=Path(tmp)/'fresh.parquet';pq.write_table(expected_parquet(table),fresh,compression='zstd');pin={'sha256':sha256_file(fresh),'bytes':fresh.stat().st_size}
                    reconcile_cache(storage,prefix,version,table,audit,captures,identity,pin)
                except (ValueError,OSError,KeyError,TypeError,RuntimeError,sqlite3.Error) as exc:
                    dest=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);dest.parent.mkdir(parents=True,exist_ok=True);os.rename(target,dest)
                    immutable(storage,str(dest.relative_to(base))+'.reason.json',encode({'approved':False,'reason':str(exc),'original_prefix':prefix}));raise TsunamiQuarantineError('invalid_candidate_detached_intact:'+str(exc)) from exc
                return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            with tempfile.TemporaryDirectory(prefix='.tsunami-building-',dir=target.parent) as tmp:
                private=Path(tmp)
                for part in ('geopackage','geoparquet','candidate','metadata','raw'):(private/part).mkdir()
                write_gpkg(table,private/'geopackage'/f'{SLUG}.gpkg');parquet=private/'geoparquet'/f'{SLUG}.parquet';pq.write_table(expected_parquet(table),parquet,compression='zstd');(private/'candidate/typed-source.parquet').write_bytes(parquet.read_bytes());verify_outputs(private,table)
                pin={'sha256':sha256_file(parquet),'bytes':parquet.stat().st_size};manifest=manifest_for(captures,identity,pin)
                # The entire private candidate is complete before first trust state
                # is installed. A failed metadata/container build cannot bind history.
                for name,body in source_payloads(captures,identity,audit,manifest,table).items():(private/name).parent.mkdir(parents=True,exist_ok=True);(private/name).write_bytes(body)
                ready={'version':version,'approved':False,'processing_fingerprint':fingerprint,'capture_fingerprint':capture_hash,'objects':{str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file()}}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table)
                captures,manifest=original_bundle(storage,version,captures,identity,manifest,create=True)
                # An existing complete outside witness returns original receipts.
                # Rebuild only the private candidate from those exact originals.
                for name,body in source_payloads(captures,identity,audit,manifest,table).items():(private/name).write_bytes(body)
                ready['objects']={str(p.relative_to(private)):sha256_file(p) for p in private.rglob('*') if p.is_file() and str(p.relative_to(private))!=READY}
                (private/READY).write_bytes(encode(ready));verify_outputs(private,table);os.rename(private,target)
        return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}
    except (ValueError,OSError,KeyError,TypeError,RuntimeError,sqlite3.Error,shapely.errors.GEOSException) as exc:
        retained=retain_failure(storage,captures,str(exc));raise TsunamiQuarantineError(str(exc)+'; retained:'+retained) from exc
