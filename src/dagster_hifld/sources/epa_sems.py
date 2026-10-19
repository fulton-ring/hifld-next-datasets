"""Complete EPA FRS SEMS-interest draft, including non-NPL sites; never release approval."""
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
from urllib.parse import urlencode

SLUG='epa-superfund-enterprise-management-system-sems-sites-1'
LANDING='https://catalog.data.gov/dataset/epa-facility-registry-service-frs-sems'
SERVICE='https://services.arcgis.com/cJ9YHowT8TU7DUyn/arcgis/rest/services/FRS_INTERESTS_SEMS/FeatureServer'
BASE=SERVICE+'/0'
ITEM='7dd2b44b4cfc4f75b034a7bbe86c0a60'
LICENSE='https://www.epa.gov/web-policies-and-procedures/epa-disclaimers'
READY='metadata/epa_sems_candidate_ready.json'
SPACE_FLOOR=300_000_000
MAX_AUX_BYTES=3_000_000
MAX_BODY_BYTES=4_000_000
INITIAL_COUNT=14242
PAGE_SIZE=200
NATIVE_CRS_JSON={'$schema': 'https://proj.org/schemas/v0.7/projjson.schema.json', 'type': 'GeographicCRS', 'name': 'NAD83', 'datum': {'type': 'GeodeticReferenceFrame', 'name': 'North American Datum 1983', 'ellipsoid': {'name': 'GRS 1980', 'semi_major_axis': 6378137, 'inverse_flattening': 298.257222101}}, 'coordinate_system': {'subtype': 'ellipsoidal', 'axis': [{'name': 'Geodetic latitude', 'abbreviation': 'Lat', 'direction': 'north', 'unit': 'degree'}, {'name': 'Geodetic longitude', 'abbreviation': 'Lon', 'direction': 'east', 'unit': 'degree'}]}, 'id': {'authority': 'EPSG', 'code': 4269}}
FIELD_DEFINITIONS=[{'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'Object ID', 'sqlType': 'sqlTypeOther', 'nullable': False, 'editable': False, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Unique identifier for each feature in this ArcGIS Online hosted feature service, practical for optimizing queries, not permanent, may change with each refresh.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'REGISTRY_ID', 'type': 'esriFieldTypeString', 'alias': 'Registry ID', 'sqlType': 'sqlTypeOther', 'length': 12, 'nullable': False, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The identification number assigned by the EPA Facility Registry System to uniquely identify a facility site.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'PRIMARY_NAME', 'type': 'esriFieldTypeString', 'alias': 'Facility Site Name', 'sqlType': 'sqlTypeOther', 'length': 150, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The public or commercial name of a facility site (i.e., the full name that commonly appears on invoices, signs, or other business documents, or as assigned by the state when the name is ambiguous).","fieldValueType":"nameOrTitle"}'}, {'name': 'LOCATION_ADDRESS', 'type': 'esriFieldTypeString', 'alias': 'Location Address', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The address that describes the physical (geographic) location of the front door or main entrance of a facility site, including urban-style street address or rural address.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'CITY_NAME', 'type': 'esriFieldTypeString', 'alias': 'City Name', 'sqlType': 'sqlTypeOther', 'length': 60, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The name of the city, town, village or other locality, when identifiable, within whose boundaries (the majority of) the facility site is located. This is not always the same as the city used for USPS mail delivery.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'COUNTY_NAME', 'type': 'esriFieldTypeString', 'alias': 'County Name', 'sqlType': 'sqlTypeOther', 'length': 35, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The name of the U.S. county or county equivalent in which the facility site is physically located.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'FIPS_CODE', 'type': 'esriFieldTypeString', 'alias': 'County and State FIPS Code ', 'sqlType': 'sqlTypeOther', 'length': 5, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The Federal Information Processing Standard (FIPS) code identifying the county or county equivalent and the state or state equivalent of the United States where the facility is located.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'STATE_CODE', 'type': 'esriFieldTypeString', 'alias': 'State USPS Code', 'sqlType': 'sqlTypeOther', 'length': 5, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The U.S. Postal Service abbreviation that represents the state or state equivalent for the U.S. and Canada.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'POSTAL_CODE', 'type': 'esriFieldTypeString', 'alias': 'Location Zip Code', 'sqlType': 'sqlTypeOther', 'length': 14, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The combination of the five digit Zone Improvement Plan (ZIP) code and the four digit extension code (if available) that represents the geographic segment that is a subunit of the ZIP Code, assigned by the U.S. Postal Service to a geographic location; or the postal zone specific to the country, other than the U.S., where the facility site is located.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'LATITUDE83', 'type': 'esriFieldTypeDouble', 'alias': 'Latitude Measure (NAD83)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The measure of the angular distance on a meridian north or south of the equator standardized to NAD83 horizontal datum. ","fieldValueType":"coordinate"}'}, {'name': 'LONGITUDE83', 'type': 'esriFieldTypeDouble', 'alias': 'Longitude Measure (NAD83)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The measure of the angular distance on a meridian east or west of the prime meridian standardized to NAD83 horizontal datum. ","fieldValueType":"coordinate"}'}, {'name': 'HUC8_CODE', 'type': 'esriFieldTypeString', 'alias': 'Hydrologic Unit Code (HUC) 8-digit ', 'sqlType': 'sqlTypeOther', 'length': 8, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The 8-digit hydrologic unit code (HUC) that represents a geographic area representing part or all of a surface drainage basin, a combination of drainage basins, or a distinct hydrologic feature.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'ACCURACY_VALUE', 'type': 'esriFieldTypeInteger', 'alias': 'Location Accuracy', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Estimated ccuracy of the geospatial location in meters; smaller values indicate better location accuracy.","fieldValueType":"measurement"}'}, {'name': 'COLLECT_MTH_DESC', 'type': 'esriFieldTypeString', 'alias': 'Location Collection Method Description', 'sqlType': 'sqlTypeOther', 'length': 250, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The text that describes the method used to determine the latitude and longitude coordinates for a point on the earth. ","fieldValueType":"description"}'}, {'name': 'REF_POINT_DESC', 'type': 'esriFieldTypeString', 'alias': 'Reference Point Description', 'sqlType': 'sqlTypeOther', 'length': 250, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Description of the reference point used for the facility\'s geographic location.","fieldValueType":"description"}'}, {'name': 'CREATE_DATE', 'type': 'esriFieldTypeDate', 'alias': 'Record Creation Date', 'sqlType': 'sqlTypeOther', 'length': 8, 'nullable': False, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"A system-generated value that represents the calendar date and time the corresponding information was first posted to the database.","fieldValueType":"dateAndTime"}', 'precision': 1}, {'name': 'UPDATE_DATE', 'type': 'esriFieldTypeDate', 'alias': 'Record Update Date', 'sqlType': 'sqlTypeOther', 'length': 8, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"A system-generated value that represents the most recent calendar date and time the corresponding information was updated in the database.","fieldValueType":"dateAndTime"}', 'precision': 1}, {'name': 'LAST_REPORTED_DATE', 'type': 'esriFieldTypeDate', 'alias': 'Last Reported Date', 'sqlType': 'sqlTypeOther', 'length': 8, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The most recent date the corresponding affiliation data was reported to the Source of Data","fieldValueType":"dateAndTime"}', 'precision': 1}, {'name': 'FAC_URL', 'type': 'esriFieldTypeString', 'alias': 'Facility URL', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Web link to the EPA Facility Registry Service page for detailed facility information.","fieldValueType":"description"}'}, {'name': 'PGM_SYS_ID', 'type': 'esriFieldTypeString', 'alias': 'Program System ID', 'sqlType': 'sqlTypeOther', 'length': 30, 'nullable': False, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The identification number, such as the permit number, assigned by an information management system that represents a facility site, waste site, operable unit, or other feature tracked by that Environmental Information System.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'PGM_SYS_ACRNM', 'type': 'esriFieldTypeString', 'alias': 'Program System Acronym', 'sqlType': 'sqlTypeOther', 'length': 20, 'nullable': False, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The acronym that represents the name of an information management system for an of the EPA program system associated with the facility.","fieldValueType":"typeOrCategory"}'}, {'name': 'INTEREST_TYPE', 'type': 'esriFieldTypeString', 'alias': 'Environmental Interest Type', 'sqlType': 'sqlTypeOther', 'length': 60, 'nullable': False, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The environmental permit or regulatory program that applies to the facility site (e.g., TRI Reporter, NPDES Major, Air Stationary Source Major, Hazardous Waste TSD, Hazardous Waste LQG, Superfund NPL).","fieldValueType":"typeOrCategory"}'}, {'name': 'PROGRAM_URL', 'type': 'esriFieldTypeString', 'alias': 'Program URL', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Web link to the general-purpose home page of the EPA environmental program related to the facility.","fieldValueType":"description"}'}, {'name': 'PGM_REPORT_URL', 'type': 'esriFieldTypeString', 'alias': 'Program Report URL', 'sqlType': 'sqlTypeOther', 'length': 11, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Direct web link to a program-level report about this facility, if available.","fieldValueType":"description"}'}, {'name': 'PUBLIC_IND', 'type': 'esriFieldTypeString', 'alias': 'Public Indicator', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Indicates whether or not the associated data is accessible by the public on the Internet (Yes or No).","fieldValueType":"binary"}'}, {'name': 'ACTIVE_STATUS', 'type': 'esriFieldTypeString', 'alias': 'Active Status', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The status of the environmental interest at the facility or site (e.g., Active, Inactive, Operating, Proposed for NPL, Deleted from Final NPL, Currently on Final NPL).","fieldValueType":"typeOrCategory"}'}, {'name': 'FEDERAL_AGENCY_NAME', 'type': 'esriFieldTypeString', 'alias': 'Federal Agency Name', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Name of the federal agency associated with the facility, if applicable.","fieldValueType":"nameOrTitle"}'}, {'name': 'HUC_12', 'type': 'esriFieldTypeString', 'alias': 'Hydrologic Unit Code (HUC) 12-digit', 'sqlType': 'sqlTypeOther', 'length': 12, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"The 12-digit hydrologic unit code (HUC) that represents a geographic area representing part or all of a surface drainage basin, a combination of drainage basins, or a distinct hydrologic feature.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'FEDERAL_LAND_IND', 'type': 'esriFieldTypeString', 'alias': 'Federal Land Indicator', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Indicates if the facility is located on federal land (Yes or No).","fieldValueType":"binary"}'}, {'name': 'FED_FACILITY_CODE', 'type': 'esriFieldTypeString', 'alias': 'Federal Facility Code', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Code indicating whether or not the facility is the property of the Federal Government. Allowable Values : Y, for Yes, the facility is the property of the Federal Government. N, for No, the facility is not the property of the Federal Government.","fieldValueType":"binary"}'}, {'name': 'EPA_REGION_CODE', 'type': 'esriFieldTypeString', 'alias': 'EPA Region Code', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"EPA region code representing the regional office responsible for the facility.","fieldValueType":"typeOrCategory"}'}, {'name': 'KEY_FIELD', 'type': 'esriFieldTypeString', 'alias': 'Key Field', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'isViewOverride': True, 'domain': None, 'defaultValue': None, 'description': '{"value":"Concatenated key field combining program acronym and system ID, uniquely identifying a record.","fieldValueType":"uniqueIdentifier"}'}]
PAGE_FIELDS=[{'name': 'OBJECTID', 'type': 'esriFieldTypeOID', 'alias': 'Object ID', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None, 'description': '{"value":"Unique identifier for each feature in this ArcGIS Online hosted feature service, practical for optimizing queries, not permanent, may change with each refresh.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'REGISTRY_ID', 'type': 'esriFieldTypeString', 'alias': 'Registry ID', 'sqlType': 'sqlTypeOther', 'length': 12, 'domain': None, 'defaultValue': None, 'description': '{"value":"The identification number assigned by the EPA Facility Registry System to uniquely identify a facility site.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'PRIMARY_NAME', 'type': 'esriFieldTypeString', 'alias': 'Facility Site Name', 'sqlType': 'sqlTypeOther', 'length': 150, 'domain': None, 'defaultValue': None, 'description': '{"value":"The public or commercial name of a facility site (i.e., the full name that commonly appears on invoices, signs, or other business documents, or as assigned by the state when the name is ambiguous).","fieldValueType":"nameOrTitle"}'}, {'name': 'LOCATION_ADDRESS', 'type': 'esriFieldTypeString', 'alias': 'Location Address', 'sqlType': 'sqlTypeOther', 'length': 100, 'domain': None, 'defaultValue': None, 'description': '{"value":"The address that describes the physical (geographic) location of the front door or main entrance of a facility site, including urban-style street address or rural address.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'CITY_NAME', 'type': 'esriFieldTypeString', 'alias': 'City Name', 'sqlType': 'sqlTypeOther', 'length': 60, 'domain': None, 'defaultValue': None, 'description': '{"value":"The name of the city, town, village or other locality, when identifiable, within whose boundaries (the majority of) the facility site is located. This is not always the same as the city used for USPS mail delivery.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'COUNTY_NAME', 'type': 'esriFieldTypeString', 'alias': 'County Name', 'sqlType': 'sqlTypeOther', 'length': 35, 'domain': None, 'defaultValue': None, 'description': '{"value":"The name of the U.S. county or county equivalent in which the facility site is physically located.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'FIPS_CODE', 'type': 'esriFieldTypeString', 'alias': 'County and State FIPS Code ', 'sqlType': 'sqlTypeOther', 'length': 5, 'domain': None, 'defaultValue': None, 'description': '{"value":"The Federal Information Processing Standard (FIPS) code identifying the county or county equivalent and the state or state equivalent of the United States where the facility is located.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'STATE_CODE', 'type': 'esriFieldTypeString', 'alias': 'State USPS Code', 'sqlType': 'sqlTypeOther', 'length': 5, 'domain': None, 'defaultValue': None, 'description': '{"value":"The U.S. Postal Service abbreviation that represents the state or state equivalent for the U.S. and Canada.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'POSTAL_CODE', 'type': 'esriFieldTypeString', 'alias': 'Location Zip Code', 'sqlType': 'sqlTypeOther', 'length': 14, 'domain': None, 'defaultValue': None, 'description': '{"value":"The combination of the five digit Zone Improvement Plan (ZIP) code and the four digit extension code (if available) that represents the geographic segment that is a subunit of the ZIP Code, assigned by the U.S. Postal Service to a geographic location; or the postal zone specific to the country, other than the U.S., where the facility site is located.","fieldValueType":"locationOrPlaceName"}'}, {'name': 'LATITUDE83', 'type': 'esriFieldTypeDouble', 'alias': 'Latitude Measure (NAD83)', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None, 'description': '{"value":"The measure of the angular distance on a meridian north or south of the equator standardized to NAD83 horizontal datum. ","fieldValueType":"coordinate"}'}, {'name': 'LONGITUDE83', 'type': 'esriFieldTypeDouble', 'alias': 'Longitude Measure (NAD83)', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None, 'description': '{"value":"The measure of the angular distance on a meridian east or west of the prime meridian standardized to NAD83 horizontal datum. ","fieldValueType":"coordinate"}'}, {'name': 'HUC8_CODE', 'type': 'esriFieldTypeString', 'alias': 'Hydrologic Unit Code (HUC) 8-digit ', 'sqlType': 'sqlTypeOther', 'length': 8, 'domain': None, 'defaultValue': None, 'description': '{"value":"The 8-digit hydrologic unit code (HUC) that represents a geographic area representing part or all of a surface drainage basin, a combination of drainage basins, or a distinct hydrologic feature.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'ACCURACY_VALUE', 'type': 'esriFieldTypeInteger', 'alias': 'Location Accuracy', 'sqlType': 'sqlTypeOther', 'domain': None, 'defaultValue': None, 'description': '{"value":"Estimated ccuracy of the geospatial location in meters; smaller values indicate better location accuracy.","fieldValueType":"measurement"}'}, {'name': 'COLLECT_MTH_DESC', 'type': 'esriFieldTypeString', 'alias': 'Location Collection Method Description', 'sqlType': 'sqlTypeOther', 'length': 250, 'domain': None, 'defaultValue': None, 'description': '{"value":"The text that describes the method used to determine the latitude and longitude coordinates for a point on the earth. ","fieldValueType":"description"}'}, {'name': 'REF_POINT_DESC', 'type': 'esriFieldTypeString', 'alias': 'Reference Point Description', 'sqlType': 'sqlTypeOther', 'length': 250, 'domain': None, 'defaultValue': None, 'description': '{"value":"Description of the reference point used for the facility\'s geographic location.","fieldValueType":"description"}'}, {'name': 'CREATE_DATE', 'type': 'esriFieldTypeDate', 'alias': 'Record Creation Date', 'sqlType': 'sqlTypeOther', 'length': 8, 'domain': None, 'defaultValue': None, 'description': '{"value":"A system-generated value that represents the calendar date and time the corresponding information was first posted to the database.","fieldValueType":"dateAndTime"}'}, {'name': 'UPDATE_DATE', 'type': 'esriFieldTypeDate', 'alias': 'Record Update Date', 'sqlType': 'sqlTypeOther', 'length': 8, 'domain': None, 'defaultValue': None, 'description': '{"value":"A system-generated value that represents the most recent calendar date and time the corresponding information was updated in the database.","fieldValueType":"dateAndTime"}'}, {'name': 'LAST_REPORTED_DATE', 'type': 'esriFieldTypeDate', 'alias': 'Last Reported Date', 'sqlType': 'sqlTypeOther', 'length': 8, 'domain': None, 'defaultValue': None, 'description': '{"value":"The most recent date the corresponding affiliation data was reported to the Source of Data","fieldValueType":"dateAndTime"}'}, {'name': 'FAC_URL', 'type': 'esriFieldTypeString', 'alias': 'Facility URL', 'sqlType': 'sqlTypeOther', 'length': 100, 'domain': None, 'defaultValue': None, 'description': '{"value":"Web link to the EPA Facility Registry Service page for detailed facility information.","fieldValueType":"description"}'}, {'name': 'PGM_SYS_ID', 'type': 'esriFieldTypeString', 'alias': 'Program System ID', 'sqlType': 'sqlTypeOther', 'length': 30, 'domain': None, 'defaultValue': None, 'description': '{"value":"The identification number, such as the permit number, assigned by an information management system that represents a facility site, waste site, operable unit, or other feature tracked by that Environmental Information System.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'PGM_SYS_ACRNM', 'type': 'esriFieldTypeString', 'alias': 'Program System Acronym', 'sqlType': 'sqlTypeOther', 'length': 20, 'domain': None, 'defaultValue': None, 'description': '{"value":"The acronym that represents the name of an information management system for an of the EPA program system associated with the facility.","fieldValueType":"typeOrCategory"}'}, {'name': 'INTEREST_TYPE', 'type': 'esriFieldTypeString', 'alias': 'Environmental Interest Type', 'sqlType': 'sqlTypeOther', 'length': 60, 'domain': None, 'defaultValue': None, 'description': '{"value":"The environmental permit or regulatory program that applies to the facility site (e.g., TRI Reporter, NPDES Major, Air Stationary Source Major, Hazardous Waste TSD, Hazardous Waste LQG, Superfund NPL).","fieldValueType":"typeOrCategory"}'}, {'name': 'PROGRAM_URL', 'type': 'esriFieldTypeString', 'alias': 'Program URL', 'sqlType': 'sqlTypeOther', 'length': 100, 'domain': None, 'defaultValue': None, 'description': '{"value":"Web link to the general-purpose home page of the EPA environmental program related to the facility.","fieldValueType":"description"}'}, {'name': 'PGM_REPORT_URL', 'type': 'esriFieldTypeString', 'alias': 'Program Report URL', 'sqlType': 'sqlTypeOther', 'length': 11, 'domain': None, 'defaultValue': None, 'description': '{"value":"Direct web link to a program-level report about this facility, if available.","fieldValueType":"description"}'}, {'name': 'PUBLIC_IND', 'type': 'esriFieldTypeString', 'alias': 'Public Indicator', 'sqlType': 'sqlTypeOther', 'length': 1, 'domain': None, 'defaultValue': None, 'description': '{"value":"Indicates whether or not the associated data is accessible by the public on the Internet (Yes or No).","fieldValueType":"binary"}'}, {'name': 'ACTIVE_STATUS', 'type': 'esriFieldTypeString', 'alias': 'Active Status', 'sqlType': 'sqlTypeOther', 'length': 50, 'domain': None, 'defaultValue': None, 'description': '{"value":"The status of the environmental interest at the facility or site (e.g., Active, Inactive, Operating, Proposed for NPL, Deleted from Final NPL, Currently on Final NPL).","fieldValueType":"typeOrCategory"}'}, {'name': 'FEDERAL_AGENCY_NAME', 'type': 'esriFieldTypeString', 'alias': 'Federal Agency Name', 'sqlType': 'sqlTypeOther', 'length': 100, 'domain': None, 'defaultValue': None, 'description': '{"value":"Name of the federal agency associated with the facility, if applicable.","fieldValueType":"nameOrTitle"}'}, {'name': 'HUC_12', 'type': 'esriFieldTypeString', 'alias': 'Hydrologic Unit Code (HUC) 12-digit', 'sqlType': 'sqlTypeOther', 'length': 12, 'domain': None, 'defaultValue': None, 'description': '{"value":"The 12-digit hydrologic unit code (HUC) that represents a geographic area representing part or all of a surface drainage basin, a combination of drainage basins, or a distinct hydrologic feature.","fieldValueType":"uniqueIdentifier"}'}, {'name': 'FEDERAL_LAND_IND', 'type': 'esriFieldTypeString', 'alias': 'Federal Land Indicator', 'sqlType': 'sqlTypeOther', 'length': 1, 'domain': None, 'defaultValue': None, 'description': '{"value":"Indicates if the facility is located on federal land (Yes or No).","fieldValueType":"binary"}'}, {'name': 'FED_FACILITY_CODE', 'type': 'esriFieldTypeString', 'alias': 'Federal Facility Code', 'sqlType': 'sqlTypeOther', 'length': 1, 'domain': None, 'defaultValue': None, 'description': '{"value":"Code indicating whether or not the facility is the property of the Federal Government. Allowable Values : Y, for Yes, the facility is the property of the Federal Government. N, for No, the facility is not the property of the Federal Government.","fieldValueType":"binary"}'}, {'name': 'EPA_REGION_CODE', 'type': 'esriFieldTypeString', 'alias': 'EPA Region Code', 'sqlType': 'sqlTypeOther', 'length': 2, 'domain': None, 'defaultValue': None, 'description': '{"value":"EPA region code representing the regional office responsible for the facility.","fieldValueType":"typeOrCategory"}'}, {'name': 'KEY_FIELD', 'type': 'esriFieldTypeString', 'alias': 'Key Field', 'sqlType': 'sqlTypeOther', 'length': 50, 'domain': None, 'defaultValue': None, 'description': '{"value":"Concatenated key field combining program acronym and system ID, uniquely identifying a record.","fieldValueType":"uniqueIdentifier"}'}]
URLS={'catalog':LANDING,'epa_frs':'https://www.epa.gov/frs/geospatial-data-download-service','epa_item':'https://www.arcgis.com/sharing/rest/content/items/'+ITEM+'?f=pjson','epa_service':SERVICE+'?f=pjson','epa_layer':BASE+'?f=pjson','count_before':BASE+'/query?where=1%3D1&returnCountOnly=true&f=json','ids_before':BASE+'/query?where=1%3D1&returnIdsOnly=true&f=json'}


class SEMSQuarantineError(ValueError):pass
class IncidentWriteError(SEMSQuarantineError):pass

def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body):
    return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise SEMSQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise SEMSQuarantineError('metadata_must_be_object_without_error')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource)or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise SEMSQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..'in root.parts or any(p.is_symlink()for p in [root,*root.parents]):raise SEMSQuarantineError('staging_path_symlink_refused')
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<SPACE_FLOOR:raise SEMSQuarantineError('disk_floor_before_acquisition')
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise SEMSQuarantineError('staging_alias_or_special_file_refused')

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise SEMSQuarantineError('immutable_capture_changed:'+key)

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
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise SEMSQuarantineError('unsafe_or_oversized_original_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise SEMSQuarantineError('changed_or_incomplete_original_input')
    return body

def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise SEMSQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names

def construction_record_key(version):return f'_construction-state/{SLUG}/{version}.json'

def incident_directory(version):return f'_construction-state/{SLUG}/incident-{version}'

def state_present(storage,version):
    key=construction_record_key(version);root=Path(storage.local_dir).absolute()
    return any((root/value).exists()for value in(key,key+'.first-construction.json',key+'.evidence'))

def refuse_observed_loss(storage,version):
    if(Path(storage.local_dir).absolute()/incident_directory(version)).exists():raise SEMSQuarantineError('permanent_observed_original_trust_loss_requires_explicit_reviewed_restore')

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
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise SEMSQuarantineError('private_write_alias_or_special_file_refused')
        os.ftruncate(descriptor,0)
        if os.write(descriptor,body)!=len(body):raise SEMSQuarantineError('incomplete_private_write:'+name)
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if candidate_bytes(root,name)!=body:raise SEMSQuarantineError('incomplete_private_write:'+name)

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

def parquet_table(table):
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':NATIVE_CRS_JSON}}}
    return table.replace_schema_metadata({b'geo':encode(geo)})

def validate_output(root,table):
    path=root/'geoparquet'/f'{SLUG}.parquet';actual=pq.read_table(path)
    if not actual.equals(parquet_table(table),check_metadata=True):raise SEMSQuarantineError('full_typed_native_output_drift')
    for w in actual['geometry'].to_pylist():
        p=shapely.from_wkb(w)
        if p.geom_type!='Point'or not p.is_valid or p.is_empty:raise SEMSQuarantineError('invalid_native_output_geometry')

def candidate_bytes(root,name):
    if Path(name).is_absolute()or'..'in Path(name).parts:raise SEMSQuarantineError('unsafe_candidate_key')
    return bounded_input(root/name,MAX_BODY_BYTES if name.endswith('.body')else(8_000_000 if name.endswith('.parquet')else MAX_AUX_BYTES))

def source_payloads(captures,identity,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/epa_sems_source_audit.json':encode(audit)}
    for n,(b,r)in captures.items():values['raw/'+n+'.body']=b;values['raw/'+n+'.receipt.json']=encode(r)
    return values

def original_context(storage,version,fingerprint,captures,identity,manifest,*,create=False):
    key=construction_record_key(version)
    if create:
        inspect_captures(captures);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,{'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(b)}/{n}.body','receipt':r}for n,(b,r)in captures.items()}}))
    else:body=load_first_construction(storage,key,version,fingerprint)
    payload=strict_json(body)['payload'];original={}
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(captures):raise SEMSQuarantineError('invalid_original_context')
    for n,item in payload['captures'].items():
        key=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{n}.body"
        if item['body_key']!=key:raise SEMSQuarantineError('invalid_original_raw_key')
        original[n]=(bounded_input(Path(storage.local_dir)/key,MAX_BODY_BYTES),item['receipt'])
    a,_=inspect_captures(original);b,_=inspect_captures(captures)
    if not a.equals(b,check_metadata=True):raise SEMSQuarantineError('original_source_population_drift')
    old=payload['manifest'];obj=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:obj[k]for k in('bytes','sha256')})
    if encode({k:v for k,v in old.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise SEMSQuarantineError('original_manifest_context_drift')
    validate_release_provenance(old);validate_output_policy(old);validate_construction_code_identity(Path(__file__).parents[3],old['code_commit'],identity['files']);return original,old

def validate_candidate(root,table,identity,captures,audit,manifest,version,*,cached=False,storage=None):
    roster=candidate_roster(root);expected=source_payloads(captures,identity,audit,manifest);names=set(expected)|{f'geoparquet/{SLUG}.parquet'}
    ready={'approved':False,'version':version,'processing_fingerprint':digest(encode(identity)),'capture_fingerprint':source_fingerprint(captures),'objects':{n:sha256_file(root/n)for n in names}}
    if not(names|{READY}).issubset(roster):raise SEMSQuarantineError('missing_candidate_object')
    if not cached and roster!=names|{READY}:raise SEMSQuarantineError('private_candidate_roster_drift')
    if cached:
        allowed={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json','metadata/source/source_manifest.json','metadata/source/data_dictionary.json','metadata/source/quality_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'}
        if roster-(names|{READY}|allowed):raise SEMSQuarantineError('unknown_cached_object')
        for name in roster-(names|{READY}):strict_json(candidate_bytes(root,name))
        for name in('metadata/source/source_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'):
            if name in roster and candidate_bytes(root,name)!=encode(manifest):raise SEMSQuarantineError('cached_source_snapshot_drift')
        if 'metadata/resolved_provenance.json'in roster:
            if storage is None:raise SEMSQuarantineError('resolved_provenance_requires_explicit_storage_context')
            projected=provenance_projection(load_resolved_source_manifest(storage,SLUG,SLUG,version).metadata)
            if encode(strict_json(candidate_bytes(root,'metadata/resolved_provenance.json')))!=encode(projected):raise SEMSQuarantineError('cached_resolved_provenance_drift')
    for n,b in expected.items():
        if candidate_bytes(root,n)!=b:raise SEMSQuarantineError('candidate_source_bytes_drift:'+n)
    if candidate_bytes(root,READY)!=encode(ready):raise SEMSQuarantineError('candidate_ready_drift')
    validate_output(root,table)

def stage_epa_sems(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir);table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));version='v0.0.0-draft-epa-sems-'+source_fingerprint(captures)[:16]+'-'+fingerprint[:16]
    base=Path(storage.local_dir).absolute();prefix=f'{SLUG}/{SLUG}/{version}';root=base/prefix;locks=base/'_locks';locks.mkdir(exist_ok=True)
    descriptor=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b')as lock:
        if os.fstat(lock.fileno()).st_nlink!=1:raise SEMSQuarantineError('linked_lock')
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
                    raise SEMSQuarantineError('observed_original_trust_loss')from exc
                if root.exists():
                    try:validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    except Exception as exc:
                        q=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q);raise SEMSQuarantineError('invalid_cache_moved_intact:'+str(q))from exc
                    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            for n,b in source_payloads(captures,identity,audit,manifest).items():write_private(temp,n,b)
            ready={'approved':False,'version':version,'processing_fingerprint':fingerprint,'capture_fingerprint':source_fingerprint(captures),'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));validate_candidate(temp,table,identity,captures,audit,manifest,version)
            captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest,create=True)
            validate_candidate(temp,table,identity,captures,audit,manifest,version);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}

def id_roster(body,expected_count):
    data=strict_json(body);ids=data.get('objectIds')
    if data.get('objectIdFieldName')!='OBJECTID'or type(ids)is not list or len(ids)!=expected_count or any(type(x)is not int for x in ids)or len(set(ids))!=expected_count:raise SEMSQuarantineError('invalid_complete_OID_witness')
    return sorted(ids)

def decode_pages(pages,expected_ids):
    fields=[x['name']for x in FIELD_DEFINITIONS];names=fields+['geometry'];rows=[];seen=set()
    type_map={'esriFieldTypeSmallInteger':pa.int16(),'esriFieldTypeInteger':pa.int32(),'esriFieldTypeOID':pa.int64(),'esriFieldTypeDate':pa.int64(),'esriFieldTypeDouble':pa.float64(),'esriFieldTypeString':pa.string()}
    for page in pages:
        if(page.get('objectIdFieldName')!='OBJECTID'or page.get('geometryType')!='esriGeometryPoint'or encode(page.get('spatialReference'))!=encode({'wkid':4269,'latestWkid':4269})
            or encode(page.get('fields'))!=encode(PAGE_FIELDS)or page.get('exceededTransferLimit')not in(None,False)
            or('exceededTransferLimit'in page and type(page['exceededTransferLimit'])is not bool)):
            raise SEMSQuarantineError('page_schema_CRS_or_transfer_limit_drift')
        if type(page.get('features'))is not list:raise SEMSQuarantineError('missing_feature_population')
        for feature in page['features']:
            attrs=feature.get('attributes');geom=feature.get('geometry');values=[]
            if type(attrs)is not dict or list(attrs)!=fields:raise SEMSQuarantineError('missing_extra_or_reordered_source_fields')
            oid=attrs['OBJECTID']
            if type(oid)is not int or oid in seen:raise SEMSQuarantineError('invalid_or_duplicate_original_OBJECTID')
            seen.add(oid)
            for decl in FIELD_DEFINITIONS:
                name=decl['name'];kind=decl['type'];v=attrs[name]
                if v is None:
                    if decl['nullable']is not True:raise SEMSQuarantineError('nonnull_source_field:'+name)
                elif kind=='esriFieldTypeString':
                    if type(v)is not str or len(v)>decl['length']:raise SEMSQuarantineError('source_string_width_type:'+name)
                elif kind in('esriFieldTypeSmallInteger','esriFieldTypeInteger','esriFieldTypeOID','esriFieldTypeDate'):
                    bound=2**(15 if kind=='esriFieldTypeSmallInteger'else 31 if kind=='esriFieldTypeInteger'else 63)
                    if type(v)is not int or not-bound<=v<bound:raise SEMSQuarantineError('source_integer_width:'+name)
                elif kind=='esriFieldTypeDouble':
                    if type(v)not in(int,float)or not math.isfinite(v)or float(v)!=v:raise SEMSQuarantineError('nonfinite_or_lossy_source_double:'+name)
                    v=float(v)
                else:raise SEMSQuarantineError('unknown_source_physical_type')
                values.append(v)
            if type(geom)is not dict or set(geom)!={'x','y'}or any(type(geom[k])not in(int,float)or not math.isfinite(geom[k])or float(geom[k])!=geom[k]for k in('x','y')):raise SEMSQuarantineError('invalid_or_extended_native_point')
            values.append(struct.pack('<BIdd',1,1,float(geom['x']),float(geom['y'])));rows.append(values)
    if sorted(seen)!=sorted(expected_ids):raise SEMSQuarantineError('incomplete_or_extra_source_ID_coverage')
    rows.sort(key=lambda v:v[fields.index('OBJECTID')]);schema=[pa.field(d['name'],type_map[d['type']],nullable=d['nullable'],metadata={b'original_esri_field':encode(d)})for d in FIELD_DEFINITIONS]+[pa.field('geometry',pa.binary(),nullable=True,metadata={b'original_geometry_type':b'esriGeometryPoint',b'original_crs':b'EPSG:4269'})]
    return pa.Table.from_arrays([pa.array([row[i]for row in rows],type=d.type)for i,d in enumerate(schema)],schema=pa.schema(schema))


def capture_urls(ids):
    result=dict(URLS)
    for offset in range(0,len(ids),PAGE_SIZE):
        result[f'page_{offset:05d}']=BASE+'/query?'+urlencode({'f':'json','objectIds':','.join(map(str,ids[offset:offset+PAGE_SIZE])),'outFields':'*','returnGeometry':'true','returnZ':'true','returnM':'true','orderByFields':'OBJECTID ASC'})
    result.update(layer_after=BASE+'?f=pjson',count_after=BASE+'/query?where=1%3D1&returnCountOnly=true&f=json',ids_after=BASE+'/query?where=1%3D1&returnIdsOnly=true&f=json')
    return result


def expected_url(name,url):
    if name in URLS:return URLS[name]
    if name=='layer_after':return BASE+'?f=pjson'
    if name=='count_after':return BASE+'/query?where=1%3D1&returnCountOnly=true&f=json'
    if name=='ids_after':return BASE+'/query?where=1%3D1&returnIdsOnly=true&f=json'
    if not name.startswith('page_'):raise SEMSQuarantineError('unknown_original_input')
    try:
        offset=int(name[5:]);query=parse_qs(urlparse(url).query,strict_parsing=True);ids=[int(x)for x in query['objectIds'][0].split(',')]
        if name!=f'page_{offset:05d}'or offset<0 or offset%PAGE_SIZE or not 0<len(ids)<=PAGE_SIZE or sorted(set(ids))!=ids:raise ValueError()
        expected=BASE+'/query?'+urlencode({'f':'json','objectIds':','.join(map(str,ids)),'outFields':'*','returnGeometry':'true','returnZ':'true','returnM':'true','orderByFields':'OBJECTID ASC'})
        if url!=expected:raise ValueError()
        return expected
    except (KeyError,TypeError,ValueError)as exc:raise SEMSQuarantineError('invalid_ID_targeted_page_URL')from exc


def validate_receipt(name,body,receipt):
    url=expected_url(name,receipt.get('url'))
    if(receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)or receipt.get('sha256')!=digest(body)or receipt.get('complete_body')is not True):raise SEMSQuarantineError('invalid_original_receipt:'+name)
    if not isinstance(receipt.get('observed_at'),str)or datetime.fromisoformat(receipt['observed_at']).tzinfo is None:raise SEMSQuarantineError('original_retrieval_timezone_required')
    return {'input_id':name+':'+digest(body),'producer':'United States Environmental Protection Agency','host':httpx.URL(url).host,
        'role':'official_distributor'if name.startswith(('epa_item','epa_service','epa_layer','page_','count_','ids_','layer_'))else'producer_publication',
        'distributor':'EPA_GEO-owned EPA ArcGIS organization cJ9YHowT8TU7DUyn, linked by official federal catalog',
        'delegation_evidence':{'producer_page_url':LANDING,'portal_item_url':URLS['epa_item'],'portal_owner':'EPA_GEO','hosted_service_url':SERVICE,'federal_catalog_url':LANDING},
        'canonical_url':url,'retrieval_url':url,'observed_final_url':url,'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'license':LICENSE,
        'attribution':'US EPA Facility Registry Service, Superfund Enterprise Management System interests; complete original item descriptions, attribution and rights retained.',
        'release_date_unknown_reason':'No verified dated data release. FRS/SEMS record Dates, service editingInfo, portal modification and retrieval times are separate values, never invented release dates.'}


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempt=f'_acquisition-attempts/{SLUG}/{uuid4().hex}';root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or any(p.is_symlink()for p in[root,*root.parents])):raise SEMSQuarantineError('unsafe_capture_root')
    plan=list(URLS.items())
    try:
        for name,url in plan:
            if root is not None:
                body=bounded_input(root/(name+'.body'),MAX_BODY_BYTES)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body)
                # Retain the actually read body before opening possibly unreadable metadata.
                immutable(storage,f'{attempt}/{name}.body-reference.json',encode({'bytes':len(body),'sha256':digest(body),'body_key':f'_raw-captures/{SLUG}/{digest(body)}/{name}.body'}))
                raw=bounded_input(root/(name+'.receipt.json'),65536)
            else:
                path=Path(storage.local_dir)/attempt/name;path.mkdir(parents=True)
                with(path/'wire.body').open('xb')as wire,(path/'decoded.body').open('xb')as decoded:
                    def retain(stream,block,target):
                        before=os.fstat(stream.fileno());written=stream.write(block);stream.flush();os.fsync(stream.fileno())
                        if written!=len(block):raise OSError('short_capture_write')
                        reader=os.open(target,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                        try:
                            actual=os.fstat(reader)
                            if(not stat.S_ISREG(actual.st_mode)or actual.st_nlink!=1 or(actual.st_dev,actual.st_ino)!=(before.st_dev,before.st_ino)
                                or actual.st_size!=before.st_size+len(block)or os.pread(reader,len(block),before.st_size)!=block):raise OSError('capture_block_readback_mismatch')
                        finally:os.close(reader)
                    def reserve(progress):
                        if shutil.disk_usage(path).free<SPACE_FLOOR+progress.reserve_bytes:raise OSError('disk_floor_before_read')
                    receipt={'url':url,'observed_at':datetime.now(timezone.utc).isoformat()};result=None
                    try:
                        with httpx.Client(follow_redirects=False,timeout=60,headers={'Accept-Encoding':'identity'})as client,client.stream('GET',url)as response:
                            result=capture_http_response(response,max_wire_bytes=MAX_BODY_BYTES,max_decoded_bytes=MAX_BODY_BYTES,reserve=reserve,
                                retain_wire=lambda b:retain(wire,b,path/'wire.body'),retain_decoded=lambda b:retain(decoded,b,path/'decoded.body'))
                            receipt.update(final_url=str(response.url),status=response.status_code,complete_body=result.complete,bytes=result.decoded_bytes_retained,
                                sha256=result.decoded_sha256,wire_bytes=result.wire_bytes_received,error_code=result.error_code)
                    finally:
                        if result is not None:readback=preserve_transfer_result(path,result)
                        wire.flush();decoded.flush()
                receipt['complete_body']=result.complete and readback;receipt['independent_sink_readback_matches']=readback
                body=bounded_input(path/'decoded.body',MAX_BODY_BYTES+1024*1024)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body);raw=encode(receipt)
            immutable(storage,f'{attempt}/{name}.raw-receipt.body',raw)
            receipt=strict_json(raw);validate_receipt(name,body,receipt);captures[name]=(body,receipt)
            if name=='ids_before':
                ids=id_roster(body,INITIAL_COUNT);count=strict_json(captures['count_before'][0]).get('count')
                if type(count)is not int or count!=len(ids):raise SEMSQuarantineError('initial_count_or_ID_coverage_drift_requires_review')
                plan.extend((n,u)for n,u in capture_urls(ids).items()if n not in URLS)
        return captures
    except Exception as exc:
        immutable(storage,attempt+'/failure.json',encode({'approved':False,'reason_type':type(exc).__name__}))
        where=quarantine(storage,captures,str(exc));raise SEMSQuarantineError('source_acquisition_failed:'+str(exc)+';retained:'+where)from exc


def manifest_for(captures,identity,output_pin):
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'EPA Facility Registry Service SEMS interests — complete official draft, including non-NPL sites','publisher':'United States Environmental Protection Agency','agency':'Environmental Protection Agency','source_url':LANDING,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(n,*item)for n,item in captures.items()],
        'transformations':[{'operation':'complete_official_ID_targeted_feature_extraction','parameters':{
            'grain':'All FRS interests in the SEMS program view, including non-NPL sites. Not NPL-only; not a claim of every underlying SEMS master record. No filtering or deduplication.',
            'source_ids':'Producer says hosted OBJECTID is temporary across refreshes. Preserve it, REGISTRY_ID, PGM_SYS_ID, KEY_FIELD and every other original column separately. No historical ID continuity or join approved.',
            'types':'Original Integer int32/Double float64/string/nullability/width/full field declarations retained. Unspecified OID physical width uses proposed bounded int64 staging, not an inferred source width.',
            'dates':'Original source Date JSON epoch-millisecond integers retained losslessly as int64 with UTC/time-reference declarations in metadata. No ISO string conversion. This proposed staging encoding and historical string-schema migration remain unapproved.',
            'geometry':'Exact native NAD83 EPSG4269 Point WKB from original binary64 x/y, no projection, geocoding, tolerance, precision change, repair or rebuilding from LATITUDE83/LONGITUDE83 attributes.',
            'relationship':'Current complete14242 FRS SEMS interest records versus historical14761 candidate. Coverage, temporary OIDs, dates, geometry, types and any field mappings need independent human acceptance.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all32 original attributes/IDs/native coordinates and physical declarations; no generic conversion or schema mapping approved.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**output_pin}]}}},
        'unsupported_output_formats':{'geopackage':'Original Date/OID/string width and SQL mappings require approved contract; no implicit conversion.','shapefile_zip':'Long field names, Date precision, nulls and string widths cannot be silently coerced.','pmtiles':'Projection, quantization and feature ID mappings remain unapproved.'}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def inspect_captures(captures):
    ids=id_roster(captures['ids_before'][0],INITIAL_COUNT);urls=capture_urls(ids)
    if set(captures)!=set(urls):raise SEMSQuarantineError('capture_roster_drift')
    for n,(b,r)in captures.items():
        validate_receipt(n,b,r)
        if r['url']!=urls[n]:raise SEMSQuarantineError('capture_request_population_drift')
    catalog=captures['catalog'][0]
    if SERVICE.encode()not in catalog or ITEM.encode()not in catalog:raise SEMSQuarantineError('missing_federal_catalog_delegation')
    item=strict_json(captures['epa_item'][0]);service=strict_json(captures['epa_service'][0])
    if(item.get('owner')!='EPA_GEO'or item.get('orgId')!='cJ9YHowT8TU7DUyn'or item.get('id')!=ITEM or item.get('url')!=SERVICE
        or service.get('serviceItemId')!=ITEM or not any(x.get('id')==0 and type(x.get('id'))is int for x in service.get('layers',[]))):raise SEMSQuarantineError('official_item_service_delegation_drift')
    before=strict_json(captures['epa_layer'][0]);after=strict_json(captures['layer_after'][0])
    for layer in[before,after]:
        if(encode(layer.get('fields'))!=encode(FIELD_DEFINITIONS)or layer.get('geometryType')!='esriGeometryPoint'or layer.get('objectIdField')!='OBJECTID'
            or type(layer.get('id'))is not int or layer['id']!=0 or layer.get('hasZ')is not False or layer.get('hasM')is not False
            or encode(layer.get('extent',{}).get('spatialReference'))!=encode({'wkid':4269,'latestWkid':4269})
            or type(layer.get('maxRecordCount'))is not int or layer['maxRecordCount']<PAGE_SIZE
            or layer.get('definitionExpression')not in(None,'')or layer.get('name')!='FRS_INTERESTS_SEMS'
            or encode(layer.get('dateFieldsTimeReference'))!=encode({'timeZone':'UTC','timeZoneIANA':'Etc/UTC','respectsDaylightSaving':False})):raise SEMSQuarantineError('physical_source_schema_geometry_or_transfer_drift')
    if encode(before.get('editingInfo'))!=encode(after.get('editingInfo')):raise SEMSQuarantineError('source_changed_during_extraction')
    for n in['count_before','count_after']:
        count=strict_json(captures[n][0]).get('count')
        if type(count)is not int or count!=INITIAL_COUNT:raise SEMSQuarantineError('complete_count_drift_requires_review')
    if id_roster(captures['ids_after'][0],INITIAL_COUNT)!=ids:raise SEMSQuarantineError('source_ID_roster_changed_during_extraction')
    pages=[]
    for name,url in urls.items():
        if not name.startswith('page_'):continue
        expected=[int(x)for x in parse_qs(urlparse(url).query)['objectIds'][0].split(',')];page=strict_json(captures[name][0])
        if sorted(x['attributes']['OBJECTID']for x in page.get('features',[]))!=expected:raise SEMSQuarantineError('targeted_page_missing_extra_ID')
        pages.append(page)
    table=decode_pages(pages,ids);interests=Counter(table['INTEREST_TYPE'].to_pylist());statuses=Counter(zip(table['INTEREST_TYPE'].to_pylist(),table['ACTIVE_STATUS'].to_pylist()))
    if not interests.get('SUPERFUND (NON-NPL)')or not interests.get('SUPERFUND NPL'):raise SEMSQuarantineError('partial_or_unrecognized_SEMS_scope')
    audit={'count':table.num_rows,'original_OBJECTIDs':ids,'original_ID_sha256':digest(encode(ids)),'registry_ID_unique_count':len(set(table['REGISTRY_ID'].to_pylist())),
        'program_ID_unique_count':len(set(table['PGM_SYS_ID'].to_pylist())),'interest_types':sorted(interests.items()),'interest_statuses':sorted(statuses.items(),key=repr),
        'crs':'EPSG4269 NAD83','field_count':32,'date_fields':{'CREATE_DATE':'UTC epoch-ms int64','UPDATE_DATE':'UTC epoch-ms int64','LAST_REPORTED_DATE':'UTC epoch-ms int64'},
        'source_date_time_reference':before['dateFieldsTimeReference'],'editingInfo':before.get('editingInfo'),'temporary_OBJECTID_warning':'Producer explicitly states hosted OBJECTID can change with each refresh; continuity not approved.',
        'coverage_semantics':'Complete FRS SEMS interest view, including active/archived/assessment and NPL/non-NPL, not every SEMS master record.','release_date_established':False}
    return table,audit


def source_fingerprint(captures):
    # Original complete page bytes bind every value, declaration and native point.
    # New real source content gets a new draft namespace; later receipt timestamps
    # and unchanged-source refreshes cannot replace its original construction.
    return digest(encode({n:digest(b)for n,(b,r)in captures.items()if n.startswith('page_')}))
