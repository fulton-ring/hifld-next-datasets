"""Complete official NCEI observation/runup reconstruction; no release acceptance.

All original HTTP bodies/receipts survive in immutable lossless archives. Native
Int16/Float32/Int32/Float64 declarations control explicit physical decoding;
original JSON numeric literals remain separately preserved without modification.
"""
from __future__ import annotations
import ast
import collections
from contextlib import contextmanager,nullcontext
from datetime import datetime,timezone
import fcntl,gzip,hashlib,importlib.metadata,io,json,math,os,platform,re,sqlite3,stat,struct,subprocess,tempfile,zipfile
from pathlib import Path,PurePosixPath
from urllib.parse import urlencode,urlsplit
from uuid import uuid4
import fiona,httpx,pyarrow as pa,pyarrow.parquet as pq,pyogrio,pyogrio.raw,pyproj,shapely
from shapely.geometry import shape,mapping
from dagster_hifld.construction_provenance import validate_construction_code_identity
from dagster_hifld.release_approval import validate_release_provenance,verifier_implementation_sha256
from dagster_hifld.resources import StagingStorageResource,PublishedStorageResource
from dagster_hifld.validation import compare_full_rows,sha256_file

SLUG='historical-tsunami-observations'
URL='https://services2.arcgis.com/C8EMgrsFcRFL6LrL/arcgis/rest/services/tsunami_observations/FeatureServer/0'
MAX_BYTES=3_000_000
MAX_ARCHIVE_BYTES=32_000_000
MAX_TOTAL_BYTES=100_000_000
FEATURE_COUNT=34495
PAGE_SIZE=500
FIDELITY_FILES=('baseline_comparison.json','baseline_comparison.md','baseline_differences.jsonl','baseline_field_changes.jsonl')
ARCHIVE_NAMES=('source_http','authority_http')
ARCHIVE_NAME='source_http.zip'
NATIVE_FIELDS = [{'name': 'ID', 'type': 'esriFieldTypeInteger', 'actualType': 'int', 'alias': 'ID', 'sqlType': 'sqlTypeInteger', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ObjectId', 'type': 'esriFieldTypeOID', 'actualType': 'int', 'alias': 'ObjectId', 'sqlType': 'sqlTypeInteger', 'nullable': False, 'editable': False, 'domain': None, 'defaultValue': None}, {'name': 'TSEVENT_ID', 'type': 'esriFieldTypeInteger', 'alias': 'TSEVENT_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'YEAR', 'type': 'esriFieldTypeSmallInteger', 'alias': 'YEAR', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'MONTH', 'type': 'esriFieldTypeSmallInteger', 'alias': 'MONTH', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAY', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DATE_STRING', 'type': 'esriFieldTypeString', 'alias': 'Date', 'sqlType': 'sqlTypeOther', 'length': 13, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ARR_DAY', 'type': 'esriFieldTypeSmallInteger', 'alias': 'ARR_DAY', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ARR_HOUR', 'type': 'esriFieldTypeSmallInteger', 'alias': 'ARR_HOUR', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'ARR_MIN', 'type': 'esriFieldTypeSmallInteger', 'alias': 'ARR_MIN', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TRAV_HOURS', 'type': 'esriFieldTypeSmallInteger', 'alias': 'TRAV_HOURS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TRAV_MINS', 'type': 'esriFieldTypeSmallInteger', 'alias': 'TRAV_MINS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'PERIOD', 'type': 'esriFieldTypeSingle', 'alias': 'PERIOD', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'FIRST_MOTION', 'type': 'esriFieldTypeString', 'alias': 'FIRST_MOTION', 'sqlType': 'sqlTypeOther', 'length': 1, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LATITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LATITUDE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LONGITUDE', 'type': 'esriFieldTypeDouble', 'alias': 'LONGITUDE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'LOCATION_NAME', 'type': 'esriFieldTypeString', 'alias': 'Location Name', 'sqlType': 'sqlTypeOther', 'length': 400, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'AREA', 'type': 'esriFieldTypeString', 'alias': 'AREA', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'COUNTRY', 'type': 'esriFieldTypeString', 'alias': 'Country', 'sqlType': 'sqlTypeOther', 'length': 100, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'REGION_CODE', 'type': 'esriFieldTypeSmallInteger', 'alias': 'REGION_CODE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'RUNUP_HT', 'type': 'esriFieldTypeSingle', 'alias': 'Runup Height (m)', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'RUNUP_HT_RANK', 'type': 'esriFieldTypeSmallInteger', 'alias': 'RUNUP_HT_RANK', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'RUNUP_HORIZ', 'type': 'esriFieldTypeSingle', 'alias': 'RUNUP_HORIZ', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TYPE_MEASUREMENT_ID', 'type': 'esriFieldTypeSmallInteger', 'alias': 'TYPE_MEASUREMENT_ID', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TYPE_MEASUREMENT', 'type': 'esriFieldTypeString', 'alias': 'Type of Measurement', 'sqlType': 'sqlTypeOther', 'length': 50, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_MILLIONS_DOLLARS', 'type': 'esriFieldTypeDouble', 'alias': 'DAMAGE_MILLIONS_DOLLARS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DAMAGE_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DAMAGE_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DAMAGE_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS', 'type': 'esriFieldTypeInteger', 'alias': 'DEATHS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'DEATHS_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DEATHS_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'DEATHS_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES', 'type': 'esriFieldTypeInteger', 'alias': 'INJURIES', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'INJURIES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'INJURIES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'INJURIES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESTROYED', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DESTROYED', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DAMAGED', 'type': 'esriFieldTypeInteger', 'alias': 'HOUSES_DAMAGED', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DAMAGED_AMOUNT_ORDER', 'type': 'esriFieldTypeSmallInteger', 'alias': 'HOUSES_DAMAGED_AMOUNT_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'HOUSES_DAM_DESCRIPTION', 'type': 'esriFieldTypeString', 'alias': 'HOUSES_DAM_DESCRIPTION', 'sqlType': 'sqlTypeOther', 'length': 40, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'COMMENTS', 'type': 'esriFieldTypeString', 'alias': 'Comments', 'sqlType': 'sqlTypeOther', 'length': 4000, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DOUBTFUL', 'type': 'esriFieldTypeString', 'alias': 'DOUBTFUL', 'sqlType': 'sqlTypeOther', 'length': 2, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DIST_FROM_SOURCE', 'type': 'esriFieldTypeInteger', 'alias': 'DIST_FROM_SOURCE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'EVENT_REGION_CODE', 'type': 'esriFieldTypeSmallInteger', 'alias': 'EVENT_REGION_CODE', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'EVENT_REGION', 'type': 'esriFieldTypeString', 'alias': 'EVENT_REGION', 'sqlType': 'sqlTypeOther', 'length': 54, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'PRED_TRAV_HOURS', 'type': 'esriFieldTypeSingle', 'alias': 'PRED_TRAV_HOURS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'PRED_TRAV_MINS', 'type': 'esriFieldTypeSmallInteger', 'alias': 'PRED_TRAV_MINS', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'URL', 'type': 'esriFieldTypeString', 'alias': 'More information about runup', 'sqlType': 'sqlTypeOther', 'length': 400, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'TSEVENT_URL', 'type': 'esriFieldTypeString', 'alias': 'More information about tsunami event', 'sqlType': 'sqlTypeOther', 'length': 400, 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}, {'name': 'DRAWING_ORDER', 'type': 'esriFieldTypeDouble', 'alias': 'DRAWING_ORDER', 'sqlType': 'sqlTypeOther', 'nullable': True, 'editable': True, 'domain': None, 'defaultValue': None}]
SOURCE_PAGE_PINS = {'page_000': '20a1e1de6dca8d02c68099c7ab1ccafe53920be3994ae3e652d94a27f8685591', 'page_001': 'a5f70d18d36e128d9ca612447db2c5ae8baf618f3371330c0d9956ede24a70b8', 'page_002': 'c7a23d571c56ef50f60c6c01fa0b9ae067266a9de2362ec9b75a4a478b102866', 'page_003': '200d389ae4c36bc02948c298185411ca388d4369eb15ba602efcc91ae74f6f16', 'page_004': '8f9fc9e13efceaad04f42405bcd1091931e4c92e2309dfb7cd675902875fbee4', 'page_005': 'eac6198d9f1f74760c800d26c531f3f7fca7eca94e2bafba5c032e034cbedc7f', 'page_006': 'd99d928326a42d4c2a07e6a461ec6db2b7ce9a5a0771e56a336dff100b776c75', 'page_007': '1703ac40eebc440f4217f5bcdca5ec9b95ab11bba64cfc627567a5fdfdfc14cc', 'page_008': 'c465ec18da416d94719a44339f5c9247bd9bb3fc5c13fe9fd570da8bcd492484', 'page_009': 'bcbf29e411d086b91a343e1724c728713dd9632a079ed4a2d0520bcddac98fb5', 'page_010': '28a61ffa010ff4de78e886f501b66ddfa40d5381156b19d3438a422a80492fb8', 'page_011': '47e1dde1e771595ef438e8e15d3235049277aaa4567a97abb7ee9707580b7594', 'page_012': 'd85ec982624ba232bca5b89fbdcd51b4450bfc697c944acee5b8333295c2835c', 'page_013': '14cf31f4042e7ee3a1e7235d365e202ef1665efb919da17287e758b71130d0b8', 'page_014': 'b6b472b04f1560f47a8be88a38ba8507acc900fd98c40807f26e4c9d3ec17e09', 'page_015': '99fa1478a85dcb2b1de2600890460a109925661e947da81f24fa23c1e1df1922', 'page_016': '6dda8a48ce9d2c8b4f06d76403727362a4d31e4728c0faf0002ad6a667c3ae37', 'page_017': 'f88343ecff8427b0337dd99dc952fe4facc676f8b633aaf19ecceb9d0a08594b', 'page_018': '0619ddd0decce687d414d6bae372b441fc4d4e5aa0680740a6477a2dfd50ccbf', 'page_019': 'd57f9fa382717048de409e9ecb0b240eb5232b5dc70e60a616d23c2a27ec93c5', 'page_020': '42e160c16a9eaad172c391ce07e01ab05e90db9f021606b231c63343f41409ba', 'page_021': 'bd5be6b61b574adf57cc9e6084387ad18a837bb4918dafafc1a2151c407a2bc8', 'page_022': '25b8da56c58cfea41827b7cbfc49b3cff3e27985dd468a61e6c6ce09cfac3fb3', 'page_023': '35f7178691800ce08f1fd990349deab0c440228c3b6f9e6c7b793c75cdbfc6cc', 'page_024': 'a19800c060df508a5d0e6d16dea90c089f8d5e6d2b34caac888de901182dc899', 'page_025': '0f04d99a75820feee87a397f911a41148a4fd489033e88199ca794e1cf5edd29', 'page_026': 'a74d34c9722cb08389f8b9178c5e40380b3fbc4206b116a9ced2ad786f590184', 'page_027': 'a87279e42474b72a9e822fc4428c08d855f9ef80c2bd98476a4e41a792c9e745', 'page_028': 'b358c9318a4c20d84a896709ad113510f008516c67fe75188720ae38a25e1ca2', 'page_029': '486cc73da82c1fee56dcecb6193b201d365d02d44aebf8f1c3996ae99d558f08', 'page_030': '66084e34a4998dbb660b22f971002006826c90d3eaa3bb49769f8879151311e6', 'page_031': '34f9ba27051024e491bd08d5378d70cb17d38c28ae49f622c4bf9e93f4a0d5e1', 'page_032': '9ca9f8b8d32c0cf8a2ec65422fb2157d95a494f722f35af9bb3d7e5031345532', 'page_033': 'a8eb5ca02d46cc714c2b10794c1613d8f6c76c302e865a66aa86fcbce5022d42', 'page_034': 'cc32c86ca5e5e3c6c48a5ca6d51cd4cabf530df9d0b941aa00dce0ae6c0b4644', 'page_035': 'e4504a37d54a3746b2a3c163d3a1488c04a0c052bf4691e7f95be97af926be3d', 'page_036': '829b9b21842c94ce9826816c9cfdc73bbbb49866b85035757e3b35d283dc72e6', 'page_037': '90b52ba7dc6e0599f568f3d3113ca22bf521e5cf8dfc35906f1abf487dc914ad', 'page_038': 'ce6e36f3f1d6c747343fa01f6474c0e50b3fa9598c2ba4ab23bee4c4ac7b5370', 'page_039': '6329fcd8ede869598358af796e63b449c22dd4f51fdc104b136747fc23e302c7', 'page_040': '2ec61fc9817b9bab3efe7f4c7c66ad97b7c16dff8e7ef2386cc25aed0b8a757b', 'page_041': 'df5d211c772f9f8f2130bebef40a3bba719c528ad9a172fa6fae09c8baf64f8b', 'page_042': 'ab9052b80c2f74228825ff633ddb254675f8699f7777c4ed51d184d946f37793', 'page_043': '604fa9ae27b3e3f9f90340fdbcba44b3c3783021c6687f6eb4929f4c92aa0dfd', 'page_044': 'e463c7494c8a6ed4022240b1bb226fe8368a823e0469923b4eaa242bc722edd6', 'page_045': '3a158127424d04ad66f30942fc87d69f5f263a4d639fd12827156b0428b1987c', 'page_046': 'ba6a7fd5168384215a67143901042f92300958dcae87e427b6ca699bd911b0eb', 'page_047': 'cdb052ceb441aea55d4b83c7356e2c0ed0fdaa9b8e830e9a135f19323ebdefe5', 'page_048': '83420b679ab85aa09a1ffbbc3162ea628cff86b2bf365c2f6a3989dba70ddbba', 'page_049': 'da36b5fcca97964c4226ae3edc8fb742af0911705f87e95c9c0f1e59df0e14a9', 'page_050': 'a31501c4524a2f2612e1da0fa7652fdbe4cd06895997f7ca902f58e8cf89dd73', 'page_051': 'd0eebf3dffc75d71c36dcae5152858647ecd89e45f405a696b73d39512207907', 'page_052': '5f9300b99fff332f9a16187d8693fbb33359c2bac233caa05cd8bb165c51e200', 'page_053': 'cf5269c244e4bddbba00ac2f83100d2487e8f9ae94aaceeed291f2867e04a881', 'page_054': 'ebabec789b9dc535d3c314e85245f0921e171eb66e7cad34596dfff36bdaa42e', 'page_055': '2265b2eaf372e8dbcd06a047df5e4eb17a88214574aa6176251b31569ebfcabc', 'page_056': '4989065711c9132929208d9a7ba1cd8e9dd2f26e90e69a40f6b61a0542f7d867', 'page_057': 'd8c7f67a05b70a3d048e42e0d0b493d4323d6f0d565fc373f967ff8735dcf910', 'page_058': '87e6de3576ee36e6d695b517d8a65058f3e6a8c615369c42e8663e3d5eaecf2f', 'page_059': 'e8dbcec2f9f1faab5b6b4d4b9cd7be4e4152d382633fee039de9052cfd503654', 'page_060': '4a8c3d0a67c44b8a71267d3828a30904b58789c5c940d3f8ecea233238d01ecd', 'page_061': '6caf522f6a507abe2fa09357b3693bea4d4afa2dbb60c8f270807eedcac77193', 'page_062': '366c812c0f3c93082224c61d9fe47d8bf39a6c8a7f371b12edbb6481fa3a2bf8', 'page_063': '28f3381d6bc082eea2d95fc4d2a80e3c8e502d1beb6f224dd465792dbfdabd38', 'page_064': 'f13763ce6e91d8876066bf795ae1258f05ee126722f2062371cbf12eee5e81c0', 'page_065': '3e61b561b963f3ff22c516e3b4de6402378f47606d4402ba5e08e8118ebdfe3e', 'page_066': 'a4a183104c4cabd6ff3638941b2503eba02e9905b771ae15f0e6eb2c2db875dc', 'page_067': 'e73183d7d38dbf21bee44d9b7533bd8b4d16dc09179a1f57f44bef13c0fd74f6', 'page_068': '5982abe179fb31112e1e74f03d68e928e3a2fd4f23f015b31a728db94a5ec4eb'}
PROPOSAL = {'approved': False, 'id_fields': ['ID', 'ObjectId'], 'geometry': True, 'geometry_nullable': False, 'coverage_fields': ['COUNTRY', 'YEAR', 'TYPE_MEASUREMENT_ID', 'TSEVENT_ID'], 'crs': 'EPSG:3857', 'columns': {'ID': {'type': 'integer', 'nullable': True}, 'ObjectId': {'type': 'integer', 'nullable': False}, 'TSEVENT_ID': {'type': 'integer', 'nullable': True}, 'YEAR': {'type': 'integer', 'nullable': True}, 'MONTH': {'type': 'integer', 'nullable': True}, 'DAY': {'type': 'integer', 'nullable': True}, 'DATE_STRING': {'type': 'string', 'nullable': True}, 'ARR_DAY': {'type': 'integer', 'nullable': True}, 'ARR_HOUR': {'type': 'integer', 'nullable': True}, 'ARR_MIN': {'type': 'integer', 'nullable': True}, 'TRAV_HOURS': {'type': 'integer', 'nullable': True}, 'TRAV_MINS': {'type': 'integer', 'nullable': True}, 'PERIOD': {'type': 'decimal', 'nullable': True}, 'FIRST_MOTION': {'type': 'string', 'nullable': True}, 'LATITUDE': {'type': 'decimal', 'nullable': True}, 'LONGITUDE': {'type': 'decimal', 'nullable': True}, 'LOCATION_NAME': {'type': 'string', 'nullable': True}, 'AREA': {'type': 'string', 'nullable': True}, 'COUNTRY': {'type': 'string', 'nullable': True}, 'REGION_CODE': {'type': 'integer', 'nullable': True}, 'RUNUP_HT': {'type': 'decimal', 'nullable': True}, 'RUNUP_HT_RANK': {'type': 'integer', 'nullable': True}, 'RUNUP_HORIZ': {'type': 'decimal', 'nullable': True}, 'TYPE_MEASUREMENT_ID': {'type': 'integer', 'nullable': True}, 'TYPE_MEASUREMENT': {'type': 'string', 'nullable': True}, 'DAMAGE_MILLIONS_DOLLARS': {'type': 'decimal', 'nullable': True}, 'DAMAGE_AMOUNT_ORDER': {'type': 'integer', 'nullable': True}, 'DAMAGE_DESCRIPTION': {'type': 'string', 'nullable': True}, 'DEATHS': {'type': 'integer', 'nullable': True}, 'DEATHS_AMOUNT_ORDER': {'type': 'integer', 'nullable': True}, 'DEATHS_DESCRIPTION': {'type': 'string', 'nullable': True}, 'INJURIES': {'type': 'integer', 'nullable': True}, 'INJURIES_AMOUNT_ORDER': {'type': 'integer', 'nullable': True}, 'INJURIES_DESCRIPTION': {'type': 'string', 'nullable': True}, 'HOUSES_DESTROYED': {'type': 'integer', 'nullable': True}, 'HOUSES_AMOUNT_ORDER': {'type': 'integer', 'nullable': True}, 'HOUSES_DESCRIPTION': {'type': 'string', 'nullable': True}, 'HOUSES_DAMAGED': {'type': 'integer', 'nullable': True}, 'HOUSES_DAMAGED_AMOUNT_ORDER': {'type': 'integer', 'nullable': True}, 'HOUSES_DAM_DESCRIPTION': {'type': 'string', 'nullable': True}, 'COMMENTS': {'type': 'string', 'nullable': True}, 'DOUBTFUL': {'type': 'string', 'nullable': True}, 'DIST_FROM_SOURCE': {'type': 'integer', 'nullable': True}, 'EVENT_REGION_CODE': {'type': 'integer', 'nullable': True}, 'EVENT_REGION': {'type': 'string', 'nullable': True}, 'PRED_TRAV_HOURS': {'type': 'decimal', 'nullable': True}, 'PRED_TRAV_MINS': {'type': 'integer', 'nullable': True}, 'URL': {'type': 'string', 'nullable': True}, 'TSEVENT_URL': {'type': 'string', 'nullable': True}, 'DRAWING_ORDER': {'type': 'decimal', 'nullable': True}}}
KIND = {'esriFieldTypeInteger': 'int32', 'esriFieldTypeOID': 'int32', 'esriFieldTypeSmallInteger': 'int16', 'esriFieldTypeDouble': 'float64', 'esriFieldTypeSingle': 'float32', 'esriFieldTypeString': 'string'}
PIN = '9b5c890bae9139e801245b0b4b205257c8c3fc134beb31c64a971bb17b2227ca'
AUTHORITY_URLS={
 'ncei_tsunamis':'https://www.ncei.noaa.gov/products/natural-hazards/tsunamis',
 'time_lapse':'https://www.ncei.noaa.gov/maps/tsunami-events/',
 'time_lapse_js':'https://www.ncei.noaa.gov/maps/tsunami-events/assets/index-LsW0PzVb.js',
 'webmap_item':'https://www.arcgis.com/sharing/rest/content/items/8bed3e9c40114d6a8b2708ceaee0b329?f=json',
 'webmap':'https://www.arcgis.com/sharing/rest/content/items/8bed3e9c40114d6a8b2708ceaee0b329/data?f=json',
 'source_item':'https://www.arcgis.com/sharing/rest/content/items/33d29de9e6c34c44ab087d78c15e05d7?f=json',
 'doi':'https://doi.org/10.7289/V5PN93H7'}
KNOWN_FINAL_URLS={'ncei_tsunamis':'https://www.ncei.noaa.gov/products/natural-hazards/tsunamis-earthquakes-volcanoes/tsunamis',
 'doi':'https://data.noaa.gov/metaview/page?xml=NOAA/NESDIS/NGDC/MGG/Hazards/iso/xml/G02151.xml&view=getDataView'}
class ObservationQuarantineError(ValueError):
    """Preserved incomplete, drifted or untrusted draft evidence."""

def encode(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode()

def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()

def strict_json(body: bytes) -> dict:
    if len(body) > MAX_BYTES:
        raise ObservationQuarantineError('response_size_limit')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ObservationQuarantineError('duplicate_json_member')
            result[key] = value
        return result
    result = json.loads(body, object_pairs_hook=pairs, parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(result, dict) or 'error' in result:
        raise ObservationQuarantineError('nonobject_or_error_json')
    return result

def require_local(storage: StagingStorageResource) -> None:
    if isinstance(storage, PublishedStorageResource) or storage.bucket or not storage.use_local or not storage.local_dir:
        raise ObservationQuarantineError('draft_requires_explicit_isolated_local_staging')

def immutable(storage: StagingStorageResource, key: str, body: bytes) -> None:
    before = storage.object_snapshot(key)
    if before is not None:
        if storage.read_key(key) != body or storage.object_snapshot(key) != before:
            raise ObservationQuarantineError(f'immutable_staging_conflict:{key}')
    else:
        storage.write_key_if_unchanged(key, body, None)

def quarantine(storage: StagingStorageResource, body: bytes, receipt: dict, reason: str) -> str:
    prefix = f'_quarantine/{SLUG}/{sha(body)}/{uuid4().hex}'
    immutable(storage, f'{prefix}/response.body', body)
    immutable(storage, f'{prefix}/receipt.json', encode(receipt))
    immutable(storage, f'{prefix}/report.json', encode({'approved': False, 'status': 'quarantined', 'reason': reason}))
    return prefix

def processing_identity() -> dict:
    """Installed source, lock/project, Python, Python distributions and native engines."""
    root = Path(__file__).parents[3]
    sources = sorted(root.joinpath('src/dagster_hifld').rglob('*.py'))
    return {'python': platform.python_version(), 'implementation': platform.python_implementation(),
            'platform': platform.platform(),
            'distributions': sorted((d.metadata['Name'], d.version) for d in importlib.metadata.distributions()),
            'pyogrio_gdal': pyogrio.__gdal_version__, 'gdal': fiona.__gdal_version__, 'geos': shapely.geos_version_string, 'proj': pyproj.proj_version_str,
            'files': {str(p.relative_to(root)): sha256_file(p) for p in sources + [root / 'uv.lock', root / 'pyproject.toml']},
            'verifier_sha256': verifier_implementation_sha256()}

def local_candidate_path(storage: StagingStorageResource, prefix: str) -> Path:
    require_local(storage)
    root=Path(storage.local_dir).resolve()
    key=storage._ensure_prefixed(prefix)
    if Path(key).is_absolute() or any(part in {'.','..'} for part in Path(key).parts):
        raise ObservationQuarantineError('unsafe_local_candidate_prefix')
    target=root/key
    current=root
    for part in target.relative_to(root).parts:
        current=current/part
        if current.is_symlink(): raise ObservationQuarantineError('linked_candidate_path')
    return target

@contextmanager
def candidate_version_lock(storage: StagingStorageResource, prefix: str):
    """One stable lock inode for lookup, validation, invalid detach and install."""
    target=local_candidate_path(storage,prefix)
    target.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(target.parent/('.'+target.name+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b') as held:
        if os.fstat(held.fileno()).st_nlink!=1:
            raise ObservationQuarantineError('linked_candidate_lock')
        fcntl.flock(held,fcntl.LOCK_EX)
        yield target

def quarantine_cached_candidate(storage: StagingStorageResource, prefix: str, reason: str, *, already_locked: bool=False) -> str | None:
    """Move only the invalid target observed under the common version lock."""
    context=nullcontext(local_candidate_path(storage,prefix)) if already_locked else candidate_version_lock(storage,prefix)
    with context as target:
        if not target.exists():
            return None
        destination=Path(storage.local_dir)/'_quarantine'/SLUG/('invalid-cache-'+uuid4().hex)
        destination.mkdir(parents=True)
        os.rename(target,destination/'candidate')
        (destination/'report.json').write_bytes(encode({'approved':False,
            'status':'quarantined_invalid_draft_cache','original_prefix':prefix,'reason':reason}))
        return str(destination)

def source_urls() -> dict[str,str]:
    urls={'layer_before':URL+'?f=pjson','count_before':URL+'/query?where=1%3D1&returnCountOnly=true&f=json',
          'ids_before':URL+'/query?where=1%3D1&returnIdsOnly=true&f=json'}
    for offset in range(0,FEATURE_COUNT,PAGE_SIZE):
        query=urlencode({'where':'1=1','outFields':'*','returnGeometry':'true','orderByFields':'ObjectId',
                         'resultOffset':str(offset),'resultRecordCount':str(PAGE_SIZE),'f':'json'})
        urls[f'page_{offset//PAGE_SIZE:03d}']=URL+'/query?'+query
    urls.update(ids_after=URL+'/query?where=1%3D1&returnIdsOnly=true&f=json',
                count_after=URL+'/query?where=1%3D1&returnCountOnly=true&f=json',layer_after=URL+'?f=pjson')
    return urls


def validate_http(body: bytes, receipt: dict, url: str, final: str) -> None:
    if (len(body)>MAX_BYTES or receipt.get('url')!=url or receipt.get('final_url')!=final
        or type(receipt.get('status')) is not int or receipt['status']!=200
        or type(receipt.get('bytes')) is not int or receipt['bytes']!=len(body) or receipt.get('sha256')!=sha(body)):
        raise ObservationQuarantineError('original_http_receipt_url_status_bytes_hash_mismatch')
    timestamp=receipt.get('fetched_at')
    if not isinstance(timestamp,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)',timestamp):
        raise ObservationQuarantineError('original_http_timestamp_requires_strict_aware_iso')
    if datetime.fromisoformat(timestamp).tzinfo is None:
        raise ObservationQuarantineError('original_http_timestamp_requires_timezone')


def unpack_captures(packs: dict[str,bytes]) -> dict[str,tuple[bytes,dict]]:
    if set(packs)!=set(ARCHIVE_NAMES):raise ObservationQuarantineError('original_http_archive_roster_mismatch')
    result={};total=0
    for archive,urls in [('source_http',source_urls()),('authority_http',AUTHORITY_URLS)]:
        body=packs[archive]
        if len(body)>MAX_ARCHIVE_BYTES:raise ObservationQuarantineError('original_http_archive_size_limit')
        with zipfile.ZipFile(io.BytesIO(body)) as z:
            names=z.namelist();expected={n+ext for n in urls for ext in ('.body','.receipt.json')}
            allowed=expected|({'complete-capture-summary.json'} if archive=='source_http' else set())
            if len(names)!=len(set(names)) or not expected.issubset(names) or set(names)-allowed:
                raise ObservationQuarantineError('original_http_archive_member_roster_drift')
            for info in z.infolist():
                if info.flag_bits&1 or info.file_size>MAX_BYTES or info.is_dir() or PurePosixPath(info.filename).is_absolute() or '..' in PurePosixPath(info.filename).parts:
                    raise ObservationQuarantineError('unsafe_original_http_archive_member')
                total+=info.file_size
                if total>MAX_TOTAL_BYTES:raise ObservationQuarantineError('original_http_archive_expansion_limit')
            for n,url in urls.items():
                raw=z.read(n+'.body');receipt=strict_json(z.read(n+'.receipt.json'))
                validate_http(raw,receipt,url,KNOWN_FINAL_URLS.get(n,url));result[archive+':'+n]=(raw,receipt)
    return result


def archive_key(name: str, body: bytes) -> str:
    return f'_raw-http-archives/{SLUG}/{sha(body)}/{name}.zip'


def acquire(storage: StagingStorageResource, capture_dir: Path|None) -> dict[str,bytes]:
    require_local(storage);packs={}
    if capture_dir is not None:
        for name in ARCHIVE_NAMES:
            body=b''
            try:
                with (capture_dir/(name+'.zip')).open('rb') as f:body=f.read(MAX_ARCHIVE_BYTES+1)
                if len(body)>MAX_ARCHIVE_BYTES:raise ObservationQuarantineError('original_http_archive_size_limit')
                packs[name]=body
            except (OSError,ValueError) as exc:
                where=quarantine(storage,body,{'capture_path':str(capture_dir),'archive':name},str(exc))
                raise ObservationQuarantineError(f'{exc}; retained:{where}') from exc
    else:
        cumulative=0
        with tempfile.TemporaryDirectory(prefix='tsunami-observation-acquisition-') as tmp:
            try:
                for archive,urls in [('source_http',source_urls()),('authority_http',AUTHORITY_URLS)]:
                    target=Path(tmp)/(archive+'.zip')
                    with zipfile.ZipFile(target,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
                        with httpx.Client(timeout=60,follow_redirects=True,headers={'Accept-Encoding':'identity'}) as client:
                            for n,url in urls.items():
                                for attempt in range(3):
                                    raw=b'';receipt={'url':url,'fetched_at':datetime.now(timezone.utc).isoformat(),'attempt':attempt+1}
                                    try:
                                        with client.stream('GET',url) as response:
                                            receipt.update(final_url=str(response.url),status=response.status_code,
                                                content_encoding=response.headers.get('content-encoding','identity'),
                                                declared_wire_length=response.headers.get('content-length'))
                                            for block in response.iter_bytes(chunk_size=65536):
                                                raw+=block;receipt['wire_bytes']=response.num_bytes_downloaded
                                                if len(raw)>MAX_BYTES or cumulative+len(raw)>MAX_TOTAL_BYTES:
                                                    raise ObservationQuarantineError('bounded_original_http_response_limit')
                                            receipt.update(bytes=len(raw),sha256=sha(raw),wire_bytes=response.num_bytes_downloaded)
                                            validate_http(raw,receipt,url,KNOWN_FINAL_URLS.get(n,url))
                                            if response.headers.get('content-length') is not None and int(response.headers['content-length'])!=response.num_bytes_downloaded:
                                                raise ObservationQuarantineError('incomplete_original_http_wire_length')
                                        cumulative+=len(raw);break
                                    except (httpx.HTTPError,OSError,ValueError,TypeError) as exc:
                                        receipt.update(bytes=len(raw),sha256=sha(raw),partial=True)
                                        quarantine(storage,raw,receipt,str(exc))
                                        if attempt==2:raise ObservationQuarantineError(f'bounded_original_http_retry_exhausted:{n}:{exc}') from exc
                                z.writestr(n+'.body',raw);z.writestr(n+'.receipt.json',encode(receipt))
                                if n in ('count_before','count_after') and strict_json(raw).get('count')!=FEATURE_COUNT:
                                    raise ObservationQuarantineError('source_population_drift_requires_review')
                    packs[archive]=target.read_bytes()
            except (ValueError,TypeError,KeyError,OSError,zipfile.BadZipFile) as exc:
                location=f'_quarantine/{SLUG}/acquisitions/{uuid4().hex}'
                for p in Path(tmp).glob('*.zip'):immutable(storage,f'{location}/{p.name}',p.read_bytes())
                immutable(storage,f'{location}/report.json',encode({'approved':False,'reason':str(exc)}))
                raise ObservationQuarantineError(f'{exc}; complete_and_partial_archives_retained:{location}') from exc
    try:unpack_captures(packs)
    except (ValueError,TypeError,KeyError,OSError,zipfile.BadZipFile) as exc:
        for n,body in packs.items():quarantine(storage,body,{'archive':n},str(exc))
        raise ObservationQuarantineError(f'original_http_archive_invalid:{exc}') from exc
    for n,body in packs.items():immutable(storage,archive_key(n,body),body)
    return packs


def validate_complete_bundle(http: dict) -> None:
    before=strict_json(http['source_http:layer_before'][0]);after=strict_json(http['source_http:layer_after'][0])
    if before.get('fields')!=NATIVE_FIELDS or after.get('fields')!=NATIVE_FIELDS or before.get('objectIdField')!='ObjectId' or before.get('geometryType')!='esriGeometryPoint':
        raise ObservationQuarantineError('native_schema_objectid_or_geometry_metadata_drift')
    if before.get('editingInfo')!=after.get('editingInfo'):
        raise ObservationQuarantineError('producer_edit_state_changed_during_extraction')
    rosters=[]
    for n in ('ids_before','ids_after'):
        j=strict_json(http['source_http:'+n][0]);ids=j.get('objectIds',[])
        if j.get('objectIdFieldName')!='ObjectId' or len(ids)!=FEATURE_COUNT or len(set(ids))!=FEATURE_COUNT or any(type(x) is not int for x in ids):
            raise ObservationQuarantineError('incomplete_or_invalid_source_objectid_census')
        rosters.append(sorted(ids))
    if rosters[0]!=rosters[1]:raise ObservationQuarantineError('concurrently_changed_objectid_population')
    for n in ('count_before','count_after'):
        if strict_json(http['source_http:'+n][0]).get('count')!=FEATURE_COUNT:raise ObservationQuarantineError('incomplete_or_changed_source_count')
    observed=[]
    for page in sorted(SOURCE_PAGE_PINS):
        raw=http['source_http:'+page][0]
        if sha(raw)!=SOURCE_PAGE_PINS[page]:raise ObservationQuarantineError('source_page_drift_requires_review:'+page)
        j=strict_json(raw);fields=[{k:f[k] for k in ('name','type','length') if k in f} for f in j.get('fields',[])]
        expected=[{k:f[k] for k in ('name','type','length') if k in f} for f in NATIVE_FIELDS]
        if fields!=expected or j.get('geometryType')!='esriGeometryPoint' or j.get('spatialReference')!={'wkid':102100,'latestWkid':3857}:
            raise ObservationQuarantineError('source_page_schema_crs_or_geometry_drift')
        page_number=int(page.rsplit('_',1)[1]);group=rosters[0][page_number*PAGE_SIZE:(page_number+1)*PAGE_SIZE]
        rows=j.get('features',[])
        if len(rows)!=len(group) or [r.get('attributes',{}).get('ObjectId') for r in rows]!=group:
            raise ObservationQuarantineError('source_page_missing_duplicate_or_reordered_objectids')
        if page==max(SOURCE_PAGE_PINS) and j.get('exceededTransferLimit'):
            raise ObservationQuarantineError('final_source_page_transfer_limit_incomplete')
        observed.extend(group)
    if observed!=rosters[0]:raise ObservationQuarantineError('incomplete_global_observation_population')


def provenance(http: dict) -> dict:
    if b'/maps/tsunami-events/' not in http['authority_http:ncei_tsunamis'][0] or b'index-LsW0PzVb.js' not in http['authority_http:time_lapse'][0] or b'8bed3e9c40114d6a8b2708ceaee0b329' not in http['authority_http:time_lapse_js'][0]:
        raise ObservationQuarantineError('official_producer_time_lapse_webmap_chain_changed')
    item=strict_json(http['authority_http:webmap_item'][0]);source_item=strict_json(http['authority_http:source_item'][0])
    if item.get('owner')!='ncei_noaa' or source_item.get('owner')!='ncei_noaa' or source_item.get('url')!=URL.rsplit('/',1)[0]:
        raise ObservationQuarantineError('official_noaa_owner_or_source_service_changed')
    webmap=strict_json(http['authority_http:webmap'][0])
    def find(items):
        for entry in items:
            if entry.get('url')==URL:return entry
            nested=find(entry.get('layers',[]))
            if nested:return nested
    if not find(webmap.get('operationalLayers',[])):
        raise ObservationQuarantineError('official_webmap_observation_layer_delegation_changed')
    inputs=[]
    for name,(body,receipt) in sorted(http.items()):
        inputs.append({'input_id':name+':'+sha(body),'producer':'NOAA National Centers for Environmental Information',
            'host':urlsplit(receipt['url']).hostname,'role':'producer_publication' if 'noaa.gov' in receipt['url'] else 'official_distributor',
            'canonical_url':receipt['url'],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],
            'retrieved_at':receipt['fetched_at'],'release_date':None,
            'release_date_unknown_reason':'Continuously curated historical observations; no source data release date declared. Citation/portal edit/fetch times are not substituted.',
            'sha256':sha(body),'bytes':len(body),'license':'https://www.noaa.gov/disclaimer',
            'attribution':'NOAA NCEI Global Historical Tsunami Database; DOI10.7289/V5PN93H7',
            'lineage':'NCEI official tsunami page selects Time Lapse app; current JavaScript selects ncei_noaa webmap8bed3e9c40114d6a8b2708ceaee0b329, which explicitly delegates tsunami_observations FeatureServer layer0. Full69-page observation extraction is distinct from tsunami event rows.',
            'authority_evidence':AUTHORITY_URLS['ncei_tsunamis'],'distributor':'Esri ArcGIS Online for ncei_noaa',
            'delegation_evidence':{'producer_app':AUTHORITY_URLS['time_lapse'],'producer_script':AUTHORITY_URLS['time_lapse_js'],'webmap':AUTHORITY_URLS['webmap']}})
    manifest={'schema_version':1,'approved':False,'processor':'HIFLD Next',
        'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'Historical Tsunami Observations — complete native working draft','publisher':'NOAA NCEI / ncei_noaa official distribution',
        'agency':'National Oceanic and Atmospheric Administration','source_url':AUTHORITY_URLS['ncei_tsunamis'],
        'license':'https://www.noaa.gov/disclaimer','inputs':inputs,
        'transformations':[{'operation':'decode_all_original_esri_json_using_declared_native_physical_types',
          'preserved':'All34495observations/all50attrs, original ID/ObjectId/TSEVENT_ID, nativeInt16/Int32/Float32/Float64/nulls/strings and exactnative3857XY. AlloriginalwireJSONbodies/receipts retained without alteration.',
          'physical_decoder':'EsriSingle explicitly decoded to IEEE754binary32; original wire decimal literals remain in original HTTP archives and full conversion diagnostics. No unapprovedDoublewiden or date interpretation.',
          'repair_or_projection':False,'mapping_approval':'not established'}],
        'unsupported_output_formats':{'shapefile_zip':'DBF field-name/value/null/type constraints and original IDs require format acceptance; original HTTP inputs retained.',
          'pmtiles':'Reprojection, quantization and visualization schema require approval; no rendering substitution.'}}
    validate_release_provenance(manifest);return manifest
def decode_rows(http: dict) -> tuple[list[dict],dict,bytes]:
    rows=[];changes=[];fields={f['name']:f for f in NATIVE_FIELDS}
    for page in sorted(SOURCE_PAGE_PINS):
        for feature in strict_json(http['source_http:'+page][0])['features']:
            props=feature.get('attributes');geometry=feature.get('geometry')
            if not isinstance(props,dict) or set(props)!=set(fields) or not isinstance(geometry,dict) or set(geometry)!={'x','y'}:
                raise ObservationQuarantineError('missing_extra_columns_or_point_dimensions')
            physical={}
            for name,field in fields.items():
                value=props[name];kind=KIND[field['type']]
                if value is None:
                    if not field['nullable'] or name in ('ID','ObjectId'):
                        raise ObservationQuarantineError('missing_original_identifier_or_nonnullable_value')
                    physical[name]=None;continue
                if kind.startswith('int'):
                    bits=int(kind[3:])
                    if type(value) is not int or not -(2**(bits-1))<=value<2**(bits-1):
                        raise ObservationQuarantineError('source_declared_integer_width_or_type_drift:'+name)
                    physical[name]=value
                elif kind.startswith('float'):
                    if type(value) not in (int,float) or not math.isfinite(value):
                        raise ObservationQuarantineError('source_declared_numeric_type_drift:'+name)
                    decoded=struct.unpack('<f',struct.pack('<f',value))[0] if kind=='float32' else float(value)
                    if not math.isfinite(decoded):raise ObservationQuarantineError('source_float_overflow:'+name)
                    physical[name]=decoded
                    if decoded!=value or type(value) is not float:
                        changes.append({'ObjectId':props['ObjectId'],'ID':props['ID'],'column':name,
                            'declared_physical_type':kind,'original_wire_json_value':value,'original_wire_python_type':type(value).__name__,
                            'declared_physical_value':decoded,'physical_bytes_le':struct.pack('<f' if kind=='float32' else '<d',decoded).hex()})
                elif type(value) is not str or len(value)>field['length']:
                    raise ObservationQuarantineError('source_string_type_or_width_drift:'+name)
                else:physical[name]=value
            if any(type(v) not in (int,float) or not math.isfinite(v) for v in geometry.values()):
                raise ObservationQuarantineError('invalid_original_point_coordinates')
            rows.append({'type':'Feature','properties':physical,'geometry':{'type':'Point','coordinates':(geometry['x'],geometry['y'])}})
    if len(rows)!=FEATURE_COUNT or any(len({r['properties'][k] for r in rows})!=FEATURE_COUNT for k in ('ID','ObjectId')):
        raise ObservationQuarantineError('source_original_id_or_full_population_drift')
    rows.sort(key=lambda r:r['properties']['ObjectId']);geometries=[shape(r['geometry']) for r in rows]
    if any(g.is_empty or not g.is_valid for g in geometries):raise ObservationQuarantineError('invalid_native_point_geometry')
    def counts(name):
        counter=collections.Counter(r['properties'][name] for r in rows)
        return [{'value':v,'count':n} for v,n in sorted(counter.items(),key=lambda pair:(type(pair[0]).__name__,repr(pair[0])))]
    audit={'approved':False,'status':'declared_physical_source_fidelity_pass','feature_count':len(rows),'crs':'EPSG:3857',
        'source_field_declarations':NATIVE_FIELDS,'bounds':shapely.total_bounds(geometries).tolist(),'invalid_geometry_count':0,'null_geometry_count':0,
        'nulls':{k:sum(r['properties'][k] is None for r in rows) for k in fields},
        'identifiers':{k:{'unique':len({r['properties'][k] for r in rows}),'nulls':sum(r['properties'][k] is None for r in rows)} for k in ('ID','ObjectId','TSEVENT_ID')},
        'coverage':{k:counts(k) for k in ('COUNTRY','TYPE_MEASUREMENT_ID','YEAR')},
        'numeric_physical_decode':{'changed_wire_representation_count':len(changes),
            'by_column':dict(collections.Counter(r['column'] for r in changes)),
            'method':'Explicit original EsriSingle IEEE754binary32/SmallInteger int16 declarations. Original wire JSON literals retained in full original archives; this does not accept a published type migration.',
            'complete_diagnostics':'metadata/physical_numeric_decodes.jsonl.gz'},
        'scope':'Complete34495globalobservation/runup records, not tsunami event locations. OriginalID/ObjectId/TSEVENT_ID and unknown or BCE date strings preserved without joins/date coercion.'}
    return rows,audit,gzip.compress(b''.join(encode(v) for v in changes),compresslevel=6,mtime=0)


def typed_table(rows: list[dict],audit: dict) -> pa.Table:
    types={'int16':pa.int16(),'int32':pa.int32(),'float32':pa.float32(),'float64':pa.float64(),'string':pa.string()}
    schema=pa.schema([pa.field(f['name'],types[KIND[f['type']]],nullable=f['nullable'],
        metadata={b'esri_declared_type':f['type'].encode(),b'original_field_declaration':encode(f)}) for f in NATIVE_FIELDS]+[pa.field('geometry',pa.binary(),nullable=False)])
    table=pa.Table.from_pylist([{**r['properties'],'geometry':shape(r['geometry']).wkb} for r in rows],schema=schema)
    geo={'version':'1.1.0','primary_column':'geometry','columns':{'geometry':{'encoding':'WKB','geometry_types':['Point'],'crs':pyproj.CRS(3857).to_json_dict(),'bbox':audit['bounds']}}}
    return table.replace_schema_metadata({b'geo':encode(geo),b'physical_decoder':b'Original Esri Int16/Float32 declarations; original wire JSON retained separately'})


def normalized_rows(rows: list[dict]) -> bytes:
    return b''.join(encode(r) for r in rows)


def write_canonical_geopackage(destination: Path,table: pa.Table) -> None:
    pyogrio.raw.write_arrow(table,destination,layer=SLUG,driver='GPKG',geometry_name='geometry',geometry_type='Point',crs='EPSG:3857',
        layer_options={'GEOMETRY_NAME':'geometry','FID':'__storage_fid'},dataset_options={'VERSION':'1.3'})
    with sqlite3.connect(destination) as db:db.execute("UPDATE gpkg_contents SET last_change='2000-01-01T00:00:00.000Z'")


def check_geopackage(path: Path,table: pa.Table) -> bytes:
    metadata,read=pyogrio.raw.read_arrow(path,layer=SLUG)
    if not pyproj.CRS(metadata['crs']).equals(pyproj.CRS(3857)) or read.column_names!=table.column_names:
        raise ObservationQuarantineError('native_geopackage_crs_or_column_loss')
    for name in table.column_names:
        if read.schema.field(name).type!=table.schema.field(name).type or not read.column(name).equals(table.column(name)):
            raise ObservationQuarantineError('native_geopackage_physical_type_value_or_geometry_loss:'+name)
    return b''.join(encode({'type':'Feature','properties':{f['name']:r[f['name']] for f in NATIVE_FIELDS},
        'geometry':mapping(shapely.from_wkb(r['geometry']))}) for r in read.to_pylist())


def compare_derivative(source: Path,candidate: Path,output: Path) -> dict:
    return compare_full_rows(source,candidate,PROPOSAL,output,baseline_sha256=sha256_file(source),candidate_sha256=sha256_file(candidate),
        baseline_reference={'role':'unapproved_original_declared_physical_source_reference','approved':False,'original_page_roster_sha256':PIN,
            'comparison_input':{'adapter':'typed-geojson-feature-jsonl-v1','sha256':sha256_file(source),'bytes':source.stat().st_size}})


def fidelity_wrapper(report: dict) -> bytes:
    return encode({'declared_physical_source_rows_byte_identical':True,'original_wire_json_retained_unmodified':True,
                   'shared_verifier':report,'retained_shared_report_prefix':'metadata/source-fidelity','approval':False})


def unapproved_baseline_comparison() -> dict:
    return {'schema_version':1,'status':'blocked','approved':False,'reason_codes':['unapproved_baseline_source_ids_native_int16_float32_schema_and_format_mapping'],'acceptance':'not_established'}


def expected_fidelity_payloads(records: bytes,directory: Path) -> dict[str,bytes]:
    original=directory/'source.jsonl';original.write_bytes(records);replay=directory/'independent-replay.jsonl';replay.write_bytes(records)
    report=compare_derivative(original,replay,directory/'comparison')
    if report.get('status')!='pass':raise ObservationQuarantineError('original_physical_source_evidence_replay_not_passed')
    return {'metadata/geopackage_source_fidelity.json':fidelity_wrapper(report),'metadata/parquet_source_fidelity.json':fidelity_wrapper(report),
        'metadata/baseline_comparison.json':encode(unapproved_baseline_comparison()),
        **{f'metadata/source-fidelity/{n}':(directory/'comparison'/n).read_bytes() for n in FIDELITY_FILES}}


def native_prebuilt_policy(body: bytes) -> dict:
    return {'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all50nativecolumns, Int16/Float32/Int32/Float64/nullability and original IDs/native3857WKB; no generic widening or bbox schema accepted.',
        'objects':[{'path':f'geoparquet/{SLUG}.parquet','sha256':sha(body),'bytes':len(body)}]}}}
def trust_keys(version: str) -> tuple[str,str,str]:
    return (f'_candidate-construction/{SLUG}/{version}.json',f'_candidate-first-construction/{SLUG}/{version}.json',
            f'_candidate-trust-incidents/{SLUG}/{version}.json')


def original_provenance_bundle(storage: StagingStorageResource,version: str,packs: dict,manifest: dict,
                               identity: dict,*,create: bool=False) -> tuple[dict,dict]:
    """Caller holds version lock; two immutable records prevent post-detach bootstrap."""
    key,marker,incident=trust_keys(version);has_bundle=storage.object_exists(key);has_marker=storage.object_exists(marker)
    if has_bundle!=has_marker:
        raise ObservationQuarantineError('missing_original_construction_bundle_or_durable_marker')
    if not has_bundle:
        if not create:raise ObservationQuarantineError('missing_original_construction_bundle_and_durable_marker')
        if storage.object_exists(incident):
            raise ObservationQuarantineError('previous_candidate_trust_failure_original_operator_records_required')
        validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        bundle={'schema_version':1,'version':version,'manifest':manifest,
                'archives':{n:{'key':archive_key(n,b),'sha256':sha(b),'bytes':len(b)} for n,b in packs.items()}}
        bundle_body=encode(bundle)
        immutable(storage,key,bundle_body)
        immutable(storage,marker,encode({'schema_version':1,'version':version,'original_bundle_sha256':sha(bundle_body)}))
    bundle_body=storage.read_key(key);bundle=strict_json(bundle_body);first=strict_json(storage.read_key(marker))
    if (set(first)!={'schema_version','version','original_bundle_sha256'} or first['schema_version']!=1 or first['version']!=version
        or first['original_bundle_sha256']!=sha(bundle_body)):
        raise ObservationQuarantineError('original_construction_durable_marker_binding_changed')
    if (set(bundle)!={'schema_version','version','manifest','archives'} or bundle['schema_version']!=1 or bundle['version']!=version
        or set(bundle['archives'])!=set(ARCHIVE_NAMES)):
        raise ObservationQuarantineError('original_construction_bundle_roster_changed')
    original={}
    for n in ARCHIVE_NAMES:
        ref=bundle['archives'][n]
        if (set(ref)!={'key','sha256','bytes'} or not isinstance(ref['sha256'],str) or not re.fullmatch('[0-9a-f]{64}',ref['sha256'])
            or ref['key']!=f'_raw-http-archives/{SLUG}/{ref["sha256"]}/{n}.zip'
            or type(ref['bytes']) is not int or not 0<ref['bytes']<=MAX_ARCHIVE_BYTES):
            raise ObservationQuarantineError('original_construction_archive_reference_changed')
        raw=storage.read_key(ref['key'])
        if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:
            raise ObservationQuarantineError('original_construction_archive_bytes_changed')
        original[n]=raw
    old_http=unpack_captures(original);fresh_http=unpack_captures(packs)
    if set(old_http)!=set(fresh_http) or any(old_http[n][0]!=fresh_http[n][0] for n in old_http):
        raise ObservationQuarantineError('original_construction_body_differs_from_fresh_complete_source')
    validate_complete_bundle(old_http);expected=provenance(old_http)
    expected.update(processing_fingerprint=manifest['processing_fingerprint'],prebuilt_output_policy=manifest['prebuilt_output_policy'])
    actual=bundle['manifest'];validate_release_provenance(actual)
    if set(actual)!=set(expected) or any(actual[k]!=v for k,v in expected.items() if k!='code_commit'):
        raise ObservationQuarantineError('original_construction_manifest_content_changed')
    validate_construction_code_identity(Path(__file__).parents[3],actual['code_commit'],identity['files'])
    return original,actual


def mark_trust_incident(storage: StagingStorageResource,version: str) -> None:
    # This durable observation prevents first-time creation after an existing
    # candidate with both outside records missing has been detached. It never
    # authorizes restoring a missing trust record from candidate content.
    immutable(storage,trust_keys(version)[2],encode({'schema_version':1,'version':version,
        'status':'previous_candidate_trust_failure_requires_original_operator_records'}))


def canonical_payloads(rows: list[dict],audit: dict,numeric_decodes: bytes,table: pa.Table,
                       packs: dict,manifest: dict,identity: dict,directory: Path) -> dict[str,bytes]:
    records=normalized_rows(rows);gp=directory/'canonical.gpkg';write_canonical_geopackage(gp,table)
    decoded=check_geopackage(gp,table)
    if decoded!=records:raise ObservationQuarantineError('geopackage_declared_physical_rows_not_byte_identical')
    original=directory/'source.jsonl';original.write_bytes(records);output=directory/'geopackage.jsonl';output.write_bytes(decoded)
    gp_report=compare_derivative(original,output,directory/'gp-comparison')
    if gp_report.get('status')!='pass':raise ObservationQuarantineError('geopackage_full_source_verifier_not_pass')
    parquet=directory/'canonical.parquet';pq.write_table(table,parquet,compression='zstd')
    read=pq.read_table(parquet)
    if not read.equals(table,check_metadata=True):raise ObservationQuarantineError('parquet_physical_schema_values_or_metadata_loss')
    parquet_rows=b''.join(encode({'type':'Feature','properties':{f['name']:r[f['name']] for f in NATIVE_FIELDS},
        'geometry':mapping(shapely.from_wkb(r['geometry']))}) for r in read.to_pylist())
    if parquet_rows!=records:raise ObservationQuarantineError('parquet_physical_full_row_reconciliation_failed')
    output.write_bytes(parquet_rows);parquet_report=compare_derivative(original,output,directory/'parquet-comparison')
    if parquet_report.get('status')!='pass':raise ObservationQuarantineError('parquet_full_source_verifier_not_pass')
    if native_prebuilt_policy(parquet.read_bytes())!=manifest['prebuilt_output_policy']:
        raise ObservationQuarantineError('native_prebuilt_policy_does_not_match_produced_bytes')
    payloads={f'raw/{n}.zip':b for n,b in packs.items()}
    payloads.update({f'geopackage/{SLUG}.gpkg':gp.read_bytes(),f'candidate/typed-source.parquet':parquet.read_bytes(),
        f'geoparquet/{SLUG}.parquet':parquet.read_bytes(),'metadata/candidate.records.jsonl.gz':gzip.compress(records,compresslevel=6,mtime=0),
        'metadata/physical_numeric_decodes.jsonl.gz':numeric_decodes,'metadata/source_manifest.json':encode(manifest),
        'metadata/processing_identity.json':encode(identity),'metadata/observations_source_audit.json':encode(audit),
        'metadata/proposed_source_contract.json':encode(PROPOSAL),'metadata/geopackage_source_fidelity.json':fidelity_wrapper(gp_report),
        'metadata/parquet_source_fidelity.json':fidelity_wrapper(parquet_report),'metadata/baseline_comparison.json':encode(unapproved_baseline_comparison()),
        **{f'metadata/source-fidelity/{n}':(directory/'gp-comparison'/n).read_bytes() for n in FIDELITY_FILES}})
    return payloads


def expected_object_names() -> set[str]:
    return {f'raw/{n}.zip' for n in ARCHIVE_NAMES}|{f'geopackage/{SLUG}.gpkg','candidate/typed-source.parquet',
        f'geoparquet/{SLUG}.parquet','metadata/candidate.records.jsonl.gz','metadata/physical_numeric_decodes.jsonl.gz',
        'metadata/source_manifest.json','metadata/processing_identity.json','metadata/observations_source_audit.json',
        'metadata/proposed_source_contract.json','metadata/geopackage_source_fidelity.json','metadata/parquet_source_fidelity.json',
        'metadata/baseline_comparison.json'}|{f'metadata/source-fidelity/{n}' for n in FIDELITY_FILES}


def reconcile_cached_candidate(storage: StagingStorageResource,prefix: str,ready: dict,rows: list[dict],audit: dict,
                               numeric_decodes: bytes,table: pa.Table,packs: dict,manifest: dict,identity: dict) -> None:
    root=local_candidate_path(storage,prefix);manifest_body=encode(manifest);snapshot='metadata/source/version_'+sha(manifest_body)+'_source_manifest.json'
    allowed={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json',
        'metadata/source/source_manifest.json','metadata/source/data_dictionary.json','metadata/source/quality_manifest.json',snapshot}
    for n in ('metadata/source/source_manifest.json',snapshot):
        if (root/n).exists() and (root/n).read_bytes()!=manifest_body:raise ObservationQuarantineError('cached_authored_source_snapshot_differs')
    paths=list(root.rglob('*'))
    if any(p.is_symlink() or (p.is_file() and p.stat().st_nlink!=1) for p in paths):raise ObservationQuarantineError('linked_candidate_object')
    actual={str(p.relative_to(root)) for p in paths if p.is_file()};expected=set(ready['objects'])|{'metadata/observations_candidate_ready.json'}
    if not expected.issubset(actual) or actual-expected-allowed:raise ObservationQuarantineError('cached_unrostered_or_missing_object')
    known={'metadata/source_manifest.json':manifest_body,'metadata/processing_identity.json':encode(identity),
        'metadata/observations_source_audit.json':encode(audit),'metadata/proposed_source_contract.json':encode(PROPOSAL),
        'metadata/physical_numeric_decodes.jsonl.gz':numeric_decodes,
        'metadata/candidate.records.jsonl.gz':gzip.compress(normalized_rows(rows),compresslevel=6,mtime=0),
        **{f'raw/{n}.zip':b for n,b in packs.items()}}
    if any((root/n).read_bytes()!=b for n,b in known.items()):raise ObservationQuarantineError('cached_original_records_manifest_or_archives_changed')
    check_geopackage(root/'geopackage'/f'{SLUG}.gpkg',table)
    if not pq.read_table(root/'candidate/typed-source.parquet').equals(table,check_metadata=True):
        raise ObservationQuarantineError('cached_native_parquet_schema_values_metadata_changed')
    if (root/f'geoparquet/{SLUG}.parquet').read_bytes()!=(root/'candidate/typed-source.parquet').read_bytes():
        raise ObservationQuarantineError('cached_native_prebuilt_parquet_changed')
    if native_prebuilt_policy((root/'candidate/typed-source.parquet').read_bytes())!=manifest['prebuilt_output_policy']:
        raise ObservationQuarantineError('cached_native_prebuilt_policy_changed')
    with tempfile.TemporaryDirectory(prefix='observations-full-cache-replay-') as tmp:
        directory=Path(tmp);gp=directory/'canonical.gpkg';write_canonical_geopackage(gp,table)
        if sha256_file(gp)!=sha256_file(root/'geopackage'/f'{SLUG}.gpkg'):
            raise ObservationQuarantineError('cached_whole_native_geopackage_bytes_changed')
        expected=expected_fidelity_payloads(normalized_rows(rows),directory)
        if any((root/n).read_bytes()!=b for n,b in expected.items()):raise ObservationQuarantineError('cached_source_reports_or_placeholder_changed')


def commit_complete_candidate(storage: StagingStorageResource,prefix: str,payloads: dict,ready: dict,
                              packs: dict,manifest: dict,identity: dict) -> None:
    with candidate_version_lock(storage,prefix) as target:
        if target.exists() or target.is_symlink():raise ObservationQuarantineError('concurrent_or_incomplete_existing_candidate')
        with tempfile.TemporaryDirectory(prefix='.candidate-build-',dir=target.parent) as tmp:
            private=StagingStorageResource(local_dir=tmp,use_local=True)
            for n,b in payloads.items():immutable(private,n,b)
            immutable(private,'metadata/observations_candidate_ready.json',encode(ready))
            original,original_manifest=original_provenance_bundle(storage,ready['version'],packs,manifest,identity,create=True)
            if original!=packs or original_manifest!=manifest:
                raise ObservationQuarantineError('concurrent_original_provenance_winner_retry_required')
            os.rename(tmp,target)


def stage_tsunami_observations_candidate(storage: StagingStorageResource,*,capture_dir: Path|None=None) -> dict:
    require_local(storage);packs=acquire(storage,capture_dir)
    try:
        http=unpack_captures(packs);validate_complete_bundle(http);rows,audit,numeric_decodes=decode_rows(http)
        manifest=provenance(http);table=typed_table(rows,audit);identity=processing_identity();fingerprint=sha(encode(identity))
        capture_fingerprint=sha(encode({n:sha(v[0]) for n,v in sorted(http.items())}));del http
        version=f'v0.0.0-draft-observations-{capture_fingerprint[:16]}-{fingerprint[:16]}';prefix=f'{SLUG}/{SLUG}/{version}'
        native_buffer=io.BytesIO();pq.write_table(table,native_buffer,compression='zstd')
        manifest.update(processing_fingerprint=fingerprint,prebuilt_output_policy=native_prebuilt_policy(native_buffer.getvalue()))
        def result():return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':FEATURE_COUNT,
            'status':'working_draft_candidate_ready','processing_fingerprint':fingerprint}
        with candidate_version_lock(storage,prefix) as target:
            if target.exists():
                try:
                    packs,manifest=original_provenance_bundle(storage,version,packs,manifest,identity)
                    marker=target/'metadata/observations_candidate_ready.json'
                    if not marker.exists():raise ObservationQuarantineError('missing_candidate_ready')
                    ready=strict_json(marker.read_bytes())
                    if (set(ready.get('objects',{}))!=expected_object_names() or ready.get('version')!=version
                        or ready.get('source_sha256')!=PIN or ready.get('processing_fingerprint')!=fingerprint
                        or ready.get('candidate_records_sha256')!=sha(normalized_rows(rows))
                        or ready.get('approved') is not False or ready.get('status')!='working_draft_candidate_ready'):
                        raise ObservationQuarantineError('cached_ready_identity_or_roster_changed')
                    for n,digest in ready['objects'].items():
                        key=prefix+'/'+n;snapshot=storage.object_snapshot(key)
                        if snapshot is None or sha(storage.read_key(key))!=digest or storage.object_snapshot(key)!=snapshot:
                            raise ObservationQuarantineError('cached_ready_object_hash_changed:'+n)
                    reconcile_cached_candidate(storage,prefix,ready,rows,audit,numeric_decodes,table,packs,manifest,identity)
                    return result()
                except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error,zipfile.BadZipFile) as exc:
                    mark_trust_incident(storage,version)
                    retained=quarantine_cached_candidate(storage,prefix,str(exc),already_locked=True)
                    raise ObservationQuarantineError(f'{exc}; complete_failed_cache_retained:{retained}') from exc
            key,first,incident=trust_keys(version)
            if storage.object_exists(key) or storage.object_exists(first):
                packs,manifest=original_provenance_bundle(storage,version,packs,manifest,identity)
            elif storage.object_exists(incident):
                raise ObservationQuarantineError('previous_candidate_trust_failure_original_operator_records_required')
        with tempfile.TemporaryDirectory(prefix='observations-private-native-build-') as tmp:
            payloads=canonical_payloads(rows,audit,numeric_decodes,table,packs,manifest,identity,Path(tmp))
            ready={'version':version,'status':'working_draft_candidate_ready','approved':False,'source_sha256':PIN,
                'candidate_records_sha256':sha(normalized_rows(rows)),'processing_fingerprint':fingerprint,
                'objects':{n:sha(b) for n,b in payloads.items()}}
            commit_complete_candidate(storage,prefix,payloads,ready,packs,manifest,identity)
        return result()
    except (ValueError,TypeError,KeyError,OSError,RuntimeError,sqlite3.Error,zipfile.BadZipFile) as exc:
        # Invalid existing caches were detached while holding their observation
        # lock. An installation conflict never detaches a newer complete winner.
        for n,b in packs.items():quarantine(storage,b,{'archive':n,'stage':'candidate_build_or_replay'},str(exc))
        raise ObservationQuarantineError(str(exc)) from exc
