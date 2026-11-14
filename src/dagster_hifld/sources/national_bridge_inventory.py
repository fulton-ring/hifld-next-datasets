"""FHWA National Bridge Inventory 2025 ASCII reconstruction draft; never release approval."""
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
from decimal import Decimal, ROUND_HALF_UP
from collections import Counter
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
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
from dagster_hifld.release_approval import validate_release_provenance, verifier_implementation_sha256
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.epa_frs_common import _validate_cached_catalog_sidecars
from dagster_hifld.validation import sha256_file

SLUG='national-bridge-inventory-nbi'
PRODUCER='United States Department of Transportation, Federal Highway Administration'
LANDING='https://www.fhwa.dot.gov/bridge/nbi/ascii.cfm'
FILES_PAGE='https://www.fhwa.dot.gov/bridge/nbi/ascii2025.cfm'
FORMAT_PAGE='https://www.fhwa.dot.gov/bridge/nbi/format.cfm'
CROSSWALK_URL='https://www.fhwa.dot.gov/bridge/snbi/datacrosswalk.xlsx'
DISCLAIMER='https://www.fhwa.dot.gov/bridge/nbi/disclaim.cfm?nbiYear=2025&nbiState=DC25'
PROGRAM='https://www.fhwa.dot.gov/bridge/nbi.cfm'
LICENSE='https://www.fhwa.dot.gov/webpolicies'
READY='metadata/nbi_candidate_ready.json'
SPACE_FLOOR=4_000_000_000
MAX_AUX_BYTES=32_000_000
MAX_BODY_BYTES=64*1024*1024
MAX_PARQUET_BYTES=1_500_000_000
RECORD_BYTES=445
SHORT_RECORD_FLOOR=393
EXPECTED_TOTAL_COUNT=624193
EXPECTED_ID_ROSTER_SHA256='ee0414c72c0a7cd7ea75767fed6eb103d4a2edc4ed9c1f89fb28974145761856'
EXPECTED_COMPOSITE_DUPLICATE_COUNT=2
EXPECTED_DUPLICATE_KEYS=[('251','N100108PDDOTNBI'),('513','000000000001826')]
EXPECTED_ZERO_COORDINATE_COUNT=6
EXPECTED_OUT_OF_ENVELOPE_COUNT=56
EXPECTED_BLANK_COORDINATE_COUNT=22
EXPECTED_PARTIAL_COORDINATE_COUNT=1
EXPECTED_SIGNED_COORDINATE_COUNT=369
EXPECTED_WHITESPACE_COORDINATE_COUNT=1
EXPECTED_SHORT_RECORD_COUNT=1
EXPECTED_BLANK_STRUCTURE_NUMBER_COUNT=0
# The frozen 2025 release embeds 7 non-ASCII bytes in NY/PR location text
# (observed 2026-11-10); columns are decoded 1:1 as ISO-8859-1 so every
# source byte is preserved verbatim instead of guessing a legacy codepage.
EXPECTED_NON_ASCII_BYTES={'NY':5,'PR':2}
# Positive-longitude Item 1 codes (64=FM,66=GU,68=MP,69=UM,70=PW) and the
# negative-latitude code (60=AS) come from the official FHWA SNBI Data
# Crosswalk rows B.L.05/B.L.06; none occur in the 2025 state-file roster.
POSITIVE_LONGITUDE_CODES=frozenset({'64','66','68','69','70'})
NEGATIVE_LATITUDE_CODES=frozenset({'60'})
NATIVE_CRS_JSON={'$schema': 'https://proj.org/schemas/v0.7/projjson.schema.json', 'type': 'GeographicCRS', 'name': 'WGS 84', 'datum': {'type': 'GeodeticReferenceFrame', 'name': 'World Geodetic System 1984', 'ellipsoid': {'name': 'WGS 84', 'semi_major_axis': 6378137, 'inverse_flattening': 298.257223563}}, 'coordinate_system': {'subtype': 'ellipsoidal', 'axis': [{'name': 'Geodetic latitude', 'abbreviation': 'Lat', 'direction': 'north', 'unit': 'degree'}, {'name': 'Geodetic longitude', 'abbreviation': 'Lon', 'direction': 'east', 'unit': 'degree'}]}, 'id': {'authority': 'EPSG', 'code': 4326}}
# Official FHWA "NBI Record Format" fixed-width layout (format.cfm, fetched
# 2026-11-10): producer item name, 1-based inclusive start/end positions and
# declared length/type. Parent rows that only group sub-items (5,6,13,28,36,
# 42,43,44,50,54,55,75,92,93,98,108) are excluded; positions cover 1-445 once.
LAYOUT=[
('STATE_CODE_001',1,3,'3/N'),('STRUCTURE_NUMBER_008',4,18,'15/AN'),('RECORD_TYPE_005A',19,19,'1/AN'),('ROUTE_SIGNING_PREFIX_005B',20,20,'1/N'),('DESIGNATED_LEVEL_OF_SERVICE_005C',21,21,'1/N'),('ROUTE_NUMBER_005D',22,26,'5/AN'),('DIRECTIONAL_SUFFIX_005E',27,27,'1/N'),('HIGHWAY_AGENCY_DISTRICT_002',28,29,'2/AN'),('COUNTY_PARISH_CODE_003',30,32,'3/N'),('PLACE_CODE_004',33,37,'5/N'),('FEATURES_INTERSECTED_006A',38,61,'24/AN'),('CRITICAL_FACILITY_INDICATOR_006B',62,62,'1/AN'),('FACILITY_CARRIED_BY_STRUCTURE_007',63,80,'18/AN'),('LOCATION_009',81,105,'25/AN'),('INVENTORY_RTE_MIN_VERT_CLEARANCE_010',106,109,'4/N'),('KILOMETERPOINT_011',110,116,'7/N'),('BASE_HIGHWAY_NETWORK_012',117,117,'1/N'),('LRS_INVENTORY_ROUTE_013A',118,127,'10/AN'),('SUBROUTE_NUMBER_013B',128,129,'2/N'),('LATITUDE_016',130,137,'8/N'),('LONGITUDE_017',138,146,'9/N'),('BYPASS_DETOUR_LENGTH_019',147,149,'3/N'),('TOLL_020',150,150,'1/N'),('MAINTENANCE_RESPONSIBILITY_021',151,152,'2/N'),('OWNER_022',153,154,'2/N'),('FUNCTIONAL_CLASS_OF_INVENTORY_RTE_026',155,156,'2/N'),('YEAR_BUILT_027',157,160,'4/N'),('LANES_ON_STRUCTURE_028A',161,162,'2/N'),('LANES_UNDER_STRUCTURE_028B',163,164,'2/N'),('AVERAGE_DAILY_TRAFFIC_029',165,170,'6/N'),('YEAR_OF_AVERAGE_DAILY_TRAFFIC_030',171,174,'4/N'),('DESIGN_LOAD_031',175,175,'1/N'),('APPROACH_ROADWAY_WIDTH_032',176,179,'4/N'),('BRIDGE_MEDIAN_033',180,180,'1/N'),('SKEW_034',181,182,'2/N'),('STRUCTURE_FLARED_035',183,183,'1/N'),('BRIDGE_RAILINGS_036A',184,184,'1/AN'),('TRANSITIONS_036B',185,185,'1/AN'),('APPROACH_GUARDRAIL_036C',186,186,'1/AN'),('APPROACH_GUARDRAIL_ENDS_036D',187,187,'1/AN'),('HISTORICAL_SIGNIFICANCE_037',188,188,'1/N'),('NAVIGATION_CONTROL_038',189,189,'1/AN'),('NAVIGATION_VERTICAL_CLEARANCE_039',190,193,'4/N'),('NAVIGATION_HORIZONTAL_CLEARANCE_040',194,198,'5/N'),('STRUCTURE_OPEN_POSTED_CLOSED_041',199,199,'1/AN'),('TYPE_OF_SERVICE_ON_BRIDGE_042A',200,200,'1/N'),('TYPE_OF_SERVICE_UNDER_BRIDGE_042B',201,201,'1/N'),('KIND_OF_MATERIAL_DESIGN_043A',202,202,'1/N'),('TYPE_OF_DESIGN_CONSTRUCTION_043B',203,204,'2/N'),('KIND_OF_MATERIAL_DESIGN_044A',205,205,'1/N'),('TYPE_OF_DESIGN_CONSTRUCTION_044B',206,207,'2/N'),('NUMBER_OF_SPANS_IN_MAIN_UNIT_045',208,210,'3/N'),('NUMBER_OF_APPROACH_SPANS_046',211,214,'4/N'),('INVENTORY_RTE_TOTAL_HORZ_CLEARANCE_047',215,217,'3/N'),('LENGTH_OF_MAXIMUM_SPAN_048',218,222,'5/N'),('STRUCTURE_LENGTH_049',223,228,'6/N'),('LEFT_CURB_SIDEWALK_WIDTH_050A',229,231,'3/N'),('RIGHT_CURB_SIDEWALK_WIDTH_050B',232,234,'3/N'),('BRIDGE_ROADWAY_WIDTH_CURB_TO_CURB_051',235,238,'4/N'),('DECK_WIDTH_OUT_TO_OUT_052',239,242,'4/N'),('MIN_VERT_CLEAR_OVER_BRIDGE_ROADWAY_053',243,246,'4/N'),('REFERENCE_FEATURE_054A',247,247,'1/AN'),('MINIMUM_VERTICAL_UNDERCLEARANCE_054B',248,251,'4/N'),('REFERENCE_FEATURE_055A',252,252,'1/AN'),('MINIMUM_LATERAL_UNDERCLEARANCE_055B',253,255,'3/N'),('MIN_LATERAL_UNDERCLEAR_ON_LEFT_056',256,258,'3/N'),('DECK_058',259,259,'1/AN'),('SUPERSTRUCTURE_059',260,260,'1/AN'),('SUBSTRUCTURE_060',261,261,'1/AN'),('CHANNEL_CHANNEL_PROTECTION_061',262,262,'1/AN'),('CULVERTS_062',263,263,'1/AN'),('METHOD_USED_TO_DETERMINE_OPERATING_RATING_063',264,264,'1/N'),('OPERATING_RATING_064',265,267,'3/N'),('METHOD_USED_TO_DETERMINE_INVENTORY_RATING_065',268,268,'1/N'),('INVENTORY_RATING_066',269,271,'3/N'),('STRUCTURAL_EVALUATION_067',272,272,'1/AN'),('DECK_GEOMETRY_068',273,273,'1/AN'),('UNDERCLEAR_VERTICAL_HORIZONTAL_069',274,274,'1/AN'),('BRIDGE_POSTING_070',275,275,'1/N'),('WATERWAY_ADEQUACY_071',276,276,'1/AN'),('APPROACH_ROADWAY_ALIGNMENT_072',277,277,'1/AN'),('TYPE_OF_WORK_PROPOSED_075A',278,279,'2/N'),('WORK_DONE_BY_075B',280,280,'1/AN'),('LENGTH_OF_STRUCTURE_IMPROVEMENT_076',281,286,'6/N'),('INSPECTION_DATE_090',287,290,'4/N'),('DESIGNATED_INSPECTION_FREQUENCY_091',291,292,'2/N'),('FRACTURE_CRITICAL_DETAILS_092A',293,295,'3/AN'),('UNDERWATER_INSPECTION_092B',296,298,'3/AN'),('OTHER_SPECIAL_INSPECTION_092C',299,301,'3/AN'),('FRACTURE_CRITICAL_DETAILS_DATE_093A',302,305,'4/AN'),('UNDERWATER_INSPECTION_DATE_093B',306,309,'4/AN'),('OTHER_SPECIAL_INSPECTION_DATE_093C',310,313,'4/AN'),('BRIDGE_IMPROVEMENT_COST_094',314,319,'6/N'),('ROADWAY_IMPROVEMENT_COST_095',320,325,'6/N'),('TOTAL_PROJECT_COST_096',326,331,'6/N'),('YEAR_OF_IMPROVEMENT_COST_ESTIMATE_097',332,335,'4/N'),('NEIGHBORING_STATE_CODE_098A',336,338,'3/AN'),('PERCENT_RESPONSIBILITY_098B',339,340,'2/N'),('BORDER_BRIDGE_STRUCTURE_NUMBER_099',341,355,'15/AN'),('STRAHNET_HIGHWAY_DESIGNATION_100',356,356,'1/N'),('PARALLEL_STRUCTURE_DESIGNATION_101',357,357,'1/AN'),('DIRECTION_OF_TRAFFIC_102',358,358,'1/N'),('TEMPORARY_STRUCTURE_DESIGNATION_103',359,359,'1/AN'),('HIGHWAY_SYSTEM_OF_INVENTORY_ROUTE_104',360,360,'1/N'),('FEDERAL_LANDS_HIGHWAYS_105',361,361,'1/N'),('YEAR_RECONSTRUCTED_106',362,365,'4/N'),('DECK_STRUCTURE_TYPE_107',366,366,'1/AN'),('TYPE_OF_WEARING_SURFACE_108A',367,367,'1/AN'),('TYPE_OF_MEMBRANE_108B',368,368,'1/AN'),('DECK_PROTECTION_108C',369,369,'1/AN'),('AVERAGE_DAILY_TRUCK_TRAFFIC_109',370,371,'2/N'),('DESIGNATED_NATIONAL_NETWORK_110',372,372,'1/N'),('PIER_ABUTMENT_PROTECTION_111',373,373,'1/N'),('NBIS_BRIDGE_LENGTH_112',374,374,'1/AN'),('SCOUR_CRITICAL_BRIDGES_113',375,375,'1/AN'),('FUTURE_AVERAGE_DAILY_TRAFFIC_114',376,381,'6/N'),('YEAR_OF_FUTURE_AVG_DAILY_TRAFFIC_115',382,385,'4/N'),('MINIMUM_NAVIGATION_VERTICAL_CLEARANCE_VERTICAL_LIFT_BRIDGE_116',386,389,'4/N'),('FEDERAL_AGENCY_INDICATOR',390,390,'1/AN'),('SUBMITTED_BY',391,392,'2/N'),('WASHINGTON_HQ_USE_RESERVED',393,433,'-'),('BRIDGE_CONDITION',434,434,'1/AN'),('LOWEST_CONDITION_CODE',435,435,'1/AN'),('DECK_AREA',436,445,'10/N'),
]
EXPECTED_FILES={
'AK':('5346d3bf7781c8fd1d71065229deb767bb6d934e71ae1853cc21820d23908074',742020,1660,'CRLF','020',0),'AL':('677739f195a8a84e0e325577a1fdd5cb734a9072254d19328f575f9640af058c',7232907,16181,'CRLF','014',0),'AR':('3a9e24e1275388fae8a59247f9bcf5eecdedb0ca42f90b0a050ba4eb99972ea4',5801166,12978,'CRLF','056',0),'AZ':('cf3415427146f676db9fd0ad68ad14fa04f020d86135b189df1164aa876afaae',3838389,8587,'CRLF','049',0),'CA':('741330e19676529ced5cddc4f9905b2299331404ec8aa4efab1ffcc164cdb81b',11610825,25975,'CRLF','069',0),'CO':('cd7f6e13146ff85a49e8e348199b6392ef93ae0c94ace83af732d06a23534874',4018530,8990,'CRLF','088',0),'CT':('1ab54eb6b15cc8aa5003db4566652a59abf7b6406f4a8abde904df55ae716061',1950261,4363,'CRLF','091',0),'DC':('7c1b9f2685b1b4dd86da88ec4084e5d5b940fd8feec6eee109a6acc967ad2b1f',116667,261,'CRLF','113',0),'DE':('9248f0c8e7770c384d3a6b7ca8de318dc88ce1f746f25ed4e87bd7421fd68ac2',390678,874,'CRLF','103',0),'FL':('f6a45e9e1c83636bfbe57584742a602b3e39fa38f029e7ace458d3c2b39e0c61',5842290,13070,'CRLF','124',0),'GA':('4880363d003735cf6f4165a5b90eaf5f3d5b6596d3b14742cc207181e1ec2240',6745230,15090,'CRLF','134',0),'HI':('9d6f7dde060c15e4287bb8bc057929a2f74cd15a87163e47beb39789ae30f695',527460,1180,'CRLF','159',0),'IA':('f59887dee2118b2da6c5136f0df0502976cbacacb7c303ca1cceb3e2313e201f',10601052,23716,'CRLF','197',0),'ID':('0243f2a416fc8513894da880e046ec92b714cc3612aa5db94de6301f8f505f8d',2076762,4646,'CRLF','160',0),'IL':('6b86338dca072bd632e8e97d0e315fd3c70b43cc73b993d2785de3c2b412e145',12036369,26927,'CRLF','175',0),'IN':('6b2164d5c5dfc94d06e583bd5ed479e7b647900f3e4e53a21a58d2d2ad0f4398',8735274,19542,'CRLF','185',0),'KS':('ee5ea740ffe77c26d04a708871c292514fc024f6067241d7f01332189a4c11e3',11126277,24891,'CRLF','207',0),'KY':('4684a0ac1f75dc657bc028ecc8dd0de5d3671707b89c2f24593bf41ed251d0c7',6521730,14590,'CRLF','214',0),'LA':('80cf3560df9e3ef14ab6a3b01003afc8cdde0c225e9f7b9a38ab448b542e01ba',5669748,12684,'CRLF','226',0),'MA':('6c494d47344898478c4baeaeb8b748efcc4c3d62cd26b4b0bf12719b23322f58',2374017,5311,'CRLF','251',0),'MD':('9a047d35f1ad755b697a63fcabf7bde7bb919fa4a5bda41a30c54e510273534b',2458500,5500,'CRLF','243',0),'ME':('2eb7566d00dca4cecab7736a3bfe4efc7557658712d25080483779fe24468733',1136274,2542,'CRLF','231',0),'MI':('0ef39f31b2934b6747d35c5cc9cac35aaaf3ce5eb75b6c60c119f6e5a59cc764',5094459,11397,'CRLF','265',0),'MN':('0ebb6a77d275bb32c1299a5d1b791a6193c398c61573277f7ab1e404a3300f75',6057297,13551,'CRLF','275',0),'MO':('0d42e2ad0ebb4d4d1cfef969d76f683abdf4092a536cf646479c64b0cda4680e',11017209,24647,'CRLF','297',0),'MS':('007b014065fb77662b77373b38206c0afbdd12837aeeb375cc8731962e58de49',7469817,16711,'CRLF','284',0),'MT':('8103d79181c7e1cd4841d2a09f4d917be927ab95b9cc946c30b56710b459260e',2340045,5235,'CRLF','308',0),'NC':('3ee91fb0f7d4f4be751468d9f43971b48d8c0b03ea9189cdaad96d3388f5f009',8646297,19343,'CRLF','374',1),'ND':('4963747671fb15bef746fc8f54619dfaa4fd42a2c1d85a5f0e464f0b13df7b48',1899750,4250,'CRLF','388',0),'NE':('a5a174d126932449af830ff30cd61c77189dcf7e07f34d6de91e812effcfc1f9',6889164,15412,'CRLF','317',0),'NH':('e83421afe6994a772784912e70aa28d3fc63b6e54595ef6d85f3b122c7f4f15f',1139403,2549,'CRLF','331',0),'NJ':('fe4bfd56c42b860cf961add133f715b21325df0e2439a853c3187ad5d493a740',3050775,6825,'CRLF','342',0),'NM':('b7c79d3dc0cd5df5c5348d355b94f22fe2d8983b1e958f9c2f8c48d4b49cb12a',1802751,4033,'CRLF','356',0),'NV':('3b16767259d054937e56e6c045038457e60d8144ebf960b8ef97016815f2834c',948981,2123,'CRLF','329',0),'NY':('f4d9297492276fd7109d2b757fe0969411485951ca4f6fe33b8de38b95c366d5',7896702,17666,'CRLF','362',0),'OH':('ffabdbae5480b6fc58260703d91ddf7cde483ba23b4ba52bbdcbdbafd48826fe',11940711,26713,'CRLF','395',0),'OK':('803f18287b9b39e8669e5c8e0d95af20eb40151c92e074fb3b2c17173d56b9c0',10247922,22926,'CRLF','406',0),'OR':('177091e6c9907c4ba1faaeaa6ee70d7153f304f32ebedb5beb09a34c4ca4d2af',3723063,8329,'CRLF','410',0),'PA':('5ad9e424fc74bf0887bea01145eb5762fdc691e1922df87e111f0922ef8f8a67',10421358,23314,'CRLF','423',0),'PR':('32fc6854916a0638e81564f1f399f7a7e24b187e27a9671731f8bfd40767f299',1087998,2434,'CRLF','721',0),'RI':('c8da15c0cd3c377c37474622371a0b502e5c62b3b1a712b315a3e2085f10bea8',351789,787,'CRLF','441',0),'SC':('8ef0dfb6e720a0fb735482cf354f2cc991ba34b8c06d1616741d329a355ed2ba',4248288,9504,'CRLF','454',0),'SD':('9aef6318346791d30ca10ff565e147cbdc3cb1197a9c746d86918f53873e141f',2629701,5883,'CRLF','468',0),'TN':('61d9fa06a3893413471afb0c23e2376a6748c03a45401fa1fa61e3e75fff591b',9107178,20374,'CRLF','474',0),'TX':('7b1e8d199b7e544df6acdcaf94a628a1feef70f25261076ac26ee05da5b43fa6',25457097,56951,'CRLF','486',0),'UT':('9d1c7f53b4a16423df11cfdfa2c2c3c1bd281b7b8e115969829abe9ff9629808',1405368,3144,'CRLF','498',0),'VA':('d7850d6f6656ff1581baefefc67ebcc220782755eacbc8017f4ba3b297566db9',6321921,14143,'CRLF','513',0),'VI':('97b40ec1ba39afbb4cbedda7ab8fe252721090a390c37b9d53b3f7920c5a06e5',10728,24,'CRLF','780',0),'VT':('4904becbc196211880734191340ecfa4f3e0c4ccf7db56d1bedaa11edfeecfd8',1281102,2866,'CRLF','501',0),'WA':('7932a6197a9a83ac162d172f3c63c5ce46a5274d578241f4addad6d0e02c89d6',3808440,8520,'CRLF','530',0),'WI':('6bdc0415fa4c71373c25918602ae8fe8a0c7ee337e43613091ad24a64d074a4c',6480606,14498,'CRLF','555',0),'WV':('30390ab47728de89a2d50f66d5be7f4b68ba04bd4c6bfabfc4c265495452130b',3283215,7345,'CRLF','543',0),'WY':('8ec2fbdfe937541a6d923a11f71d6488ff5d938aa16225da27aada3f7d1e0cb8',1402686,3138,'CRLF','560',0),
}
URLS={'landing':LANDING,'files_page':FILES_PAGE,'format':FORMAT_PAGE,'crosswalk':CROSSWALK_URL,'disclaimer':DISCLAIMER}
for _token in EXPECTED_FILES: URLS['state_'+_token]='https://www.fhwa.dot.gov/bridge/nbi/2025/'+_token+'25.txt'
del _token
TOKENS=tuple(EXPECTED_FILES)


class NBIQuarantineError(ValueError):pass
class IncidentWriteError(NBIQuarantineError):pass


def encode(value):
    return (json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()

def digest(body):
    return hashlib.sha256(body).hexdigest()

def strict_json(body):
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise NBIQuarantineError('duplicate_json_key')
            result[key]=value
        return result
    value=json.loads(body,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
    if not isinstance(value,dict) or 'error' in value: raise NBIQuarantineError('metadata_must_be_object_without_error')
    return value

def local_only(storage):
    if isinstance(storage,PublishedStorageResource)or storage.bucket or not storage.use_local or not storage.local_dir or storage.prefix:
        raise NBIQuarantineError('explicit_isolated_local_staging_required')
    root=Path(storage.local_dir).absolute()
    if '..'in root.parts or any(p.is_symlink()for p in [root,*root.parents]):raise NBIQuarantineError('staging_path_symlink_refused')
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<SPACE_FLOOR:raise NBIQuarantineError('disk_floor_before_acquisition')
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise NBIQuarantineError('staging_alias_or_special_file_refused')

def immutable(storage,key,body):
    snapshot=storage.object_snapshot(key)
    if snapshot is None: storage.write_key_if_unchanged(key,body,None)
    elif storage.read_key(key)!=body or storage.object_snapshot(key)!=snapshot:
        raise NBIQuarantineError('immutable_capture_changed:'+key)

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
        if not stat.S_ISREG(before.st_mode)or before.st_nlink!=1 or before.st_size>limit:raise NBIQuarantineError('unsafe_or_oversized_original_input')
        body=stream.read(limit+1);after=os.fstat(stream.fileno())
    identity=lambda v:(v.st_dev,v.st_ino,v.st_size,v.st_mtime_ns,v.st_ctime_ns,v.st_nlink)
    if len(body)!=before.st_size or len(body)>limit or identity(before)!=identity(after):raise NBIQuarantineError('changed_or_incomplete_original_input')
    return body

def candidate_roster(root):
    names=set()
    for path in root.rglob('*'):
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode):continue
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise NBIQuarantineError('candidate_alias_or_special_file_refused')
        names.add(str(path.relative_to(root)))
    return names

def construction_record_key(version):return f'_construction-state/{SLUG}/{version}.json'

def incident_directory(version):return f'_construction-state/{SLUG}/incident-{version}'

def state_present(storage,version):
    key=construction_record_key(version);root=Path(storage.local_dir).absolute()
    return any((root/value).exists()for value in(key,key+'.first-construction.json',key+'.evidence'))

def refuse_observed_loss(storage,version):
    if(Path(storage.local_dir).absolute()/incident_directory(version)).exists():raise NBIQuarantineError('permanent_observed_original_trust_loss_requires_explicit_reviewed_restore')

def record_observed_loss(storage,version,fingerprint,reason):
    key=incident_directory(version)+'/record.json'
    body=canonical_construction_bundle(version,fingerprint,{'approved':False,'kind':'observed_original_trust_loss','reason':reason,
        'operator_action':'Restore exact original witnesses and explicitly retain/review this negative incident; no automatic bootstrap.'})
    seal_first_construction(storage,key,version,fingerprint,body)

def write_private(root,name,body):
    path=root/name;path.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        info=os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode)or info.st_nlink!=1:raise NBIQuarantineError('private_write_alias_or_special_file_refused')
        os.ftruncate(descriptor,0)
        if os.write(descriptor,body)!=len(body):raise NBIQuarantineError('incomplete_private_write:'+name)
        os.fsync(descriptor)
    finally:os.close(descriptor)
    if candidate_bytes(root,name)!=body:raise NBIQuarantineError('incomplete_private_write:'+name)

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
    if not actual.equals(parquet_table(table),check_metadata=True):raise NBIQuarantineError('full_typed_native_output_drift')
    for w in actual['geometry'].to_pylist():
        if w is None:continue
        p=shapely.from_wkb(w)
        if p.geom_type!='Point'or not p.is_valid or p.is_empty:raise NBIQuarantineError('invalid_native_output_geometry')

def candidate_bytes(root,name):
    if Path(name).is_absolute()or'..'in Path(name).parts:raise NBIQuarantineError('unsafe_candidate_key')
    return bounded_input(root/name,MAX_BODY_BYTES if name.endswith('.body')else(MAX_PARQUET_BYTES if name.endswith('.parquet')else MAX_AUX_BYTES))

def source_payloads(captures,identity,audit,manifest):
    values={'metadata/source_manifest.json':encode(manifest),'metadata/processing_identity.json':encode(identity),'metadata/nbi_source_audit.json':encode(audit)}
    for n,(b,r)in captures.items():values['raw/'+n+'.body']=b;values['raw/'+n+'.receipt.json']=encode(r)
    return values

def original_context(storage,version,fingerprint,captures,identity,manifest,*,create=False):
    key=construction_record_key(version)
    if create:
        inspect_captures(captures);validate_construction_code_identity(Path(__file__).parents[3],manifest['code_commit'],identity['files'])
        body=seal_first_construction(storage,key,version,fingerprint,canonical_construction_bundle(version,fingerprint,{'manifest':manifest,'captures':{n:{'body_key':f'_raw-captures/{SLUG}/{digest(b)}/{n}.body','receipt':r}for n,(b,r)in captures.items()}}))
    else:body=load_first_construction(storage,key,version,fingerprint)
    payload=strict_json(body)['payload'];original={}
    if set(payload)!={'manifest','captures'}or set(payload['captures'])!=set(captures):raise NBIQuarantineError('invalid_original_context')
    for n,item in payload['captures'].items():
        key=f"_raw-captures/{SLUG}/{item['receipt']['sha256']}/{n}.body"
        if item['body_key']!=key:raise NBIQuarantineError('invalid_original_raw_key')
        original[n]=(bounded_input(Path(storage.local_dir)/key,MAX_BODY_BYTES),item['receipt'])
    a,_=inspect_captures(original);b,_=inspect_captures(captures)
    if not a.equals(b,check_metadata=True):raise NBIQuarantineError('original_source_population_drift')
    old=payload['manifest'];obj=manifest['prebuilt_output_policy']['formats']['geoparquet']['objects'][0]
    expected=manifest_for(original,identity,{k:obj[k]for k in('bytes','sha256')})
    if encode({k:v for k,v in old.items()if k!='code_commit'})!=encode({k:v for k,v in expected.items()if k!='code_commit'}):raise NBIQuarantineError('original_manifest_context_drift')
    validate_release_provenance(old);validate_output_policy(old);validate_construction_code_identity(Path(__file__).parents[3],old['code_commit'],identity['files']);return original,old

def validate_candidate(root,table,identity,captures,audit,manifest,version,*,cached=False,storage=None):
    roster=candidate_roster(root);expected=source_payloads(captures,identity,audit,manifest);names=set(expected)|{f'geoparquet/{SLUG}.parquet'}
    ready={'approved':False,'version':version,'processing_fingerprint':digest(encode(identity)),'capture_fingerprint':source_fingerprint(captures),'objects':{n:sha256_file(root/n)for n in names}}
    if not(names|{READY}).issubset(roster):raise NBIQuarantineError('missing_candidate_object')
    if not cached and roster!=names|{READY}:raise NBIQuarantineError('private_candidate_roster_drift')
    if cached:
        allowed={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/geoparquet_layout.json','metadata/resolved_provenance.json','metadata/source/source_manifest.json','metadata/source/data_dictionary.json','metadata/source/quality_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'}
        if roster-(names|{READY}|allowed):raise NBIQuarantineError('unknown_cached_object')
        for name in roster-(names|{READY}):strict_json(candidate_bytes(root,name))
        snapshot_names={'metadata/source/source_manifest.json',
            'metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'}
        catalog_sidecars={'metadata/quality_manifest.json','metadata/data_dictionary.json','metadata/resolved_provenance.json',
            'metadata/source/quality_manifest.json','metadata/source/data_dictionary.json'}
        for name in('metadata/source/source_manifest.json','metadata/source/version_'+digest(encode(manifest))+'_source_manifest.json'):
            if name in roster and candidate_bytes(root,name)!=encode(manifest):raise NBIQuarantineError('cached_source_snapshot_drift')
        if roster & snapshot_names and roster & snapshot_names != snapshot_names:
            raise NBIQuarantineError('cached_source_manifest_snapshot_incomplete')
        if roster & catalog_sidecars:
            if not{'metadata/quality_manifest.json','metadata/data_dictionary.json'}<=roster:
                raise NBIQuarantineError('cached_catalog_metadata_incomplete')
            if roster & snapshot_names != snapshot_names:
                raise NBIQuarantineError('cached_source_manifest_snapshot_incomplete')
        if roster & catalog_sidecars:
            if storage is None:raise NBIQuarantineError('catalog_validation_requires_explicit_storage_context')
            prefix=f'{SLUG}/{SLUG}/{version}'
            actual_keys={storage._ensure_prefixed(f'{prefix}/{name}') for name in roster}
            try:_validate_cached_catalog_sidecars(storage,prefix,actual_keys)
            except (OSError,UnicodeError,TypeError,ValueError) as exc:
                raise NBIQuarantineError('cached_catalog_sidecar_semantics_invalid') from exc
    for n,b in expected.items():
        if candidate_bytes(root,n)!=b:raise NBIQuarantineError('candidate_source_bytes_drift:'+n)
    if candidate_bytes(root,READY)!=encode(ready):raise NBIQuarantineError('candidate_ready_drift')
    validate_output(root,table)


def split_state_body(body,token):
    if not body.endswith(b'\n'):raise NBIQuarantineError('state_file_missing_final_record_terminator:'+token)
    lines=body.split(b'\n')[:-1]
    if not lines:raise NBIQuarantineError('state_file_empty:'+token)
    payload_crlf=all(l.endswith(b'\r')for l in lines);payload_lf=not any(l.endswith(b'\r')for l in lines)
    style='CRLF'if payload_crlf else('LF'if payload_lf else 'MIXED')
    payloads=[];short=0
    for l in lines:
        payload=l[:-1]if payload_crlf else l
        if len(payload)==RECORD_BYTES:pass
        elif SHORT_RECORD_FLOOR<=len(payload)<RECORD_BYTES:short+=1
        else:raise NBIQuarantineError('state_record_length_outside_official_layout:'+token)
        payloads.append(payload)
    return payloads,style,short


def coordinate_decimal(raw,token):
    text=raw.decode('ascii')
    sign=None
    if text[:1] in '+-':
        sign=-1 if text[0]=='-' else 1
        text=text[1:]
    degrees=len(text)-6
    if degrees<1 or len(text)>9:raise NBIQuarantineError('state_coordinate_not_fixed_width_digits:'+token)
    # The producer transition tool splits the raw field by position and
    # numerically coerces each DMS segment (tolerating pad whitespace), so a
    # space-shifted submission such as b' 3612109' parses as 3d61m21.09s.
    seg_deg=text[:degrees].strip();seg_min=text[degrees:degrees+2].strip()
    seg_sec=text[degrees+2:degrees+4].strip();seg_hun=text[degrees+4:degrees+6].strip()
    if not(seg_deg.isdigit()and seg_min.isdigit()and seg_sec.isdigit()and seg_hun.isdigit()):
        raise NBIQuarantineError('state_coordinate_not_fixed_width_digits:'+token)
    value=Decimal(seg_deg)+Decimal(seg_min)/60+Decimal(seg_sec+'.'+seg_hun)/3600
    return sign,value.quantize(Decimal('0.000001'),rounding=ROUND_HALF_UP)


def geometry_for(payload,token):
    state=payload[0:3].decode('ascii')
    raw16=payload[129:137];raw17=payload[137:146]
    blank16=raw16.strip()==b'';blank17=raw17.strip()==b''
    if blank16 and blank17:return None,'blank',False,False,False
    if blank16 or blank17:return None,'partial',False,False,False
    sign16,lat=coordinate_decimal(raw16,token);sign17,lon=coordinate_decimal(raw17,token)
    trimmed=state.lstrip('0')or '0'
    if trimmed in NEGATIVE_LATITUDE_CODES:lat=-lat
    if trimmed not in POSITIVE_LONGITUDE_CODES:lon=-lon
    if sign16 is not None and lat!=0 and sign16!=(1 if lat>0 else -1):raise NBIQuarantineError('state_coordinate_sign_contradicts_documented_hemisphere:'+token)
    if sign17 is not None and lon!=0 and sign17!=(1 if lon>0 else -1):raise NBIQuarantineError('state_coordinate_sign_contradicts_documented_hemisphere:'+token)
    signed=sign16 is not None or sign17 is not None
    padded=b' ' in raw16 or b' ' in raw17
    zero=lat==0 and lon==0
    return struct.pack('<BIdd',1,1,float(lon),float(lat)),'present',signed,padded,zero


def parse_state_file(body,token,expected):
    if digest(body)!=expected[0]or len(body)!=expected[1]:raise NBIQuarantineError('state_file_bytes_drift:'+token)
    non_ascii=sum(1 for b in body if b>127)
    if non_ascii!=EXPECTED_NON_ASCII_BYTES.get(token,0):raise NBIQuarantineError('state_non_ascii_byte_drift_requires_review:'+token)
    payloads,style,short=split_state_body(body,token)
    if len(payloads)!=expected[2]or style!=expected[3]or short!=expected[5]:raise NBIQuarantineError('state_file_shape_drift:'+token)
    expected_state=expected[4]
    columns={n:[] for n,_,_,_ in LAYOUT};geometry=[];ids=[];zero=blank=partial=signed=padded=0;out_of_envelope=0;blank_structure=0
    for payload in payloads:
        state=payload[0:3]
        if state.decode('ascii')!=expected_state:raise NBIQuarantineError('state_item1_code_drift:'+token)
        for n,s,e,_ in LAYOUT:
            columns[n].append(payload[s-1:e].decode('iso-8859-1')if len(payload)>=e else None)
        structure=columns[STRUCTURE_NUMBER][len(ids)]
        if not structure.strip():blank_structure+=1
        ids.append((expected_state,structure))
        point,category,was_signed,was_padded,was_zero=geometry_for(payload,token)
        if point is None:
            if category=='blank':blank+=1
            else:partial+=1
        else:
            if was_signed:signed+=1
            if was_padded:padded+=1
            if was_zero:zero+=1
            else:
                lon_value=struct.unpack('<d',point[5:13])[0];lat_value=struct.unpack('<d',point[13:21])[0]
                if not(-180<=lon_value<=180 and -90<=lat_value<=90):out_of_envelope+=1
        geometry.append(point)
    arrays=[pa.array(columns[n],type=pa.string())for n,_,_,_ in LAYOUT]+[pa.array(geometry,type=pa.binary())]
    return arrays,ids,{'rows':len(payloads),'style':style,'short':short,'zero':zero,'blank':blank,'partial':partial,'signed':signed,'padded':padded,'out_of_envelope':out_of_envelope,'blank_structure':blank_structure,'non_ascii_bytes':non_ascii}


def build_schema():
    fields=[pa.field(n,pa.string(),nullable=True,metadata={b'nbi_ascii_layout':encode({'name':n,'start':s,'end':e,'format':t})})for n,s,e,t in LAYOUT]
    fields.append(pa.field('geometry',pa.binary(),nullable=True,metadata={b'original_crs':b'EPSG:4326',b'original_geometry_type':b'esriGeometryPoint-equivalent NBI DMS-derived point'}))
    return pa.schema(fields)

STRUCTURE_NUMBER=next(n for n,_,_,_ in LAYOUT if n=='STRUCTURE_NUMBER_008')


def state_tokens(body):
    return {t.decode('ascii')for t in re.findall(rb'nbiState=([A-Z]{2})25',body)}


def inspect_captures(captures):
    if set(captures)!=set(URLS):raise NBIQuarantineError('capture_roster_drift')
    for n,(b,r)in captures.items():
        validate_receipt(n,b,r)
        if r['url']!=URLS[n]:raise NBIQuarantineError('capture_request_population_drift')
    tokens=state_tokens(captures['files_page'][0])
    if tokens!=set(TOKENS):raise NBIQuarantineError('producer_state_file_roster_drift')
    arrays=[];roster=[];per_state={};duplicates=Counter()
    for token in TOKENS:
        body=captures['state_'+token][0]
        columns,ids,stats=parse_state_file(body,token,EXPECTED_FILES[token])
        arrays.append(columns);roster.extend(ids);duplicates.update(ids);per_state[token]=stats
    if sum(p['blank_structure']for p in per_state.values())!=EXPECTED_BLANK_STRUCTURE_NUMBER_COUNT:raise NBIQuarantineError('blank_structure_number_drift_requires_review')
    blank_structure=sum(p['blank_structure']for p in per_state.values())
    non_ascii={t:per_state[t]['non_ascii_bytes']for t in TOKENS if per_state[t]['non_ascii_bytes']}
    extra=sum(v-1 for v in duplicates.values()if v>1)
    duplicate_keys=sorted(k for k,v in duplicates.items()if v>1)
    if extra!=EXPECTED_COMPOSITE_DUPLICATE_COUNT or duplicate_keys!=sorted(EXPECTED_DUPLICATE_KEYS):raise NBIQuarantineError('composite_ID_duplicate_drift_requires_review')
    if sum(duplicates.values())!=EXPECTED_TOTAL_COUNT:raise NBIQuarantineError('complete_count_drift_requires_review')
    roster_sha=digest(('\n'.join(sorted(f'{s}|{n}'for s,n in duplicates))+'\n').encode())
    if roster_sha!=EXPECTED_ID_ROSTER_SHA256:raise NBIQuarantineError('complete_ID_roster_drift_requires_review')
    zero=sum(p['zero']for p in per_state.values());blank=sum(p['blank']for p in per_state.values())
    partial=sum(p['partial']for p in per_state.values());signed=sum(p['signed']for p in per_state.values())
    padded=sum(p['padded']for p in per_state.values())
    ooe=sum(p['out_of_envelope']for p in per_state.values());short=sum(p['short']for p in per_state.values())
    if zero!=EXPECTED_ZERO_COORDINATE_COUNT:raise NBIQuarantineError('zero_coordinate_count_drift_requires_review')
    if ooe!=EXPECTED_OUT_OF_ENVELOPE_COUNT:raise NBIQuarantineError('out_of_envelope_coordinate_count_drift_requires_review')
    if blank!=EXPECTED_BLANK_COORDINATE_COUNT or partial!=EXPECTED_PARTIAL_COORDINATE_COUNT:raise NBIQuarantineError('blank_or_partial_coordinate_drift_requires_review')
    if signed!=EXPECTED_SIGNED_COORDINATE_COUNT:raise NBIQuarantineError('signed_coordinate_count_drift_requires_review')
    if padded!=EXPECTED_WHITESPACE_COORDINATE_COUNT:raise NBIQuarantineError('whitespace_padded_coordinate_count_drift_requires_review')
    if short!=EXPECTED_SHORT_RECORD_COUNT:raise NBIQuarantineError('short_record_count_drift_requires_review')
    columns=[pa.concat_arrays([a[i]for a in arrays])for i in range(len(LAYOUT)+1)]
    table=pa.Table.from_arrays(columns,schema=build_schema())
    audit={'count':table.num_rows,'field_count':len(LAYOUT),'state_rows':{t:per_state[t]['rows']for t in TOKENS},
        'state_item1_codes':{t:EXPECTED_FILES[t][4]for t in TOKENS},
        'composite_ID_sha256':roster_sha,'composite_ID_duplicate_keys':[list(k)for k in duplicate_keys],
        'zero_coordinate_count':zero,'blank_coordinate_count':blank,'partial_coordinate_count':partial,
        'signed_coordinate_values':signed,'whitespace_padded_coordinate_values':padded,'out_of_envelope_coordinate_count':ooe,'short_tail_records':short,
        'blank_structure_number_count':blank_structure,'non_ascii_bytes':non_ascii,
        'crs':'EPSG:4326 (WGS 84) decimal degrees, matching the producer SNBI submittal formats B.L.05/B.L.06 and the archived NTAD distribution spatial reference',
        'coordinate_basis':'Points are derived only from fixed-width Items 16/17 by the official FHWA SNBI Data Crosswalk conversion (DMS to decimal degrees, 6-decimal quantization, documented Item 1 hemisphere rules, producer position-split segment coercion). Blank or partially blank source coordinates yield null geometry; raw fixed-width strings are preserved unchanged in LATITUDE_016/LONGITUDE_017. Signed (369), space-padded (1, producing 4.022525/-8.036781 exactly as the producer SNBI transition outputs), zero (6) and out-of-envelope (56) source values are preserved with pinned counts. Coding Guide datum is unspecified; producer GeoJSON and archived NTAD layer both declare WGS 84.',
        'release_basis':'2025 is the final Coding Guide ASCII submission year (FHWA ascii.cfm). All 53 state files were posted in one producer release event (HTTP Last-Modified 2025-06-12 16:53-16:55 GMT, observed 2026-11-10); per-input server Last-Modified values are retained in raw receipts.',
        'archive_relationship':'The archived HIFLD inventory (DataLumos 239684, NTAD vintage) is an USDOT/BTS redistribution of FHWA data with derived typed/metric fields. This draft reconstructs from the producer directly; total record count 624193 equals the archived NTAD layer count observed 2026-11-08. SNBI JSON/GeoJSON successors are not equivalent populations (e.g. Idaho ASCII 4646 records vs producer SNBI GeoJSON 4643 features; blank/partial-coordinate ASCII records absent).',
        'temporary_objectID_warning':'none: no producer row identifier exists in the ASCII release; the composite (STATE_CODE_001, STRUCTURE_NUMBER_008) pair is a proposed join key with pinned exact duplicate pairs, not an approved unique ID.',
        'release_date_established':True,'release_date':'2025-06-12','approved':False}
    return table,audit


def source_fingerprint(captures):
    # Original state-file bytes bind every record; the frozen 2025 release
    # keeps its first draft namespace and unchanged refreshes reuse it.
    return digest(encode({n:digest(b)for n,(b,r)in captures.items()if n.startswith('state_')}))


def expected_url(name,url):
    if name in URLS:
        if url!=URLS[name]:raise NBIQuarantineError('unexpected_original_input_url')
        return url
    raise NBIQuarantineError('unknown_original_input')


def validate_receipt(name,body,receipt):
    url=expected_url(name,receipt.get('url'))
    if(receipt.get('url')!=url or receipt.get('final_url')!=url or type(receipt.get('status'))is not int or receipt['status']!=200
        or type(receipt.get('bytes'))is not int or receipt['bytes']!=len(body)or receipt.get('sha256')!=digest(body)or receipt.get('complete_body')is not True):raise NBIQuarantineError('invalid_original_receipt:'+name)
    if not isinstance(receipt.get('observed_at'),str)or datetime.fromisoformat(receipt['observed_at']).tzinfo is None:raise NBIQuarantineError('original_retrieval_timezone_required')
    server_last_modified=receipt.get('server_last_modified')
    if server_last_modified is not None and (type(server_last_modified)is not str or not server_last_modified):raise NBIQuarantineError('invalid_server_last_modified:'+name)
    if name.startswith('state_'):
        release={'release_date':parsedate_to_datetime(server_last_modified).date().isoformat()}if server_last_modified else {'release_date_unknown_reason':'No server Last-Modified available for this state file.'}
    else:
        release={'release_date_unknown_reason':'FHWA documentation-page edit time is not a data release date; the 2025 ASCII release is established per state file by its server Last-Modified.'}
    return {'input_id':name+':'+digest(body),'producer':PRODUCER,'host':httpx.URL(url).host,
        'role':'producer_publication','distributor':'FHWA first-party publication on www.fhwa.dot.gov (original producer; no mirror or portal intermediary)',
        'delegation_evidence':{'producer_program_url':PROGRAM,'landing_url':LANDING,'files_page_url':FILES_PAGE,'format_url':FORMAT_PAGE,'data_crosswalk_url':CROSSWALK_URL,'download_disclaimer_url':DISCLAIMER},
        'canonical_url':url,'retrieval_url':url,'observed_final_url':url,'retrieved_at':receipt['observed_at'],'http_status':200,'sha256':digest(body),'bytes':len(body),'server_last_modified':server_last_modified,'license':LICENSE,
        'attribution':'FHWA National Bridge Inventory, 2025 ASCII state files; 23 U.S.C. 409 applies (FHWA download disclaimer); complete original item descriptions, attribution and rights retained.',
        **release}


def acquire(storage,capture_dir=None):
    local_only(storage);captures={};attempt=f'_acquisition-attempts/{SLUG}/{uuid4().hex}';root=Path(capture_dir).absolute()if capture_dir is not None else None
    if root is not None and('..'in root.parts or any(p.is_symlink()for p in[root,*root.parents])):raise NBIQuarantineError('unsafe_capture_root')
    try:
        for name,url in URLS.items():
            if root is not None:
                body=bounded_input(root/(name+'.body'),MAX_BODY_BYTES)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body)
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
                                sha256=result.decoded_sha256,wire_bytes=result.wire_bytes_received,error_code=result.error_code,
                                server_last_modified=response.headers.get('last-modified'))
                    finally:
                        if result is not None:readback=preserve_transfer_result(path,result)
                        wire.flush();decoded.flush()
                receipt['complete_body']=result.complete and readback;receipt['independent_sink_readback_matches']=readback
                body=bounded_input(path/'decoded.body',MAX_BODY_BYTES+1024*1024)
                immutable(storage,f'_raw-captures/{SLUG}/{digest(body)}/{name}.body',body);raw=encode(receipt)
            immutable(storage,f'{attempt}/{name}.raw-receipt.body',raw)
            receipt=strict_json(raw);validate_receipt(name,body,receipt);captures[name]=(body,receipt)
            if name=='files_page' and state_tokens(body)!=set(TOKENS):raise NBIQuarantineError('producer_state_file_roster_drift')
        return captures
    except Exception as exc:
        immutable(storage,attempt+'/failure.json',encode({'approved':False,'reason_type':type(exc).__name__}))
        where=quarantine(storage,captures,str(exc));raise NBIQuarantineError('source_acquisition_failed:'+str(exc)+';retained:'+where)from exc


def manifest_for(captures,identity,output_pin):
    manifest={'schema_version':1,'processor':'HIFLD Next','approved':False,'code_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=Path(__file__).parents[3],text=True).strip(),
        'title':'National Bridge Inventory (NBI) — FHWA 2025 ASCII reconstruction draft','publisher':PRODUCER,'agency':'Federal Highway Administration','source_url':LANDING,'license':LICENSE,
        'processing_fingerprint':digest(encode(identity)),'inputs':[validate_receipt(n,*captures[n])for n in sorted(captures)],
        'transformations':[{'operation':'complete_fixed_width_ascii_reconstruction','parameters':{
            'grain':'All 624193 records of the official FHWA 2025 NBI ASCII release (50 states, DC, PR, VI). No filtering, deduplication, reordering or repair.',
            'source_ids':'Every record keeps its exact fixed-width slice for all 124 official layout items, preserving leading zeros and embedded/trailing spaces. The composite (STATE_CODE_001, STRUCTURE_NUMBER_008) pair is proposed as the join key; its two exact duplicate pairs are pinned in the audit, not silently merged.',
            'types':'All 124 items are published as exact source-width strings with start/end/format metadata from the producer NBI Record Format page; no numeric coercion, unit conversion or trimming. Columns decode 1:1 as ISO-8859-1 so every source byte is preserved verbatim (7 non-ASCII bytes in NY/PR location text; counts pinned). The reserved Washington HQ Use block and computed CAT10/CAT23/CAT29 tail columns are retained; the single producer-truncated NC record stores null tail values, matching the producer SNBI transition output.',
            'geometry':'Points are derived only from Items 16/17 by the official FHWA SNBI Data Crosswalk rules (DMS to decimal degrees, 6-decimal quantization, negative latitude for Item 1 code 60, negative longitude except codes 64/66/68/69/70, producer position-split segment coercion). Blank or partially blank coordinates yield null geometry; all-zero and out-of-envelope source values are preserved and their counts pinned. EPSG:4326 per producer GeoJSON and the archived NTAD layer; Coding Guide datum is unspecified.',
            'relationship':'2025 is the final Coding Guide ASCII year; SNBI Excel/JSON/GeoJSON are a changed successor with a different population and schema. The archived HIFLD copy (DataLumos 239684) is an USDOT/BTS NTAD redistribution with derived typed/metric fields; its 624193-record count equals this producer release. Source, schema/ID contract, coordinate basis, rights and publication acceptance remain pending human decisions.'}}],
        'prebuilt_output_policy':{'schema_version':1,'formats':{'geoparquet':{'reason':'Preserve all 124 original fixed-width ASCII items as exact strings plus the crosswalk-derived point geometry; no generic conversion or schema mapping approved.','objects':[{'path':'geoparquet/'+SLUG+'.parquet',**output_pin}]}}},
        'unsupported_output_formats':{'geopackage':'Fixed-width string widths, blanks-as-nulls tail and the proposed composite ID need an approved contract; no implicit conversion.','shapefile_zip':'124-character-limit and width/null semantics cannot be silently coerced.','pmtiles':'Projection, quantization and feature ID mappings remain unapproved.'}}
    validate_release_provenance(manifest);validate_output_policy(manifest);return manifest


def stage_national_bridge_inventory(storage,*,capture_dir=None):
    local_only(storage);captures=acquire(storage,capture_dir);table,audit=inspect_captures(captures);identity=processing_identity();fingerprint=digest(encode(identity));version='v0.0.0-draft-fhwa-nbi-2025-'+source_fingerprint(captures)[:16]+'-'+fingerprint[:16]
    base=Path(storage.local_dir).absolute();prefix=f'{SLUG}/{SLUG}/{version}';root=base/prefix;locks=base/'_locks';locks.mkdir(exist_ok=True)
    descriptor=os.open(locks/(SLUG+'.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'r+b')as lock:
        if os.fstat(lock.fileno()).st_nlink!=1:raise NBIQuarantineError('linked_lock')
        fcntl.flock(lock,fcntl.LOCK_EX);refuse_observed_loss(storage,version)
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
                    raise NBIQuarantineError('observed_original_trust_loss')from exc
                if root.exists():
                    try:
                        validate_candidate(root,table,identity,captures,audit,manifest,version,cached=True,storage=storage)
                    except Exception as exc:
                        q=base/'_quarantine'/SLUG/('invalid-candidate-'+uuid4().hex);q.parent.mkdir(parents=True,exist_ok=True);os.rename(root,q);raise NBIQuarantineError('invalid_cache_moved_intact:'+str(q))from exc
                    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':True}
            for n,b in source_payloads(captures,identity,audit,manifest).items():write_private(temp,n,b)
            ready={'approved':False,'version':version,'processing_fingerprint':fingerprint,'capture_fingerprint':source_fingerprint(captures),'objects':{str(p.relative_to(temp)):sha256_file(p)for p in temp.rglob('*')if p.is_file()}};write_private(temp,READY,encode(ready));validate_candidate(temp,table,identity,captures,audit,manifest,version)
            captures,manifest=original_context(storage,version,fingerprint,captures,identity,manifest,create=True)
            validate_candidate(temp,table,identity,captures,audit,manifest,version);root.parent.mkdir(parents=True,exist_ok=True);os.rename(temp,root)
    return {'dataset_slug':SLUG,'file_slug':SLUG,'version':version,'feature_count':table.num_rows,'reused':False}
