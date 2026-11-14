"""Parked record for the retired USGS National Atlas Nonferrous Metal Processing Plants layer.

The producer's original National Atlas release (``minop7x020_nt00024.tar.gz`` on
``dds.cr.usgs.gov``, publication 200512, Minerals Information Team operations
active in 2003) has been removed from the live server (HTTPS 404 observed
2026-11-13) and the inventoried DataLumos project 240272 V1 is a banned archive
source.  The producer's ``mrdata.usgs.gov`` publication of the same 2003
Minerals Information Team survey ("Active Mines and Mineral Processing Plants in
the United States in 2003") is a one-time release whose FGDC metadata declares
progress Complete and update frequency "None planned" (verified 2026-11-13), so
under the current source policy it is not a maintained successor input.  The
producer's 2026 NMIC facility release (DOI 10.5066/P1BUPUAM) publishes no
coordinate columns, so it cannot recreate this entity-point layer.  No
maintained USGS producer publication capable of reconstructing the layer is
known, so the dataset is parked: every staging attempt fails closed and this
module defines and performs no fetching, no decoding, no candidate output and no
acceptance.  Resuming requires an explicit maintainer decision (park, retire,
accept an explicitly provided preserved producer capture, or name a maintained
source).
"""
from __future__ import annotations
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit

SLUG='nonferrous-metal-processing-plants'
PARK_REASON='no_maintained_producer_publication_park_or_retire'
PARK_STATEMENT=('no maintained USGS producer publication can recreate the retired '
    'nonferrous-metal-processing-plants entity-point layer: the original National Atlas release is '
    'retired (HTTPS 404 observed 2026-11-13), the producer mineplant survey publication declares '
    'progress Complete and update frequency None planned, and the producer 2026 NMIC facility release '
    'publishes no coordinates; resuming requires an explicit maintainer decision (park, retire, '
    'accept an explicitly provided preserved producer capture, or name a maintained source)')
class NonferrousProcessingPlantsQuarantineError(ValueError): pass

# Producer-contract research record.  All objects are USGS-owned; the module
# never fetches them.  Digests were observed on 2026-11-13 anonymous captures.
LEGACY_METADATA_URL='https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop7x020.xml'
MINEPLANT_ZIP_URL='https://mrdata.usgs.gov/mineplant/mineplant.zip'
MINEPLANT_METADATA_URL='https://mrdata.usgs.gov/metadata/mineplant.xml'
PINS={'usgs_mineplant_zip':{'url':MINEPLANT_ZIP_URL,'bytes':513191,'sha256':'6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e','observed':'2026-11-13','role':'research_record_producer_publication_one_time_release'},
      'usgs_mineplant_metadata':{'url':MINEPLANT_METADATA_URL,'bytes':26366,'sha256':'419629edaf825f2d55ed1dd03fe3a9921d0d35a8240c208b44b53d30c8b9ead0','observed':'2026-11-13','role':'research_record_producer_release_status'},
      'legacy_metadata':{'url':LEGACY_METADATA_URL,'bytes':20018,'sha256':'1ff2b2c8910aca0fe86e3241c0c3c3e1c45a24c396c4401c074c110a98b7c9ee','observed':'2026-11-13','role':'research_record_retired_producer_scope_anchor'}}
MINEPLANT_METADATA_TITLE='Active Mines and Mineral Processing Plants in the United States in 2003'
MINEPLANT_METADATA_ATTRS=('COMMODITY','ID','SITE_NAME','COMPANY_NA','STATE_LOCA','COUNTY','LATITUDE','LONGITUDE','PLANT_MIN')
MINEPLANT_METADATA_METD='20201230'
MINEPLANT_GROUND_CONDITION='2003'
MINEPLANT_PROGRESS='Complete'
MINEPLANT_UPDATE='None planned'
# Producer scope of the retired National Atlas layer, pinned from the surviving
# USGS legacy FGDC metadata (publication 200512, metadata date 20060713).
PROCESSING_COMMODITIES=('Aluminum','Antimony','Beryllium','Cadmium','Copper','Gold','Iron','Lead',
    'Magnesium metal','Rhenium','Selenium','Silver','Strontium','Titanium metal','Zinc')
ATLAS_GENERIC_THEMEKEYS=('economy','Minerals','Mines','Mining operations','Minerals operations',
    'Processing plants','Nonferrous metals')
PLANT_ROLES=('M/P','P')
NONFERROUS_PLANTS_ROWS=71
ATLAS_TITLE='Nonferrous Metal Processing Plants - Direct Download'
ATLAS_PUBDATE='200512'
ATLAS_PROCSTEP_DATE='200511'
ATLAS_METD='20060713'
ATLAS_ONLINK='http://dds.cr.usgs.gov/pub/data/nationalatlas/minop7x020_nt00024.tar.gz'
ATLAS_ID_FIELD='Minop7x020'
ATLAS_ID_RANGE=('1','71')
ATLAS_PLANT_MIN_DOMAIN=('M/P','P')
ATLAS_LATITUDE_RANGE=('27.883','48.847')
ATLAS_LONGITUDE_RANGE=('-122.706','-71.299')
ATLAS_ENTITY_TEXT=('Facilities that process and distribute nonferrous metals, or that extract and '
    'process nonferrous metals.')
PARK_EVIDENCE={'decision':'park or retire; no maintained USGS producer publication can recreate the assigned entity-point layer',
    'original_release':{'url':ATLAS_ONLINK,'publisher':'U.S. Geological Survey National Atlas (producer publication 200512)',
        'status':'retired from the live server; HTTPS 404 observed 2026-11-13 (http 301 redirects to https)','scope_anchor':LEGACY_METADATA_URL},
    'banned_archive':'DataLumos project 240272 V1 is a banned archive source and was never fetched',
    'mineplant_survey_publication':{'url':MINEPLANT_ZIP_URL,'metadata_url':MINEPLANT_METADATA_URL,
        'publisher':'U.S. Geological Survey National Minerals Information Center',
        'declared_ground_condition':MINEPLANT_GROUND_CONDITION,'declared_progress':MINEPLANT_PROGRESS,
        'declared_update_frequency':MINEPLANT_UPDATE,'verified':'2026-11-13 anonymous retrieval matching the pinned digest',
        'eligibility':'one-time release with no planned refresh; excluded from pipeline inputs by the current source policy'},
    'current_alternatives_checked':[
        {'product':'Mines, smelters, refineries, and recycling facilities in the United States (USGS NMIC)',
         'doi':'10.5066/P1BUPUAM','sciencebase_item':'6a3edaf71ba49b7c2e263528','published':'20260629',
         'data_period':'20241204..20260430','check_date':'2026-11-13',
         'finding':'tabular worksheet only; 56 columns with no coordinate columns (Location Description is town/state text, First-Level Administrative Division is state), so it cannot recreate the entity-point layer; its own FGDC metadata also declares progress Complete and update None planned'},
        {'product':'ScienceBase catalog search for maintained nonferrous processing-plant point products',
         'check_date':'2026-11-13',
         'finding':'the producer catalog returns only the 2003 mineplant survey item (6a3edaf71ba49b7c2e263528 is the NMIC facility release; the mineplant product item 5005bc31e4b052a0c18e3485) and 2010 BLM REA derivative copies; no maintained nonferrous processing-plant point publication exists'},
        {'product':'Mineral Resource Data System (MRDS)','url':'https://mrdata.usgs.gov/mrds/',
         'check_date':'2026-11-12','finding':'dated prior project research: updates ceased and the product is a materially different global deposit database, not a maintained publication of active US operations'},
        {'product':'USMIN mineral deposit areas','url':'https://mrdata.usgs.gov/deposit/',
         'check_date':'2026-11-12','finding':'dated prior project research: developing critical-minerals product, different scope'}]}

def producer_hosts():
    return {urlsplit(pin['url']).hostname for pin in PINS.values()}

def parse_atlas_scope(body):
    """Validate the surviving USGS legacy FGDC document against the pinned scope facts.

    Producer-contract record exercised by synthetic-fixture tests; never a data
    input and never fetched by this module.
    """
    root=ET.fromstring(body)
    def text(path): return (root.findtext(path) or '').strip()
    if text('.//citation//title')!=ATLAS_TITLE or text('.//citation//pubdate')!=ATLAS_PUBDATE:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_metadata_title_or_pubdate_drift')
    if text('.//onlink')!=ATLAS_ONLINK:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_original_download_link_drift')
    if text('.//procstep//procdate')!=ATLAS_PROCSTEP_DATE or text('.//metd')!=ATLAS_METD:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_metadata_process_or_metadata_date_drift')
    if text('.//horizdn')!='North American Datum of 1983':raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_metadata_datum_drift')
    declared=int(text('.//ptvctcnt'))
    if declared!=NONFERROUS_PLANTS_ROWS:raise NonferrousProcessingPlantsQuarantineError('atlas_declared_entity_point_count_drift')
    id_attr=next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip()==ATLAS_ID_FIELD),None)
    if id_attr is None or [e.text.strip() for e in id_attr.findall('.//rdommin')]+[e.text.strip() for e in id_attr.findall('.//rdommax')]!=list(ATLAS_ID_RANGE):raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_identifier_range_drift')
    plant_attr=next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip().lower()=='plant_min'),None)
    domain=tuple(e.text.strip() for e in plant_attr.findall('.//edomv')) if plant_attr is not None else ()
    if domain!=ATLAS_PLANT_MIN_DOMAIN:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_plant_mine_domain_drift')
    if next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip().lower()=='commodity'),None) is None:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_commodity_attribute_missing')
    themekeys=[e.text.strip() for e in root.findall('.//themekey')]
    covered={key for key in themekeys if key in PROCESSING_COMMODITIES}
    if covered!=set(PROCESSING_COMMODITIES):raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_commodity_class_scope_drift')
    unexpected=[key for key in themekeys if key not in PROCESSING_COMMODITIES and key not in ATLAS_GENERIC_THEMEKEYS]
    if unexpected:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_unexpected_commodity_class_drift:'+unexpected[0])
    if 'Processing plants' not in themekeys or 'Nonferrous metals' not in themekeys:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_theme_scope_drift')
    if ATLAS_ENTITY_TEXT not in (root.findtext('.//enttypd') or ''):raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_entity_description_drift')
    def declared_range(label):
        attr=next((attr for attr in root.findall('.//attr') if (attr.findtext('attrlabl') or '').strip().lower()==label),None)
        if attr is None:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_'+label+'_attribute_missing')
        values=[e.text.strip() for e in attr.findall('.//rdommin')]+[e.text.strip() for e in attr.findall('.//rdommax')]
        if len(values)!=2:raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_'+label+'_range_incomplete')
        return tuple(values)
    latitude_range=declared_range('latitude');longitude_range=declared_range('longitude')
    if (latitude_range,longitude_range)!=(ATLAS_LATITUDE_RANGE,ATLAS_LONGITUDE_RANGE):raise NonferrousProcessingPlantsQuarantineError('atlas_legacy_declared_coordinate_range_drift')
    return {'approved':False,'purpose':'pin the producer scope of the retired National Atlas nonferrous-metal-processing-plants layer; producer-contract record, never a data input','atlas_layer':ATLAS_TITLE,'atlas_pubdate':ATLAS_PUBDATE,'atlas_process_date':ATLAS_PROCSTEP_DATE,'atlas_metadata_date':ATLAS_METD,'atlas_original_download':ATLAS_ONLINK,'declared_entity_points':declared,'declared_plant_min_domain':list(ATLAS_PLANT_MIN_DOMAIN),'declared_commodity_classes':list(PROCESSING_COMMODITIES),'declared_latitude_range':list(ATLAS_LATITUDE_RANGE),'declared_longitude_range':list(ATLAS_LONGITUDE_RANGE),'source':'U.S. Geological Survey National Geospatial Technical Operations Center legacy metadata WAF; retained producer metadata'}

def parse_mineplant_release_status(body):
    """Validate the producer's FGDC release-status document against the pinned facts.

    The pinned document declares a one-time release (progress Complete, update
    frequency None planned), which is why the mineplant package is not a
    maintained successor input.  If USGS ever publishes a different release
    status there, this parser fails closed so the change is re-evaluated
    explicitly instead of being absorbed silently.
    """
    root=ET.fromstring(body)
    def text(path): return (root.findtext(path) or '').strip()
    if text('.//citation//title')!=MINEPLANT_METADATA_TITLE:raise NonferrousProcessingPlantsQuarantineError('mineplant_release_status_title_drift')
    labels=tuple((attr.findtext('attrlabl') or '').strip() for attr in root.findall('.//attr'))
    if labels!=MINEPLANT_METADATA_ATTRS:raise NonferrousProcessingPlantsQuarantineError('mineplant_release_status_attribute_roster_drift')
    if text('.//metd')!=MINEPLANT_METADATA_METD:raise NonferrousProcessingPlantsQuarantineError('mineplant_release_status_metadata_date_drift')
    progress=text('.//progress');update=text('.//update')
    if progress!=MINEPLANT_PROGRESS or update!=MINEPLANT_UPDATE:raise NonferrousProcessingPlantsQuarantineError('mineplant_release_status_changed_re_evaluate_source_eligibility:'+progress+'/'+update)
    return {'approved':False,'title':MINEPLANT_METADATA_TITLE,'metadata_date':MINEPLANT_METADATA_METD,'declared_ground_condition':MINEPLANT_GROUND_CONDITION,'declared_progress':MINEPLANT_PROGRESS,'declared_update_frequency':MINEPLANT_UPDATE,'one_time_release':True,'eligibility':'one-time producer release with no planned refresh; not a maintained successor and excluded from pipeline inputs by the current source policy'}

def stage_nonferrous_metal_processing_plants(storage=None,*,capture_dir=None,package_id=None,**_unused):
    """Fail closed: the dataset is parked; nothing is fetched, read or written."""
    raise NonferrousProcessingPlantsQuarantineError(PARK_REASON+': '+PARK_STATEMENT)
