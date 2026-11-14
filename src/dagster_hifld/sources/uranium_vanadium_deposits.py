"""Parked record for the HIFLD Uranium and Vanadium Deposits layer.

The DHS HIFLD Open crosswalk (row "Uranium and Vanadium Deposits", Agency
U.S. Geological Survey, Status Deprecate, DOI 10.3886/E241367V1) points the
retired HIFLD Open item 2c9b52ce91be4384931b64dd551cb6ea — whose
services1.arcgis.com FeatureServer answered only ArcGIS "Invalid URL" error
documents when probed 2026-11-14 — to the producer's Mineral Resources Data
System (MRDS) publication. The producer states on the live MRDS landing page
(observed 2026-11-14) that as of 2011 USGS has ceased systematic updates to
MRDS and is working to create a new database focused primarily on the
conterminous US, that MRDS remains its best collection only for locations
outside the United States, and that Alaska is covered by the
continuing-development Alaska Resource Data File. A frozen release with no
maintained refresh cadence is not a pipeline input under the current source
policy, so the earlier draft reconstruction of the mrds-trim digest subset was
removed. The producer's current alternatives cannot recreate this nationwide
uranium/vanadium deposit-point layer either: USMIN is a developing
critical-minerals database whose published commodity packages include no
uranium and no vanadium, ARDF covers Alaska only, and the Earth MRI focus-area
release and the curated 681-deposit "Critical mineral deposits of the United
States" release are materially different products. No maintained USGS producer
publication capable of reconstructing the layer is known, so the dataset is
parked: every staging attempt fails closed and this module defines and
performs no fetching, no decoding, no staging and no candidate output.
Resuming requires an explicit maintainer decision (park permanently, retire,
or name a maintained producer source).
"""
from __future__ import annotations
from urllib.parse import urlsplit

SLUG='uranium-and-vanadium-deposits'
PARK_REASON='no_maintained_producer_publication_park_or_retire'
PARK_STATEMENT=('no maintained USGS producer publication can recreate the retired '
    'uranium-and-vanadium-deposits entity-point layer: the producer MRDS publication is frozen '
    '(systematic updates ceased 2011), USMIN is a developing critical-minerals database with no '
    'uranium or vanadium commodity packages, ARDF covers Alaska only, and the Earth MRI focus-area '
    'and curated 681-deposit releases are materially different products; resuming requires an '
    'explicit maintainer decision (park permanently, retire, or name a maintained producer source)')
class UraniumVanadiumDepositsQuarantineError(ValueError): pass

# Producer-contract research record.  All objects are USGS-owned; the module
# never fetches them.  Digests were observed on 2026-11-14 anonymous captures.
MRDS_LANDING_URL='https://mrdata.usgs.gov/mrds/'
MRDS_TRIM_URL='https://mrdata.usgs.gov/mrds/mrds-trim.zip'
USMIN_LANDING_URL='https://mrdata.usgs.gov/deposit/'
ARDF_LANDING_URL='https://mrdata.usgs.gov/ardf/'
PINS={'usgs_mrds_landing':{'url':MRDS_LANDING_URL,'bytes':20610,'sha256':'793b6de964e124c11fe915630d59d63a056f4dce05ea577fe9d9ac2d6c4452ac','observed':'2026-11-14','role':'research_record_producer_release_status'},
      'usgs_mrds_trim_zip':{'url':MRDS_TRIM_URL,'bytes':11236725,'sha256':'f24f995faaa6f5c66415971b1bb0fa1906273adb37f82c1098266aece47f6a84','observed':'2026-11-14','role':'research_record_producer_frozen_publication'},
      'usgs_usmin_landing':{'url':USMIN_LANDING_URL,'bytes':40121,'sha256':'49014311524b79db7af267a8e2f0b7cd6a201d7dd46ed3caf6fba5ab990ba502','observed':'2026-11-14','role':'research_record_developing_successor_scope'},
      'usgs_ardf_landing':{'url':ARDF_LANDING_URL,'bytes':15022,'sha256':'82b4b73adebe5cc8cfe49a8358fe838f0c0c799c7e440a220cb9c9499fadff44','observed':'2026-11-14','role':'research_record_alaska_continuing_development'}}
MRDS_LANDING_TITLE='Mineral Resources Data System (MRDS)'
MRDS_STATUS_SENTENCE=('Current status: As of 2011, USGS has ceased systematic updates to MRDS, and is working '
    'to create a new database, focused primarily on the conterminous US. For locations outside the United States, '
    'MRDS remains the best collection of reports that USGS has available. For locations in Alaska, the Alaska '
    'Resource Data File remains the most coherent collection of such reports and is in continuing development.')
USMIN_LANDING_TITLE='USMIN Mineral Deposit Database'
USMIN_STATUS_SENTENCE=('A developing national-scale geospatial database that will be an authoritative source of '
    'information on the most important mines, mineral deposits, and mineral districts of the United States.')
USMIN_PACKAGE_COMMODITIES=('Cobalt','Gallium','Germanium','Graphite','Indium','Lithium','Niobium','REE',
                           'Rhenium','Sn','Tantalum','Tellurium','Tungsten_ver2')
USMIN_PACKAGE_TRIPWIRES=('USGS_Uranium_US','USGS_Vanadium_US')
PARK_EVIDENCE={'decision':'park or retire; no maintained USGS producer publication can recreate the assigned uranium-and-vanadium-deposits entity-point layer',
    'retired_hifld_item':{'item':'2c9b52ce91be4384931b64dd551cb6ea',
        'status':'retired HIFLD Open item; its services1.arcgis.com FeatureServer answered only ArcGIS "Invalid URL" error documents when probed 2026-11-14',
        'crosswalk':'DHS HIFLD Open crosswalk DOI 10.3886/E241367V1 row "Uranium and Vanadium Deposits": Status Deprecate, replacement pointer the USGS MRDS publication page'},
    'banned_archive':'DataLumos project 240898 is a banned archive source and was never fetched',
    'frozen_producer_publication':{'url':MRDS_TRIM_URL,'landing':MRDS_LANDING_URL,
        'publisher':'U.S. Geological Survey Mineral Resources Data System (MRDS)',
        'bytes':11236725,'sha256':'f24f995faaa6f5c66415971b1bb0fa1906273adb37f82c1098266aece47f6a84',
        'producer_status_statement':MRDS_STATUS_SENTENCE,
        'verified':'2026-11-14 anonymous retrieval of the live landing page matching the pinned digest; the trim package digest was observed on 2026-11-14 during the superseded draft capture',
        'eligibility':'systematic updates ceased 2011; frozen release with no maintained refresh cadence; excluded from pipeline inputs by the current source policy'},
    'current_alternatives_checked':[
        {'product':'USMIN Mineral Deposit Database','url':USMIN_LANDING_URL,'check_date':'2026-11-14',
         'finding':'self-declared developing database of critical-mineral deposits with reported production or resource estimates; its published commodity packages (Cobalt, Gallium, Germanium, Graphite, Indium, Lithium, Niobium, REE, Rhenium, Sn, Tantalum, Tellurium, Tungsten) include no uranium and no vanadium package, so it cannot recreate this two-commodity layer; the announced USMIN uranium compilation targets deposits with records of significant past production or resources only, a different population'},
        {'product':'Alaska Resource Data File (ARDF)','url':ARDF_LANDING_URL,'check_date':'2026-11-14',
         'finding':'the producer-designated continuing-development collection, but for Alaska locations only; no nationwide coverage of uranium or vanadium deposits'},
        {'product':'Earth MRI focus areas for 13 critical minerals including uranium and vanadium (Dicken and others, 2021)','doi':'10.5066/P9WA7JZY','check_date':'2026-11-14',
         'finding':'regional focus-area polygons with supplemental tables, not deposit points; cannot recreate the entity-point layer'},
        {'product':'Critical mineral deposits of the United States (ver. 3.0, September 2026)','doi':'10.5066/p9k1hbnt','check_date':'2026-11-14',
         'finding':'curated 681-deposit database gleaned from MRDS, ARDF, USMIN and literature for Earth MRI priorities; a materially different and far smaller population than the assigned layer'}]}

def producer_hosts():
    return {urlsplit(pin['url']).hostname for pin in PINS.values()}

def _html_title(text):
    start=text.find('<title>');end=text.find('</title>')
    if start==-1 or end<start:return ''
    return text[start+7:end].strip()

def parse_mrds_status(body):
    """Validate the producer's live MRDS status statement against the pinned frozen-release facts.

    Producer-contract record exercised by synthetic-fixture tests; never a data
    input and never fetched by this module.  If USGS ever publishes a different
    release status for MRDS there, this parser fails closed so the change is
    re-evaluated explicitly instead of being absorbed silently.
    """
    text=body.decode('utf-8','replace')
    if _html_title(text)!=MRDS_LANDING_TITLE:raise UraniumVanadiumDepositsQuarantineError('mrds_status_title_drift')
    if MRDS_STATUS_SENTENCE not in text:raise UraniumVanadiumDepositsQuarantineError('mrds_release_status_changed_re_evaluate_source_eligibility')
    return {'approved':False,'title':MRDS_LANDING_TITLE,'updates_ceased':'2011',
        'best_available_claim_scope':'locations outside the United States',
        'alaska_alternative':'Alaska Resource Data File (ARDF), continuing development',
        'successor_status':'producer working to create a new database focused primarily on the conterminous US; not published as a maintained successor for this layer',
        'frozen_release':True,
        'eligibility':'frozen producer release with no maintained refresh cadence; not a pipeline input under the current source policy'}

def parse_usmin_status(body):
    """Validate the producer's live USMIN scope statement and package roster.

    USMIN self-identifies as developing and publishes commodity packages for a
    fixed roster of critical minerals that includes neither uranium nor
    vanadium.  If USMIN ever publishes a uranium or vanadium package, changes
    the developing scope statement, or alters the package roster, this parser
    fails closed so a potential maintained successor is re-evaluated
    explicitly instead of being absorbed silently.
    """
    text=body.decode('utf-8','replace')
    if _html_title(text)!=USMIN_LANDING_TITLE:raise UraniumVanadiumDepositsQuarantineError('usmin_status_title_drift')
    if USMIN_STATUS_SENTENCE not in text:raise UraniumVanadiumDepositsQuarantineError('usmin_release_status_changed_re_evaluate_source_eligibility')
    published=[token for token in USMIN_PACKAGE_TRIPWIRES if token in text]
    if published:raise UraniumVanadiumDepositsQuarantineError('usmin_uranium_or_vanadium_package_published_re_evaluate_source_eligibility:'+','.join(published))
    missing=[name for name in USMIN_PACKAGE_COMMODITIES if 'USGS_'+name+'_US' not in text]
    if missing:raise UraniumVanadiumDepositsQuarantineError('usmin_package_roster_drift:'+','.join(missing))
    return {'approved':False,'title':USMIN_LANDING_TITLE,'self_description':USMIN_STATUS_SENTENCE,
        'developing':True,'uranium_vanadium_packages':[],
        'published_commodity_packages':list(USMIN_PACKAGE_COMMODITIES),
        'eligibility':'developing critical-minerals product without uranium or vanadium commodity packages; cannot recreate the two-commodity entity-point layer and is not a maintained successor for it'}

def stage_uranium_vanadium_deposits(storage=None,*,capture_dir=None,**_unused):
    """Fail closed: the dataset is parked; nothing is fetched, read or written."""
    raise UraniumVanadiumDepositsQuarantineError(PARK_REASON+': '+PARK_STATEMENT)
