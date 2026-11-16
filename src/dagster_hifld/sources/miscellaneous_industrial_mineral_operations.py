"""Parked record for the retired USGS National Atlas Miscellaneous Industrial Minerals Operations layer.

The producer's original National Atlas release (``minop5x020_nt00022.tar.gz`` on
``dds.cr.usgs.gov``, publication 200512, Minerals Information Team operations surveyed as
active in 2003, "Facilities that extract, process, and (or) distribute miscellaneous
industrial minerals") has been removed from the live server (HTTP 404 after the host's
http->https redirect, observed 2026-11-10 and re-verified 2026-11-16) and the inventoried
DataLumos project 239600 V1 is a banned archive source.  The producer's ``mrdata.usgs.gov``
publication of the same 2003 Minerals Information Team survey ("Active Mines and Mineral
Processing Plants in the United States in 2003") is a one-time release whose FGDC metadata
declares progress Complete and update frequency "None planned" (digest-verified 2026-11-16),
so under the current source policy it is not a maintained successor input; adopting it would
also change record identity (``id`` 1..6841 instead of the original ``Minop5x020`` 1..305
roster) and round coordinates from the archived DBF's six decimals to three, so it is not
silently substituted.  The producer's 2026 NMIC facility release (DOI 10.5066/P1BUPUAM)
publishes no coordinate columns, so it cannot recreate this entity-point layer.  No
maintained USGS producer publication capable of reconstructing the layer is known, so the
dataset is parked: every staging attempt fails closed and this module defines and performs
no fetching, no decoding, no candidate output and no acceptance.  The parsers below fail
closed if the producer ever changes the one-time release status or the surviving legacy
scope document, so a potential maintained successor is re-evaluated explicitly instead of
being absorbed silently.  Resuming requires an explicit maintainer decision (park, retire,
accept an explicitly provided preserved producer capture, or name a maintained source).
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from urllib.parse import urlsplit

SLUG = 'miscellaneous-industrial-mineral-operations'
PARK_REASON = 'no_maintained_producer_publication_park_or_retire'
PARK_STATEMENT = ('no maintained USGS producer publication can recreate the retired '
    'miscellaneous-industrial-mineral-operations entity-point layer: the original National Atlas release is '
    'retired (404 observed 2026-11-10, re-verified 2026-11-16), the producer mineplant survey publication '
    'declares progress Complete and update frequency None planned, and the producer 2026 NMIC facility release '
    'publishes no coordinates; resuming requires an explicit maintainer decision (park, retire, accept an '
    'explicitly provided preserved producer capture, or name a maintained source)')


class MiscellaneousIndustrialMineralsQuarantineError(ValueError):
    pass


# Producer-contract research record.  All objects are USGS-owned; the module
# never fetches them.  Digests were observed on 2026-11-16 anonymous captures.
LEGACY_METADATA_URL = 'https://thor-f5.er.usgs.gov/ngtoc/metadata/waf/atlas_legacy/minop5x020.xml'
MINEPLANT_ZIP_URL = 'https://mrdata.usgs.gov/mineplant/mineplant.zip'
MINEPLANT_CSV_ZIP_URL = 'https://mrdata.usgs.gov/mineplant/mineplant-csv.zip'
MINEPLANT_METADATA_URL = 'https://mrdata.usgs.gov/metadata/mineplant.xml'
PINS = {
    'legacy_metadata': {
        'url': LEGACY_METADATA_URL, 'bytes': 20018,
        'sha256': '1ff2b2c8910aca0fe86e3241c0c3c3e1c45a24c396c4401c074c110a98b7c9ee',
        'observed': '2026-11-16',
        'role': 'research_record_retired_producer_scope_anchor'},
    'usgs_mineplant_zip': {
        'url': MINEPLANT_ZIP_URL, 'bytes': 513191,
        'sha256': '6dabd3bbedcb1fbf0c5155fb349228951a9b2cbd065311484eb479c25997ee4e',
        'observed': '2026-11-16',
        'role': 'research_record_producer_publication_one_time_release'},
    'usgs_mineplant_csv_zip': {
        'url': MINEPLANT_CSV_ZIP_URL, 'bytes': 193253,
        'sha256': 'aac018066ca3bb9b0c4ac40438c86901f80e27787b3b9ecdd4de9234d2e6b0b1',
        'observed': '2026-11-16',
        'role': 'research_record_producer_publication_one_time_release_payload'},
    'usgs_mineplant_metadata': {
        'url': MINEPLANT_METADATA_URL, 'bytes': 26366,
        'sha256': '419629edaf825f2d55ed1dd03fe3a9921d0d35a8240c208b44b53d30c8b9ead0',
        'observed': '2026-11-16',
        'role': 'research_record_producer_release_status'},
}
MINEPLANT_METADATA_TITLE = 'Active Mines and Mineral Processing Plants in the United States in 2003'
MINEPLANT_METADATA_ATTRS = ('COMMODITY', 'ID', 'SITE_NAME', 'COMPANY_NA', 'STATE_LOCA',
                            'COUNTY', 'LATITUDE', 'LONGITUDE', 'PLANT_MIN')
MINEPLANT_METADATA_METD = '20201230'
MINEPLANT_GROUND_CONDITION = '2003'
MINEPLANT_PROGRESS = 'Complete'
MINEPLANT_UPDATE = 'None planned'
# Producer scope of the retired National Atlas layer, pinned from the surviving
# USGS legacy FGDC metadata (publication 200512, metadata date 20060713).
ATLAS_TITLE = 'Miscellaneous Industrial Minerals Operations - Direct Download'
ATLAS_PUBDATE = '200512'
ATLAS_PROCSTEP_DATE = '200511'
ATLAS_METD = '20060713'
ATLAS_ONLINK = 'http://dds.cr.usgs.gov/pub/data/nationalatlas/minop5x020_nt00022.tar.gz'
ATLAS_ID_FIELD = 'Minop5x020'
ATLAS_ID_RANGE = ('1', '305')
ATLAS_PLANT_MIN_DOMAIN = ('M', 'M/P', 'P')
ATLAS_LATITUDE_RANGE = ('25.925', '48.565')
ATLAS_LONGITUDE_RANGE = ('-122.782', '-73.112')
MISCELLANEOUS_COMMODITIES = ('Barite', 'Bromine', 'Diatomite', 'Fullers earth', 'Iodine',
                             'Iron oxide pigments', 'Kaolin', 'Lime', 'Lithium', 'Salt',
                             'Soda ash', 'Sodium sulfate', 'Titanium dioxide pigment',
                             'Titanium minerals', 'Trona')
# Commodity classes of the other seven layers of the same National Atlas
# mineral-operations series (minop1x020 agricultural, minop2x020 construction,
# minop3x020/minop4x020 ferrous, minop6x020 nonferrous mines, minop7x020 nonferrous
# processing, minop8x020 refractory/abrasive), read from their surviving legacy FGDC
# documents (all verified 2026-11-16). Diatomite and Trona are shared with the
# refractory/abrasive layer, so they stay in MISCELLANEOUS_COMMODITIES and are not
# exclusion indicators.
ATLAS_EXCLUDED_COMMODITIES = ('Aluminum', 'Antimony', 'Abrasive minerals', 'Ball clay',
                              'Bentonite', 'Boron', 'Cadmium', 'Cement', 'Chromium', 'Cobalt',
                              'Columbium', 'Common clay and Shale', 'Copper', 'Dimension stone',
                              'Feldspar', 'Garnet', 'Gemstones', 'Germanium', 'Gold', 'Gypsum',
                              'Iron', 'Kyanite', 'Lead', 'Magnesium compounds', 'Magnesium metal',
                              'Manganese', 'Mica', 'Molybdenum', 'Nickel', 'Olivine', 'Peat',
                              'Perlite', 'Phosphate', 'Potash', 'Pumice', 'Pyrophyllite',
                              'Refractory minerals', 'Rhenium', 'Selenium', 'Silica', 'Silicon',
                              'Silver', 'Strontium', 'Sulfur', 'Talc', 'Tantalum', 'Titanium metal',
                              'Tungsten', 'Vermiculite', 'Wollastonite', 'Zeolites', 'Zircon', 'Zinc')
MISCELLANEOUS_ROWS = 304


def producer_hosts():
    return {urlsplit(pin['url']).hostname for pin in PINS.values()}


def parse_atlas_scope(body):
    """Validate the surviving USGS legacy FGDC document against the pinned scope facts.

    Producer-contract record exercised by synthetic-fixture tests; never a data
    input and never fetched by this module.
    """
    root = ET.fromstring(body)

    def text(path):
        return (root.findtext(path) or '').strip()

    if text('.//citation//title') != ATLAS_TITLE or text('.//citation//pubdate') != ATLAS_PUBDATE:
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_metadata_title_or_pubdate_drift')
    if text('.//onlink') != ATLAS_ONLINK:
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_original_download_link_drift')
    if text('.//procstep//procdate') != ATLAS_PROCSTEP_DATE or text('.//metd') != ATLAS_METD:
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_metadata_process_or_metadata_date_drift')
    if text('.//horizdn') != 'North American Datum of 1983':
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_metadata_datum_drift')
    declared = int(text('.//ptvctcnt'))
    if declared != MISCELLANEOUS_ROWS:
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_declared_entity_point_count_drift')
    id_attr = next((attr for attr in root.findall('.//attr')
                    if (attr.findtext('attrlabl') or '').strip() == ATLAS_ID_FIELD), None)
    if id_attr is None or [e.text.strip() for e in id_attr.findall('.//rdommin')] \
            + [e.text.strip() for e in id_attr.findall('.//rdommax')] != list(ATLAS_ID_RANGE):
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_identifier_range_drift')
    plant_attr = next((attr for attr in root.findall('.//attr')
                       if (attr.findtext('attrlabl') or '').strip().lower() == 'plant_min'), None)
    domain = tuple(e.text.strip() for e in plant_attr.findall('.//edomv')) if plant_attr is not None else ()
    if domain != ATLAS_PLANT_MIN_DOMAIN:
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_plant_mine_domain_drift')
    if next((attr for attr in root.findall('.//attr')
             if (attr.findtext('attrlabl') or '').strip().lower() == 'commodity'), None) is None:
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_commodity_attribute_missing')
    themekeys = [e.text.strip() for e in root.findall('.//themekey')]
    covered = {key for key in themekeys if key in MISCELLANEOUS_COMMODITIES}
    if covered != set(MISCELLANEOUS_COMMODITIES) \
            or any(commodity in themekeys for commodity in ATLAS_EXCLUDED_COMMODITIES):
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_commodity_class_scope_drift')
    if 'miscellaneous industrial minerals' not in (root.findtext('.//enttypd') or '').lower():
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_entity_description_drift')

    def declared_range(label):
        attr = next((attr for attr in root.findall('.//attr')
                     if (attr.findtext('attrlabl') or '').strip().lower() == label), None)
        if attr is None:
            raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_' + label + '_attribute_missing')
        values = [e.text.strip() for e in attr.findall('.//rdommin')] \
            + [e.text.strip() for e in attr.findall('.//rdommax')]
        if len(values) != 2:
            raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_' + label + '_range_incomplete')
        return tuple(values)

    latitude_range = declared_range('latitude')
    longitude_range = declared_range('longitude')
    if (latitude_range, longitude_range) != (ATLAS_LATITUDE_RANGE, ATLAS_LONGITUDE_RANGE):
        raise MiscellaneousIndustrialMineralsQuarantineError('atlas_legacy_declared_coordinate_range_drift')
    return {'approved': False,
            'purpose': 'pin the producer scope of the retired National Atlas miscellaneous-industrial-minerals '
                       'layer; producer-contract record, never a data input',
            'atlas_layer': ATLAS_TITLE, 'atlas_pubdate': ATLAS_PUBDATE,
            'atlas_process_date': ATLAS_PROCSTEP_DATE, 'atlas_metadata_date': ATLAS_METD,
            'atlas_original_download': ATLAS_ONLINK, 'declared_entity_points': declared,
            'declared_plant_mine_domain': list(ATLAS_PLANT_MIN_DOMAIN),
            'declared_commodity_classes': list(MISCELLANEOUS_COMMODITIES),
            'excluded_commodity_classes': list(ATLAS_EXCLUDED_COMMODITIES),
            'declared_latitude_range': list(ATLAS_LATITUDE_RANGE),
            'declared_longitude_range': list(ATLAS_LONGITUDE_RANGE),
            'source': 'U.S. Geological Survey National Geospatial Technical Operations Center legacy metadata '
                      'WAF; retained producer metadata'}


def parse_mineplant_release_status(body):
    """Validate the producer's FGDC release-status document against the pinned facts.

    The pinned document declares a one-time release (progress Complete, update
    frequency None planned), which is why the mineplant package is not a
    maintained successor input.  If USGS ever publishes a different release
    status there, this parser fails closed so the change is re-evaluated
    explicitly instead of being absorbed silently.
    """
    root = ET.fromstring(body)

    def text(path):
        return (root.findtext(path) or '').strip()

    if text('.//citation//title') != MINEPLANT_METADATA_TITLE:
        raise MiscellaneousIndustrialMineralsQuarantineError('mineplant_release_status_title_drift')
    labels = tuple((attr.findtext('attrlabl') or '').strip() for attr in root.findall('.//attr'))
    if labels != MINEPLANT_METADATA_ATTRS:
        raise MiscellaneousIndustrialMineralsQuarantineError('mineplant_release_status_attribute_roster_drift')
    if text('.//metd') != MINEPLANT_METADATA_METD:
        raise MiscellaneousIndustrialMineralsQuarantineError('mineplant_release_status_metadata_date_drift')
    progress = text('.//progress')
    update = text('.//update')
    if progress != MINEPLANT_PROGRESS or update != MINEPLANT_UPDATE:
        raise MiscellaneousIndustrialMineralsQuarantineError(
            'mineplant_release_status_changed_re_evaluate_source_eligibility:' + progress + '/' + update)
    return {'approved': False, 'title': MINEPLANT_METADATA_TITLE, 'metadata_date': MINEPLANT_METADATA_METD,
            'declared_ground_condition': MINEPLANT_GROUND_CONDITION, 'declared_progress': MINEPLANT_PROGRESS,
            'declared_update_frequency': MINEPLANT_UPDATE, 'one_time_release': True,
            'eligibility': 'one-time producer release with no planned refresh; not a maintained successor and '
                           'excluded from pipeline inputs by the current source policy'}


PARK_EVIDENCE = {
    'decision': 'park or retire; no maintained USGS producer publication can recreate the assigned '
                'entity-point layer',
    'original_release': {
        'url': ATLAS_ONLINK, 'publisher': 'U.S. Geological Survey National Atlas (producer publication 200512)',
        'status': 'retired from the live server; HTTP 404 observed 2026-11-10 and re-verified 2026-11-16 '
                  'after the host redirect to https',
        'scope_anchor': LEGACY_METADATA_URL},
    'banned_archive': 'DataLumos project 239600 V1 (the inventory download location) is a banned archive '
                      'source and was never fetched',
    'mineplant_survey_publication': {
        'url': MINEPLANT_ZIP_URL, 'csv_url': MINEPLANT_CSV_ZIP_URL, 'metadata_url': MINEPLANT_METADATA_URL,
        'publisher': 'U.S. Geological Survey National Minerals Information Center',
        'declared_ground_condition': MINEPLANT_GROUND_CONDITION, 'declared_progress': MINEPLANT_PROGRESS,
        'declared_update_frequency': MINEPLANT_UPDATE,
        'verified': '2026-11-16 anonymous retrieval matching the pinned digests',
        'scope_match': 'the producer CSV 15-commodity filter yields exactly 304 rows with per-commodity counts '
                       'identical to the archived layer recorded facts (Lime 89, Salt 68, Barite 29, Kaolin 28, '
                       'Fullers Earth 24, Iron Oxide Pigments 19, Diatomite 11, Titanium Dioxide Pigment 10, '
                       'Bromine 7, Soda Ash 7, Titanium Minerals 5, Iodine 3, Sodium Sulfate 2, Lithium 1, '
                       'Trona 1; 43 states; Plant_min M 37 / M-P 164 / P 103), re-derived from the producer '
                       'CSV on 2026-11-16',
        'record_identity_difference': 'a different product, not a faithful reconstruction: CSV id 1..6841 '
                                      '(6785 rows) instead of the Minop5x020 1..305 roster (304 rows, one '
                                      'producer gap at 187), one multi-commodity site split into two rows, '
                                      'and 3-decimal coordinate export instead of the archived DBF 6-decimal '
                                      'values',
        'eligibility': 'one-time release with no planned refresh; excluded from pipeline inputs by the '
                       'current source policy'},
    'current_alternatives_checked': [
        {'product': 'Mines, smelters, refineries, and recycling facilities in the United States (USGS NMIC)',
         'doi': '10.5066/P1BUPUAM', 'sciencebase_item': '6a3edaf71ba49b7c2e263528', 'published': '20260629',
         'data_period': '20241204..20260430', 'check_date': '2026-11-16',
         'finding': 'tabular worksheet only; no coordinate columns (location is town/state text), so it '
                    'cannot recreate the entity-point layer; its own FGDC metadata also declares progress '
                    'Complete and update None planned (verified by dated project research 2026-11-13)'},
        {'product': 'Mineral Resource Data System (MRDS)', 'url': 'https://mrdata.usgs.gov/mrds/',
         'check_date': '2026-11-12',
         'finding': 'dated prior project research: updates ceased and the product is a materially different '
                    'global deposit database, not a maintained publication of active US operations'},
        {'product': 'USMIN mineral deposit areas', 'url': 'https://mrdata.usgs.gov/deposit/',
         'check_date': '2026-11-12',
         'finding': 'dated prior project research: developing critical-minerals product, different scope'},
        {'product': 'ScienceBase and TNM Access searches for the layer identifier and title',
         'check_date': '2026-11-16',
         'finding': 'no ScienceBase item for minop5x020 or Miscellaneous Industrial Minerals Operations; no '
                    'current USGS distribution of the retired layer was found there'}]}


def stage_miscellaneous_industrial_mineral_operations(storage=None, *, capture_dir=None, package_id=None,
                                                      **_unused):
    """Fail closed: the dataset is parked; nothing is fetched, read or written."""
    raise MiscellaneousIndustrialMineralsQuarantineError(PARK_REASON + ': ' + PARK_STATEMENT)
