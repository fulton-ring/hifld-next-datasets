"""Parked record for the HIFLD "US Coal Fields" polygon layer.

The assigned dataset is the "US Coal Fields" polygon layer of the retired HIFLD
Open catalog (inventory ID ``us-coal-fields-1``, Agency U.S. Geological Survey).
The DHS crosswalk row records Status "Deprecate" (New Hosting N/A,
justification "No longer supported as part of the HIFLD mission") and the
inventory Download Location is the DataLumos project 240310 archive — a banned
archive source that was never fetched.

Two USGS producer publications were investigated and both fail the current
source policy's maintained-publication requirement:

- The origin publication, USGS Open-File Report 2012-1205, "Coal Fields of the
  Conterminous United States: National Coal Resource Assessment Updated
  Version" (East, J.A., first posted April 2, 2013), is a completed one-time
  release: its producer FGDC metadata (``Downloads/Metadata/Coal_Fields.xml``,
  digest-pinned below) declares ``<progress>Complete</progress>`` and
  ``<update>None planned</update>``. It is permanently available but not
  maintained, so it is not a pipeline input.
- The live NCRDS "CoalByRank" MapServer layer 0 ("CoalFieldsbyRank") on USGS's
  own ArcGIS Server (``energy.usgs.gov``, currentVersion 12.1) serves 602
  records equal to the OFR release (verified 2026-11-14: attributes
  row-for-row, served rings vertex-identical after projection), but the
  producer publishes no maintenance or refresh evidence for this exact layer:
  the layer document exposes no ``editingInfo``, ``lastEditDate`` or
  ``editFieldsInfo`` (checked 2026-11-14); the service iteminfo carries no
  modified/created dates; the service ``info/metadata`` CreaDate (2024-03-29)
  is metadata creation, not a data update; the EUSEF project's Data Management
  task states it provides "a central administrative repository for existing
  solid energy fuel data" ensuring products "continue to be available" —
  availability preservation, not refresh; and the National Coal Resource
  Assessment science topic is marked "Active Completed". A live service
  serving an unchanged one-time release without producer maintenance evidence
  is not a maintained publication under the source policy.

No other USGS product recreates this national coal-fields polygon layer (see
``PARK_EVIDENCE``), so the dataset is parked: every staging attempt fails
closed and this module defines and performs no fetching, no decoding, no
staging and no candidate output. The parsers below fail closed if the producer
ever changes the one-time release status or publishes maintenance/edit
metadata on the live layer, so a potential maintained successor is
re-evaluated explicitly instead of being absorbed silently. Resuming requires
an explicit maintainer decision (park permanently, retire, or name a
maintained producer source).
"""
from __future__ import annotations

import json
import re
from urllib.parse import urlsplit

SLUG = 'us-coal-fields-1'
PARK_REASON = 'no_maintained_producer_publication_park_or_retire'
PARK_STATEMENT = ('no maintained USGS producer publication of the national coal-fields polygon layer is '
    'established: the origin publication OFR 2012-1205 is a completed one-time release (FGDC Progress '
    'Complete, maintenance and update frequency None planned) and the live producer NCRDS CoalByRank '
    'service serves that unchanged release with no producer maintenance or edit metadata; resuming '
    'requires an explicit maintainer decision (park permanently, retire, or name a maintained producer '
    'source)')


class UsCoalFieldsQuarantineError(ValueError):
    pass


# Producer-contract research record. All URLs are USGS-owned; the module never
# fetches them. Digests were observed on anonymous 2026-11-14 captures.
OFR_DOWNLOADS_URL = 'https://pubs.usgs.gov/of/2012/1205/Downloads/'
OFR_FGDC_METADATA_URL = 'https://pubs.usgs.gov/of/2012/1205/Downloads/Metadata/Coal_Fields.xml'
OFR_GIS_ZIP_URL = 'https://pubs.usgs.gov/of/2012/1205/Downloads/GIS.zip'
NCRDS_DIRECTORY_URL = 'https://energy.usgs.gov/arcgis/rest/services/NCRDS?f=pjson'
COALBYRANK_SERVICE_URL = 'https://energy.usgs.gov/arcgis/rest/services/NCRDS/CoalByRank/MapServer'
COALBYRANK_LAYER_URL = COALBYRANK_SERVICE_URL + '/0'
EUSEF_DATA_MANAGEMENT_URL = ('https://www.usgs.gov/centers/geology-energy-and-minerals-science-center/'
                             'science/evolving-utilization-solid-energy-fuels')
PINS = {
    'usgs_ofr_fgdc_metadata': {
        'url': OFR_FGDC_METADATA_URL, 'bytes': 43681,
        'sha256': '17299068e32ab1bffce6391fd803ce70a6dd08fea91de49772e432fbb2833ba5',
        'observed': '2026-11-14', 'role': 'research_record_one_time_release_status'},
    'usgs_ofr_gis_zip': {
        'url': OFR_GIS_ZIP_URL, 'bytes': 230322811,
        'sha256': 'e491215d0a937cae48cc5e9a2763ea43c85a4d11c183ec7c5b1ac559d2a585a5',
        'observed': '2026-11-14', 'role': 'research_record_one_time_release_payload'},
    'usgs_ncrds_service_directory': {
        'url': NCRDS_DIRECTORY_URL, 'bytes': 438,
        'sha256': '050896107a35e8dcc00c1df05b384e57ae2653b4297dd10476660844d8564212',
        'observed': '2026-11-14', 'role': 'research_record_live_service_roster'},
    'usgs_eusef_data_management': {
        'url': EUSEF_DATA_MANAGEMENT_URL, 'bytes': 302895,
        'sha256': 'e33225f2c29dab06dd1f34fe5533385f400da7f591299a5050b3f923592a0cb8',
        'observed': '2026-11-14', 'role': 'research_record_availability_repository_statement'},
}
OFR_TITLE = 'Coal Fields of the Conterminous United States: National Coal Resource Assessment Updated Version'
OFR_FIRST_POSTED = '2013-04-02'
EUSEF_DATA_MANAGEMENT_SENTENCE = ('this task provides a central administrative repository for existing solid '
    'energy fuel data, ensuring that these valuable USGS database products will continue to be available to '
    'interested users')
COALBYRANK_LAYER_NAME = 'CoalFieldsbyRank'


def producer_hosts():
    return {urlsplit(pin['url']).hostname for pin in PINS.values()}


def parse_ofr_release_status(body):
    """Validate the producer FGDC status of the origin OFR 2012-1205 release.

    Producer-contract record exercised by synthetic-fixture tests; never a
    data input and never fetched by this module. The release must still
    declare progress Complete with maintenance and update frequency None
    planned. If USGS ever declares a different progress state or a planned
    maintenance/update frequency there, this parser fails closed so a
    maintained or superseded release is re-evaluated explicitly instead of
    being absorbed silently.
    """
    text = body.decode('utf-8', 'replace')
    compact = re.sub(r'\s+', '', text).lower()
    if '<progress>complete</progress>' not in compact:
        raise UsCoalFieldsQuarantineError('ofr_release_status_changed_re_evaluate_source_eligibility')
    if '<update>noneplanned</update>' not in compact:
        raise UsCoalFieldsQuarantineError('ofr_maintenance_frequency_changed_re_evaluate_source_eligibility')
    return {'approved': False, 'progress': 'Complete', 'update_frequency': 'None planned',
            'one_time_release': True,
            'eligibility': 'completed one-time publication, permanently available but not maintained; '
                           'not a pipeline input under the current source policy'}


def parse_layer_maintenance(body):
    """Validate the live CoalByRank layer document for maintenance indicators.

    The live service document observed 2026-11-14 exposes no editingInfo,
    lastEditDate or editFieldsInfo: the producer publishes no maintenance or
    edit metadata for this layer. If any of those indicators ever appears,
    the producer has begun serving edit metadata for the layer and this
    parser fails closed so a potential maintained successor is re-evaluated
    explicitly instead of being absorbed silently.
    """
    try:
        doc = json.loads(body.decode('utf-8', 'replace'))
    except ValueError as error:
        raise UsCoalFieldsQuarantineError('coalbyrank_layer_document_unparseable') from error
    indicators = {}
    for name in ('editingInfo', 'lastEditDate', 'editFieldsInfo'):
        value = doc.get(name)
        if value not in (None, {}):
            raise UsCoalFieldsQuarantineError(
                'coalbyrank_maintenance_metadata_published_re_evaluate_source_eligibility:' + name)
        indicators[name] = value
    return {'approved': False, 'live_service': True, 'layer_name': doc.get('name'),
            'editing_info': indicators['editingInfo'], 'last_edit_date': indicators['lastEditDate'],
            'edit_fields_info': indicators['editFieldsInfo'],
            'eligibility': 'live producer service with no maintenance or edit metadata; serves the '
                           'unchanged one-time OFR 2012-1205 release; not established as a maintained '
                           'publication under the current source policy'}


PARK_EVIDENCE = {
    'decision': 'park or retire; no maintained USGS producer publication of the national coal-fields '
                'polygon layer is established',
    'retired_hifld_item': {
        'inventory': 'us-coal-fields-1 "US Coal Fields"',
        'crosswalk': 'DHS HIFLD Open crosswalk DOI 10.3886/E241367V1 row "US Coal Fields": Status Deprecate, '
                     'New Hosting N/A, justification "No longer supported as part of the HIFLD mission", '
                     'external landing page the OFR 2012-1205 Downloads directory',
        'banned_archive': 'DataLumos project 240310 (the inventory Download Location) is a banned archive '
                          'source and was never fetched'},
    'one_time_origin_publication': {
        'url': OFR_DOWNLOADS_URL, 'publisher': 'U.S. Geological Survey',
        'title': OFR_TITLE, 'first_posted': OFR_FIRST_POSTED,
        'fgdc_metadata': PINS['usgs_ofr_fgdc_metadata'],
        'producer_status': 'FGDC <progress>Complete</progress> and <update>None planned</update>; the '
                           'official landing page describes the 2021 revision as a Simplified Presentation '
                           'Format that changed no data',
        'verified': '2026-11-14 anonymous retrieval of the FGDC metadata matching the pinned digest (the '
                    'same digest was captured by the superseded draft and matched by the independent '
                    'reviewer earlier the same day)',
        'eligibility': 'completed one-time publication with maintenance and update frequency None planned; '
                       'permanently available but not maintained; excluded from pipeline inputs by the '
                       'current source policy'},
    'live_service_candidate_checked': {
        'url': COALBYRANK_LAYER_URL, 'publisher': 'U.S. Geological Survey (Energy Resources Program)',
        'service': 'NCRDS CoalByRank MapServer layer 0 "CoalFieldsbyRank" on energy.usgs.gov '
                   '(producer-owned ArcGIS Server, currentVersion 12.1)',
        'content_fidelity_verified': '2026-11-14: all 602 served records equal the OFR 2012-1205 '
                                     'Coal_Fields rows exactly in record order and the served Web Mercator '
                                     'rings are vertex-identical to the NAD83 source after projection',
        'maintenance_evidence_checked_and_absent': [
            'layer document exposes no editingInfo, lastEditDate or editFieldsInfo (checked 2026-11-14)',
            'service iteminfo carries no modified or created dates (checked 2026-11-14)',
            'service info/metadata CreaDate 2024-03-29 is metadata creation, not a data update',
            'the EUSEF Data Management task (page updated 2024-08-28) states it provides a central '
            'administrative repository for existing solid energy fuel data ensuring products continue to '
            'be available — availability preservation, not refresh',
            'the National Coal Resource Assessment science topic is marked "Active Completed"'],
        'eligibility': 'a live service serving an unchanged one-time release without producer maintenance '
                       'evidence is not a maintained publication under the current source policy'},
    'current_alternatives_checked': [
        {'product': 'NCRDS CoalByAge and CoalProvinces MapServices',
         'url': 'https://energy.usgs.gov/arcgis/rest/services/NCRDS', 'check_date': '2026-11-14',
         'finding': 'sibling services of the same origin publication OFR 2012-1205 with the same absent '
                    'maintenance metadata; they are renderings of the one-time release, not separate '
                    'maintained products'},
        {'product': 'EUSEF Data Management task',
         'url': EUSEF_DATA_MANAGEMENT_URL, 'check_date': '2026-11-14',
         'finding': 'producer statement of availability preservation ("central administrative repository '
                    'for existing solid energy fuel data ... will continue to be available"); describes '
                    'no update or refresh of the coal-fields mapping'},
        {'product': 'Maps of Energy Occurrence task',
         'url': 'https://www.usgs.gov/centers/geology-energy-and-minerals-science-center/science/'
                'maps-energy-occurrence', 'check_date': '2026-11-14',
         'finding': 'task page last updated 2018-11-13; its listed publications are coking-coal sample '
                    'data releases and a Mongolia GIS compilation; no national US coal-fields layer '
                    'product'},
        {'product': 'U.S. Coal Resources and Reserves Assessments (EUSEF task)',
         'url': EUSEF_DATA_MANAGEMENT_URL, 'check_date': '2026-11-14',
         'finding': 'regional-scale coal resource and reserve assessments of major coal basins; no '
                    'national coal-fields polygon layer publication'}]}


def stage_us_coal_fields(storage=None, **_unused):
    """Fail closed: the dataset is parked; nothing is fetched, read or written."""
    raise UsCoalFieldsQuarantineError(PARK_REASON + ': ' + PARK_STATEMENT)
