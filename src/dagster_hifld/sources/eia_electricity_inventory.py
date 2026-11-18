"""Maintained EIA-860M operating inventory, retained as unapproved native tables.

Coordinates remain attributes until their datum is established. Proposed,
retired and canceled projects are retained in the raw workbook only.
Construction witnesses currently support local staging only; remote staging is
rejected before capture or writes rather than attempting a remote protocol.
"""
from __future__ import annotations

from pathlib import Path

import httpx
import pyarrow as pa

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.capture_common import processing_identity
from dagster_hifld.sources.eia_860m_common import (
    APPROVALS, DOCUMENT_URLS, MONTHS, NS, OPERATING_SHEETS, PAGE_URL, REQUIRED_FIELDS,
    capture_workbook_documents, latest_release, normalize_existing, read_existing_workbook,
    supporting_evidence_hashes, write_native_candidate,
)

DATASET_SLUG = 'eia-860m-electricity-inventory'
GENERATOR_FILE = 'existing-generators'
PLANT_FILE = 'plant-reference'

LIST_FIELDS = {'Plant Names', 'Plant States', 'generator_keys', 'statuses'}
TYPE_OVERRIDES = {'generator_count': pa.int64(), **{field: pa.list_(pa.string()) for field in LIST_FIELDS}}


def capture_inventory(storage: StagingStorageResource, *, capture_directory: str|None=None, replay: bool=False, client: httpx.Client|None=None) -> dict:
    """Construct two native drafts with local-only immutable construction state."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource): raise ValueError('explicit_staging_resource_required')
    if not (storage.use_local or not storage.bucket): raise ValueError('eia860m_requires_local_staging_construction_protocol')
    if replay and not capture_directory: raise ValueError('replay_requires_capture_directory')
    capture = capture_workbook_documents(storage, dataset_slug=DATASET_SLUG, capture_directory=capture_directory, replay=replay, client=client)
    identity = processing_identity(Path(__file__).parents[3]); results = []
    for file_slug,rows in ((GENERATOR_FILE,capture.generators),(PLANT_FILE,capture.plants)):
        fields={}
        for field in rows[0]:
            native=file_slug==GENERATOR_FILE and field not in {'eia_generator_key','eia_source_sheet'}
            fields[field]={'source_fields':[field] if native else ['Plant ID','Generator ID'] if field in {'eia_generator_key','generator_keys','generator_count'} else ['Latitude','Longitude'] if field in {'Latitude','Longitude','coordinate_status'} else ['Plant Name'] if field=='Plant Names' else ['Plant State'] if field=='Plant States' else ['Status'] if field=='statuses' else [],'input_ids':['workbook'],'basis':'publisher' if native else 'derived','description':'Native XLSX cached lexical cell value; null means absent cell. Source column heading retained.' if native else 'Derived from existing generator rows; source generator records and workbook retained for inspection.'}
            if '(MW)' in field: fields[field]['units']='MW'
            if '(MWh)' in field: fields[field]['units']='MWh'
        fields['Plant ID']['source_fields']=['Plant ID']
        fields['Plant ID']['identifier_meaning']='Native lexical EIA Plant ID; grouping key, never renumbered.'
        if file_slug==GENERATOR_FILE:
            fields['Generator ID']['identifier_meaning']='Native lexical generator identifier within Plant ID; leading zeros preserved.'
            fields['eia_generator_key']['identifier_meaning']='JSON array of native lexical Plant ID and Generator ID; unambiguous composite identity.'
            fields['Unit Code']['identifier_meaning']='Native lexical Unit Code attribute, distinct from Generator ID; not used as generator identity or as a deduplication key. Unit grouping semantics are not established by this draft.'
            fields['eia_source_sheet']['source_fields'] = ['workbook sheet name']
            fields['eia_source_sheet']['description'] = 'Exact retained Operating or Operating_PR sheet name, identifying the producer source population.'
            fields['Status']['description'] = 'Native operating-sheet status text including OP, SB, OA, or OS prefix; retained verbatim. Detailed code/domain and sentinel definitions require source-contract review.'
        source_relationship = {'relationship':'unverified','evidence_input_ids':['workbook','inventory_page'],
                               'unknown_reason':'Equivalence to legacy HIFLD plant and generator populations has not been established; this is a source-native operating inventory draft.'}
        transformations=[{'operation':'Retain all existing-generator source columns as lexical cached cell values; exclude nonexisting generator sheets from transformed outputs','parameters':{'sheets':list(OPERATING_SHEETS),'period':capture.period,'native_columns':capture.layout['operating_sheets']['Operating']['columns']}},{'operation':'Group plant references by native Plant ID without capacity aggregation or coordinate averaging','parameters':{'coordinate_policy':'only unanimous numeric coordinate pairs retained; conflicts and missing coordinates remain null','coordinate_datum':None,'geometry_created':False}}]
        result = write_native_candidate(
            storage, dataset_slug=DATASET_SLUG, file_slug=file_slug, rows=rows, capture=capture, identity=identity,
            title=f'EIA-860M {file_slug} ({capture.period}; draft)',
            description=capture.audit['population_scope']+' Preliminary estimates subject to correction. Native nonspatial draft; coordinate datum and acceptance remain unresolved. Plant reference groups existing generator records by native Plant ID; no capacity aggregation or conflicting-coordinate averaging.',
            field_definitions=fields,
            coverage={key:capture.audit[key] for key in ('population_scope','state_counts','puerto_rico_generators','status_counts','generator_count','plant_count','coordinate_datum','coordinate_gate')},
            transformations=transformations, lineage=[PAGE_URL,*DOCUMENT_URLS.values()], source_relationship=source_relationship,
            audit_document=capture.audit, type_overrides=TYPE_OVERRIDES,
            format_transformations={'parquet': [
                {'name': 'Preserve native cached XLSX values as nullable lexical strings, with composite JSON-string generator identity; formulas without cached values rejected.', 'input_ids': ['workbook']},
                {'name': 'Plant reference groups lexical Plant ID; lists retain names, states, statuses and generator keys, generator_count is integer. Coordinates remain strings or null; no geometry or CRS assigned.', 'input_ids': ['workbook']},
            ]})
        results.append(result)
    return {'candidates':results,'source_period':capture.period,'generator_count':len(capture.generators),'plant_count':len(capture.plants),'source_population_complete':True,'capture_directory':str(capture.root),'refresh_status':'STOPPED',**APPROVALS}
