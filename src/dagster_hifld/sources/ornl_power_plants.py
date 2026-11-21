"""Power Plants candidate aggregated from the current EIA-860M operating inventory.

The dataset keeps the inventory identity `power-plants-1`; the producer is EIA,
not the historical HIFLD/ORNL archive. Coordinates remain native lexical
attributes because the producer datum is not established, so this is a
nonspatial draft with every release approval false.
"""
from __future__ import annotations

import math
from collections import defaultdict
from pathlib import Path

import httpx
import pyarrow as pa

from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.capture_common import processing_identity
from dagster_hifld.sources.eia_860m_common import (
    APPROVALS, DOCUMENT_URLS, OPERATING_SHEETS, PAGE_URL,
    capture_workbook_documents, write_native_candidate,
)

DATASET_SLUG = FILE_SLUG = 'power-plants-1'
AGGREGATION_FIELDS = {
    'Plant ID', 'Plant Name', 'Plant State', 'County', 'Latitude', 'Longitude',
    'Nameplate Capacity (MW)', 'Net Summer Capacity (MW)', 'Technology', 'Energy Source Code',
}
CAPACITY_FIELDS = ('Nameplate Capacity (MW)', 'Net Summer Capacity (MW)')
TYPE_OVERRIDES = {
    'generator_count': pa.int64(),
    'total_nameplate_capacity_mw': pa.float64(),
    'total_net_summer_capacity_mw': pa.float64(),
    'Plant Names': pa.list_(pa.string()),
    'Plant States': pa.list_(pa.string()),
    'Plant Counties': pa.list_(pa.string()),
    'statuses': pa.list_(pa.string()),
}


def _capacity(value, field):
    if value is None or not value.strip(): return None
    try: numeric = float(value)
    except ValueError: raise ValueError('invalid_source_capacity:' + field) from None
    if not math.isfinite(numeric) or numeric < 0: raise ValueError('invalid_source_capacity:' + field)
    return numeric


def _total(values):
    present = [value for value in values if value is not None]
    return math.fsum(present) if present else None


def _primary(rows, field, capacities):
    totals = defaultdict(list)
    for row, capacity in zip(rows, capacities):
        value = row.get(field)
        if value is None or not value.strip(): continue
        totals[value.strip()].append(capacity or 0.0)
    if not totals: return None
    return sorted(((math.fsum(values), name) for name, values in totals.items()), key=lambda item: (-item[0], item[1]))[0][1]


def aggregate_plants(generators: list[dict]) -> list[dict]:
    """Aggregate retained operating generators by native EIA Plant ID."""
    groups = defaultdict(list)
    for record in generators:
        groups[record['Plant ID']].append(record)
    plants = []
    for plant, rows in sorted(groups.items(), key=lambda item: int(item[0])):
        coordinates = {(r.get('Latitude'), r.get('Longitude')) for r in rows}
        missing = any(x is None or not x.strip() or y is None or not y.strip() for x, y in coordinates)
        numeric = {(float(x), float(y)) for x, y in coordinates if x and x.strip() and y and y.strip()}
        coordinate_status = 'conflicting' if len(numeric) > 1 else 'missing' if missing or not numeric else 'consistent'
        lat, lon = min(coordinates) if coordinate_status == 'consistent' else (None, None)
        nameplate = [_capacity(r.get('Nameplate Capacity (MW)'), 'Nameplate Capacity (MW)') for r in rows]
        summer = [_capacity(r.get('Net Summer Capacity (MW)'), 'Net Summer Capacity (MW)') for r in rows]
        plants.append({
            'Plant ID': plant,
            'Plant Names': sorted({r.get('Plant Name') for r in rows if r.get('Plant Name')}),
            'Plant States': sorted({r['Plant State'] for r in rows}),
            'Plant Counties': sorted({r.get('County') for r in rows if r.get('County')}),
            'Latitude': lat,
            'Longitude': lon,
            'coordinate_status': coordinate_status,
            'generator_count': len(rows),
            'total_nameplate_capacity_mw': _total(nameplate),
            'total_net_summer_capacity_mw': _total(summer),
            'primary_technology': _primary(rows, 'Technology', nameplate),
            'primary_energy_source_code': _primary(rows, 'Energy Source Code', nameplate),
            'statuses': sorted({r['Status'] for r in rows}),
        })
    return plants


def _field_definitions() -> dict:
    def derived(source_fields, description, **extra):
        return {'source_fields': source_fields, 'input_ids': ['workbook'], 'basis': 'derived', 'description': description, **extra}
    return {
        'Plant ID': {'source_fields': ['Plant ID'], 'input_ids': ['workbook'], 'basis': 'publisher',
                     'identifier_meaning': 'Native lexical EIA Plant ID; aggregation key, never renumbered.',
                     'description': 'Native XLSX cached lexical cell value; null means absent cell.'},
        'Plant Names': derived(['Plant Name'], 'Sorted distinct native plant names across the retained operating generator rows.'),
        'Plant States': derived(['Plant State'], 'Sorted distinct native plant states across the retained operating generator rows.'),
        'Plant Counties': derived(['County'], 'Sorted distinct native county names across the retained operating generator rows.'),
        'Latitude': derived(['Latitude', 'Longitude'], 'Native lexical latitude retained only when every retained generator at the plant has the same non-blank numeric pair; otherwise null. No datum or geometry is assigned.'),
        'Longitude': derived(['Latitude', 'Longitude'], 'Native lexical longitude retained only when every retained generator at the plant has the same non-blank numeric pair; otherwise null. No datum or geometry is assigned.'),
        'coordinate_status': derived(['Latitude', 'Longitude'], 'One of consistent, missing or conflicting for the plant coordinate pair; coordinates are never averaged.'),
        'generator_count': derived(['Plant ID'], 'Number of retained operating generator rows aggregated to this Plant ID.', units='count'),
        'total_nameplate_capacity_mw': derived(['Nameplate Capacity (MW)'], 'Sum of numeric native nameplate capacity cells; blank cells excluded and invalid values fail capture.', units='MW'),
        'total_net_summer_capacity_mw': derived(['Net Summer Capacity (MW)'], 'Sum of numeric native net summer capacity cells; blank cells excluded and invalid values fail capture.', units='MW'),
        'primary_technology': derived(['Technology', 'Nameplate Capacity (MW)'], 'Technology with the largest summed nameplate capacity at the plant; ties broken lexicographically. No imputation.'),
        'primary_energy_source_code': derived(['Energy Source Code', 'Nameplate Capacity (MW)'], 'Energy source code with the largest summed nameplate capacity at the plant; ties broken lexicographically. No imputation.'),
        'statuses': derived(['Status'], 'Sorted distinct native operating-sheet status strings across the retained generator rows.'),
    }


def capture_power_plants(storage: StagingStorageResource, *, capture_directory: str|None=None, replay: bool=False, client: httpx.Client|None=None) -> dict:
    """Construct the unapproved nonspatial plant aggregate from the current workbook."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource): raise ValueError('explicit_staging_resource_required')
    if not (storage.use_local or not storage.bucket): raise ValueError('eia860m_requires_local_staging_construction_protocol')
    if replay and not capture_directory: raise ValueError('replay_requires_capture_directory')
    capture = capture_workbook_documents(storage, dataset_slug=DATASET_SLUG, capture_directory=capture_directory, replay=replay, client=client)
    columns = set(capture.layout['operating_sheets']['Operating']['columns'])
    if not AGGREGATION_FIELDS <= columns: raise ValueError('power_plants_aggregation_schema_drift')
    plants = aggregate_plants(capture.generators)
    if not plants: raise ValueError('empty_power_plants_population')
    identity = processing_identity(Path(__file__).parents[3])
    coverage = {
        'population_scope': 'Operating and Operating_PR sheets, including source operating/standby/out-of-service statuses, aggregated by native Plant ID. Excludes planned, retired and canceled sheets; raw workbook retained.',
        'generator_count': len(capture.generators),
        'plant_count': len(plants),
        'state_counts': capture.audit['state_counts'],
        'status_counts': capture.audit['status_counts'],
        'puerto_rico_generators': capture.audit['puerto_rico_generators'],
        'plant_coordinate_counts': capture.audit['plant_coordinate_counts'],
        'coordinate_datum': None,
        'coordinate_gate': 'Producer datum not established; coordinates remain native lexical attributes, no geometry or assumed CRS.',
        'source_population_complete': True,
    }
    audit_document = {
        **capture.audit,
        'capacity_policy': 'Nameplate and net summer capacity summed from numeric native cells with math.fsum; blank cells excluded; negative or non-finite values fail capture.',
        'primary_fuel_policy': 'Technology and energy source code selected by largest summed nameplate capacity per plant; ties broken lexicographically; no imputation.',
        'aggregation_key': 'Native lexical EIA Plant ID.',
        'source_population_complete': True,
    }
    source_relationship = {
        'relationship': 'unverified', 'evidence_input_ids': ['workbook', 'inventory_page'],
        'unknown_reason': 'Equivalence to the historical HIFLD power-plant population and its identifiers is not established; this is a source-native aggregation of the current EIA-860M operating inventory.',
    }
    transformations = [
        {'operation': 'Aggregate retained EIA-860M operating generators by native Plant ID',
         'parameters': {'sheets': list(OPERATING_SHEETS), 'period': capture.period,
                        'capacity_fields': list(CAPACITY_FIELDS),
                        'primary_fields': ['Technology', 'Energy Source Code'],
                        'coordinate_policy': 'only unanimous numeric coordinate pairs retained; conflicts and missing coordinates remain null',
                        'coordinate_datum': None, 'geometry_created': False}},
    ]
    format_transformations = {'parquet': [
        {'name': 'Native lexical identifiers and coordinates remain strings; capacity totals are floats and generator_count is an integer. No geometry or CRS is assigned.', 'input_ids': ['workbook']},
        {'name': 'Plant names, states, counties and statuses are sorted distinct lists derived from the retained generator rows.', 'input_ids': ['workbook']},
    ]}
    result = write_native_candidate(
        storage, dataset_slug=DATASET_SLUG, file_slug=FILE_SLUG, rows=plants, capture=capture, identity=identity,
        title=f'Power Plants (EIA-860M {capture.period}; draft)',
        description='Operating EIA-860M generators aggregated by native Plant ID with total nameplate and net summer capacity and the capacity-dominant technology and energy source. Native nonspatial draft; coordinate datum, equivalence and release acceptance remain unresolved.',
        field_definitions=_field_definitions(), coverage=coverage, transformations=transformations,
        lineage=[PAGE_URL, *DOCUMENT_URLS.values()], source_relationship=source_relationship,
        audit_document=audit_document, type_overrides=TYPE_OVERRIDES,
        format_transformations=format_transformations)
    return {**result, 'generator_count': len(capture.generators), 'plant_count': len(plants),
            'source_period': capture.period, 'source_population_complete': True, **APPROVALS}
