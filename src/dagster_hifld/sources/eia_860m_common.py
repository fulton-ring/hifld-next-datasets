"""Shared EIA-860M workbook selection, parsing and native candidate construction.

Dataset adapters supply their own output fields, provenance descriptions and
aggregation. This module owns the producer-page release selection, the bounded
XLSX reader, the operating-generator normalization and the immutable
nonspatial candidate contract.
"""
from __future__ import annotations

import io
import json
import math
import re
import tempfile
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from xml.etree import ElementTree as ET

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from dagster_hifld.construction_state import load_first_construction
from dagster_hifld.provenance import validate_provenance
from dagster_hifld.resources import PublishedStorageResource, StagingStorageResource
from dagster_hifld.sources.capture_common import (
    candidate_prefix, capture_get, content_derived_version, processing_identity,
    retain_failed_capture, seal_ready_candidate, validate_capture_request_identity,
    verify_cached_candidate, write_immutable,
)
from dagster_hifld.sources.esri_common import canonical, sha

PAGE_URL = 'https://www.eia.gov/electricity/data/eia860m/'
DOCUMENT_URLS = {
    'inventory_page': PAGE_URL,
    'monthly_instructions': 'https://www.eia.gov/survey/form/eia_860m/instructions.pdf',
    'annual_instructions': 'https://www.eia.gov/survey/form/eia_860/instructions.pdf',
    'reuse_policy': 'https://www.eia.gov/about/copyrights_reuse.php',
}
MONTHS = ('january','february','march','april','may','june','july','august','september','october','november','december')
NS = {'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
OPERATING_SHEETS = ('Operating','Operating_PR')
REQUIRED_FIELDS = {'Plant ID','Generator ID','Unit Code','Status','Plant State','Plant Name','Latitude','Longitude','Nameplate Capacity (MW)'}
APPROVALS = {key:False for key in ('source_accepted','baseline_accepted','rights_accepted','coverage_accepted','schema_contract_accepted','id_contract_accepted','mapping_approved','release_accepted','promotion_allowed')}


class _Links(HTMLParser):
    def __init__(self):
        super().__init__(); self.urls = []
    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.urls.extend(v for k,v in attrs if k == 'href' and v)


def latest_release(html: str, *, as_of: date | None = None) -> tuple[str,str]:
    """Select active official workbook anchors; HTML comments are never links."""
    parser = _Links(); parser.feed(html)
    cutoff = (as_of or datetime.now(timezone.utc).date()).strftime('%Y-%m')
    found = []
    for href in parser.urls:
        url = urljoin(PAGE_URL, href)
        match = re.fullmatch(re.escape(PAGE_URL) + r'(?:archive/)?xls/([a-z]+)_generator(20\d\d)\.xlsx', url)
        if match and match[1] in MONTHS:
            period = f'{match[2]}-{MONTHS.index(match[1])+1:02d}'
            if period < cutoff:
                found.append((period, url))
    if not found: raise ValueError('no_active_eia860m_release')
    period = max(p for p,_ in found)
    urls = {u for p,u in found if p == period}
    if len(urls) != 1: raise ValueError('ambiguous_latest_eia860m_release')
    return period, urls.pop()


class _EvidenceText(HTMLParser):
    """Keep EIA content containers and links, excluding site navigation."""
    def __init__(self, classes):
        super().__init__(); self.classes = classes; self.hidden = 0
        self.containers = []; self.all = []; self.content = []
    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style', 'nav', 'footer'}: self.hidden += 1
        if tag in {'div', 'main'}:
            classes = dict(attrs).get('class') or ''
            selected = tag == 'main' or bool(self.classes.intersection(classes.split()))
            self.containers.append(selected or bool(self.containers and self.containers[-1]))
        if tag == 'a' and not self.hidden:
            for key, value in attrs:
                if key == 'href' and value: self.handle_data(value)
    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'nav', 'footer'}: self.hidden = max(0, self.hidden - 1)
        if tag in {'div', 'main'} and self.containers: self.containers.pop()
    def handle_data(self, data):
        if not self.hidden:
            self.all.append(data)
            if self.containers and self.containers[-1]: self.content.append(data)


def supporting_evidence_hashes(captures: dict, period: str, url: str) -> dict:
    """Identify selection and supporting content independently of GET timestamps."""
    hashes = {}
    for name, (body, _) in captures.items():
        if name == 'workbook': continue
        if name in {'inventory_page', 'reuse_policy'}:
            classes = {'article', 'notice'} if name == 'inventory_page' else {'main_col'}
            parser = _EvidenceText(classes); parser.feed(body.decode('utf-8'))
            text = ' '.join(' '.join(parser.content or parser.all).split())
            text = re.sub(r'\b(?:page\s*)?views\s*:?\s*[\d,]+\b', '', text, flags=re.I)
            text = ' '.join(text.split())
            hashes[name] = sha(canonical({'period': period, 'url': url, 'content': text})) if name == 'inventory_page' else sha(text.encode())
        else:
            hashes[name] = sha(body)
    return hashes


def _sheet_rows(archive, path, strings):
    with archive.open(path) as stream:
        for _, element in ET.iterparse(stream, events=('end',)):
            if element.tag != '{'+NS['s']+'}row': continue
            cells = {}
            for cell in element:
                column = re.fullmatch(r'([A-Z]+)\d+', cell.attrib['r'])[1]
                value = cell.find('s:v', NS)
                text = value.text if value is not None else None
                if cell.find('s:f', NS) is not None and text is None:
                    raise ValueError('xlsx_uncached_formula_cell')
                kind = cell.attrib.get('t')
                if kind == 's': text = strings[int(text)] if text is not None else None
                elif kind == 'inlineStr': text = ''.join(cell.find('s:is',NS).itertext())
                elif kind == 'e': raise ValueError('xlsx_error_cell')
                if text is not None: cells[column] = text
            yield int(element.attrib['r']), cells
            element.clear()


def read_existing_workbook(body: bytes, period: str) -> tuple[dict,dict]:
    """Read source lexical cached cell values with bounded ZIP/XML expansion."""
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = archive.infolist()
        if len(members)>200 or len({m.filename for m in members})!=len(members) or sum(m.file_size for m in members)>350_000_000:
            raise ValueError('xlsx_expansion_limit')
        strings = []
        with archive.open('xl/sharedStrings.xml') as stream:
            for _,element in ET.iterparse(stream,events=('end',)):
                if element.tag == '{'+NS['s']+'}si':
                    strings.append(''.join(element.itertext())); element.clear()
        relationships = {r.attrib['Id']:r.attrib['Target'] for r in ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))}
        sheets = ET.fromstring(archive.read('xl/workbook.xml')).find('s:sheets',NS)
        names = [s.attrib['name'] for s in sheets]
        if not set(OPERATING_SHEETS)<=set(names): raise ValueError('missing_operating_or_pr_sheet')
        result, schemas = {}, {}
        year,month = period.split('-'); title_period = f'{MONTHS[int(month)-1].title()} {year}'
        for sheet in sheets:
            name=sheet.attrib['name']
            if name not in OPERATING_SHEETS: continue
            target=relationships[sheet.attrib['{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id']]
            path=target.lstrip('/') if target.startswith('/') else 'xl/'+target
            if path not in archive.namelist(): raise ValueError('xlsx_sheet_path_invalid')
            rows=[]; headers=None; title=None
            for number,cells in _sheet_rows(archive,path,strings):
                if number==1:
                    title=cells.get('A','')
                    if not re.search(r'\bas of '+re.escape(title_period)+r'(?: - Puerto Rico)?$',title): raise ValueError('workbook_period_mismatch')
                elif number==3:
                    headers=cells
                    if len(set(headers.values()))!=len(headers) or not REQUIRED_FIELDS<=set(headers.values()): raise ValueError('operating_schema_drift')
                elif number>3 and any(v.strip() for v in cells.values()):
                    if set(cells)=={'A'} and cells['A'].startswith('NOTES:'): continue
                    if headers is None or set(cells)-set(headers): raise ValueError('unexpected_data_columns')
                    rows.append({field:cells.get(col) for col,field in headers.items()})
                    if len(rows)>100_000: raise ValueError('operating_row_limit')
            if not title or not headers or not rows: raise ValueError('empty_operating_sheet')
            result[name]=rows; schemas[name]={'title':title,'columns':list(headers.values()),'rows':len(rows)}
        if schemas['Operating']['columns']!=schemas['Operating_PR']['columns']: raise ValueError('pr_schema_drift')
        return result, {'workbook_sheets':names,'operating_sheets':schemas,'cell_value_policy':'All native columns retained as lexical cached XLSX values; formulas/styles and other sheets preserved in raw workbook.'}


def normalize_existing(sheets: dict) -> tuple[list,list,dict]:
    """Retain source rows, validate composite IDs and derive conservative plants."""
    if set(sheets)!=set(OPERATING_SHEETS): raise ValueError('missing_operating_or_pr_sheet')
    records=[]; seen=set(); groups=defaultdict(list); statuses=Counter(); states=Counter()
    for sheet in OPERATING_SHEETS:
        for row in sheets[sheet]:
            plant,generator,status=(row.get(k) for k in ('Plant ID','Generator ID','Status'))
            if any(not isinstance(v,str) or not v.strip() for v in (plant,generator,status)): raise ValueError('missing_identity_or_status')
            if not re.fullmatch(r'\d+',plant) or plant!=plant.strip() or generator!=generator.strip(): raise ValueError('invalid_lexical_identity')
            if not re.match(r'^\((OP|SB|OA|OS)\) ',status): raise ValueError('unexpected_existing_status')
            if not re.fullmatch(r'[A-Z]{2}',row.get('Plant State') or ''): raise ValueError('missing_or_invalid_plant_state')
            if (row.get('Plant State')=='PR') != (sheet=='Operating_PR'): raise ValueError('pr_coverage_mismatch')
            key=json.dumps([plant,generator],separators=(',',':'),ensure_ascii=False)
            if key in seen: raise ValueError('duplicate_generator_identity')
            seen.add(key)
            for field,limit in (('Latitude',90),('Longitude',180)):
                value=row.get(field)
                if value is not None and value.strip():
                    try: numeric=float(value)
                    except ValueError: raise ValueError('invalid_source_coordinate') from None
                    if not math.isfinite(numeric) or abs(numeric)>limit: raise ValueError('invalid_source_coordinate')
            record={**row,'eia_generator_key':key,'eia_source_sheet':sheet}
            records.append(record);groups[plant].append(record);statuses[status]+=1;states[row.get('Plant State')]+=1
    plants=[]; coordinate_counts=Counter()
    for plant,rows in sorted(groups.items()):
        coordinates={(r.get('Latitude'),r.get('Longitude')) for r in rows}
        missing=any(x is None or not x.strip() or y is None or not y.strip() for x,y in coordinates)
        numeric={(float(x),float(y)) for x,y in coordinates if x and x.strip() and y and y.strip()}
        state='conflicting' if len(numeric)>1 else 'missing' if missing or not numeric else 'consistent'
        coordinate_counts[state]+=1
        lat,lon=min(coordinates) if state=='consistent' else (None,None)
        plants.append({'Plant ID':plant,'Plant Names':sorted({r.get('Plant Name') for r in rows if r.get('Plant Name')}),'Plant States':sorted({r['Plant State'] for r in rows}),'generator_keys':sorted(r['eia_generator_key'] for r in rows),'generator_count':len(rows),'statuses':sorted({r['Status'] for r in rows}),'Latitude':lat,'Longitude':lon,'coordinate_status':state})
    return records,plants,{'generator_count':len(records),'plant_count':len(plants),'status_counts':dict(statuses),'state_counts':dict(states),'puerto_rico_generators':states.get('PR',0),'plant_coordinate_counts':dict(coordinate_counts),'coordinate_datum':None,'geospatial_release_ready':False,'coordinate_gate':'Producer datum not established; coordinates remain native lexical attributes, no geometry or assumed CRS.','population_scope':'Operating and Operating_PR sheets, including source operating/standby/out-of-service statuses. Excludes planned, retired, canceled sheets; raw workbook retained.','source_population_complete':True}


def records_parquet(records: list[dict], type_overrides: dict[str, pa.DataType] | None = None) -> bytes:
    """Write native records and verify a full readback before returning bytes."""
    overrides = type_overrides or {}
    columns = {}
    for field in records[0]:
        values = [row[field] for row in records]
        kind = overrides.get(field)
        if kind is None:
            present = [value for value in values if value is not None]
            if present and all(isinstance(value, list) for value in present):
                kind = pa.list_(pa.string())
            elif present and all(isinstance(value, int) and not isinstance(value, bool) for value in present):
                kind = pa.int64()
            elif present and all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in present):
                kind = pa.float64()
            else:
                kind = pa.string()
        columns[field] = pa.array(values, type=kind)
    sink = pa.BufferOutputStream(); pq.write_table(pa.table(columns), sink, compression='zstd')
    body = sink.getvalue().to_pybytes()
    if pq.read_table(pa.BufferReader(body)).to_pylist() != records: raise ValueError('full_parquet_readback_mismatch')
    return body


@dataclass(frozen=True)
class InventoryCapture:
    captures: dict
    root: Path
    period: str
    workbook_url: str
    sheets: dict
    layout: dict
    generators: list
    plants: list
    audit: dict
    evidence_hashes: dict
    observation: str


def _retained(root, name, url):
    body=(root/f'{name}.body').read_bytes();wire=(root/f'{name}.wire').read_bytes();receipt=json.loads((root/f'{name}.receipt.json').read_bytes())
    validate_capture_request_identity(name,receipt,url)
    if receipt.get('complete_body') is not True or receipt.get('status')!=200 or receipt.get('bytes')!=len(body) or receipt.get('sha256')!=sha(body) or receipt.get('wire_bytes')!=len(wire) or receipt.get('wire_sha256')!=sha(wire): raise ValueError('invalid_retained_capture:'+name)
    return body,receipt


def capture_workbook_documents(storage: StagingStorageResource, *, dataset_slug: str, capture_directory: str|None=None, replay: bool=False, client: httpx.Client|None=None) -> InventoryCapture:
    """Capture the producer page, supporting documents and selected workbook."""
    if not isinstance(storage,StagingStorageResource) or isinstance(storage,PublishedStorageResource): raise ValueError('explicit_staging_resource_required')
    if not (storage.use_local or not storage.bucket): raise ValueError('eia860m_requires_local_staging_construction_protocol')
    if replay and not capture_directory: raise ValueError('replay_requires_capture_directory')
    root=Path(capture_directory) if capture_directory else Path(tempfile.mkdtemp(prefix='eia860m-'))
    if not replay and root.exists() and any(root.iterdir()): raise ValueError('capture_directory_requires_fresh_attempt_or_explicit_replay')
    captures={}; owned=client is None; active=client or httpx.Client(timeout=60,follow_redirects=False)
    try:
        for name,url in DOCUMENT_URLS.items():
            captures[name]=_retained(root,name,url) if replay else capture_get(active,url,root,name,max_bytes=8_000_000)
        observed = datetime.fromisoformat(captures['inventory_page'][1]['observed_at'].replace('Z', '+00:00'))
        period,url=latest_release(captures['inventory_page'][0].decode('utf-8'), as_of=observed.date())
        captures['workbook']=_retained(root,'workbook',url) if replay else capture_get(active,url,root,'workbook',max_bytes=32_000_000)
        sheets,layout=read_existing_workbook(captures['workbook'][0],period)
        generators,plants,audit=normalize_existing(sheets)
    except Exception as exc:
        retain_failed_capture(storage,root,dataset_slug,'current',exc,error=ValueError)
        raise
    finally:
        if owned: active.close()
    # Retain every observation, including later no-op GETs, outside immutable candidates.
    receipts = {name: receipt for name, (_, receipt) in captures.items()}
    observation = sha(canonical(receipts))
    for name, (body, receipt) in captures.items():
        prefix = f'_capture-observations/{dataset_slug}/{observation}/{name}'
        for suffix, value in (('body', body), ('wire', (root/f'{name}.wire').read_bytes()), ('receipt.json', canonical(receipt)+b'\n')):
            write_immutable(storage, f'{prefix}.{suffix}', value)
    evidence_hashes = supporting_evidence_hashes(captures, period, url)
    return InventoryCapture(captures=captures,root=root,period=period,workbook_url=url,sheets=sheets,layout=layout,generators=generators,plants=plants,audit=audit,evidence_hashes=evidence_hashes,observation=observation)


def write_native_candidate(storage: StagingStorageResource, *, dataset_slug: str, file_slug: str, rows: list[dict],
                           capture: InventoryCapture, identity: dict, title: str, description: str,
                           field_definitions: dict, coverage: dict, transformations: list,
                           format_transformations: dict, lineage: list, source_relationship: dict,
                           audit_document: dict, type_overrides: dict[str, pa.DataType] | None = None,
                           schema_extra: dict | None = None) -> dict:
    """Write one immutable native nonspatial candidate with reuse verification."""
    record_sha = sha(canonical(rows))
    schema_sha = sha(canonical({'layout': capture.layout, 'output_fields': list(rows[0]),
                                'supporting_evidence_sha256': capture.evidence_hashes, **(schema_extra or {})}))
    workbook_sha = capture.captures['workbook'][1]['sha256']
    # Workbook hash makes silently corrected same-period releases distinct.
    version = content_derived_version(record_sha, workbook_sha, schema_sha, identity)
    prefix = candidate_prefix(dataset_slug, file_slug, version)
    state = f'_construction-state/{dataset_slug}/{file_slug}/{version}.json'
    fingerprint = sha(canonical(identity))
    result = {'dataset_slug':dataset_slug,'file_slug':file_slug,'version':version,'row_count':len(rows),
              'source_period':capture.period,'source_population_complete':True,'capture_directory':str(capture.root),
              'geospatial_release_ready':False,**APPROVALS}
    if storage.object_snapshot(state) is not None:
        payload=json.loads(load_first_construction(storage,state,version,fingerprint))['payload']
        verify_cached_candidate(storage,Path(__file__).parents[3],state,prefix,version,fingerprint,identity,record_sha,schema_sha,payload['objects'],payload['capture_receipts'])
        return {**result,'reused':True}
    manifest={'schema_version':1,'approved':False,'title':title,'description':description,
              'publisher':'U.S. Energy Information Administration','agency':'U.S. Department of Energy',
              'source_url':capture.workbook_url,'license':None,'date_issued':None,'date_modified':None,'source_modified':None,
              'source_period':capture.period,'source_vintage':capture.period,'lineage':lineage,
              'source_record_sha256':record_sha,'source_schema_sha256':schema_sha,'workbook_sha256':workbook_sha,
              'refresh_status':'STOPPED','coordinate_datum':None,
              'publication_approval':'not_established_by_source_capture',**APPROVALS}
    inputs=[];documentation=[]
    for name,(_,receipt) in capture.captures.items():
        purpose='data' if name=='workbook' else 'license' if name=='reuse_policy' else 'methodology'
        inputs.append({'input_id':name,'producer':'U.S. Energy Information Administration','host':urlsplit(receipt['url']).hostname,'role':'producer_publication','purpose':purpose,'canonical_url':receipt['url'],'retrieval_url':receipt['url'],'observed_final_url':receipt['final_url'],'retrieved_at':receipt['observed_at'],'http_status':receipt['status'],'bytes':receipt['bytes'],'sha256':receipt['sha256'],'wire_bytes':receipt['wire_bytes'],'wire_sha256':receipt['wire_sha256'],'release_date':None,'release_date_unknown_reason':'The inventory period is cross-checked against workbook titles; this capture does not establish a document-specific producer release date.','license':'Producer EIA copyright and reuse policy captured; applicability and exceptions require rights acceptance.','attribution':'U.S. Energy Information Administration'})
        inputs[-1]['refresh'] = {
            'maintenance_status': 'maintained' if name in {'workbook', 'inventory_page'} else 'unverified',
            'maintenance_evidence_input_id': 'inventory_page' if name in {'workbook', 'inventory_page'} else name,
            'latest_evidence_input_id': 'inventory_page' if name == 'workbook' else name,
            'latest_checked_at': receipt['observed_at'],
            'selection_rule': 'Latest active official workbook anchor for a completed month at page observation time; matching workbook titles required.' if name in {'workbook', 'inventory_page'} else 'Pinned official supporting-document URL; complete GET retained.',
            'source_state_fingerprint': receipt['sha256'] if name == 'workbook' else capture.evidence_hashes[name],
            'unknown_reasons': {'producer_release_date': 'Document-specific producer release date not established.', **({'maintenance': 'Supporting document revision cadence and current edition not independently established.'} if name not in {'workbook', 'inventory_page'} else {})},
        }
        if name in {'workbook', 'inventory_page'}:
            inputs[-1]['refresh'].update({'source_release_id': capture.period, 'data_period': capture.period, 'update_cadence': 'monthly'})
        documentation.append({'path':f'raw/{name}.body','input_id':name,'purpose':'schema' if name=='workbook' else purpose,'media_type':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' if name=='workbook' else 'application/pdf' if name.endswith('instructions') else 'text/html','bytes':receipt['bytes'],'sha256':receipt['sha256']})
    manifest.update({'processor':'HIFLD Next','code_commit':identity['code_commit'],'inputs':inputs,'documentation':documentation,
                     'field_definitions':field_definitions,'coverage':coverage,'source_relationship':source_relationship,
                     'transformations':transformations,'supporting_evidence_sha256':capture.evidence_hashes,
                     'capture_observation':capture.observation,'format_transformations':format_transformations})
    validate_provenance(manifest)
    parquet = records_parquet(rows, type_overrides)
    outputs={f'unknown/{file_slug}.parquet':parquet,'metadata/source_manifest.json':canonical(manifest)+b'\n',
             'metadata/source_audit.json':canonical({**audit_document,**capture.layout,'source_period':capture.period,
                                                      'workbook_url':capture.workbook_url,'source_record_sha256':record_sha,
                                                      'source_schema_sha256':schema_sha,**APPROVALS})+b'\n',
             'metadata/processing_identity.json':canonical(identity)+b'\n',
             'metadata/candidate_ready.json':canonical({'approved':False,'version':version,**APPROVALS})+b'\n'}
    for name,(body,receipt) in capture.captures.items():
        outputs.update({f'raw/{name}.body':body,f'raw/{name}.wire':(capture.root/f'{name}.wire').read_bytes(),f'raw/{name}.receipt.json':canonical(receipt)+b'\n'})
    objects={}
    for name,body in outputs.items():
        write_immutable(storage,f'{prefix}/{name}',body)
        if storage.read_key(f'{prefix}/{name}')!=body: raise ValueError('candidate_readback_mismatch')
        objects[name]={'bytes':len(body),'sha256':sha(body)}
    seal_ready_candidate(storage,state,version,fingerprint,{'processing_identity':identity,'source_record_sha256':record_sha,
                                                          'source_schema_sha256':schema_sha,
                                                          'capture_receipts':{name:r for name,(_,r) in capture.captures.items()},
                                                          'objects':objects})
    return {**result,'reused':False}
